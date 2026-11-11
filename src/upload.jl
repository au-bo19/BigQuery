# Writing a table. The rows are encoded as newline-delimited JSON, gzipped, and
# handed to a load job through a resumable upload, a chunk at a time, so the
# encoded data never exists in full. Load jobs cost nothing and are atomic: the
# table changes only if every row was accepted.

# Bytes sent per request. Chunks other than the last must be a multiple of 256 KiB.
const UPLOAD_CHUNK_UNIT = 256 * 1024
const UPLOAD_CHUNK_BYTES = Ref(8 * 1024 * 1024)

# BigQuery loads a gzip file of at most 4 GB (an uncompressed one may be 5 TB).
# A table that would compress to more than that is uploaded as it is.
const GZIP_LIMIT_BYTES = Ref(4_000_000_000)
const GZIP_SAMPLE_ROWS = 2_000
const GZIP_BATCH_BYTES = 1024 * 1024      # encoded rows handed to zlib at a time

const WRITE_DISPOSITIONS = Dict(:fail => "WRITE_EMPTY", :append => "WRITE_APPEND", :replace => "WRITE_TRUNCATE")

table_meta_path(t::TableRef) =
    "projects/$(escapeuri(t.project))/datasets/$(escapeuri(t.dataset))/tables/$(escapeuri(t.table))"

"The names and columns of any Tables.jl table."
function table_columns(data)
    Tables.istable(data) || throw(ArgumentError(
        "the data to upload must be a table (a DataFrame, or anything else that implements Tables.jl), " *
        "got a $(typeof(data))"))
    source = Tables.columns(data)
    names = String[]
    cols = AbstractVector[]
    for name in Tables.columnnames(source)
        col = Tables.getcolumn(source, name)
        push!(names, string(name))
        push!(cols, (col isa AbstractVector && !Base.has_offset_axes(col)) ? col : collect(col))
    end
    isempty(cols) && throw(ArgumentError("the table to upload has no columns"))
    n = length(first(cols))
    all(c -> length(c) == n, cols) || throw(ArgumentError("the columns to upload differ in length"))
    return names, cols
end

"The columns of an existing table, or `nothing` if there is no such table."
function existing_fields(conn::Connection, table::TableRef)
    meta = try
        api_json(conn, "GET", table_meta_path(table))
    catch e
        (e isa BigQueryError && e.status == 404) || rethrow()
        return nothing
    end
    schema = get(meta, "schema", nothing)
    raw = schema isa AbstractDict ? get(schema, "fields", nothing) : nothing
    return raw isa AbstractVector ? WriteField[WriteField(x) for x in raw] : WriteField[]
end

"Pair each column of the data with the table column it goes to. BigQuery column names ignore case."
function match_columns(table::TableRef, names::Vector{String}, table_fields::Vector{WriteField})
    exact = Dict(f.name => f for f in table_fields)
    folded = Dict(lowercase(f.name) => f for f in table_fields)
    fields = WriteField[]
    unknown = String[]
    for name in names
        f = get(() -> get(folded, lowercase(name), nothing), exact, name)
        f === nothing ? push!(unknown, name) : push!(fields, f)
    end
    isempty(unknown) || throw(ArgumentError(
        "table $table has no column named " * join(("`$u`" for u in unknown), ", ") *
        "; it has " * join((f.name for f in table_fields), ", ")))
    return fields
end

# --- resumable upload ---------------------------------------------------------

mutable struct UploadSession
    conn::Connection
    url::String
    offset::Int               # bytes the server has confirmed
end

function start_upload(conn::Connection, job::AbstractDict)
    url = conn.endpoint * "/upload/bigquery/v2/projects/" * escapeuri(conn.project) * "/jobs?uploadType=resumable"
    resp = send_request(conn, "POST", url; payload = Vector{UInt8}(codeunits(JSON.json(job))),
                        extra_headers = ["X-Upload-Content-Type" => "application/octet-stream"])
    location = header_value(resp.headers, "location")
    (location === nothing || isempty(location)) && throw(BigQueryError(
        "BigQuery accepted the load job but did not open an upload session for it"))
    return UploadSession(conn, String(location), 0)
end

"How many bytes the server holds, from the `Range: bytes=0-N` header of a 308 answer."
function confirmed_bytes(resp::HTTPResponse)
    range = header_value(resp.headers, "range")
    range === nothing && return 0
    m = match(r"^bytes=0-(\d+)$", strip(range))
    m === nothing && throw(BigQueryError("unexpected Range header during an upload: $range"))
    return parse(Int, m.captures[1]) + 1
end

"""
Upload `data` as the bytes following `session.offset`. `total` is the size of
the whole upload when `data` is its last part, otherwise `nothing`. Returns the
job the server created once the upload is complete, `nothing` before that.

The server may keep only part of what a request carried, and a request can fail
after its bytes were stored. So every answer is read for the position the server
has reached, and after a failure that position is asked for before resending.
"""
function send_chunk!(session::UploadSession, data::Vector{UInt8}, total::Union{Nothing,Int})
    conn = session.conn
    stop = session.offset + length(data)
    size = total === nothing ? "*" : string(total)
    failures = 0
    reauthed = false
    probe = false
    while true
        start = session.offset
        body = probe ? UInt8[] : start == stop - length(data) ? data : data[end-(stop-start)+1:end]
        headers = Pair{String,String}[
            "Authorization" => "Bearer " * access_token(conn),
            "User-Agent" => USER_AGENT,
            "Content-Type" => "application/octet-stream",
            "Content-Range" => isempty(body) ? "bytes */$size" : "bytes $start-$(stop - 1)/$size",
        ]
        resp = try
            http_request(conn.downloader, "PUT", session.url; headers = headers, body = body, timeout = 3600)
        catch e
            (e isa NetworkError && failures < conn.max_retries) || rethrow()
            nothing
        end
        if resp === nothing
            failures += 1
            sleep(retry_delay(failures))
            probe = true
        elseif resp.status == 200 || resp.status == 201
            total === nothing && throw(BigQueryError("BigQuery ended the upload before all rows were sent"))
            session.offset = stop
            return JSON.parse(String(resp.body))
        elseif resp.status == 308
            confirmed = confirmed_bytes(resp)
            start <= confirmed <= stop || throw(BigQueryError(
                "the upload is out of step: BigQuery holds $confirmed bytes, expected between $start and $stop"))
            session.offset = confirmed
            (confirmed == stop && total === nothing) && return nothing
            if !probe && confirmed == start
                # Nothing was taken (or the finished upload was not acknowledged).
                failures += 1
                failures > conn.max_retries && throw(BigQueryError("the upload to BigQuery is not making progress"))
                sleep(retry_delay(failures))
            elseif confirmed > start
                failures = 0
            end
            probe = false
        elseif resp.status == 401 && !reauthed
            invalidate!(conn.credentials)
            reauthed = true
        else
            err = parse_api_error(resp, "BigQuery upload")
            (is_retryable(resp.status, err.reason) && failures < conn.max_retries) || throw(err)
            failures += 1
            sleep(retry_delay(failures))
            probe = true
        end
    end
end

"The rows whose compressed size stands for the whole table's: evenly spread, so sorted data does not mislead."
sample_rows(nrows::Int) =
    nrows <= GZIP_SAMPLE_ROWS ? collect(1:nrows) : round.(Int, range(1, nrows; length = GZIP_SAMPLE_ROWS))

"""
Whether the table can be uploaded gzipped, judged by compressing a sample of
its rows. Rows far apart have less in common than neighbours, so the estimate
errs on the large side; a quarter of the limit is kept in hand all the same.
"""
function gzip_fits(encoders, nrows::Int)
    sample = sample_rows(nrows)
    io = IOBuffer()
    for i in sample
        write_row(io, encoders, i)
    end
    estimate = length(gzip_bytes(take!(io))) / length(sample) * nrows
    return estimate <= 0.75 * GZIP_LIMIT_BYTES[]
end

"Send the whole chunks that `io` holds and keep the rest, so that something is always left for the final request."
function send_full_chunks!(session::UploadSession, io::IOBuffer, chunk::Int)
    position(io) > chunk || return nothing
    data = take!(io)
    pos = 0
    while length(data) - pos > chunk
        send_chunk!(session, data[pos+1:pos+chunk], nothing)
        pos += chunk
    end
    write(io, view(data, pos+1:length(data)))
    return nothing
end

"""
Encode all rows and upload them, returning the load job that results. With
`gzip`, returns `nothing` instead if the compressed rows turn out not to fit in
the file size BigQuery accepts; nothing has been loaded then, as a load job
only starts once its upload is complete.
"""
function upload_rows(session::UploadSession, encoders, nrows::Int, progress::Progress; gzip::Bool)
    chunk = max(UPLOAD_CHUNK_UNIT, UPLOAD_CHUNK_BYTES[] ÷ UPLOAD_CHUNK_UNIT * UPLOAD_CHUNK_UNIT)
    limit = GZIP_LIMIT_BYTES[]
    rows = IOBuffer()                 # encoded rows
    packed = IOBuffer()               # the same compressed, when compressing
    gz = gzip ? GzipWriter() : nothing
    reported = 0
    try
        for i in 1:nrows
            write_row(rows, encoders, i)
            if gz === nothing
                position(rows) > chunk || continue
                send_full_chunks!(session, rows, chunk)
            else
                position(rows) >= GZIP_BATCH_BYTES || continue
                deflate!(packed, gz, take!(rows))
                session.offset + position(packed) > limit && return nothing
                position(packed) > chunk || continue
                send_full_chunks!(session, packed, chunk)
            end
            advance!(progress, i - reported)
            reported = i
        end
        if gz !== nothing
            deflate!(packed, gz, take!(rows); finish = true)
            session.offset + position(packed) > limit && return nothing
        end
        data = take!(gz === nothing ? rows : packed)
        job = send_chunk!(session, data, session.offset + length(data))
        advance!(progress, nrows - reported)
        return job
    finally
        gz === nothing || close(gz)
    end
end

# --- the load job -------------------------------------------------------------

function load_job_error(status::AbstractDict)
    result = status["errorResult"]
    message = string(something(get(result, "message", nothing), "the load job failed"))
    reason = string(something(get(result, "reason", nothing), ""))
    # The first error only says that rows were rejected; the list says which and why.
    details = String[]
    errors = get(status, "errors", nothing)
    if errors isa AbstractVector
        for e in errors
            e isa AbstractDict || continue
            detail = string(something(get(e, "message", nothing), ""))
            (isempty(detail) || detail == message || detail in details) && continue
            push!(details, detail)
            length(details) == 5 && break
        end
    end
    isempty(details) || (message *= "\n  " * join(details, "\n  "))
    return BigQueryError(message, reason, 0)
end

"Wait for a load job to finish and return its final description. Throws if the job failed."
function wait_for_job(conn::Connection, job, job_id::String, location::Union{Nothing,String})
    delay = 0.5
    try
        while true
            job isa AbstractDict || throw(BigQueryError("unexpected answer from BigQuery about load job $job_id"))
            ref = get(job, "jobReference", nothing)
            loc = ref isa AbstractDict ? get(ref, "location", nothing) : nothing
            loc isa AbstractString && (location = String(loc))
            status = get(job, "status", nothing)
            if status isa AbstractDict && get(status, "state", nothing) == "DONE"
                get(status, "errorResult", nothing) isa AbstractDict && throw(load_job_error(status))
                return job
            end
            sleep(delay)
            delay = min(delay * 1.5, 5.0)
            job = api_json(conn, "GET", job_path(conn.project, job_id); query = ["location" => location])
        end
    catch e
        # Ctrl-C while waiting: do not leave the load running.
        e isa InterruptException && cancel_job(conn, conn.project, job_id, location)
        rethrow()
    end
end

function loaded_rows(job::AbstractDict, sent::Int)
    stats = get(job, "statistics", nothing)
    load = stats isa AbstractDict ? get(stats, "load", nothing) : nothing
    rows = load isa AbstractDict ? get(load, "outputRows", nothing) : nothing
    return rows === nothing ? sent : parse_count(rows)
end

"""
    bq_write_table(con, table, data; if_exists, types, compress, progress, ...) -> (table, rows, job_id)

Upload `data` to a BigQuery table, creating the table if it does not exist.
`data` is a `DataFrame` or any other Tables.jl table. The upload is a load job:
it costs nothing, and either every row arrives or none does.

```julia
bq_write_table(con, "my_dataset.orders", df)
bq_write_table(con, "my-project.my_dataset.orders", more; if_exists = :append)
bq_write_table(con, "my_dataset.orders", df; if_exists = :replace, types = (placed_at = "DATETIME",))
```

Keywords:

* `if_exists` — what to do when the table already exists: `:fail` (default)
  throws, `:append` adds the rows to it, `:replace` overwrites the table,
  schema included.
* `types` — BigQuery types for columns of a new table, where the default is not
  what you want: a `NamedTuple` or `Dict` of column name => type, such as
  `"DATETIME"`, `"NUMERIC"` or `"ARRAY<DATE>"`. A type can also be a `Dict`
  describing the column as the REST API does, for example
  `Dict("type" => "NUMERIC", "precision" => 10, "scale" => 2)`.
* `compress` — gzip the rows on their way (default `true`), which makes the
  upload several times smaller. BigQuery takes compressed uploads of up to
  4 GB; a table that would be larger even compressed is sent uncompressed
  instead, whatever this says.
* `progress` — show a progress line for long uploads (default: in interactive sessions).
* `location` — where to run the load job; by default that of the connection,
  or else of the destination dataset.
* `labels` — job labels.

A new table gets its column types from the Julia types: `Int` → `INT64`,
`Float64` → `FLOAT64`, `Bool` → `BOOL`, `String` → `STRING`, `Date` → `DATE`,
`DateTime` → `TIMESTAMP` (taken as UTC), `Time` → `TIME`, `BigFloat` →
`BIGNUMERIC`, `Vector{UInt8}` → `BYTES`, other vectors → `ARRAY`, `NamedTuple`
→ `STRUCT`. `missing` and `nothing` become `NULL`, and every column is nullable.

When appending, the table's own schema decides how values are written, so a
`DateTime` column goes into a `DATETIME` or a `TIMESTAMP` column alike, and
`types` is not used. Columns are matched by name; ones the data lacks are left
`NULL`. Text is accepted for a column of any type and parsed by BigQuery, which
is the way to write exact `NUMERIC` values or timestamps with microseconds.

Returns the table's full name, the number of rows loaded and the job's id.
"""
function bq_write_table(conn::Connection, table::AbstractString, data;
                        if_exists::Symbol = :fail,
                        types = nothing,
                        compress::Bool = true,
                        location::Union{Nothing,AbstractString} = conn.location,
                        labels::Union{Nothing,AbstractDict} = nothing,
                        progress::Bool = isinteractive())
    check_open(conn)
    haskey(WRITE_DISPOSITIONS, if_exists) ||
        throw(ArgumentError("if_exists must be :fail, :append or :replace"))
    ref = parse_table(conn, table)
    names, cols = table_columns(data)
    nrows = length(first(cols))

    # Replacing a table replaces its schema too, so what is there does not matter.
    existing = if_exists == :replace ? nothing : existing_fields(conn, ref)
    load = Dict{String,Any}(
        "destinationTable" => Dict{String,Any}("projectId" => ref.project, "datasetId" => ref.dataset,
                                               "tableId" => ref.table),
        "sourceFormat" => "NEWLINE_DELIMITED_JSON",
        "writeDisposition" => WRITE_DISPOSITIONS[if_exists],
        "createDisposition" => "CREATE_IF_NEEDED",
    )
    if existing === nothing
        fields = infer_fields(names, cols, types)
        load["schema"] = Dict{String,Any}("fields" => Any[schema_json(f) for f in fields])
    elseif if_exists == :fail
        throw(BigQueryError("table $ref already exists; pass `if_exists = :append` to add rows to it " *
                            "or `if_exists = :replace` to overwrite it", "duplicate", 0))
    else
        fields = match_columns(ref, names, existing)
    end

    job_id = "bqjl_" * string(uuid4())
    reference = Dict{String,Any}("projectId" => conn.project, "jobId" => job_id)
    location === nothing || (reference["location"] = String(location))
    configuration = Dict{String,Any}("load" => load)
    labels === nothing || (configuration["labels"] = Dict{String,Any}(string(k) => string(v) for (k, v) in labels))

    description = Dict{String,Any}("jobReference" => reference, "configuration" => configuration)

    encoders = row_encoders(fields, cols)
    # Nothing to compress in an empty table, which is sent as an empty upload.
    gzip = compress && nrows > 0 && gzip_fits(encoders, nrows)
    job = nothing
    while true
        session = start_upload(conn, description)
        bar = Progress(progress, nrows)
        job = try
            upload_rows(session, encoders, nrows, bar; gzip = gzip)
        finally
            finish!(bar)
        end
        job === nothing || break
        # The estimate was wrong. The abandoned upload never became a job.
        @warn "The table does not fit in the 4 GB that BigQuery accepts as a compressed upload; " *
              "starting again without compression. Pass `compress = false` to skip the attempt."
        gzip = false
    end
    job = wait_for_job(conn, job, job_id, location === nothing ? nothing : String(location))
    return (table = string(ref), rows = loaded_rows(job, nrows), job_id = job_id)
end
