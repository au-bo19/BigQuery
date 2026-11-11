# Running queries and assembling results.

# Rows requested with the query itself. Results that fit come back in a single
# round trip; anything bigger is downloaded in bulk afterwards.
const FIRST_PAGE_ROWS = 10_000
const POLL_TIMEOUT_MS = 10_000

"""
    BigQuery.QueryResult

What `DBInterface.execute` returns: the result columns plus some facts about
the job. It is a Tables.jl table, so `DataFrame(result)` (or any other sink)
works. Fields of interest: `job_id`, `location`, `total_rows`, `affected_rows`
(DML statements), `bytes_processed`, `cache_hit` and `method` (`:inline`,
`:arrow` or `:rest`, how the rows were downloaded).
"""
struct QueryResult
    names::Vector{Symbol}
    columns::Vector{AbstractVector}
    lookup::Dict{Symbol,Int}
    job_id::String
    location::String
    total_rows::Int
    affected_rows::Union{Nothing,Int}
    bytes_processed::Union{Nothing,Int}
    cache_hit::Bool
    method::Symbol
end

Tables.istable(::Type{QueryResult}) = true
Tables.columnaccess(::Type{QueryResult}) = true
Tables.columns(r::QueryResult) = r
Tables.columnnames(r::QueryResult) = getfield(r, :names)
Tables.getcolumn(r::QueryResult, i::Int) = getfield(r, :columns)[i]
Tables.getcolumn(r::QueryResult, name::Symbol) = getfield(r, :columns)[getfield(r, :lookup)[name]]
Tables.schema(r::QueryResult) =
    Tables.Schema(getfield(r, :names), Type[eltype(c) for c in getfield(r, :columns)])

function Base.show(io::IO, r::QueryResult)
    print(io, "BigQuery.QueryResult: ", r.total_rows, " rows x ", length(r.names), " columns")
    isempty(r.job_id) || print(io, ", job ", r.job_id)
    r.affected_rows === nothing || print(io, ", ", r.affected_rows, " rows affected")
    r.bytes_processed === nothing || print(io, ", ", r.bytes_processed, " bytes processed")
    r.cache_hit && print(io, " (cached)")
end

function unique_names(names::Vector{String})
    seen = Set{Symbol}()
    out = Symbol[]
    for n in names
        s = Symbol(n)
        k = 0
        while s in seen
            k += 1
            s = Symbol(n, "_", k)
        end
        push!(seen, s)
        push!(out, s)
    end
    return out
end

function make_result(fields::Vector{Field}, columns::Vector{Any}; job_id = "", location = "",
                     affected_rows = nothing, bytes_processed = nothing, cache_hit = false,
                     method = :inline)
    names = unique_names(String[f.name for f in fields])
    cols = AbstractVector[narrow_column(c) for c in columns]
    nrows = isempty(cols) ? 0 : length(first(cols))
    lookup = Dict{Symbol,Int}(n => i for (i, n) in enumerate(names))
    return QueryResult(names, cols, lookup, job_id, location, nrows, affected_rows,
                       bytes_processed, cache_hit, method)
end

to_dataframe(r::QueryResult) = DataFrame(r.columns, r.names; copycols = false)

parse_count(x::AbstractString) = something(tryparse(Int, x), 0)
parse_count(x::Integer) = Int(x)
parse_count(x) = 0
optional_count(x) = x === nothing ? nothing : parse_count(x)

"Accepts `project.dataset.table`, `dataset.table` or `project:dataset.table`, optionally back-quoted."
function parse_table(conn::Connection, name::AbstractString)
    parts = split(replace(strip(name), '`' => "", ':' => '.'), '.')
    if length(parts) == 3
        return TableRef(String(parts[1]), String(parts[2]), String(parts[3]))
    elseif length(parts) == 2
        return TableRef(conn.project, String(parts[1]), String(parts[2]))
    elseif length(parts) == 1 && conn.dataset !== nothing
        ds = split(conn.dataset, '.')
        return length(ds) == 2 ? TableRef(String(ds[1]), String(ds[2]), String(parts[1])) :
                                 TableRef(conn.project, String(ds[1]), String(parts[1]))
    end
    throw(ArgumentError("expected a table name like \"project.dataset.table\", got \"$name\""))
end

function dataset_reference(conn::Connection, dataset::AbstractString)
    parts = split(replace(dataset, '`' => "", ':' => '.'), '.')
    length(parts) == 2 && return Dict{String,Any}("projectId" => String(parts[1]), "datasetId" => String(parts[2]))
    length(parts) == 1 && return Dict{String,Any}("projectId" => conn.project, "datasetId" => String(parts[1]))
    throw(ArgumentError("expected a dataset like \"dataset\" or \"project.dataset\", got \"$dataset\""))
end

# A top-level ORDER BY means row order matters: read the result on one stream.
has_order_by(sql::AbstractString) = occursin(r"\border\s+by\b"i, sql)

function job_path(project::AbstractString, job_id::AbstractString)
    return "projects/$(escapeuri(project))/jobs/$(escapeuri(job_id))"
end

function cancel_job(conn::Connection, project::String, job_id::String, location::Union{Nothing,String})
    try
        api_call(conn, "POST", job_path(project, job_id) * "/cancel"; query = ["location" => location],
                 body = Dict{String,Any}())
    catch
    end
    return nothing
end

function destination_table(conn::Connection, project::String, job_id::String, location::Union{Nothing,String})
    job = api_json(conn, "GET", job_path(project, job_id); query = ["location" => location])
    config = get(job, "configuration", nothing)
    query = config isa AbstractDict ? get(config, "query", nothing) : nothing
    dest = query isa AbstractDict ? get(query, "destinationTable", nothing) : nothing
    dest isa AbstractDict || return nothing
    for key in ("projectId", "datasetId", "tableId")
        get(dest, key, nothing) isa AbstractString || return nothing
    end
    return TableRef(String(dest["projectId"]), String(dest["datasetId"]), String(dest["tableId"]))
end

function storage_fallback_warning(e)
    reason = sprint(showerror, e)
    length(reason) > 500 && (reason = first(reason, 500) * "...")
    @warn """
    The BigQuery Storage Read API could not be used; falling back to the slower REST download.
    Reason: $reason
    The Storage API needs the `bigquery.readsessions.create` permission (role "BigQuery Read Session User")
    on the billing project and HTTP/2 access to bigquerystorage.googleapis.com.
    Pass `method = :rest` to skip this attempt.""" maxlog = 3
end

"""
Try the Storage Read API; return `nothing` when the caller should use REST
instead. With `method = :auto` any failure of the fast path (no permission, no
HTTP/2, a data type Arrow.jl cannot decode, a row count that does not match
`expected_rows`, ...) falls back to REST with a warning; with `method = :arrow`
it is an error.
"""
function try_arrow(conn::Connection, method::Symbol, table::TableRef, fields::Vector{Field};
                   expected_rows::Union{Nothing,Int} = nothing, kwargs...)
    method == :rest && return nothing
    (method == :auto && !conn.storage_ok) && return nothing
    try
        cols = read_table_arrow(conn, table, fields; kwargs...)
        got = isempty(cols) ? 0 : length(first(cols))
        (expected_rows === nothing || got == expected_rows) || throw(BigQueryError(
            "the Storage Read API returned $got rows but the query produced $expected_rows"))
        return cols
    catch e
        (method == :auto && !(e isa InterruptException)) || rethrow()
        (e isa GrpcError && e.code == 1) && rethrow()        # cancelled
        storage_fallback_warning(e)
        # A permission problem will not fix itself; do not retry on every query.
        (e isa GrpcError && e.code in (7, 12)) && (conn.storage_ok = false)
        return nothing
    end
end

function check_method(method::Symbol)
    method in (:auto, :arrow, :rest) ||
        throw(ArgumentError("method must be :auto, :arrow or :rest"))
    return nothing
end

"""
Run `sql` and download its result. Returns a [`QueryResult`](@ref).
"""
function run_query(conn::Connection, sql::AbstractString;
                   params = nothing,
                   method::Symbol = :auto,
                   max_streams::Integer = 8,
                   compression::Symbol = :zstd,
                   concurrency::Integer = 8,
                   page_size::Union{Nothing,Integer} = nothing,
                   progress::Bool = isinteractive(),
                   dataset::Union{Nothing,AbstractString} = conn.dataset,
                   location::Union{Nothing,AbstractString} = conn.location,
                   use_cache::Bool = true,
                   max_bytes_billed::Union{Nothing,Integer} = nothing,
                   labels::Union{Nothing,AbstractDict} = nothing)
    check_open(conn)
    check_method(method)
    body = Dict{String,Any}(
        "query" => String(sql),
        "useLegacySql" => false,
        "useQueryCache" => use_cache,
        "timeoutMs" => POLL_TIMEOUT_MS,
        "maxResults" => FIRST_PAGE_ROWS,
        "requestId" => string(uuid4()),       # makes retrying the POST safe
        "formatOptions" => Dict{String,Any}("useInt64Timestamp" => true),
    )
    location === nothing || (body["location"] = String(location))
    dataset === nothing || (body["defaultDataset"] = dataset_reference(conn, dataset))
    max_bytes_billed === nothing || (body["maximumBytesBilled"] = string(max_bytes_billed))
    labels === nothing || (body["labels"] = Dict{String,Any}(string(k) => string(v) for (k, v) in labels))
    encoded = encode_params(params)
    if encoded !== nothing
        body["parameterMode"] = encoded[1]
        body["queryParameters"] = encoded[2]
    end

    project_path = "projects/" * escapeuri(conn.project)
    s, meta, range = split_response(api_call(conn, "POST", project_path * "/queries"; body = body))

    job_project = conn.project
    job_id = ""
    job_location = location === nothing ? nothing : String(location)
    ref = get(meta, "jobReference", nothing)
    if ref isa AbstractDict
        job_project = string(something(get(ref, "projectId", nothing), conn.project))
        job_id = string(something(get(ref, "jobId", nothing), ""))
        loc = get(ref, "location", nothing)
        loc isa AbstractString && (job_location = String(loc))
    end

    # Long-running query: wait for it, a poll at a time.
    try
        while get(meta, "jobComplete", false) !== true
            isempty(job_id) && throw(BigQueryError("BigQuery did not return a job reference for an unfinished query"))
            raw = api_call(conn, "GET", project_path * "/queries/" * escapeuri(job_id); query = [
                "location" => job_location,
                "timeoutMs" => POLL_TIMEOUT_MS,
                "maxResults" => FIRST_PAGE_ROWS,
                "formatOptions.useInt64Timestamp" => "true",
            ])
            s, meta, range = split_response(raw)
        end
    catch e
        # Ctrl-C while waiting: do not leave the query running (and billing).
        (e isa InterruptException && !isempty(job_id)) && cancel_job(conn, job_project, job_id, job_location)
        rethrow()
    end

    fields = parse_schema(get(meta, "schema", nothing))
    total = parse_count(get(meta, "totalRows", 0))
    stats = (job_id = job_id,
             location = something(job_location, ""),
             affected_rows = optional_count(get(meta, "numDmlAffectedRows", nothing)),
             bytes_processed = optional_count(get(meta, "totalBytesProcessed", nothing)),
             cache_hit = get(meta, "cacheHit", false) === true)

    first_cols = new_columns(fields)
    n1 = range === nothing ? 0 : decode_rows!(first_cols, fields, s, range)
    page_token = string(something(get(meta, "pageToken", nothing), ""))
    if isempty(fields) || n1 >= total || isempty(page_token)
        return make_result(fields, first_cols; stats..., method = :inline)
    end

    # The result did not fit in the first page: bulk download.
    isempty(job_id) && throw(BigQueryError("BigQuery returned a partial result without a job to page through"))
    dest = try
        destination_table(conn, job_project, job_id, job_location)
    catch e
        e isa BigQueryError || rethrow()
        nothing
    end
    bar = Progress(progress, total)
    try
        if dest !== nothing
            cols = try_arrow(conn, method, dest, fields; expected_rows = total,
                             ordered = has_order_by(sql), max_streams = max_streams,
                             compression = compression, progress = bar)
            cols === nothing || return make_result(fields, cols; stats..., method = :arrow)
            finish!(bar)
        elseif method == :arrow
            throw(BigQueryError("this query has no result table to read with the Storage Read API " *
                                "(scripts, for example); use method = :auto or :rest"))
        end

        bar = Progress(progress, total)
        advance!(bar, n1)
        chunks = Vector{Any}[first_cols]
        if dest !== nothing
            append!(chunks, read_table_rest(conn, dest, fields, total; start = n1,
                bytes_per_row = length(range) / max(n1, 1), concurrency = concurrency,
                page_size = page_size, progress = bar))
        else
            append!(chunks, read_query_pages(conn, job_project, job_id, job_location, fields, page_token, bar))
        end
        cols = concat_chunks(chunks, length(fields))
        got = isempty(cols) ? 0 : length(first(cols))
        got == total || throw(BigQueryError(
            "downloaded $got rows but the query produced $total; please retry"))
        return make_result(fields, cols; stats..., method = :rest)
    finally
        finish!(bar)
    end
end

"""
    bq_query(con, sql; params, method, progress, ...) -> DataFrame

Run a GoogleSQL query and return the whole result as a `DataFrame`, however
large it is.

```julia
df = bq_query(con, "SELECT name, SUM(number) AS n FROM `bigquery-public-data.usa_names.usa_1910_2013` GROUP BY name")
df = bq_query(con, "SELECT * FROM my_dataset.events WHERE day = @day AND country IN UNNEST(@cc)";
              params = (day = Date(2024, 1, 31), cc = ["DE", "FR"]))
```

Keywords:

* `params` — query parameters: a `NamedTuple`/`Dict` for `@name` placeholders,
  a `Tuple`/`Vector` for `?` placeholders. See [`bq_param`](@ref) for explicit types.
* `method` — how large results are downloaded. `:auto` (default) uses the
  Storage Read API and falls back to REST paging if it is unavailable; `:arrow`
  and `:rest` force one or the other.
* `progress` — show a progress line for long downloads (default: in interactive sessions).
* `max_streams` — parallel Storage API streams (default 8).
* `compression` — Storage API compression: `:zstd` (default), `:lz4`, `:none`.
* `concurrency`, `page_size` — parallel requests and rows per request for the REST path.
* `dataset`, `location` — override the connection's defaults for this query.
* `use_cache` — allow BigQuery's query cache (default `true`).
* `max_bytes_billed` — fail instead of running a query that would bill more than this.
* `labels` — job labels.

Statements that return no rows (DDL, DML) give an empty `DataFrame`; use
`DBInterface.execute` to get the number of affected rows.
"""
bq_query(conn::Connection, sql::AbstractString; kwargs...) = to_dataframe(run_query(conn, sql; kwargs...))

quote_identifier(name::AbstractString) = "`" * replace(name, "`" => "\\`") * "`"

"""
    bq_read_table(con, table; columns, where, method, progress, ...) -> DataFrame

Download a table directly, without running a query (and so without query
cost when the Storage Read API or plain REST paging can be used).

```julia
df = bq_read_table(con, "my-project.my_dataset.my_table")
df = bq_read_table(con, "my_dataset.my_table"; columns = ["id", "amount"], where = "amount > 100")
```

`columns` selects top-level columns; `where` is a SQL filter applied on the
server. Views and external tables are read by running `SELECT ... FROM table`.
"""
function bq_read_table(conn::Connection, table::AbstractString;
                       columns = nothing,
                       where::Union{Nothing,AbstractString} = nothing,
                       method::Symbol = :auto,
                       max_streams::Integer = 8,
                       compression::Symbol = :zstd,
                       concurrency::Integer = 8,
                       page_size::Union{Nothing,Integer} = nothing,
                       progress::Bool = isinteractive())
    check_open(conn)
    check_method(method)
    ref = parse_table(conn, table)
    path = "projects/$(escapeuri(ref.project))/datasets/$(escapeuri(ref.dataset))/tables/$(escapeuri(ref.table))"
    meta = api_json(conn, "GET", path)
    fields = parse_schema(get(meta, "schema", nothing))
    selected = String[]
    if columns !== nothing
        selected = String[string(c) for c in columns]
        by_name = Dict(f.name => f for f in fields)
        for name in selected
            haskey(by_name, name) || throw(ArgumentError("table $ref has no column `$name`"))
        end
        fields = Field[by_name[name] for name in selected]
    end
    kind = uppercase(string(something(get(meta, "type", nothing), "TABLE")))

    via_query = function ()
        select = isempty(selected) ? "*" : join(map(quote_identifier, selected), ", ")
        sql = "SELECT $select FROM `$(ref.project).$(ref.dataset).$(ref.table)`"
        where === nothing || (sql *= " WHERE " * where)
        return to_dataframe(run_query(conn, sql; method = method, max_streams = max_streams,
            compression = compression, concurrency = concurrency, page_size = page_size,
            progress = progress))
    end
    kind in ("VIEW", "EXTERNAL") && return via_query()

    num_rows = parse_count(get(meta, "numRows", 0))
    bar = Progress(progress && where === nothing, num_rows)
    try
        cols = try_arrow(conn, method, ref, fields; max_streams = max_streams, compression = compression,
                         selected_fields = selected, row_restriction = something(where, ""), progress = bar)
        cols === nothing || return to_dataframe(make_result(fields, cols; method = :arrow))
    finally
        finish!(bar)
    end

    # REST: tabledata.list cannot filter, so a `where` needs a query.
    where === nothing || return via_query()
    selected_param = isempty(selected) ? nothing : join(selected, ",")
    raw = api_call(conn, "GET", table_data_path(ref); query = [
        "maxResults" => FIRST_PAGE_ROWS,
        "formatOptions.useInt64Timestamp" => "true",
        "selectedFields" => selected_param,
    ])
    s, page_meta, range = split_response(raw)
    total = parse_count(get(page_meta, "totalRows", num_rows))
    first_cols = new_columns(fields)
    n1 = range === nothing ? 0 : decode_rows!(first_cols, fields, s, range)
    chunks = Vector{Any}[first_cols]
    bar = Progress(progress, total)
    try
        advance!(bar, n1)
        if n1 > 0 && n1 < total
            append!(chunks, read_table_rest(conn, ref, fields, total; start = n1,
                bytes_per_row = length(range) / n1, concurrency = concurrency,
                page_size = page_size, selected = selected_param, progress = bar))
        end
    finally
        finish!(bar)
    end
    return to_dataframe(make_result(fields, concat_chunks(chunks, length(fields)); method = :rest))
end
