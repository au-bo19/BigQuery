# Run queries and make the results.

# The number of rows that the query request asks for. If the result has fewer rows,
# one request is sufficient. If it has more rows, the package downloads all the rows
# after the query.
const FIRST_PAGE_ROWS = 10_000
const POLL_TIMEOUT_MS = 10_000

"""
    BigQuery.QueryResult

The result of `DBInterface.execute`: the result columns and data about the job. It
is a Tables.jl table, thus `DataFrame(result)` (or a different sink) works.
Important fields: `job_id`, `location`, `total_rows`, `affected_rows` (for DML
statements), `bytes_processed`, `cache_hit` and `method`. `method` shows how the
package downloaded the rows: `:inline`, `:arrow` or `:rest`.
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

"Accepts `project.dataset.table`, `dataset.table` or `project:dataset.table`, optionally in back quotes."
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

"The index after the end of the quoted text that starts at `i` (a quote character)."
function skip_quoted(s::AbstractVector{UInt8}, i::Int)
    n = length(s)
    q = s[i]
    triple = i + 2 <= n && s[i+1] == q && s[i+2] == q
    j = triple ? i + 3 : i + 1
    while j <= n
        c = s[j]
        if c == UInt8('\\')
            j += 2
        elseif c == q && (!triple || (j + 2 <= n && s[j+1] == q && s[j+2] == q))
            return triple ? j + 3 : j + 1
        else
            j += 1
        end
    end
    return n + 1
end

"""
Return true if the statement has an `ORDER BY` at the top level. Then the row
sequence is important, and the package reads the result on one stream.

The function ignores comments, string literals, quoted identifiers and all the text
in parentheses. Thus an `ORDER BY` in a window (`OVER (ORDER BY x)`), in a subquery
or in an aggregate function does not count.
"""
function has_order_by(sql::AbstractString)
    s = codeunits(String(sql))
    n = length(s)
    out = IOBuffer()
    depth = 0
    i = 1
    while i <= n
        c = s[i]
        if c == UInt8('#') || (c == UInt8('-') && i < n && s[i+1] == UInt8('-'))
            while i <= n && s[i] != UInt8('\n')
                i += 1
            end
            write(out, UInt8(' '))
        elseif c == UInt8('/') && i < n && s[i+1] == UInt8('*')
            j = i + 2
            while j < n && !(s[j] == UInt8('*') && s[j+1] == UInt8('/'))
                j += 1
            end
            i = j + 2
            write(out, UInt8(' '))
        elseif c == UInt8('\'') || c == UInt8('"') || c == UInt8('`')
            i = skip_quoted(s, i)
            write(out, UInt8(' '))
        else
            if c == UInt8('(')
                depth += 1
            elseif c == UInt8(')')
                depth = max(depth - 1, 0)
            end
            write(out, depth == 0 && c != UInt8(')') ? c : UInt8(' '))
            i += 1
        end
    end
    return occursin(r"\border\s+by\b"i, String(take!(out)))
end

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
Try the Storage Read API. Return `nothing` if the caller must use REST.

With `method = :auto`, a failure of the fast procedure causes a warning, and the
caller then uses REST. Examples of failures: no permission, no HTTP/2, a data type
that Arrow.jl cannot decode, or a row count that is different from `expected_rows`.
With `method = :arrow`, a failure is an error.
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
        (e isa GrpcError && e.code == 1) && rethrow()        # the call was cancelled
        storage_fallback_warning(e)
        # A permission problem does not go away. Do not try again on each query.
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
Run `sql` and download its result. Return a [`QueryResult`](@ref).
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
        "requestId" => string(uuid4()),       # with this ID, a second POST of the same request is safe
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

    # A long query: wait for it, one poll at a time.
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
        # Ctrl-C during the wait: cancel the query. Then it does not continue, and BigQuery does not bill it.
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

    # The first page did not contain all the rows: download them in bulk.
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

Run a GoogleSQL query. Return the full result as a `DataFrame`, for all result sizes.

```julia
df = bq_query(con, "SELECT name, SUM(number) AS n FROM `bigquery-public-data.usa_names.usa_1910_2013` GROUP BY name")
df = bq_query(con, "SELECT * FROM my_dataset.events WHERE day = @day AND country IN UNNEST(@cc)";
              params = (day = Date(2024, 1, 31), cc = ["DE", "FR"]))
```

Keywords:

* `params`: the query parameters. Use a `NamedTuple`/`Dict` for `@name`
  placeholders, and a `Tuple`/`Vector` for `?` placeholders. For explicit types,
  refer to [`bq_param`](@ref).
* `method`: how the package downloads large results. `:auto` (the default) uses
  the Storage Read API. If that API is not available, it uses REST pages. `:arrow`
  and `:rest` select one of the two methods.
* `progress`: show a progress line for long downloads (the default is true in
  interactive sessions).
* `max_streams`: the number of parallel Storage API streams (default 8).
* `compression`: the Storage API compression: `:zstd` (default), `:lz4` or `:none`.
* `concurrency`, `page_size`: the number of parallel requests, and the rows in
  each request, for REST pages.
* `dataset`, `location`: use these values for this query, not the defaults of the
  connection.
* `use_cache`: permit BigQuery to use its query cache (default `true`).
* `max_bytes_billed`: if the query bills more than this, BigQuery does not run it.
  The query fails.
* `labels`: the job labels.

Statements that return no rows (DDL, DML) give an empty `DataFrame`. To get the
number of affected rows, use `DBInterface.execute`.
"""
bq_query(conn::Connection, sql::AbstractString; kwargs...) = to_dataframe(run_query(conn, sql; kwargs...))

quote_identifier(name::AbstractString) = "`" * replace(name, "`" => "\\`") * "`"

"""
    bq_read_table(con, table; columns, where, method, progress, ...) -> DataFrame

Download a table directly, without a query. If the package can use the Storage
Read API or REST pages, there is no query cost.

```julia
df = bq_read_table(con, "my-project.my_dataset.my_table")
df = bq_read_table(con, "my_dataset.my_table"; columns = ["id", "amount"], where = "amount > 100")
```

`columns` selects top-level columns. The result has the columns in the sequence of
`columns`. `where` is a SQL filter that the server applies. For views and external
tables, the package runs `SELECT ... FROM table`.
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
    wanted = String[]      # the columns in the sequence of the caller
    selected = String[]    # the same columns in the sequence of the table schema
    if columns !== nothing
        wanted = String[string(c) for c in columns]
        allunique(wanted) || throw(ArgumentError("`columns` names a column more than one time"))
        position = Dict(f.name => i for (i, f) in enumerate(fields))
        for name in wanted
            haskey(position, name) || throw(ArgumentError("table $ref has no column `$name`"))
        end
        # Ask for the columns in the sequence of the table schema. tabledata.list
        # returns the columns in that sequence, not in the sequence of the request.
        # At the end, the result gets the sequence of `columns`.
        selected = sort(wanted; by = name -> position[name])
        fields = Field[fields[position[name]] for name in selected]
    end
    kind = uppercase(string(something(get(meta, "type", nothing), "TABLE")))

    # Put the columns in the sequence of `columns`, then make the data frame.
    function finish(cols, how::Symbol)
        if !isempty(wanted)
            perm = [findfirst(==(name), selected) for name in wanted]
            return to_dataframe(make_result(fields[perm], cols[perm]; method = how))
        end
        return to_dataframe(make_result(fields, cols; method = how))
    end

    via_query = function ()
        select = isempty(wanted) ? "*" : join(map(quote_identifier, wanted), ", ")
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
        cols === nothing || return finish(cols, :arrow)
    finally
        finish!(bar)
    end

    # REST: tabledata.list cannot apply a filter. Thus a `where` requires a query.
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
    return finish(concat_chunks(chunks, length(fields)), :rest)
end
