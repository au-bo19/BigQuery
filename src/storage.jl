# The fast procedure: the BigQuery Storage Read API. The rows come as Arrow record
# batches over a number of parallel gRPC streams.

struct TableRef
    project::String
    dataset::String
    table::String
end

table_path(t::TableRef) = "projects/$(t.project)/datasets/$(t.dataset)/tables/$(t.table)"
Base.show(io::IO, t::TableRef) = print(io, t.project, ".", t.dataset, ".", t.table)

const STORAGE_SERVICE = "/google.cloud.bigquery.storage.v1.BigQueryRead/"

# When the record batches in the buffer reach this size, decode them. Thus the raw
# data and the decoded data are never both in memory in full.
const ARROW_FLUSH_BYTES = 32 * 1024 * 1024

function create_read_session(conn::Connection, table::TableRef; max_streams::Integer,
                             compression::Symbol, selected_fields = String[],
                             row_restriction::AbstractString = "")
    request = encode_create_read_session("projects/" * conn.project, table_path(table);
        max_streams = max_streams, compression = compression,
        selected_fields = selected_fields, row_restriction = row_restriction)
    url = conn.storage_endpoint * STORAGE_SERVICE * "CreateReadSession"
    routing = "read_session.table=" * escapeuri(table_path(table))
    attempt = 0
    reauthed = false
    while true
        attempt += 1
        response = Ref{Union{Nothing,Vector{UInt8}}}(nothing)
        try
            grpc_call(msg -> (response[] = msg), conn.downloader, url, access_token(conn), request;
                      routing = routing, user_project = user_project(conn))
            response[] === nothing && throw(GrpcError(13, "CreateReadSession returned no message"))
            return decode_read_session(response[])
        catch e
            e isa GrpcError || rethrow()
            if e.code == 16 && !reauthed
                invalidate!(conn.credentials)
                reauthed = true
                continue
            end
            # Only a small number of attempts here. If the Storage API is not available
            # (firewall, proxy), the caller must get to the REST procedure quickly.
            (grpc_retryable(e) && attempt <= min(conn.max_retries, 2)) || rethrow()
            sleep(retry_delay(attempt))
        end
    end
end

"""
Read one stream to its end. Return a vector of decoded chunks `(names, columns)`.
If the stream stops too early, the function starts it again from the last row that
it received.
"""
function read_stream(conn::Connection, stream::String, schema::Vector{UInt8},
                     progress::Progress, cancel::CancelGroup)
    chunks = Tuple{Vector{Symbol},Vector{Any}}[]
    pending = IOBuffer()
    write(pending, schema)
    pending_rows = 0
    offset = 0

    function flush_pending()
        pending_rows == 0 && return nothing
        push!(chunks, decode_arrow(take!(pending)))
        write(pending, schema)
        pending_rows = 0
        return nothing
    end

    function on_message(msg::Vector{UInt8})
        range, rows = decode_read_rows_response(msg)
        isempty(range) && return nothing
        write(pending, view(msg, range))
        pending_rows += rows
        offset += rows
        advance!(progress, rows)
        position(pending) >= ARROW_FLUSH_BYTES && flush_pending()
        return nothing
    end

    url = conn.storage_endpoint * STORAGE_SERVICE * "ReadRows"
    routing = "read_stream=" * escapeuri(stream)
    project = user_project(conn)
    failures = 0
    reauthed = false
    while true
        before = offset
        try
            grpc_call(on_message, conn.downloader, url, access_token(conn),
                      encode_read_rows(stream, offset); routing = routing,
                      user_project = project, cancel = cancel)
            break
        catch e
            e isa GrpcError || rethrow()
            if e.code == 16 && !reauthed
                invalidate!(conn.credentials)
                reauthed = true
                continue
            end
            failures = offset > before ? 1 : failures + 1    # after progress, the count starts again
            (grpc_retryable(e) && failures <= conn.max_retries) || rethrow()
            sleep(retry_delay(failures))
        end
    end
    flush_pending()
    return chunks
end

unwrap_task_error(e) = e isa TaskFailedException ? unwrap_task_error(e.task.exception) :
                       (e isa CompositeException && !isempty(e.exceptions)) ? unwrap_task_error(first(e.exceptions)) : e

"""
Download `table` through the Storage Read API. Return its columns in the sequence
of `fields`. With `ordered = true`, the function uses one stream. This keeps the
row sequence of the table (the result of an `ORDER BY`).
"""
function read_table_arrow(conn::Connection, table::TableRef, fields::Vector{Field};
                          ordered::Bool = false, max_streams::Integer = 8,
                          compression::Symbol = :zstd, selected_fields = String[],
                          row_restriction::AbstractString = "",
                          progress::Progress = Progress(false, 0))
    session = create_read_session(conn, table;
        max_streams = ordered ? 1 : max_streams, compression = compression,
        selected_fields = selected_fields, row_restriction = row_restriction)
    isempty(session.streams) && return new_columns(fields)   # there is no data to read

    cancel = CancelGroup()
    tasks = map(session.streams) do stream
        Threads.@spawn try
            read_stream(conn, stream, session.arrow_schema, progress, cancel)
        catch
            cancel!(cancel)      # one stream failed: stop the other streams immediately
            rethrow()
        end
    end
    results = Vector{Any}(undef, length(tasks))
    failure = nothing
    for (k, task) in enumerate(tasks)
        try
            results[k] = fetch(task)
        catch e
            # Keep the first real error, stop the other streams, and wait for all the tasks.
            if failure === nothing || (failure isa GrpcError && failure.code == 1)
                failure = unwrap_task_error(e)
            end
            cancel!(cancel)
        end
    end
    failure === nothing || throw(failure)

    names = Symbol[]
    chunks = Vector{Any}[]
    for stream_chunks in results, (chunk_names, cols) in stream_chunks
        isempty(names) && (names = chunk_names)
        push!(chunks, cols)
    end
    isempty(chunks) && return new_columns(fields)
    cols = concat_chunks(chunks, length(names))
    return align_columns(fields, names, cols)
end
