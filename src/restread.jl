# Fallback path: page through results with the REST API (tabledata.list),
# fetching and decoding several pages concurrently. Needs no extra permissions
# beyond those required to run the query.

const REST_TARGET_PAGE_BYTES = 8 * 1024 * 1024    # responses are capped at 10 MB
const REST_MAX_PAGE_ROWS = 50_000
const REST_MIN_PAGE_ROWS = 100

table_data_path(t::TableRef) =
    "projects/$(escapeuri(t.project))/datasets/$(escapeuri(t.dataset))/tables/$(escapeuri(t.table))/data"

"Rows `start .. start+count-1` (0-based) of a table, as a chunk of columns."
function fetch_table_chunk(conn::Connection, table::TableRef, fields::Vector{Field},
                           start::Int, count::Int, selected::Union{Nothing,String}, progress::Progress)
    cols = new_columns(fields)
    got = 0
    while got < count
        raw = api_call(conn, "GET", table_data_path(table); query = [
            "startIndex" => start + got,
            "maxResults" => count - got,
            "formatOptions.useInt64Timestamp" => "true",
            "selectedFields" => selected,
        ])
        s = String(raw)
        range = get(top_level_spans(s), "rows", nothing)
        m = range === nothing ? 0 : decode_rows!(cols, fields, s, range)
        # The server returns fewer rows than asked when a page would exceed its
        # size cap; keep going until this chunk is complete.
        m == 0 && throw(BigQueryError(
            "BigQuery returned no rows at offset $(start + got) of $(table); expected $(count - got) more"))
        got += m
        advance!(progress, m)
    end
    return cols
end

function rows_per_page(bytes_per_row::Real, page_size::Union{Nothing,Integer})
    page_size === nothing || return max(1, Int(page_size))
    estimate = bytes_per_row > 0 ? floor(Int, REST_TARGET_PAGE_BYTES / bytes_per_row) : 10_000
    return clamp(estimate, REST_MIN_PAGE_ROWS, REST_MAX_PAGE_ROWS)
end

"""
Download rows `start .. total-1` of `table` with `concurrency` parallel page
requests, returning the chunks in row order.
"""
function read_table_rest(conn::Connection, table::TableRef, fields::Vector{Field}, total::Int;
                         start::Int = 0, bytes_per_row::Real = 0, concurrency::Integer = 8,
                         page_size::Union{Nothing,Integer} = nothing,
                         selected::Union{Nothing,String} = nothing,
                         progress::Progress = Progress(false, 0))
    remaining = total - start
    remaining <= 0 && return Vector{Any}[]
    per_page = rows_per_page(bytes_per_row, page_size)
    nchunks = cld(remaining, per_page)
    chunks = Vector{Vector{Any}}(undef, nchunks)
    next = Threads.Atomic{Int}(1)
    failure = Ref{Any}(nothing)
    failure_lock = ReentrantLock()
    @sync for _ in 1:min(Int(concurrency), nchunks)
        Threads.@spawn begin
            while failure[] === nothing
                k = Threads.atomic_add!(next, 1)
                k > nchunks && break
                chunk_start = start + (k - 1) * per_page
                count = min(per_page, total - chunk_start)
                try
                    chunks[k] = fetch_table_chunk(conn, table, fields, chunk_start, count, selected, progress)
                catch e
                    lock(failure_lock) do
                        failure[] === nothing && (failure[] = e)
                    end
                    break
                end
            end
        end
    end
    failure[] === nothing || throw(failure[])
    return chunks
end

"""
Page through a query's results with jobs.getQueryResults. Sequential (each page
needs the previous page's token); only used when a job has no destination
table to read from, as with scripts.
"""
function read_query_pages(conn::Connection, project::String, job_id::String,
                          location::Union{Nothing,String}, fields::Vector{Field},
                          page_token::String, progress::Progress)
    chunks = Vector{Any}[]
    token = page_token
    while !isempty(token)
        raw = api_call(conn, "GET", "projects/$(escapeuri(project))/queries/$(escapeuri(job_id))"; query = [
            "location" => location,
            "pageToken" => token,
            "formatOptions.useInt64Timestamp" => "true",
        ])
        s, meta, range = split_response(raw)
        cols = new_columns(fields)
        m = range === nothing ? 0 : decode_rows!(cols, fields, s, range)
        m > 0 && push!(chunks, cols)
        advance!(progress, m)
        token = string(something(get(meta, "pageToken", nothing), ""))
    end
    return chunks
end
