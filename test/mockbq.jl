# A small in-process imitation of the BigQuery REST API (and Google's OAuth
# token endpoint), enough to exercise BigQuery.jl without a Google account.

module MockBQ

using HTTP, JSON, CodecZlib, Sockets, Dates

export Server, add_table!, add_query!, stop!, requests, reset_log!

mutable struct Table
    schema::Any
    rows::Vector{String}      # pre-rendered `{"f":[...]}` rows
    kind::String
end

mutable struct Server
    http::Any
    port::Int
    lock::ReentrantLock
    tables::Dict{String,Table}          # "dataset.table"
    queries::Dict{String,Any}           # sql => behaviour
    jobs::Dict{String,Any}              # job id => (table key, polls left, ...)
    log::Vector{Any}
    max_page_rows::Int                  # the server-side cap on rows per response
    fail_data_requests::Int             # answer this many data requests with a 503
    reject_tokens::Set{String}          # tokens answered with 401
    tokens_issued::Int
    job_counter::Int
    uploads::Dict{String,Any}           # upload id => the job and the bytes received so far
    upload_accept_bytes::Int            # keep at most this many bytes of each upload request
    fail_upload_requests::Int           # answer this many upload requests with a 503, storing nothing
    lose_upload_responses::Int          # store the bytes of this many upload requests, then answer 503
    load_polls::Int                     # report a load job as RUNNING this many times
    max_gzip_bytes::Int                 # load jobs fail on a gzip file larger than this (4 GB in BigQuery)
end

url(s::Server) = "http://127.0.0.1:$(s.port)"

json_response(status, obj) = HTTP.Response(status, ["Content-Type" => "application/json"], JSON.json(obj))

function error_response(status, reason, message)
    return json_response(status, Dict("error" => Dict(
        "code" => status, "message" => message,
        "errors" => Any[Dict("reason" => reason, "message" => message, "domain" => "global")])))
end

function add_table!(s::Server, key::String, schema, rows::Vector{String}; kind = "TABLE")
    lock(s.lock) do
        s.tables[key] = Table(schema, rows, kind)
    end
end

"""
Register what a SQL text does. `behaviour` keys: `table` (result table key),
`error` (`(reason, message)`), `polls` (answer "not complete" this many times),
`script` (no destination table), `dml` (affected rows), `anonymous` (hide the
job's destination table lookup behind an error).
"""
add_query!(s::Server, sql::String; kwargs...) = lock(() -> (s.queries[sql] = Dict{Symbol,Any}(kwargs)), s.lock)

requests(s::Server) = lock(() -> copy(s.log), s.lock)
reset_log!(s::Server) = lock(() -> empty!(s.log), s.lock)

function page(s::Server, table::Table, start::Int, max_results::Int; with_schema = true)
    total = length(table.rows)
    count = max(0, min(max_results, s.max_page_rows, total - start))
    rows = table.rows[start+1:start+count]
    io = IOBuffer()
    print(io, "{\"kind\":\"bigquery#mock\",")
    with_schema && print(io, "\"schema\":", JSON.json(table.schema), ",")
    print(io, "\"totalRows\":\"", total, "\",")
    if count > 0
        # Rows first, token after, with odd spacing: the client must not rely on layout.
        print(io, "\"rows\": [", join(rows, " ,\n "), "] ,")
    end
    start + count < total && print(io, "\"pageToken\":\"idx:", start + count, "\",")
    print(io, "\"jobComplete\":true}")
    return String(take!(io))
end

function gzip_response(req::HTTP.Request, body::String)
    if occursin("gzip", HTTP.header(req, "Accept-Encoding", ""))
        return HTTP.Response(200, ["Content-Type" => "application/json", "Content-Encoding" => "gzip"],
                             transcode(GzipCompressor, Vector{UInt8}(body)))
    end
    return HTTP.Response(200, ["Content-Type" => "application/json"], body)
end

function int_param(params, key, default)
    value = get(params, key, nothing)
    value isa Integer && return Int(value)
    value isa AbstractString && return something(tryparse(Int, value), default)
    return default
end

function handle(s::Server, req::HTTP.Request)
    uri = HTTP.URI(req.target)
    path = uri.path
    params = HTTP.queryparams(uri)
    body = String(copy(req.body))
    lock(s.lock) do
        push!(s.log, (method = req.method, path = path, params = params, body = body,
                      auth = HTTP.header(req, "Authorization", ""),
                      range = HTTP.header(req, "Content-Range", "")))
    end

    if path == "/token"
        form = HTTP.queryparams(body)
        if get(form, "refresh_token", "") == "revoked"
            return json_response(400, Dict("error" => "invalid_grant", "error_description" => "Token has been expired or revoked."))
        end
        token = lock(() -> "mock-token-$(s.tokens_issued += 1)", s.lock)
        return json_response(200, Dict("access_token" => token, "expires_in" => 3600, "token_type" => "Bearer"))
    end

    # --- the VM metadata server --------------------------------------------
    if startswith(path, "/computeMetadata/v1/")
        HTTP.header(req, "Metadata-Flavor", "") == "Google" ||
            return HTTP.Response(403, "Missing Metadata-Flavor:Google header.")
        flavor = ["Metadata-Flavor" => "Google"]
        endswith(path, "/service-accounts/default/email") &&
            return HTTP.Response(200, flavor, "vm-sa@mock.iam.gserviceaccount.com")
        endswith(path, "/project/project-id") && return HTTP.Response(200, flavor, "metadata-project")
        endswith(path, "/service-accounts/default/token") &&
            return HTTP.Response(200, vcat(flavor, ["Content-Type" => "application/json"]),
                                 JSON.json(Dict("access_token" => "metadata-token", "expires_in" => 3599)))
        return HTTP.Response(404, flavor, "not found")
    end

    auth = HTTP.header(req, "Authorization", "")
    token = replace(auth, "Bearer " => "")
    if !startswith(auth, "Bearer ") || token in s.reject_tokens
        return error_response(401, "authError", "Invalid Credentials")
    end

    # --- resumable uploads (load jobs) --------------------------------------
    m = match(r"^/upload/bigquery/v2/projects/([^/]+)/jobs$", path)
    m === nothing || return handle_upload(s, req, String(m.captures[1]), path, params)

    m = match(r"^/bigquery/v2/projects/([^/]+)/(.*)$", path)
    m === nothing && return error_response(404, "notFound", "Not found: $path")
    project, rest = m.captures

    # --- jobs.query -------------------------------------------------------
    if req.method == "POST" && rest == "queries"
        request = JSON.parse(body)
        behaviour = lock(() -> get(s.queries, request["query"], nothing), s.lock)
        behaviour === nothing && return error_response(400, "invalidQuery",
            "Syntax error: the mock does not know this query: $(request["query"])")
        if haskey(behaviour, :error)
            reason, message = behaviour[:error]
            return error_response(reason == "accessDenied" ? 403 : 400, reason, message)
        end
        job_id = lock(() -> "job_$(s.job_counter += 1)", s.lock)
        job = Dict{Symbol,Any}(:behaviour => behaviour, :polls => get(behaviour, :polls, 0), :cancelled => false)
        lock(() -> (s.jobs[job_id] = job), s.lock)
        ref = Dict("projectId" => project, "jobId" => job_id, "location" => "US")
        if job[:polls] > 0
            return json_response(200, Dict("jobComplete" => false, "jobReference" => ref))
        end
        return query_page(s, req, job, ref, 0, int_param(request, "maxResults", typemax(Int)))
    end

    # --- jobs.getQueryResults ----------------------------------------------
    m = match(r"^queries/([^/]+)$", rest)
    if req.method == "GET" && m !== nothing
        job_id = m.captures[1]
        job = lock(() -> get(s.jobs, job_id, nothing), s.lock)
        job === nothing && return error_response(404, "notFound", "Not found: Job $job_id")
        ref = Dict("projectId" => project, "jobId" => job_id, "location" => "US")
        if job[:polls] > 0
            job[:polls] -= 1
            return json_response(200, Dict("jobComplete" => false, "jobReference" => ref))
        end
        start = int_param(params, "startIndex", 0)
        token = get(params, "pageToken", "")
        isempty(token) || (start = parse(Int, replace(token, "idx:" => "")))
        return query_page(s, req, job, ref, start, int_param(params, "maxResults", typemax(Int)))
    end

    # --- jobs.get / jobs.cancel ---------------------------------------------
    m = match(r"^jobs/([^/]+)(/cancel)?$", rest)
    if m !== nothing
        job_id = m.captures[1]
        job = lock(() -> get(s.jobs, job_id, nothing), s.lock)
        job === nothing && return error_response(404, "notFound", "Not found: Job $job_id")
        if m.captures[2] !== nothing
            job[:cancelled] = true
            return json_response(200, Dict("job" => Dict("id" => job_id)))
        end
        if haskey(job, :load)
            running = lock(() -> job[:polls] > 0 ? (job[:polls] -= 1; true) : false, s.lock)
            return json_response(200, running ? running_job(job[:load]) : job[:load])
        end
        behaviour = job[:behaviour]
        query_config = Dict{String,Any}("query" => "...")
        if haskey(behaviour, :table) && !get(behaviour, :script, false)
            dataset, table = split(behaviour[:table], '.')
            query_config["destinationTable"] = Dict("projectId" => project, "datasetId" => dataset, "tableId" => table)
        end
        return json_response(200, Dict("id" => job_id, "configuration" => Dict("query" => query_config),
                                       "status" => Dict("state" => "DONE")))
    end

    # --- tables.get / tabledata.list ----------------------------------------
    m = match(r"^datasets/([^/]+)/tables/([^/]+)(/data)?$", rest)
    if req.method == "GET" && m !== nothing
        key = m.captures[1] * "." * m.captures[2]
        table = lock(() -> get(s.tables, key, nothing), s.lock)
        table === nothing && return error_response(404, "notFound", "Not found: Table $project:$key")
        if m.captures[3] === nothing
            return json_response(200, Dict("schema" => table.schema, "numRows" => string(length(table.rows)),
                                           "type" => table.kind))
        end
        fail = lock(s.lock) do
            s.fail_data_requests > 0 ? (s.fail_data_requests -= 1; true) : false
        end
        fail && return error_response(503, "backendError", "Service unavailable, try again")
        start = int_param(params, "startIndex", 0)
        selected = get(params, "selectedFields", "")
        if !isempty(selected)
            names = split(selected, ',')
            idx = [findfirst(f -> f["name"] == n, table.schema["fields"]) for n in names]
            table = Table(Dict("fields" => table.schema["fields"][idx]),
                          [JSON.json(Dict("f" => JSON.parse(r)["f"][idx])) for r in table.rows], table.kind)
        end
        return gzip_response(req, page(s, table, start, int_param(params, "maxResults", typemax(Int)); with_schema = false))
    end

    return error_response(404, "notFound", "Not found: $path")
end

function query_page(s::Server, req, job, ref, start::Int, max_results::Int)
    behaviour = job[:behaviour]
    extra = Dict{String,Any}("jobReference" => ref, "totalBytesProcessed" => "12345", "cacheHit" => false)
    if haskey(behaviour, :dml)
        return json_response(200, merge(extra, Dict("jobComplete" => true, "numDmlAffectedRows" => string(behaviour[:dml]))))
    end
    table = lock(() -> s.tables[behaviour[:table]], s.lock)
    body = page(s, table, start, max_results)
    # Splice the job facts into the page object.
    body = "{" * chop(JSON.json(extra); head = 1, tail = 1) * "," * chop(body; head = 1, tail = 0)
    return gzip_response(req, body)
end

# --- load jobs ----------------------------------------------------------------

struct LoadError <: Exception
    reason::String
    message::String
    details::Vector{String}
end

mismatch(field, v) =
    error("Could not convert value to $(field["type"]). Field: $(field["name"]); Value: $(JSON.json(v))")

function timestamp_micros(v::AbstractString)
    m = match(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,6}))?Z$", v)
    m === nothing && return nothing
    seconds = Dates.value(DateTime(m.captures[1]) - DateTime(1970)) ÷ 1000
    return seconds * 1_000_000 + parse(Int, rpad(something(m.captures[2], ""), 6, '0'))
end

"""
Check one value of a newline-delimited JSON row against its column, strictly,
and convert it to the way the REST API returns values of that type.
"""
function load_value(v, field; element = false)
    typ = uppercase(field["type"])
    mode = get(field, "mode", "NULLABLE")
    if mode == "REPEATED" && !element
        v === nothing && return Any[]
        v isa AbstractVector || error("Field $(field["name"]) is repeated but was given a single value")
        return Any[Dict("v" => load_value(x, field; element = true)) for x in v]
    end
    if v === nothing
        element && error("Field $(field["name"]): array elements cannot be NULL")
        mode == "REQUIRED" && error("Missing required field: $(field["name"])")
        return nothing
    end
    if typ in ("RECORD", "STRUCT")
        v isa AbstractDict || mismatch(field, v)
        return Dict("f" => Any[Dict("v" => load_value(get(v, sub["name"], nothing), sub)) for sub in field["fields"]])
    elseif typ in ("INTEGER", "INT64")
        (v isa Integer && !(v isa Bool)) && return string(v)
        (v isa AbstractString && tryparse(Int64, v) !== nothing) && return v
    elseif typ in ("FLOAT", "FLOAT64")
        (v isa Real && !(v isa Bool)) && return string(Float64(v))
        if v isa AbstractString
            low = lowercase(v)
            low == "nan" && return "NaN"
            low in ("inf", "+inf", "infinity", "+infinity") && return "Infinity"
            low in ("-inf", "-infinity") && return "-Infinity"
            tryparse(Float64, v) !== nothing && return v
        end
    elseif typ in ("NUMERIC", "BIGNUMERIC")
        (v isa Real && !(v isa Bool)) && return string(v)
        (v isa AbstractString && occursin(r"^-?\d+(\.\d+)?$", v)) && return v
    elseif typ in ("BOOLEAN", "BOOL")
        v isa Bool && return string(v)
        (v isa AbstractString && lowercase(v) in ("true", "false")) && return lowercase(v)
    elseif typ in ("STRING", "GEOGRAPHY")
        v isa AbstractString && return v
    elseif typ == "JSON"
        return JSON.json(v)
    elseif typ == "BYTES"
        (v isa AbstractString && occursin(r"^[A-Za-z0-9+/]*={0,2}$", v) && length(v) % 4 == 0) && return v
    elseif typ == "TIMESTAMP"
        micros = v isa AbstractString ? timestamp_micros(v) : nothing
        micros === nothing || return string(micros)
    elseif typ == "DATE"
        (v isa AbstractString && occursin(r"^\d{4}-\d\d-\d\d$", v)) && return v
    elseif typ == "DATETIME"
        (v isa AbstractString && occursin(r"^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d(\.\d{1,6})?$", v)) && return v
    elseif typ == "TIME"
        (v isa AbstractString && occursin(r"^\d\d:\d\d:\d\d(\.\d{1,6})?$", v)) && return v
    end
    mismatch(field, v)
end

"Apply a finished upload to the tables, as a load job would. Returns the number of rows loaded."
function apply_load!(s::Server, key::String, config, data::String)
    disposition = get(config, "writeDisposition", "WRITE_APPEND")
    table = get(s.tables, key, nothing)
    if table === nothing && get(config, "createDisposition", "CREATE_IF_NEEDED") == "CREATE_NEVER"
        throw(LoadError("notFound", "Not found: Table $key", String[]))
    end
    if table !== nothing && disposition == "WRITE_EMPTY" && !isempty(table.rows)
        throw(LoadError("duplicate", "Already Exists: Table $key", String[]))
    end
    keep = table !== nothing && disposition != "WRITE_TRUNCATE"
    schema = keep ? table.schema : get(config, "schema", nothing)
    schema === nothing && throw(LoadError("invalid", "No schema specified on job or table.", String[]))
    fields = schema["fields"]
    known = Set(f["name"] for f in fields)
    rows = String[]
    position = 0
    for line in split(data, '\n')
        if !isempty(line)
            try
                row = JSON.parse(line)
                for name in keys(row)
                    name in known || error("No such field: $name.")
                end
                cells = Any[Dict("v" => load_value(get(row, f["name"], nothing), f)) for f in fields]
                push!(rows, JSON.json(Dict("f" => cells)))
            catch e
                e isa InterruptException && rethrow()
                throw(LoadError("invalid",
                    "Error while reading data, error message: JSON table encountered too many errors, " *
                    "giving up. Rows: $(length(rows) + 1); errors: 1. Please look into the errors[] " *
                    "collection for more details.",
                    ["Error while reading data, error message: JSON parsing error in row starting at " *
                     "position $position: $(sprint(showerror, e))"]))
            end
        end
        position += ncodeunits(line) + 1
    end
    if keep
        append!(table.rows, rows)
    else
        s.tables[key] = Table(schema, rows, "TABLE")
    end
    return length(rows)
end

function running_job(resource)
    job = copy(resource)
    job["status"] = Dict("state" => "RUNNING")
    return job
end

function finish_load(s::Server, project::String, upload)
    request = upload[:job]
    config = request["configuration"]["load"]
    ref = Dict{String,Any}(request["jobReference"])
    haskey(ref, "location") || (ref["location"] = "US")
    dest = config["destinationTable"]
    status = Dict{String,Any}("state" => "DONE")
    statistics = Dict{String,Any}()
    try
        # Compression is not announced: a gzip file is recognised by how it starts.
        data = upload[:data]
        upload[:gzip] = length(data) >= 2 && data[1] == 0x1f && data[2] == 0x8b
        (upload[:gzip] && length(data) > s.max_gzip_bytes) && throw(LoadError("invalid",
            "Error while reading data, error message: Input files compressed with gzip are larger than the maximum file size allowed.", String[]))
        text = String(upload[:gzip] ? transcode(GzipDecompressor, copy(data)) : copy(data))
        n = apply_load!(s, dest["datasetId"] * "." * dest["tableId"], config, text)
        statistics["load"] = Dict("outputRows" => string(n), "inputFileBytes" => string(length(data)))
    catch e
        e isa LoadError || rethrow()
        first_error = Dict("reason" => e.reason, "message" => e.message)
        status["errorResult"] = first_error
        status["errors"] = vcat(Any[first_error], Any[Dict("reason" => e.reason, "message" => d) for d in e.details])
    end
    resource = Dict{String,Any}("kind" => "bigquery#job", "id" => "$project:$(ref["location"]).$(ref["jobId"])",
        "jobReference" => ref, "configuration" => request["configuration"], "status" => status,
        "statistics" => statistics)
    s.jobs[ref["jobId"]] = Dict{Symbol,Any}(:load => resource, :polls => s.load_polls, :cancelled => false)
    return s.load_polls > 0 ? running_job(resource) : resource
end

resume_incomplete(received::Int) =
    HTTP.Response(308, received > 0 ? ["Range" => "bytes=0-$(received - 1)"] : Pair{String,String}[], "")

unavailable() = error_response(503, "backendError", "Service unavailable, try again")

"The resumable upload protocol: one POST opens a session, PUTs carry the bytes."
function handle_upload(s::Server, req::HTTP.Request, project::String, path::AbstractString, params)
    if req.method == "POST"
        get(params, "uploadType", "") == "resumable" ||
            return error_response(400, "invalid", "the mock only supports resumable uploads")
        id = lock(s.lock) do
            id = "upload_$(length(s.uploads) + 1)"
            s.uploads[id] = Dict{Symbol,Any}(:job => JSON.parse(String(copy(req.body))), :data => UInt8[],
                                             :done => nothing)
            id
        end
        return HTTP.Response(200, ["Location" => url(s) * path * "?uploadType=resumable&upload_id=" * id], "")
    end
    req.method == "PUT" || return error_response(405, "invalid", "Method not allowed")
    lock(s.lock) do
        upload = get(s.uploads, get(params, "upload_id", ""), nothing)
        upload === nothing && return error_response(404, "notFound", "Upload session not found")
        if s.fail_upload_requests > 0
            s.fail_upload_requests -= 1
            return unavailable()
        end
        m = match(r"^bytes (?:(\d+)-(\d+)|\*)/(\d+|\*)$", HTTP.header(req, "Content-Range", ""))
        m === nothing && return error_response(400, "invalid", "Invalid Content-Range")
        upload[:done] === nothing || return json_response(200, upload[:done])
        total = m.captures[3] == "*" ? nothing : parse(Int, m.captures[3])
        data = upload[:data]
        if m.captures[1] !== nothing
            first, last = parse(Int, m.captures[1]), parse(Int, m.captures[2])
            last - first + 1 == length(req.body) ||
                return error_response(400, "invalid", "Content-Range does not match the body size")
            # Only the last chunk may have an odd size.
            (total === nothing && length(req.body) % (256 * 1024) != 0) &&
                return error_response(400, "invalid", "Chunk size must be a multiple of 262144 bytes")
            # Bytes at the wrong offset are ignored; the 308 below says where to resume.
            first == length(data) && append!(data, req.body[1:min(length(req.body), s.upload_accept_bytes)])
        elseif !isempty(req.body)
            return error_response(400, "invalid", "A status request must not carry data")
        end
        lost = s.lose_upload_responses > 0
        lost && (s.lose_upload_responses -= 1)
        if total !== nothing && length(data) == total
            upload[:done] = finish_load(s, project, upload)
            return lost ? unavailable() : json_response(200, upload[:done])
        end
        (total !== nothing && length(data) > total) &&
            return error_response(400, "invalid", "More bytes were uploaded than announced")
        return lost ? unavailable() : resume_incomplete(length(data))
    end
end

function Server(; max_page_rows::Int = typemax(Int))
    s = Server(nothing, 0, ReentrantLock(), Dict{String,Table}(), Dict{String,Any}(), Dict{String,Any}(),
               Any[], max_page_rows, 0, Set{String}(), 0, 0,
               Dict{String,Any}(), typemax(Int), 0, 0, 0, 4_000_000_000)
    port, listener = Sockets.listenany(Sockets.ip"127.0.0.1", 18_200 + rand(0:2000))
    http = HTTP.serve!("127.0.0.1", Int(port); server = listener, verbose = false) do req
        try
            handle(s, req)
        catch e
            @error "mock BigQuery server failed" exception = (e, catch_backtrace())
            error_response(500, "mockFailure", sprint(showerror, e))
        end
    end
    s.http = http
    s.port = Int(port)
    return s
end

stop!(s::Server) = close(s.http)

end # module
