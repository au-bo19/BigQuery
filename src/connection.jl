"""
    BigQuery.Connection

A connection to BigQuery: a billing project plus credentials. Create one with
[`bq_connect`](@ref) (or `DBInterface.connect(BigQuery.Connection, ...)`).
"""
mutable struct Connection <: DBInterface.Connection
    project::String
    dataset::Union{Nothing,String}
    location::Union{Nothing,String}
    credentials::Credentials
    downloader::Downloads.Downloader
    endpoint::String
    storage_endpoint::String
    max_retries::Int
    # Set to false once the Storage Read API has refused us, so later queries go
    # straight to REST instead of failing over every time.
    storage_ok::Bool
    isopen::Bool
end

function Base.show(io::IO, c::Connection)
    print(io, "BigQuery.Connection(project = \"", c.project, "\"")
    c.dataset === nothing || print(io, ", dataset = \"", c.dataset, "\"")
    c.location === nothing || print(io, ", location = \"", c.location, "\"")
    print(io, ", auth = ", c.credentials, c.isopen ? ")" : ", closed)")
end

"""
    bq_connect([project]; dataset, location, credentials, token, scopes) -> Connection

Connect to BigQuery. `project` is the project that queries are billed to; when
left out it is taken from `GOOGLE_CLOUD_PROJECT` or from the credentials.

Authentication, first match wins:

* `token = "ya29..."` — an access token you already have, or a zero-argument
  function returning one (called again whenever a fresh token is needed);
* `credentials = "key.json"` — path to a service account key or to a
  `gcloud auth application-default login` file (a parsed `Dict` works too);
* otherwise Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`,
  the gcloud application-default login, the VM metadata server, the gcloud CLI.

Other keywords:

* `dataset` — default dataset for unqualified table names (`"dataset"` or
  `"project.dataset"`);
* `location` — job location such as `"EU"` or `"us-central1"`; usually inferred
  by BigQuery from the tables in the query;
* `scopes` — OAuth scopes for service account keys. Add
  `"https://www.googleapis.com/auth/drive.readonly"` to query Google Sheets
  backed tables.

# Examples
```julia
con = bq_connect("my-project")
con = bq_connect("my-project"; credentials = "C:/keys/sa.json", location = "EU")
```
"""
function bq_connect(project::Union{Nothing,AbstractString} = nothing;
                    dataset::Union{Nothing,AbstractString} = nothing,
                    location::Union{Nothing,AbstractString} = nothing,
                    credentials = nothing,
                    token = nothing,
                    scopes = DEFAULT_SCOPES,
                    endpoint::AbstractString = "https://bigquery.googleapis.com",
                    storage_endpoint::AbstractString = "https://bigquerystorage.googleapis.com",
                    max_retries::Integer = 5)
    dl = make_downloader()
    creds = if token !== nothing
        token isa AbstractString ? StaticToken(String(strip(token))) : TokenProvider(token)
    elseif credentials isa Credentials
        credentials
    elseif credentials isa AbstractString
        credentials_from_file(credentials; scopes = scopes)
    elseif credentials isa AbstractDict
        credentials_from_info(credentials; scopes = scopes)
    elseif credentials === nothing
        default_credentials(dl; scopes = scopes)
    else
        throw(ArgumentError("`credentials` must be a file path, a Dict, or left out"))
    end

    proj = project === nothing ? "" : String(project)
    if isempty(proj)
        proj = get(ENV, "GOOGLE_CLOUD_PROJECT", get(ENV, "GCLOUD_PROJECT", ""))
    end
    if isempty(proj)
        proj = something(default_project(creds, dl), "")
    end
    isempty(proj) && throw(ArgumentError(
        "no billing project: pass it as bq_connect(\"my-project\") or set GOOGLE_CLOUD_PROJECT"))

    return Connection(proj,
        dataset === nothing ? nothing : String(dataset),
        location === nothing ? nothing : String(location),
        creds, dl,
        String(rstrip(endpoint, '/')), String(rstrip(storage_endpoint, '/')),
        Int(max_retries), true, true)
end

access_token(conn::Connection) = access_token(conn.credentials, conn.downloader)

function check_open(conn::Connection)
    conn.isopen || throw(ArgumentError("this BigQuery connection has been closed"))
    return nothing
end

"""
Send an authorized request with an optional JSON `payload` and return the
successful response. Retries transient failures (network errors, 429/5xx, rate
limits) with exponential backoff and refreshes the access token once on a 401.
"""
function send_request(conn::Connection, method::AbstractString, url::AbstractString;
                      payload::Union{Nothing,Vector{UInt8}} = nothing,
                      extra_headers = Pair{String,String}[])
    attempt = 0
    reauthed = false
    while true
        attempt += 1
        headers = Pair{String,String}[
            "Authorization" => "Bearer " * access_token(conn),
            "User-Agent" => USER_AGENT,
            "Accept" => "application/json",
        ]
        payload === nothing || push!(headers, "Content-Type" => "application/json")
        append!(headers, extra_headers)
        resp = try
            http_request(conn.downloader, method, url; headers = headers, body = payload, timeout = 600)
        catch e
            (e isa NetworkError && attempt <= conn.max_retries) || rethrow()
            nothing
        end
        if resp === nothing
            sleep(retry_delay(attempt))
            continue
        end
        200 <= resp.status < 300 && return resp
        if resp.status == 401 && !reauthed
            invalidate!(conn.credentials)
            reauthed = true
            continue
        end
        err = parse_api_error(resp, "BigQuery request")
        if is_retryable(resp.status, err.reason) && attempt <= conn.max_retries
            sleep(retry_delay(attempt))
            continue
        end
        throw(err)
    end
end

"Call the BigQuery REST API and return the raw response body. See [`send_request`](@ref)."
function api_call(conn::Connection, method::AbstractString, path::AbstractString;
                  query = (), body = nothing)
    url = conn.endpoint * "/bigquery/v2/" * path
    qs = query_string(query)
    isempty(qs) || (url *= "?" * qs)
    payload = body === nothing ? nothing : Vector{UInt8}(codeunits(JSON.json(body)))
    return send_request(conn, method, url; payload = payload).body
end

api_json(conn::Connection, method::AbstractString, path::AbstractString; kwargs...) =
    JSON.parse(String(api_call(conn, method, path; kwargs...)))
