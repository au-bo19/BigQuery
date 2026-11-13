"""
    BigQuery.Connection

A connection to BigQuery: a billing project and credentials. To make a connection,
use [`bq_connect`](@ref) or `DBInterface.connect(BigQuery.Connection, ...)`.
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
    # The package sets this field to false when the Storage Read API refuses a request.
    # Then the subsequent queries go directly to REST. They do not try that API again.
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

Connect to BigQuery. BigQuery bills the queries to `project`. If you do not give
`project`, the package uses `GOOGLE_CLOUD_PROJECT` or the project in the credentials.

The package uses the first authentication method in this list that applies:

* `token = "ya29..."`: an access token that you have. This can also be a function
  with no arguments that returns a token. The package calls the function again
  each time that it must have a new token.
* `credentials = "key.json"`: the path to a service account key, or to the file from
  `gcloud auth application-default login`. A parsed `Dict` is also possible.
* Application Default Credentials: `GOOGLE_APPLICATION_CREDENTIALS`, the gcloud
  application-default login, the VM metadata server, and then the gcloud CLI.

Other keywords:

* `dataset`: the default dataset for table names without a dataset (`"dataset"` or
  `"project.dataset"`).
* `location`: the job location, for example `"EU"` or `"us-central1"`. Usually,
  BigQuery finds the location from the tables in the query.
* `scopes`: the OAuth scopes for service account keys. To query tables that use
  Google Sheets, add `"https://www.googleapis.com/auth/drive.readonly"`.

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

"The quota project to send in the `x-goog-user-project` header, or an empty string."
user_project(conn::Connection) = something(quota_project(conn.credentials), "")

"""
The headers that identify the caller: the access token, the user agent, and the
quota project if the credentials have one.
"""
function auth_headers(conn::Connection)
    headers = Pair{String,String}[
        "Authorization" => "Bearer " * access_token(conn),
        "User-Agent" => USER_AGENT,
    ]
    project = user_project(conn)
    isempty(project) || push!(headers, "x-goog-user-project" => project)
    return headers
end

function check_open(conn::Connection)
    conn.isopen || throw(ArgumentError("this BigQuery connection has been closed"))
    return nothing
end

"""
Send an authorized request with an optional JSON `payload`. Return the response if
it is successful. After a temporary failure (a network error, 429/5xx or a rate
limit), the function tries again. The time between the attempts increases
exponentially. After a 401, the function gets a new access token one time.
"""
function send_request(conn::Connection, method::AbstractString, url::AbstractString;
                      payload::Union{Nothing,Vector{UInt8}} = nothing,
                      extra_headers = Pair{String,String}[])
    attempt = 0
    reauthed = false
    while true
        attempt += 1
        headers = auth_headers(conn)
        push!(headers, "Accept" => "application/json")
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

"Call the BigQuery REST API and return the raw response body. Refer to [`send_request`](@ref)."
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
