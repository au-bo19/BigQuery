# HTTP plumbing on top of the Downloads standard library (libcurl).
#
# libcurl gives us HTTP/2 (needed for gRPC), transparent gzip and connection
# reuse without adding any dependency that is not shipped with Julia itself.

"""
    BigQueryError

Thrown when BigQuery (or Google's auth service) rejects a request. `message` is
the server's explanation, `reason` its machine-readable code (for example
`invalidQuery`, `notFound`, `accessDenied`) and `status` the HTTP status, or 0
when the error did not come from an HTTP response.
"""
struct BigQueryError <: Exception
    message::String
    reason::String
    status::Int
end

BigQueryError(message::AbstractString) = BigQueryError(String(message), "", 0)

function Base.showerror(io::IO, e::BigQueryError)
    print(io, "BigQueryError: ", e.message)
    extra = String[]
    isempty(e.reason) || push!(extra, "reason: " * e.reason)
    e.status == 0 || push!(extra, "HTTP " * string(e.status))
    isempty(extra) || print(io, " [", join(extra, ", "), "]")
end

"The request never produced an HTTP response (DNS, TLS, connection reset, timeout)."
struct NetworkError <: Exception
    message::String
end

Base.showerror(io::IO, e::NetworkError) = print(io, "NetworkError: ", e.message)

struct HTTPResponse
    status::Int
    headers::Vector{Pair{String,String}}
    body::Vector{UInt8}
end

const USER_AGENT = "BigQuery.jl/0.1 (gzip)"
const GRPC_CONTENT_TYPE = "application/grpc"

# CURL_HTTP_VERSION_2_PRIOR_KNOWLEDGE: speak HTTP/2 on a plain-text connection.
# Only used when the storage endpoint is overridden with an http:// URL (tests,
# local emulators); real endpoints negotiate HTTP/2 through TLS.
const CURL_HTTP2_PRIOR_KNOWLEDGE = 5

function is_grpc_request(headers)
    for h in headers
        h isa Pair || continue
        lowercase(String(first(h))) == "content-type" && last(h) == GRPC_CONTENT_TYPE && return true
    end
    return false
end

const CURLOPT_POSTFIELDSIZE = 60
const REQUEST_BODY_KEY = :BigQuery_request_body

"""
Run `f()` (a `Downloads.request` call) with `body` as the request body.

The body is handed to libcurl as an in-memory POST (`CURLOPT_COPYPOSTFIELDS`)
from the downloader's hook rather than through Downloads' `input` stream. The
stream-based upload has a race on reused connections: libcurl can ask for data
before the feeding task has started, after which the body is sent empty and the
request fails with "client read function EOF fail".
"""
with_request_body(f, body::Vector{UInt8}) = task_local_storage(f, REQUEST_BODY_KEY, body)

function make_downloader()
    dl = Downloads.Downloader()
    dl.easy_hook = function (easy, info)
        body = get(task_local_storage(), REQUEST_BODY_KEY, nothing)
        if body isa Vector{UInt8}
            # The size must be set first: libcurl copies that many bytes immediately.
            Curl.setopt(easy, CURLOPT_POSTFIELDSIZE, length(body))
            Curl.setopt(easy, Curl.CURLOPT_COPYPOSTFIELDS, body)
        end
        if is_grpc_request(info.headers)
            # A read stream can sit idle while the server throttles or prepares data.
            Curl.setopt(easy, Curl.CURLOPT_LOW_SPEED_TIME, 600)
            if startswith(info.url, "http://")
                Curl.setopt(easy, Curl.CURLOPT_HTTP_VERSION, CURL_HTTP2_PRIOR_KNOWLEDGE)
            end
        else
            Curl.setopt(easy, Curl.CURLOPT_ACCEPT_ENCODING, "gzip")
            Curl.setopt(easy, Curl.CURLOPT_LOW_SPEED_TIME, 120)
        end
        return nothing
    end
    return dl
end

function escapeuri(s::AbstractString)
    io = IOBuffer()
    for b in codeunits(String(s))
        if (UInt8('A') <= b <= UInt8('Z')) || (UInt8('a') <= b <= UInt8('z')) ||
           (UInt8('0') <= b <= UInt8('9')) || b == UInt8('-') || b == UInt8('_') ||
           b == UInt8('.') || b == UInt8('~')
            write(io, b)
        else
            print(io, '%', uppercase(string(b, base = 16, pad = 2)))
        end
    end
    return String(take!(io))
end

function unescapeuri(s::AbstractString)
    bytes = codeunits(String(s))
    out = UInt8[]
    i = 1
    n = length(bytes)
    while i <= n
        b = bytes[i]
        if b == UInt8('%') && i + 2 <= n
            v = tryparse(UInt8, String(bytes[i+1:i+2]); base = 16)
            if v !== nothing
                push!(out, v)
                i += 3
                continue
            end
        end
        push!(out, b)
        i += 1
    end
    return String(out)
end

function query_string(pairs)
    parts = String[]
    for (k, v) in pairs
        v === nothing && continue
        push!(parts, escapeuri(string(k)) * "=" * escapeuri(string(v)))
    end
    return join(parts, "&")
end

# Strip the query string so that error messages never echo request parameters.
url_for_message(url::AbstractString) = String(first(split(url, '?')))

function http_request(dl::Downloads.Downloader, method::AbstractString, url::AbstractString;
                      headers = Pair{String,String}[], body::Union{Nothing,Vector{UInt8}} = nothing,
                      timeout::Real = Inf)
    out = IOBuffer()
    hdrs = Any[h for h in headers]
    resp = if body === nothing
        Downloads.request(url; method = method, output = out, headers = hdrs,
                          downloader = dl, throw = false, timeout = timeout)
    else
        # Without this libcurl waits for a "100 Continue" before uploading over HTTP/1.1.
        push!(hdrs, "Expect" => nothing)
        with_request_body(body) do
            Downloads.request(url; method = method, output = out, headers = hdrs,
                              downloader = dl, throw = false, timeout = timeout)
        end
    end
    if resp isa Downloads.RequestError
        throw(NetworkError(string(method, " ", url_for_message(url), " failed: ", resp.message)))
    end
    return HTTPResponse(resp.status, resp.headers, take!(out))
end

function parse_api_error(resp::HTTPResponse, what::AbstractString)
    text = String(copy(resp.body))
    message = ""
    reason = ""
    try
        data = JSON.parse(text)
        err = data isa AbstractDict ? get(data, "error", nothing) : nothing
        if err isa AbstractDict
            message = string(something(get(err, "message", nothing), ""))
            errs = get(err, "errors", nothing)
            if errs isa AbstractVector && !isempty(errs) && first(errs) isa AbstractDict
                reason = string(something(get(first(errs), "reason", nothing), ""))
            end
            if isempty(reason)
                reason = string(something(get(err, "status", nothing), ""))
            end
        elseif err isa AbstractString
            # OAuth-style errors: {"error": "...", "error_description": "..."}
            reason = String(err)
            message = string(something(get(data, "error_description", nothing), err))
        end
    catch
    end
    if isempty(message)
        snippet = length(text) > 300 ? first(text, 300) * "..." : text
        message = string(what, " failed with HTTP ", resp.status, isempty(snippet) ? "" : ": " * snippet)
    end
    return BigQueryError(message, reason, resp.status)
end

retry_delay(attempt::Integer) = min(32.0, 2.0^(attempt - 1)) * (0.5 + rand() / 2)

function is_retryable(status::Integer, reason::AbstractString)
    status in (429, 500, 502, 503, 504) && return true
    return reason in ("rateLimitExceeded", "backendError", "internalError")
end
