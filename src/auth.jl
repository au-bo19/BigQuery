# Google credentials: everything needed to turn "who am I" into an OAuth access
# token, implemented natively so no external tool is needed at run time.

const DEFAULT_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
const GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"

abstract type Credentials end

mutable struct TokenCache
    lock::ReentrantLock
    token::String
    expires::Float64
end
TokenCache() = TokenCache(ReentrantLock(), "", 0.0)

"A fixed access token supplied by the caller. It cannot be refreshed."
struct StaticToken <: Credentials
    token::String
end

"A user-supplied function returning a fresh access token whenever it is called."
struct TokenProvider <: Credentials
    f::Any
    cache::TokenCache
end
TokenProvider(f) = TokenProvider(f, TokenCache())

"A service account key (the JSON file downloaded from the Cloud console)."
struct ServiceAccount <: Credentials
    client_email::String
    private_key::String
    private_key_id::String
    token_uri::String
    project_id::Union{Nothing,String}
    scopes::Vector{String}
    cache::TokenCache
end

"User credentials with a refresh token, as written by `gcloud auth application-default login`."
struct AuthorizedUser <: Credentials
    client_id::String
    client_secret::String
    refresh_token::String
    token_uri::String
    quota_project_id::Union{Nothing,String}
    cache::TokenCache
end

"The metadata server available on Compute Engine VMs, GKE, Cloud Run, Vertex Workbench, ..."
struct MetadataServer <: Credentials
    host::String
    cache::TokenCache
end
MetadataServer() = MetadataServer(get(ENV, "GCE_METADATA_HOST", "metadata.google.internal"), TokenCache())

"Last resort: ask an installed `gcloud` CLI for a token."
struct GcloudCLI <: Credentials
    path::String
    cache::TokenCache
end

Base.show(io::IO, ::StaticToken) = print(io, "access token (static)")
Base.show(io::IO, ::TokenProvider) = print(io, "access token (provider function)")
Base.show(io::IO, c::ServiceAccount) = print(io, "service account ", c.client_email)
Base.show(io::IO, ::AuthorizedUser) = print(io, "user credentials (refresh token)")
Base.show(io::IO, ::MetadataServer) = print(io, "metadata server (VM service account)")
Base.show(io::IO, ::GcloudCLI) = print(io, "gcloud CLI")

# --- token retrieval --------------------------------------------------------

access_token(c::StaticToken, dl::Downloads.Downloader) = c.token
invalidate!(c::StaticToken) = nothing

function access_token(c::Credentials, dl::Downloads.Downloader)
    cache = c.cache
    lock(cache.lock)
    try
        if isempty(cache.token) || time() > cache.expires - 120
            token, expires = fetch_token(c, dl)
            cache.token = token
            cache.expires = expires
        end
        return cache.token
    finally
        unlock(cache.lock)
    end
end

function invalidate!(c::Credentials)
    cache = c.cache
    lock(cache.lock)
    try
        cache.token = ""
        cache.expires = 0.0
    finally
        unlock(cache.lock)
    end
    return nothing
end

function fetch_token(c::TokenProvider, dl::Downloads.Downloader)
    token = c.f()
    token isa AbstractString || throw(ArgumentError("the token function must return a String"))
    return String(strip(token)), time() + 300
end

function token_request(dl::Downloads.Downloader, uri::AbstractString, form)
    body = Vector{UInt8}(codeunits(query_string(form)))
    resp = http_request(dl, "POST", uri;
        headers = ["Content-Type" => "application/x-www-form-urlencoded",
                   "Accept" => "application/json", "User-Agent" => USER_AGENT],
        body = body, timeout = 60)
    data = try
        JSON.parse(String(copy(resp.body)))
    catch
        nothing
    end
    if resp.status != 200 || !(data isa AbstractDict) || !(get(data, "access_token", nothing) isa AbstractString)
        err = parse_api_error(resp, "Token request")
        throw(BigQueryError("could not obtain an access token: " * err.message, err.reason, resp.status))
    end
    lifetime = get(data, "expires_in", 3600)
    lifetime = lifetime isa Number ? Float64(lifetime) : 3600.0
    return String(data["access_token"]), time() + lifetime
end

function fetch_token(c::AuthorizedUser, dl::Downloads.Downloader)
    try
        return token_request(dl, c.token_uri, [
            "grant_type" => "refresh_token",
            "client_id" => c.client_id,
            "client_secret" => c.client_secret,
            "refresh_token" => c.refresh_token,
        ])
    catch e
        if e isa BigQueryError && e.reason == "invalid_grant"
            throw(BigQueryError(e.message * " -- the saved login has expired or was revoked; " *
                "run `gcloud auth application-default login` again (or switch to a service account key).",
                e.reason, e.status))
        end
        rethrow()
    end
end

function fetch_token(c::ServiceAccount, dl::Downloads.Downloader)
    now = floor(Int, time())
    header = Dict{String,Any}("alg" => "RS256", "typ" => "JWT")
    isempty(c.private_key_id) || (header["kid"] = c.private_key_id)
    claims = Dict{String,Any}(
        "iss" => c.client_email,
        "scope" => join(c.scopes, " "),
        "aud" => c.token_uri,
        "iat" => now,
        "exp" => now + 3600,
    )
    signing_input = base64url(JSON.json(header)) * "." * base64url(JSON.json(claims))
    signature = rs256_sign(c.private_key, Vector{UInt8}(codeunits(signing_input)))
    assertion = signing_input * "." * base64url(signature)
    return token_request(dl, c.token_uri, [
        "grant_type" => "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion" => assertion,
    ])
end

metadata_url(c::MetadataServer, path::AbstractString) =
    "http://" * c.host * "/computeMetadata/v1/" * path

function metadata_get(c::MetadataServer, dl::Downloads.Downloader, path::AbstractString; timeout::Real = 10)
    return http_request(dl, "GET", metadata_url(c, path);
        headers = ["Metadata-Flavor" => "Google"], timeout = timeout)
end

function fetch_token(c::MetadataServer, dl::Downloads.Downloader)
    resp = metadata_get(c, dl, "instance/service-accounts/default/token")
    resp.status == 200 || throw(BigQueryError(
        "the metadata server refused to issue a token (HTTP $(resp.status)); " *
        "does this VM have a service account attached?", "authError", resp.status))
    data = JSON.parse(String(copy(resp.body)))
    lifetime = get(data, "expires_in", 300)
    return String(data["access_token"]), time() + Float64(lifetime)
end

function metadata_available(c::MetadataServer, dl::Downloads.Downloader)
    lowercase(get(ENV, "NO_GCE_CHECK", "")) in ("1", "true", "yes") && return false
    try
        resp = metadata_get(c, dl, "instance/service-accounts/default/email"; timeout = 2)
        return resp.status == 200 &&
               any(h -> h[1] == "metadata-flavor" && h[2] == "Google", resp.headers)
    catch
        return false
    end
end

function find_gcloud()
    for name in (Sys.iswindows() ? ("gcloud.cmd", "gcloud.exe", "gcloud") : ("gcloud",))
        path = Sys.which(name)
        path === nothing || return path
    end
    return nothing
end

function fetch_token(c::GcloudCLI, dl::Downloads.Downloader)
    cmd = Sys.iswindows() && endswith(lowercase(c.path), ".cmd") ?
        `cmd /c $(c.path) auth print-access-token` : `$(c.path) auth print-access-token`
    out = try
        read(pipeline(cmd; stderr = devnull), String)
    catch
        throw(BigQueryError("`gcloud auth print-access-token` failed; run `gcloud auth login` first.", "authError", 0))
    end
    token = String(strip(out))
    isempty(token) && throw(BigQueryError("`gcloud auth print-access-token` returned nothing.", "authError", 0))
    # gcloud does not report the expiry; refresh well inside the one-hour lifetime.
    return token, time() + 1500
end

# --- RS256 (service account JWT signing) through the OpenSSL bundled with Julia

base64url(data::Vector{UInt8}) = replace(rstrip(base64encode(data), '='), '+' => '-', '/' => '_')
base64url(s::AbstractString) = base64url(Vector{UInt8}(codeunits(String(s))))

function rs256_sign(pem::AbstractString, data::Vector{UInt8})
    pem = String(pem)
    bio = ccall((:BIO_new_mem_buf, libcrypto), Ptr{Cvoid}, (Ptr{UInt8}, Cint), pem, sizeof(pem))
    bio == C_NULL && error("OpenSSL: could not allocate a memory buffer")
    pkey = GC.@preserve pem ccall((:PEM_read_bio_PrivateKey, libcrypto), Ptr{Cvoid},
        (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}), bio, C_NULL, C_NULL, C_NULL)
    ccall((:BIO_free, libcrypto), Cint, (Ptr{Cvoid},), bio)
    pkey == C_NULL && throw(ArgumentError(
        "the service account key's `private_key` is not a readable PEM private key"))
    ctx = ccall((:EVP_MD_CTX_new, libcrypto), Ptr{Cvoid}, ())
    try
        md = ccall((:EVP_sha256, libcrypto), Ptr{Cvoid}, ())
        ok = ccall((:EVP_DigestSignInit, libcrypto), Cint,
            (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}), ctx, C_NULL, md, C_NULL, pkey)
        ok == 1 || error("OpenSSL: EVP_DigestSignInit failed")
        siglen = Ref{Csize_t}(0)
        ok = ccall((:EVP_DigestSign, libcrypto), Cint,
            (Ptr{Cvoid}, Ptr{UInt8}, Ref{Csize_t}, Ptr{UInt8}, Csize_t), ctx, C_NULL, siglen, data, length(data))
        ok == 1 || error("OpenSSL: EVP_DigestSign (size query) failed")
        sig = Vector{UInt8}(undef, siglen[])
        ok = ccall((:EVP_DigestSign, libcrypto), Cint,
            (Ptr{Cvoid}, Ptr{UInt8}, Ref{Csize_t}, Ptr{UInt8}, Csize_t), ctx, sig, siglen, data, length(data))
        ok == 1 || error("OpenSSL: EVP_DigestSign failed")
        resize!(sig, siglen[])
        return sig
    finally
        ccall((:EVP_MD_CTX_free, libcrypto), Cvoid, (Ptr{Cvoid},), ctx)
        ccall((:EVP_PKEY_free, libcrypto), Cvoid, (Ptr{Cvoid},), pkey)
    end
end

# --- building credentials ---------------------------------------------------

optional_string(x) = (x isa AbstractString && !isempty(x)) ? String(x) : nothing

function credentials_from_info(info::AbstractDict; scopes = DEFAULT_SCOPES)
    kind = string(something(get(info, "type", nothing), ""))
    if kind == "service_account"
        for key in ("client_email", "private_key")
            get(info, key, nothing) isa AbstractString ||
                throw(ArgumentError("service account key is missing the `$key` field"))
        end
        return ServiceAccount(
            String(info["client_email"]),
            String(info["private_key"]),
            string(something(get(info, "private_key_id", nothing), "")),
            string(something(get(info, "token_uri", nothing), GOOGLE_TOKEN_URI)),
            optional_string(get(info, "project_id", nothing)),
            String[String(s) for s in scopes],
            TokenCache())
    elseif kind == "authorized_user"
        for key in ("client_id", "client_secret", "refresh_token")
            get(info, key, nothing) isa AbstractString ||
                throw(ArgumentError("user credentials file is missing the `$key` field"))
        end
        return AuthorizedUser(
            String(info["client_id"]),
            String(info["client_secret"]),
            String(info["refresh_token"]),
            string(something(get(info, "token_uri", nothing), GOOGLE_TOKEN_URI)),
            optional_string(get(info, "quota_project_id", nothing)),
            TokenCache())
    else
        throw(ArgumentError(
            "unsupported credentials type \"$kind\"; BigQuery.jl understands `service_account` keys " *
            "and `authorized_user` files (from `gcloud auth application-default login`). " *
            "For anything else pass `token = () -> \"...\"` to `bq_connect`."))
    end
end

function credentials_from_file(path::AbstractString; scopes = DEFAULT_SCOPES)
    isfile(path) || throw(ArgumentError("credentials file not found: $path"))
    info = try
        JSON.parse(read(path, String))
    catch
        throw(ArgumentError("credentials file is not valid JSON: $path"))
    end
    info isa AbstractDict || throw(ArgumentError("credentials file is not a JSON object: $path"))
    return credentials_from_info(info; scopes = scopes)
end

function well_known_adc_path()
    dir = get(ENV, "CLOUDSDK_CONFIG", "")
    if isempty(dir)
        dir = Sys.iswindows() ? joinpath(get(ENV, "APPDATA", homedir()), "gcloud") :
                                joinpath(homedir(), ".config", "gcloud")
    end
    return joinpath(dir, "application_default_credentials.json")
end

"""
Find credentials the way Google's own client libraries do (Application Default
Credentials), in this order:

1. the file named by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable;
2. the file written by `gcloud auth application-default login`;
3. the metadata server (when running on Google Cloud);
4. an installed `gcloud` CLI.
"""
function default_credentials(dl::Downloads.Downloader; scopes = DEFAULT_SCOPES)
    path = get(ENV, "GOOGLE_APPLICATION_CREDENTIALS", "")
    if !isempty(path)
        isfile(path) || throw(ArgumentError(
            "GOOGLE_APPLICATION_CREDENTIALS points to a file that does not exist: $path"))
        return credentials_from_file(path; scopes = scopes)
    end
    adc = well_known_adc_path()
    isfile(adc) && return credentials_from_file(adc; scopes = scopes)
    md = MetadataServer()
    metadata_available(md, dl) && return md
    gcloud = find_gcloud()
    gcloud === nothing || return GcloudCLI(gcloud, TokenCache())
    throw(BigQueryError("""
        no Google credentials found. Use one of:
          * bq_connect(project; credentials = "path/to/service-account-key.json")
          * set the GOOGLE_APPLICATION_CREDENTIALS environment variable to such a key file
          * run `gcloud auth application-default login` once on this machine
          * run on a Google Cloud VM with a service account attached
          * bq_connect(project; token = "ya29....") with an access token you already have""",
        "noCredentials", 0))
end

default_project(::Credentials, ::Downloads.Downloader) = nothing
default_project(c::ServiceAccount, ::Downloads.Downloader) = c.project_id
default_project(c::AuthorizedUser, ::Downloads.Downloader) = c.quota_project_id

function default_project(c::MetadataServer, dl::Downloads.Downloader)
    try
        resp = metadata_get(c, dl, "project/project-id"; timeout = 5)
        resp.status == 200 || return nothing
        return optional_string(strip(String(copy(resp.body))))
    catch
        return nothing
    end
end
