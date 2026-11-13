const libcrypto = B.libcrypto

# A temporary RSA key that the tests make when they start. The repository contains no secret.
function generate_rsa_pem()
    pkey = ccall((:EVP_PKEY_Q_keygen, libcrypto), Ptr{Cvoid},
                 (Ptr{Cvoid}, Ptr{Cvoid}, Cstring, Csize_t...), C_NULL, C_NULL, "RSA", 2048)
    pkey == C_NULL && error("could not generate a test RSA key")
    bio = ccall((:BIO_new, libcrypto), Ptr{Cvoid}, (Ptr{Cvoid},), ccall((:BIO_s_mem, libcrypto), Ptr{Cvoid}, ()))
    ok = ccall((:PEM_write_bio_PrivateKey, libcrypto), Cint,
               (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Cint, Ptr{Cvoid}, Ptr{Cvoid}),
               bio, pkey, C_NULL, C_NULL, 0, C_NULL, C_NULL)
    ok == 1 || error("could not serialize the test RSA key")
    data = Ref{Ptr{UInt8}}(C_NULL)
    len = ccall((:BIO_ctrl, libcrypto), Clong, (Ptr{Cvoid}, Cint, Clong, Ref{Ptr{UInt8}}), bio, 3, 0, data)
    pem = unsafe_string(data[], len)
    ccall((:BIO_free, libcrypto), Cint, (Ptr{Cvoid},), bio)
    ccall((:EVP_PKEY_free, libcrypto), Cvoid, (Ptr{Cvoid},), pkey)
    return pem
end

function rs256_verify(pem::String, data::Vector{UInt8}, signature::Vector{UInt8})
    bio = ccall((:BIO_new_mem_buf, libcrypto), Ptr{Cvoid}, (Ptr{UInt8}, Cint), pem, sizeof(pem))
    pkey = ccall((:PEM_read_bio_PrivateKey, libcrypto), Ptr{Cvoid},
                 (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}), bio, C_NULL, C_NULL, C_NULL)
    ccall((:BIO_free, libcrypto), Cint, (Ptr{Cvoid},), bio)
    ctx = ccall((:EVP_MD_CTX_new, libcrypto), Ptr{Cvoid}, ())
    ccall((:EVP_DigestVerifyInit, libcrypto), Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid}),
          ctx, C_NULL, ccall((:EVP_sha256, libcrypto), Ptr{Cvoid}, ()), C_NULL, pkey)
    result = ccall((:EVP_DigestVerify, libcrypto), Cint, (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t),
                   ctx, signature, length(signature), data, length(data))
    ccall((:EVP_MD_CTX_free, libcrypto), Cvoid, (Ptr{Cvoid},), ctx)
    ccall((:EVP_PKEY_free, libcrypto), Cvoid, (Ptr{Cvoid},), pkey)
    return result == 1
end

function base64url_decode(s::AbstractString)
    s = replace(s, '-' => '+', '_' => '/')
    return base64decode(s * "="^mod(-length(s), 4))
end

const TEST_KEY = generate_rsa_pem()

"Decode an application/x-www-form-urlencoded body."
function form_fields(body::AbstractString)
    fields = Dict{String,String}()
    for kv in split(body, '&')
        k, v = split(kv, '='; limit = 2)
        fields[B.unescapeuri(k)] = B.unescapeuri(v)
    end
    return fields
end

# An environment that hides the real credentials on the computer that runs the tests.
function clean_env(f, extra::Pair...)
    withenv(f, "GOOGLE_APPLICATION_CREDENTIALS" => nothing, "GOOGLE_CLOUD_PROJECT" => nothing,
            "GCLOUD_PROJECT" => nothing, "CLOUDSDK_CONFIG" => mktempdir(), "NO_GCE_CHECK" => "true",
            "GCE_METADATA_HOST" => nothing, "PATH" => "", extra...)
end

@testset "RS256 signing" begin
    data = Vector{UInt8}("header.claims")
    signature = B.rs256_sign(TEST_KEY, data)
    @test length(signature) == 256
    @test rs256_verify(TEST_KEY, data, signature)
    @test !rs256_verify(TEST_KEY, Vector{UInt8}("header.tampered"), signature)
    @test_throws ArgumentError B.rs256_sign("not a key", data)
    @test B.base64url(UInt8[0xfb, 0xff, 0xfe]) == "-__-" && B.base64url(UInt8[0x00]) == "AA"
end

@testset "authentication" begin
    server = MockBQ.Server()
    base = MockBQ.url(server)
    add_table!(server, "ds.one", Dict("fields" => Any[Dict("name" => "x", "type" => "INTEGER")]), ["{\"f\":[{\"v\":\"1\"}]}"])
    add_query!(server, "SELECT 1"; table = "ds.one")
    try
        @testset "service account key" begin
            key = Dict("type" => "service_account", "client_email" => "sa@proj.iam.gserviceaccount.com",
                       "private_key" => TEST_KEY, "private_key_id" => "kid123", "project_id" => "sa-project",
                       "token_uri" => base * "/token")
            path = joinpath(mktempdir(), "key.json")
            write(path, JSON.json(key))
            reset_log!(server)
            con = clean_env() do
                bq_connect(; credentials = path, endpoint = base)
            end
            @test con.project == "sa-project"                 # from the key file
            @test occursin("sa@proj.iam.gserviceaccount.com", sprint(show, con))
            @test !occursin("PRIVATE", sprint(show, con))
            @test bq_query(con, "SELECT 1").x == [1]
            bq_query(con, "SELECT 1")                         # the package uses the same token again
            log = requests(server)
            token_calls = filter(r -> r.path == "/token", log)
            @test length(token_calls) == 1
            form = form_fields(token_calls[1].body)
            @test form["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer"
            header, claims, signature = split(form["assertion"], '.')
            @test JSON.parse(String(base64url_decode(header))) == Dict("alg" => "RS256", "typ" => "JWT", "kid" => "kid123")
            c = JSON.parse(String(base64url_decode(claims)))
            @test c["iss"] == "sa@proj.iam.gserviceaccount.com"
            @test c["aud"] == base * "/token"
            @test c["scope"] == "https://www.googleapis.com/auth/cloud-platform"
            @test c["exp"] - c["iat"] == 3600 && abs(c["iat"] - time()) < 60
            @test rs256_verify(TEST_KEY, Vector{UInt8}(header * "." * claims), base64url_decode(signature))
            api_calls = filter(r -> startswith(r.path, "/bigquery"), log)
            @test all(r -> r.auth == "Bearer mock-token-1", api_calls)
            @test all(r -> isempty(r.user_project), api_calls)      # only user credentials have a quota project

            # After a 401 (an expired or revoked token), the package gets a new token one time, automatically.
            push!(server.reject_tokens, "mock-token-1")
            @test bq_query(con, "SELECT 1").x == [1]
            @test count(r -> r.path == "/token", requests(server)) == 2

            # Scopes from the user.
            con2 = bq_connect("p"; credentials = key, endpoint = base,
                              scopes = ["https://www.googleapis.com/auth/bigquery", "https://www.googleapis.com/auth/drive.readonly"])
            reset_log!(server)
            bq_query(con2, "SELECT 1")
            form = form_fields(first(requests(server)).body)
            claims = JSON.parse(String(base64url_decode(split(form["assertion"], '.')[2])))
            @test claims["scope"] == "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/drive.readonly"
        end

        @testset "gcloud application-default login file" begin
            adc = Dict("type" => "authorized_user", "client_id" => "cid", "client_secret" => "csecret",
                       "refresh_token" => "rtoken", "quota_project_id" => "quota-project", "token_uri" => base * "/token")
            dir = mktempdir()
            write(joinpath(dir, "application_default_credentials.json"), JSON.json(adc))
            reset_log!(server)
            # The package finds the file in the gcloud configuration directory, as on a computer that has used gcloud.
            con = clean_env("CLOUDSDK_CONFIG" => dir) do
                bq_connect(; endpoint = base)
            end
            @test con.credentials isa B.AuthorizedUser && con.project == "quota-project"
            @test bq_query(con, "SELECT 1").x == [1]
            form = form_fields(first(requests(server)).body)
            @test form == Dict("grant_type" => "refresh_token", "client_id" => "cid",
                               "client_secret" => "csecret", "refresh_token" => "rtoken")
            # User credentials send their quota project, as the Google client libraries do.
            api_calls = filter(r -> startswith(r.path, "/bigquery"), requests(server))
            @test !isempty(api_calls) && all(r -> r.user_project == "quota-project", api_calls)
            @test B.user_project(con) == "quota-project"
            no_quota = bq_connect("p"; credentials = delete!(copy(adc), "quota_project_id"), endpoint = base)
            @test B.user_project(no_quota) == ""
            @test !any(p -> first(p) == "x-goog-user-project", B.auth_headers(no_quota))

            # GOOGLE_APPLICATION_CREDENTIALS has priority, and an explicit project has priority over the project in the file.
            path = joinpath(dir, "application_default_credentials.json")
            con = clean_env("GOOGLE_APPLICATION_CREDENTIALS" => path, "GOOGLE_CLOUD_PROJECT" => "env-project") do
                bq_connect(; endpoint = base)
            end
            @test con.project == "env-project"
            con = clean_env("GOOGLE_APPLICATION_CREDENTIALS" => path, "GOOGLE_CLOUD_PROJECT" => "env-project") do
                bq_connect("explicit"; endpoint = base)
            end
            @test con.project == "explicit"

            revoked = merge(adc, Dict("refresh_token" => "revoked"))
            con = bq_connect("p"; credentials = revoked, endpoint = base)
            err = try
                bq_query(con, "SELECT 1")
            catch e
                e
            end
            @test err isa BigQueryError && err.reason == "invalid_grant"
            @test occursin("gcloud auth application-default login", err.message)
        end

        @testset "access tokens" begin
            con = bq_connect("p"; token = "  static-token\n", endpoint = base)
            reset_log!(server)
            @test bq_query(con, "SELECT 1").x == [1]
            @test first(requests(server)).auth == "Bearer static-token"
            # The package cannot refresh a static token. Thus the 401 goes to the caller.
            push!(server.reject_tokens, "static-token")
            err = try
                bq_query(con, "SELECT 1")
            catch e
                e
            end
            @test err isa BigQueryError && err.status == 401

            calls = Ref(0)
            con = bq_connect("p"; token = () -> "provided-$(calls[] += 1)", endpoint = base)
            bq_query(con, "SELECT 1")
            bq_query(con, "SELECT 1")
            @test calls[] == 1
            push!(server.reject_tokens, "provided-1")
            reset_log!(server)
            @test bq_query(con, "SELECT 1").x == [1]
            @test calls[] == 2 && last(requests(server)).auth == "Bearer provided-2"
        end

        @testset "metadata server (running on Google Cloud)" begin
            host = replace(base, "http://" => "")
            con = clean_env("GCE_METADATA_HOST" => host, "NO_GCE_CHECK" => nothing) do
                bq_connect(; endpoint = base)
            end
            @test con.credentials isa B.MetadataServer && con.project == "metadata-project"
            reset_log!(server)
            @test bq_query(con, "SELECT 1").x == [1]
            @test last(requests(server)).auth == "Bearer metadata-token"
        end

        @testset "no credentials" begin
            err = try
                clean_env(() -> bq_connect("p"; endpoint = base))
            catch e
                e
            end
            @test err isa BigQueryError && err.reason == "noCredentials"
            @test occursin("service-account-key.json", err.message)
            @test_throws ArgumentError clean_env(() -> bq_connect("p"; credentials = "does/not/exist.json"))
            @test_throws ArgumentError clean_env("GOOGLE_APPLICATION_CREDENTIALS" => "nope.json") do
                bq_connect("p")
            end
            @test_throws ArgumentError bq_connect("p"; credentials = Dict("type" => "external_account"))
            @test_throws ArgumentError clean_env(() -> bq_connect(; token = "t"))     # there is no project
        end
    finally
        stop!(server)
    end
end
