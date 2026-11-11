# End-to-end tests of the Storage Read API path (gRPC over real HTTP/2, Arrow
# payloads) against a scripted server. The server is a ~70 line Node script, so
# these tests run only where `node` is installed; the package itself never needs it.

frames(messages) = reduce(vcat, [B.grpc_frame(m) for m in messages]; init = UInt8[])

function logged_requests(dir)
    path = joinpath(dir, "requests.log")
    isfile(path) || return Any[]
    return Any[JSON.parse(line) for line in eachline(path) if !isempty(line)]
end

clear_requests(dir) = rm(joinpath(dir, "requests.log"); force = true)

const PLAN_GENERATION = Ref(0)

function write_plan(dir, calls)
    PLAN_GENERATION[] += 1
    write(joinpath(dir, "plan.json"), JSON.json(Dict("generation" => PLAN_GENERATION[], "calls" => calls)))
    clear_requests(dir)
end

step(file; status = 0, message = "", chunk = 1500) =
    Dict("file" => file, "status" => status, "message" => message, "chunk" => chunk)

# Read protobuf field `number` (a varint) from the top level of a message.
function varint_field(message::Vector{UInt8}, number::Int)
    found = 0
    B.pb_foreach(message, 1, length(message)) do field, wire, value, range
        (field == number && wire == 0) && (found = Int(value))
    end
    return found
end

node = Sys.which("node")
if node === nothing
    @info "node not found: skipping the Storage Read API end-to-end tests"
else
    @testset "Storage Read API (mock gRPC server)" begin
        dir = mktempdir()
        write_plan(dir, Dict())
        proc = open(`$node $(joinpath(@__DIR__, "grpc_mock_server.js")) $dir`, "r")
        port = parse(Int, replace(readline(proc), "PORT " => ""))
        rest = MockBQ.Server()
        try
            n = 30_000
            truth = truth_table(n)
            arrow = arrow_columns(truth)
            add_table!(rest, "ds.big", SCHEMA, rest_rows(truth))
            add_query!(rest, "SELECT big"; table = "ds.big")
            add_query!(rest, "SELECT big ORDER BY id"; table = "ds.big")
            con = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                             storage_endpoint = "http://127.0.0.1:$port", max_retries = 3)

            # Three streams; each sends several record batches.
            prefix = "projects/billing-proj/locations/us/sessions/S1/streams/"
            layout = Dict("a" => [1:4_000, 4_001:9_000, 9_001:10_000],
                          "b" => [10_001:12_000, 12_001:19_000, 19_001:20_000, 20_001:22_000],
                          "c" => [22_001:30_000])
            schema = UInt8[]
            for (stream, ranges) in layout
                schema, batches = arrow_messages(arrow, ranges; compress = :zstd)
                responses = [encode_read_rows_response(b, length(r)) for (b, r) in zip(batches, ranges)]
                write(joinpath(dir, "$stream.bin"), frames(responses))
                # Pieces for the interrupted-stream scenarios.
                write(joinpath(dir, "$stream.head.bin"), frames(responses[1:1]))
                write(joinpath(dir, "$stream.tail.bin"), frames(responses[2:end]))
                cut = frames(responses[1:min(2, end)])
                write(joinpath(dir, "$stream.cut.bin"), cut[1:end-37])           # ends mid-message
            end
            session(streams) = frames([encode_read_session("projects/billing-proj/locations/us/sessions/S1",
                                                           schema, [prefix * s for s in streams], n)])
            write(joinpath(dir, "session.bin"), session(["a", "b", "c"]))
            write(joinpath(dir, "session_empty.bin"), session(String[]))
            write(joinpath(dir, "session_ab.bin"), session(["a", "b"]))
            healthy = Dict{String,Any}("CreateReadSession" => [step("session.bin")],
                                       (("ReadRows:" * prefix * s) => [step("$s.bin")] for s in ("a", "b", "c"))...)

            @testset "parallel streams" begin
                write_plan(dir, healthy)
                res = B.run_query(con, "SELECT big"; progress = false)
                @test res.method == :arrow && res.total_rows == n
                check_table(DataFrame(res), truth)

                log = logged_requests(dir)
                create = only(filter(r -> r["method"] == "CreateReadSession", log))
                @test create["path"] == "/google.cloud.bigquery.storage.v1.BigQueryRead/CreateReadSession"
                @test create["contentType"] == "application/grpc" && create["te"] == "trailers"
                @test create["authorization"] == "Bearer tok"
                @test create["routing"] == "read_session.table=projects%2Fbilling-proj%2Fdatasets%2Fds%2Ftables%2Fbig"
                @test create["flag"] == 0
                message = base64decode(create["message"])
                @test create["declaredLength"] == length(message)
                @test message == B.encode_create_read_session("projects/billing-proj",
                    "projects/billing-proj/datasets/ds/tables/big"; max_streams = 8, compression = :zstd)
                reads = filter(r -> r["method"] == "ReadRows", log)
                @test length(reads) == 3
                @test sort([r["routing"] for r in reads]) == ["read_stream=" * B.escapeuri(prefix * s) for s in ("a", "b", "c")]
                @test all(r -> varint_field(base64decode(r["message"]), 2) == 0, reads)

                check_table(bq_query(con, "SELECT big"; progress = false, method = :arrow, max_streams = 2, compression = :none), truth)
                create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(dir)[end-3:end]))
                @test base64decode(create["message"]) == B.encode_create_read_session("projects/billing-proj",
                    "projects/billing-proj/datasets/ds/tables/big"; max_streams = 2, compression = :none)
            end

            @testset "ORDER BY reads a single stream" begin
                write_plan(dir, healthy)
                check_table(bq_query(con, "SELECT big ORDER BY id"; progress = false), truth)
                create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(dir)))
                @test varint_field(base64decode(create["message"]), 3) == 1
            end

            @testset "tiny network chunks" begin
                plan = copy(healthy)
                plan["ReadRows:" * prefix * "a"] = [step("a.bin"; chunk = 7)]
                plan["CreateReadSession"] = [step("session.bin"; chunk = 3)]
                write_plan(dir, plan)
                check_table(bq_query(con, "SELECT big"; progress = false), truth)
            end

            @testset "a dropped stream resumes where it stopped" begin
                plan = copy(healthy)
                # First attempt delivers one batch and then fails; the retry must ask for the rest only.
                plan["ReadRows:" * prefix * "b"] = [step("b.head.bin"; status = 14, message = "connection reset"), step("b.tail.bin")]
                # First attempt ends in the middle of a message without an error status.
                plan["ReadRows:" * prefix * "a"] = [step("a.cut.bin"), step("a.tail.bin")]
                write_plan(dir, plan)
                check_table(bq_query(con, "SELECT big"; progress = false), truth)
                reads = filter(r -> r["method"] == "ReadRows", logged_requests(dir))
                offsets(s) = [varint_field(base64decode(r["message"]), 2) for r in reads if endswith(r["routing"], B.escapeuri(prefix * s))]
                @test offsets("b") == [0, 2_000]
                @test offsets("a") == [0, 4_000]
                @test offsets("c") == [0]
            end

            @testset "errors in a stream" begin
                plan = copy(healthy)
                plan["ReadRows:" * prefix * "b"] = [step("b.head.bin"; status = 3, message = "request failed: bad stream")]
                write_plan(dir, plan)
                err = try
                    bq_query(con, "SELECT big"; progress = false, method = :arrow)
                catch e
                    e
                end
                @test err isa B.GrpcError && err.code == 3 && err.message == "request failed: bad stream"
                @test occursin("INVALID_ARGUMENT", sprint(showerror, err))

                # Persistent unavailability gives up after max_retries.
                plan["ReadRows:" * prefix * "b"] = [step(nothing; status = 14, message = "down")]
                write_plan(dir, plan)
                quick = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                                   storage_endpoint = "http://127.0.0.1:$port", max_retries = 1)
                @test_throws B.GrpcError bq_query(quick, "SELECT big"; progress = false, method = :arrow)
                @test count(r -> r["method"] == "ReadRows" && endswith(r["routing"], B.escapeuri(prefix * "b")),
                            logged_requests(dir)) == 2

                # Missing rows are never returned silently.
                write_plan(dir, merge(healthy, Dict("CreateReadSession" => [step("session_ab.bin")])))
                err = try
                    bq_query(con, "SELECT big"; progress = false, method = :arrow)
                catch e
                    e
                end
                @test err isa BigQueryError && occursin("22000 rows", err.message)
                # ...and in :auto mode the complete result is fetched over REST instead.
                res = @test_logs (:warn, r"returned 22000 rows") B.run_query(con, "SELECT big"; progress = false)
                @test res.method == :rest
                check_table(DataFrame(res), truth)
            end

            @testset "undecodable Arrow data falls back in :auto mode" begin
                write(joinpath(dir, "garbage.bin"), frames([encode_read_rows_response(rand(UInt8, 500), 10_000)]))
                plan = copy(healthy)
                plan["ReadRows:" * prefix * "a"] = [step("garbage.bin")]
                write_plan(dir, plan)
                @test_throws Exception bq_query(con, "SELECT big"; progress = false, method = :arrow)
                res = @test_logs (:warn, r"falling back") B.run_query(con, "SELECT big"; progress = false)
                @test res.method == :rest && con.storage_ok
                check_table(DataFrame(res), truth)
            end

            @testset "progress line" begin
                write_plan(dir, healthy)
                path = joinpath(dir, "stderr.txt")
                bar = B.Progress(true, 30_000)
                bar.started -= 5                       # pretend the download has been running a while
                open(path, "w") do io
                    redirect_stderr(io) do
                        B.advance!(bar, 12_345)
                        B.finish!(bar)
                    end
                end
                text = read(path, String)
                @test occursin("BigQuery: 12.3k / 30.0k rows", text) && endswith(text, "\r")
                @test B.format_count(999) == "999" && B.format_count(5_250_000) == "5.25M"
                quiet = B.Progress(false, 10)
                B.advance!(quiet, 10)
                @test quiet.done[] == 10 && !quiet.printed
            end

            @testset "falling back to REST" begin
                plan = copy(healthy)
                plan["CreateReadSession"] = [step(nothing; status = 7,
                    message = "Permission denied: bigquery.readsessions.create on project billing-proj")]
                write_plan(dir, plan)
                fresh = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                                   storage_endpoint = "http://127.0.0.1:$port", max_retries = 3)
                @test_throws B.GrpcError bq_query(fresh, "SELECT big"; progress = false, method = :arrow)
                res = @test_logs (:warn, r"falling back to the slower REST download") B.run_query(fresh, "SELECT big"; progress = false)
                @test res.method == :rest
                check_table(DataFrame(res), truth)
                @test fresh.storage_ok == false
                # Not attempted again on this connection (and no further warning).
                clear_requests(dir)
                res = @test_logs B.run_query(fresh, "SELECT big"; progress = false)
                @test res.method == :rest && isempty(logged_requests(dir))

                # Storage endpoint unreachable altogether.
                offline = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                                     storage_endpoint = "http://127.0.0.1:9", max_retries = 0)
                res = @test_logs (:warn, r"falling back") B.run_query(offline, "SELECT big"; progress = false)
                @test res.method == :rest && offline.storage_ok
                check_table(DataFrame(res), truth)
            end

            @testset "bq_read_table" begin
                write_plan(dir, healthy)
                check_table(bq_read_table(con, "ds.big"; progress = false), truth)
                create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(dir)))
                @test base64decode(create["message"]) == B.encode_create_read_session("projects/billing-proj",
                    "projects/billing-proj/datasets/ds/tables/big"; max_streams = 8, compression = :zstd)

                # Column selection and a row filter are sent to the server; a filter matching nothing yields no streams.
                write_plan(dir, merge(healthy, Dict("CreateReadSession" => [step("session_empty.bin")])))
                df = bq_read_table(con, "billing-proj.ds.big"; columns = ["name", "id"], where = "id < 0", progress = false)
                @test nrow(df) == 0 && names(df) == ["name", "id"] && eltype(df.id) == Int64
                create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(dir)))
                @test base64decode(create["message"]) == B.encode_create_read_session("projects/billing-proj",
                    "projects/billing-proj/datasets/ds/tables/big"; max_streams = 8, compression = :zstd,
                    selected_fields = ["name", "id"], row_restriction = "id < 0")
            end
        finally
            stop!(rest)
            kill(proc)
        end
    end
end
