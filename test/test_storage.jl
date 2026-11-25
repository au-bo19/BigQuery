# End-to-end tests of the Storage Read API procedure (gRPC over real HTTP/2, Arrow
# payloads) against a scripted server in Julia (grpcmock.jl).

frames(messages) = reduce(vcat, [B.grpc_frame(m) for m in messages]; init = UInt8[])

logged_requests(grpc) = GrpcMock.requests(grpc)
clear_requests(grpc) = GrpcMock.clear_requests!(grpc)
write_plan(grpc, calls) = GrpcMock.set_plan!(grpc, calls)

step(file; status = 0, message = "", chunk = 1500) =
    Dict("file" => file, "status" => status, "message" => message, "chunk" => chunk)

# Read the protobuf field `number` (a varint) from the top level of a message.
function varint_field(message::Vector{UInt8}, number::Int)
    found = 0
    B.pb_foreach(message, 1, length(message)) do field, wire, value, range
        (field == number && wire == 0) && (found = Int(value))
    end
    return found
end

@testset "Storage Read API (mock gRPC server)" begin
    dir = mktempdir()
    grpc = GrpcMock.Server(dir)
    port = grpc.port
    rest = MockBQ.Server()
    try
        n = 30_000
        truth = truth_table(n)
        arrow = arrow_columns(truth)
        add_table!(rest, "ds.big", SCHEMA, rest_rows(truth))
        add_query!(rest, "SELECT big"; table = "ds.big")
        add_query!(rest, "SELECT big ORDER BY id"; table = "ds.big")
        add_query!(rest, "SELECT big, ROW_NUMBER() OVER (ORDER BY id) AS r"; table = "ds.big")
        con = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                         storage_endpoint = "http://127.0.0.1:$port", max_retries = 3, show_billed = false)

        # Three streams. Each stream sends a number of record batches.
        prefix = "projects/billing-proj/locations/us/sessions/S1/streams/"
        layout = Dict("a" => [1:4_000, 4_001:9_000, 9_001:10_000],
                      "b" => [10_001:12_000, 12_001:19_000, 19_001:20_000, 20_001:22_000],
                      "c" => [22_001:30_000])
        schema = UInt8[]
        for (stream, ranges) in layout
            schema, batches = arrow_messages(arrow, ranges; compress = :zstd)
            responses = [encode_read_rows_response(b, length(r)) for (b, r) in zip(batches, ranges)]
            write(joinpath(dir, "$stream.bin"), frames(responses))
            # Parts for the tests of streams that stop too early.
            write(joinpath(dir, "$stream.head.bin"), frames(responses[1:1]))
            write(joinpath(dir, "$stream.tail.bin"), frames(responses[2:end]))
            cut = frames(responses[1:min(2, end)])
            write(joinpath(dir, "$stream.cut.bin"), cut[1:end-37])           # ends in the middle of a message
        end
        session(streams) = frames([encode_read_session("projects/billing-proj/locations/us/sessions/S1",
                                                       schema, [prefix * s for s in streams], n)])
        write(joinpath(dir, "session.bin"), session(["a", "b", "c"]))
        write(joinpath(dir, "session_empty.bin"), session(String[]))
        write(joinpath(dir, "session_ab.bin"), session(["a", "b"]))
        healthy = Dict{String,Any}("CreateReadSession" => [step("session.bin")],
                                   (("ReadRows:" * prefix * s) => [step("$s.bin")] for s in ("a", "b", "c"))...)

        @testset "parallel streams" begin
            write_plan(grpc, healthy)
            res = B.run_query(con, "SELECT big"; progress = false)
            @test res.method == :arrow && res.total_rows == n
            check_table(DataFrame(res), truth)

            log = logged_requests(grpc)
            create = only(filter(r -> r["method"] == "CreateReadSession", log))
            @test create["path"] == "/google.cloud.bigquery.storage.v1.BigQueryRead/CreateReadSession"
            @test create["contentType"] == "application/grpc" && create["te"] == "trailers"
            @test create["authorization"] == "Bearer tok"
            @test create["routing"] == "read_session.table=projects%2Fbilling-proj%2Fdatasets%2Fds%2Ftables%2Fbig"
            @test create["flag"] == 0
            message = create["message"]
            @test create["declaredLength"] == length(message)
            @test message == B.encode_create_read_session("projects/billing-proj",
                "projects/billing-proj/datasets/ds/tables/big"; max_streams = 8, compression = :zstd)
            reads = filter(r -> r["method"] == "ReadRows", log)
            @test length(reads) == 3
            @test sort([r["routing"] for r in reads]) == ["read_stream=" * B.escapeuri(prefix * s) for s in ("a", "b", "c")]
            @test all(r -> varint_field(r["message"], 2) == 0, reads)

            check_table(bq_query(con, "SELECT big"; progress = false, method = :arrow, max_streams = 2, compression = :none), truth)
            create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(grpc)[end-3:end]))
            @test create["message"] == B.encode_create_read_session("projects/billing-proj",
                "projects/billing-proj/datasets/ds/tables/big"; max_streams = 2, compression = :none)
        end

        @testset "ORDER BY reads a single stream" begin
            write_plan(grpc, healthy)
            check_table(bq_query(con, "SELECT big ORDER BY id"; progress = false), truth)
            create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(grpc)))
            @test varint_field(create["message"], 3) == 1

            # An ORDER BY in a window function does not set the row sequence of the result.
            write_plan(grpc, healthy)
            check_table(bq_query(con, "SELECT big, ROW_NUMBER() OVER (ORDER BY id) AS r"; progress = false), truth)
            create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(grpc)))
            @test varint_field(create["message"], 3) == 8
        end

        @testset "tiny network chunks" begin
            plan = copy(healthy)
            plan["ReadRows:" * prefix * "a"] = [step("a.bin"; chunk = 7)]
            plan["CreateReadSession"] = [step("session.bin"; chunk = 3)]
            write_plan(grpc, plan)
            check_table(bq_query(con, "SELECT big"; progress = false), truth)
        end

        @testset "a dropped stream resumes where it stopped" begin
            plan = copy(healthy)
            # The first attempt sends one batch and then fails. The next attempt must ask only for the remaining rows.
            plan["ReadRows:" * prefix * "b"] = [step("b.head.bin"; status = 14, message = "connection reset"), step("b.tail.bin")]
            # The first attempt ends in the middle of a message, without an error status.
            plan["ReadRows:" * prefix * "a"] = [step("a.cut.bin"), step("a.tail.bin")]
            write_plan(grpc, plan)
            check_table(bq_query(con, "SELECT big"; progress = false), truth)
            reads = filter(r -> r["method"] == "ReadRows", logged_requests(grpc))
            offsets(s) = [varint_field(r["message"], 2) for r in reads if endswith(r["routing"], B.escapeuri(prefix * s))]
            @test offsets("b") == [0, 2_000]
            @test offsets("a") == [0, 4_000]
            @test offsets("c") == [0]
        end

        @testset "errors in a stream" begin
            plan = copy(healthy)
            plan["ReadRows:" * prefix * "b"] = [step("b.head.bin"; status = 3, message = "request failed: bad stream")]
            write_plan(grpc, plan)
            err = try
                bq_query(con, "SELECT big"; progress = false, method = :arrow)
            catch e
                e
            end
            @test err isa B.GrpcError && err.code == 3 && err.message == "request failed: bad stream"
            @test occursin("INVALID_ARGUMENT", sprint(showerror, err))

            # If the server is not available for a long time, the client stops after max_retries.
            plan["ReadRows:" * prefix * "b"] = [step(nothing; status = 14, message = "down")]
            write_plan(grpc, plan)
            quick = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                               storage_endpoint = "http://127.0.0.1:$port", max_retries = 1, show_billed = false)
            @test_throws B.GrpcError bq_query(quick, "SELECT big"; progress = false, method = :arrow)
            @test count(r -> r["method"] == "ReadRows" && endswith(r["routing"], B.escapeuri(prefix * "b")),
                        logged_requests(grpc)) == 2

            # If rows are missing, the client always gives an error.
            write_plan(grpc, merge(healthy, Dict("CreateReadSession" => [step("session_ab.bin")])))
            err = try
                bq_query(con, "SELECT big"; progress = false, method = :arrow)
            catch e
                e
            end
            @test err isa BigQueryError && occursin("22000 rows", err.message)
            # ... and in the :auto mode, the client gets the complete result through REST.
            res = @test_logs (:warn, r"returned 22000 rows") B.run_query(con, "SELECT big"; progress = false)
            @test res.method == :rest
            check_table(DataFrame(res), truth)
        end

        @testset "undecodable Arrow data falls back in :auto mode" begin
            write(joinpath(dir, "garbage.bin"), frames([encode_read_rows_response(rand(UInt8, 500), 10_000)]))
            plan = copy(healthy)
            plan["ReadRows:" * prefix * "a"] = [step("garbage.bin")]
            write_plan(grpc, plan)
            @test_throws Exception bq_query(con, "SELECT big"; progress = false, method = :arrow)
            res = @test_logs (:warn, r"falling back") B.run_query(con, "SELECT big"; progress = false)
            @test res.method == :rest && con.storage_ok
            check_table(DataFrame(res), truth)
        end

        @testset "progress line" begin
            write_plan(grpc, healthy)
            path = joinpath(dir, "stderr.txt")
            bar = B.Progress(true, 30_000; tick = false)
            bar.started -= 5                       # the download started 5 seconds before
            open(path, "w") do io
                redirect_stderr(io) do
                    B.advance!(bar, 12_345)
                    B.draw!(bar)
                    B.finish!(bar)
                    B.draw!(bar)                   # a late timer call prints nothing
                end
            end
            text = read(path, String)
            @test occursin("BigQuery: 12.3k / 30.0k rows", text) && endswith(text, "\r")
            @test count("BigQuery", text) == 1
            @test B.format_count(999) == "999" && B.format_count(5_250_000) == "5.25M"
            quiet = B.Progress(false, 10)
            B.advance!(quiet, 10)
            @test quiet.done[] == 10 && !quiet.printed && quiet.timer === nothing
            @test B.byte_counter(quiet) === nothing

            # While the query runs, the line shows the time.
            running = B.Progress(true, -1; tick = false)
            running.started -= 7
            @test startswith(B.status_line(running, time()), "BigQuery: running query (7s)")

            # The timer draws the line without new rows: the line appears on time.
            open(path, "w") do io
                redirect_stderr(io) do
                    ticking = B.Progress(true, 1_000_000)
                    sleep(B.PROGRESS_DELAY + 0.6)
                    B.finish!(ticking)
                end
            end
            text = read(path, String)
            @test count("BigQuery: 0 / 1.0M rows", text) >= 2 && endswith(text, "\r")

            # Bytes in flight count as rows, with the rows per byte of the complete pages.
            est = B.Progress(true, 10_000; tick = false)
            first_page = B.byte_counter(est; inflight = false)
            first_page(0, 1_000, 0, 0)
            @test est.inflight[] == 0              # a query response: not in flight
            B.sample!(est, first_page, 100)        # 100 rows in 1000 bytes
            @test B.shown_count(est) == 0
            page = B.byte_counter(est)
            page(0, 2_000, 0, 0)
            page(0, 5_000, 0, 0)
            @test page.received == 5_000 && B.shown_count(est) == 500
            B.settle!(est, page, 400)              # fewer rows than the estimate: the line does not go back
            @test est.done[] == 400 && est.inflight[] == 0 && B.shown_count(est) == 500
            retry = B.byte_counter(est)
            retry(0, 3_000, 0, 0)
            retry(0, 1_000, 0, 0)                  # the request started again
            @test retry.received == 4_000 && est.inflight[] == 4_000
            big = B.byte_counter(est)
            big(0, 10^9, 0, 0)
            @test B.shown_count(est) == 10_000     # never more than the total
            B.restart!(est)
            @test est.done[] == 0 && est.inflight[] == 0 && B.shown_count(est) == 0
            @test est.sample_rows[] == 500 && est.sample_bytes[] == 6_000      # the samples stay

            # The REST download reports the bytes of each page through Downloads.
            rest_bar = B.Progress(true, n; tick = false)
            B.read_table_rest(con, B.TableRef("billing-proj", "ds", "big"), B.parse_schema(SCHEMA), n;
                              page_size = 4_000, progress = rest_bar)
            @test rest_bar.done[] == n && rest_bar.inflight[] == 0
            @test rest_bar.sample_rows[] == n && rest_bar.sample_bytes[] > 0
            B.finish!(rest_bar)
        end

        @testset "falling back to REST" begin
            plan = copy(healthy)
            plan["CreateReadSession"] = [step(nothing; status = 7,
                message = "Permission denied: bigquery.readsessions.create on project billing-proj")]
            write_plan(grpc, plan)
            fresh = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                               storage_endpoint = "http://127.0.0.1:$port", max_retries = 3, show_billed = false)
            @test_throws B.GrpcError bq_query(fresh, "SELECT big"; progress = false, method = :arrow)
            res = @test_logs (:warn, r"falling back to the slower REST download") B.run_query(fresh, "SELECT big"; progress = false)
            @test res.method == :rest
            check_table(DataFrame(res), truth)
            @test fresh.storage_ok == false
            # The connection does not try the Storage API again, and it shows no more warnings.
            clear_requests(grpc)
            res = @test_logs B.run_query(fresh, "SELECT big"; progress = false)
            @test res.method == :rest && isempty(logged_requests(grpc))

            # The client cannot connect to the storage endpoint.
            offline = bq_connect("billing-proj"; token = "tok", endpoint = MockBQ.url(rest),
                                 storage_endpoint = "http://127.0.0.1:9", max_retries = 0, show_billed = false)
            res = @test_logs (:warn, r"falling back") B.run_query(offline, "SELECT big"; progress = false)
            @test res.method == :rest && offline.storage_ok
            check_table(DataFrame(res), truth)
        end

        @testset "bq_read_table" begin
            write_plan(grpc, healthy)
            check_table(bq_read_table(con, "ds.big"; progress = false), truth)
            create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(grpc)))
            @test create["message"] == B.encode_create_read_session("projects/billing-proj",
                "projects/billing-proj/datasets/ds/tables/big"; max_streams = 8, compression = :zstd)

            # The client sends the column selection (in the sequence of the schema) and the row
            # filter to the server. If no row agrees with the filter, the session has no streams.
            write_plan(grpc, merge(healthy, Dict("CreateReadSession" => [step("session_empty.bin")])))
            df = bq_read_table(con, "billing-proj.ds.big"; columns = ["name", "id"], where = "id < 0", progress = false)
            @test nrow(df) == 0 && names(df) == ["name", "id"] && eltype(df.id) == Int64
            create = only(filter(r -> r["method"] == "CreateReadSession", logged_requests(grpc)))
            @test create["message"] == B.encode_create_read_session("projects/billing-proj",
                "projects/billing-proj/datasets/ds/tables/big"; max_streams = 8, compression = :zstd,
                selected_fields = ["id", "name"], row_restriction = "id < 0")

            # A column selection in a sequence that is different from the schema sequence.
            write_plan(grpc, healthy)
            df = bq_read_table(con, "ds.big"; columns = ["name", "id"], progress = false)
            @test names(df) == ["name", "id"] && isequal(df.name, truth.name) && df.id == truth.id
        end
    finally
        stop!(rest)
        GrpcMock.stop!(grpc)
    end
end
