@testset "queries over REST (mock BigQuery)" begin
    server = MockBQ.Server()
    base = MockBQ.url(server)
    con = bq_connect("proj"; token = "t", endpoint = base, max_retries = 5)
    small = truth_table(50)
    large = truth_table(25_000)
    add_table!(server, "ds.small", SCHEMA, rest_rows(small))
    add_table!(server, "ds.large", SCHEMA, rest_rows(large))
    add_table!(server, "ds.empty", SCHEMA, String[])
    add_query!(server, "SELECT small"; table = "ds.small")
    add_query!(server, "SELECT large"; table = "ds.large")
    add_query!(server, "SELECT empty"; table = "ds.empty")
    try
        @testset "small result: one round trip" begin
            reset_log!(server)
            df = bq_query(con, "SELECT small")
            @test df isa DataFrame
            check_table(df, small)
            log = requests(server)
            @test length(log) == 1 && log[1].method == "POST" && log[1].path == "/bigquery/v2/projects/proj/queries"
            body = JSON.parse(log[1].body)
            @test body["query"] == "SELECT small" && body["useLegacySql"] === false
            @test body["formatOptions"]["useInt64Timestamp"] === true
            @test !haskey(body, "location") && !haskey(body, "defaultDataset") && !haskey(body, "queryParameters")
            @test length(body["requestId"]) == 36
        end

        @testset "empty result keeps column types" begin
            df = bq_query(con, "SELECT empty")
            @test nrow(df) == 0 && names(df) == [string(k) for k in keys(small)]
            @test eltype(df.id) == Int64 && eltype(df.ts) == DateTime && eltype(df.tags) == Vector{String}
        end

        @testset "large result: parallel paging" begin
            reset_log!(server)
            df = bq_query(con, "SELECT large"; method = :rest, progress = false)
            check_table(df, large)
            pages = filter(r -> endswith(r.path, "/tables/large/data"), requests(server))
            @test length(pages) >= 1
            @test all(r -> get(r.params, "formatOptions.useInt64Timestamp", "") == "true", pages)

            # The server caps pages well below what is asked for (as BigQuery does at 10 MB);
            # every row must still arrive, in order, exactly once.
            server.max_page_rows = 1_234
            for concurrency in (1, 8)
                df = bq_query(con, "SELECT large"; method = :rest, progress = false,
                              concurrency = concurrency, page_size = 3_000)
                check_table(df, large)
            end
            df = bq_query(con, "SELECT large"; method = :rest, progress = false)
            check_table(df, large)
            df = bq_query(con, "SELECT large"; method = :rest, progress = false, page_size = 777)
            check_table(df, large)
            server.max_page_rows = typemax(Int)
        end

        @testset "transient server errors are retried" begin
            server.fail_data_requests = 3
            df = bq_query(con, "SELECT large"; method = :rest, progress = false)
            check_table(df, large)
            @test server.fail_data_requests == 0

            impatient = bq_connect("proj"; token = "t", endpoint = base, max_retries = 0)
            server.fail_data_requests = 1_000
            err = try
                bq_query(impatient, "SELECT large"; method = :rest, progress = false)
            catch e
                e
            end
            server.fail_data_requests = 0
            @test err isa BigQueryError && err.status == 503 && err.reason == "backendError"
        end

        @testset "slow queries are polled" begin
            add_query!(server, "SELECT slow"; table = "ds.small", polls = 3)
            reset_log!(server)
            check_table(bq_query(con, "SELECT slow"), small)
            polls = filter(r -> r.method == "GET" && occursin("/queries/job_", r.path), requests(server))
            @test length(polls) == 4          # three "not yet" answers, then the result
            @test all(r -> r.params["location"] == "US", polls)      # the job's location is carried along
        end

        @testset "errors" begin
            add_query!(server, "SELEC 1"; error = ("invalidQuery", "Syntax error: Unexpected identifier \"SELEC\" at [1:1]"))
            add_query!(server, "SELECT secret"; error = ("accessDenied", "Access Denied: Table proj:ds.secret"))
            err = try
                bq_query(con, "SELEC 1")
            catch e
                e
            end
            @test err isa BigQueryError && err.status == 400 && err.reason == "invalidQuery"
            @test occursin("Unexpected identifier", err.message)
            @test occursin("Unexpected identifier", sprint(showerror, err))
            err = try
                bq_query(con, "SELECT secret")
            catch e
                e
            end
            @test err isa BigQueryError && err.status == 403 && err.reason == "accessDenied"
            @test_throws ArgumentError bq_query(con, "SELECT small"; method = :fast)

            down = bq_connect("proj"; token = "t", endpoint = "http://127.0.0.1:9", max_retries = 0)
            @test_throws B.NetworkError bq_query(down, "SELECT small")
        end

        @testset "statements without rows" begin
            add_query!(server, "DELETE FROM t WHERE x"; dml = 17)
            df = bq_query(con, "DELETE FROM t WHERE x")
            @test size(df) == (0, 0)
            res = DBInterface.execute(con, "DELETE FROM t WHERE x")
            @test res.affected_rows == 17 && res.total_rows == 0 && res.bytes_processed == 12345
        end

        @testset "scripts: no destination table" begin
            add_query!(server, "BEGIN SELECT large; END"; table = "ds.large", script = true)
            reset_log!(server)
            df = bq_query(con, "BEGIN SELECT large; END"; progress = false)
            check_table(df, large)
            @test !any(r -> endswith(r.path, "/data"), requests(server))
            @test any(r -> haskey(r.params, "pageToken"), requests(server))
            @test_throws BigQueryError bq_query(con, "BEGIN SELECT large; END"; method = :arrow, progress = false)
        end

        @testset "request options" begin
            con2 = bq_connect("proj"; token = "t", endpoint = base, dataset = "other-proj.my_ds", location = "EU")
            reset_log!(server)
            bq_query(con2, "SELECT small"; max_bytes_billed = 10^9, use_cache = false, labels = Dict("team" => "data"))
            body = JSON.parse(first(requests(server)).body)
            @test body["location"] == "EU"
            @test body["defaultDataset"] == Dict("projectId" => "other-proj", "datasetId" => "my_ds")
            @test body["maximumBytesBilled"] == "1000000000" && body["useQueryCache"] === false
            @test body["labels"] == Dict("team" => "data")
            reset_log!(server)
            bq_query(con2, "SELECT small"; dataset = "plain_ds", location = "US")
            body = JSON.parse(first(requests(server)).body)
            @test body["defaultDataset"] == Dict("projectId" => "proj", "datasetId" => "plain_ds") && body["location"] == "US"
        end

        @testset "query parameters" begin
            reset_log!(server)
            bq_query(con, "SELECT small"; params = (
                n = 5, x = 1.5, s = "a'b", flag = true, day = Date(2024, 1, 31),
                ts = DateTime(2024, 1, 31, 12, 34, 56, 789), dt = bq_param(DateTime(2024, 1, 31, 1, 2, 3), "datetime"),
                ids = [1, 2, 3], nothing_here = bq_param(missing, "STRING"), none = bq_param(Int[], "ARRAY<INT64>")))
            body = JSON.parse(first(requests(server)).body)
            @test body["parameterMode"] == "NAMED"
            p = Dict(q["name"] => q for q in body["queryParameters"])
            scalar(name) = (p[name]["parameterType"]["type"], p[name]["parameterValue"]["value"])
            @test scalar("n") == ("INT64", "5")
            @test scalar("x") == ("FLOAT64", "1.5")
            @test scalar("s") == ("STRING", "a'b")
            @test scalar("flag") == ("BOOL", "true")
            @test scalar("day") == ("DATE", "2024-01-31")
            @test scalar("ts") == ("TIMESTAMP", "2024-01-31 12:34:56.789+00:00")
            @test scalar("dt") == ("DATETIME", "2024-01-31T01:02:03.000")
            @test scalar("nothing_here") == ("STRING", nothing)
            @test p["ids"]["parameterType"] == Dict("type" => "ARRAY", "arrayType" => Dict("type" => "INT64"))
            @test p["ids"]["parameterValue"]["arrayValues"] == [Dict("value" => "1"), Dict("value" => "2"), Dict("value" => "3")]
            @test p["none"]["parameterValue"]["arrayValues"] == []

            reset_log!(server)
            bq_query(con, "SELECT small"; params = [10, "x"])
            body = JSON.parse(first(requests(server)).body)
            @test body["parameterMode"] == "POSITIONAL"
            @test [q["parameterType"]["type"] for q in body["queryParameters"]] == ["INT64", "STRING"]
            @test !haskey(body["queryParameters"][1], "name")

            @test_throws ArgumentError bq_query(con, "SELECT small"; params = (x = missing,))
            @test_throws ArgumentError bq_query(con, "SELECT small"; params = (x = Int[],))
            @test_throws ArgumentError bq_query(con, "SELECT small"; params = (x = 1 + 2im,))
        end

        @testset "DBInterface" begin
            dbi = DBInterface.connect(BigQuery.Connection, "proj"; token = "t", endpoint = base)
            res = DBInterface.execute(dbi, "SELECT small")
            @test res isa BigQuery.QueryResult && Tables.istable(res)
            @test res.total_rows == 50 && res.method == :inline && startswith(res.job_id, "job_") && res.location == "US"
            check_table(DataFrame(res), small)
            @test Tables.columnnames(res) == collect(keys(small))
            @test Tables.getcolumn(res, :id) == small.id && Tables.getcolumn(res, 1) == small.id
            @test Tables.schema(res).types[1] == Int64
            @test length(Tables.rowtable(res)) == 50
            @test occursin("50 rows x 12 columns", sprint(show, res))

            reset_log!(server)
            DBInterface.execute(dbi, "SELECT small", (id = 7,))
            @test JSON.parse(first(requests(server)).body)["queryParameters"][1]["name"] == "id"
            reset_log!(server)
            DBInterface.execute(dbi, "SELECT small", [7])
            @test JSON.parse(first(requests(server)).body)["parameterMode"] == "POSITIONAL"
            stmt = DBInterface.prepare(dbi, "SELECT small")
            @test DBInterface.execute(stmt).total_rows == 50

            @test isopen(dbi)
            DBInterface.close!(dbi)
            @test !isopen(dbi)
            @test_throws ArgumentError DBInterface.execute(dbi, "SELECT small")
        end

        @testset "duplicate column names" begin
            schema = Dict("fields" => Any[Dict("name" => "a", "type" => "INTEGER"), Dict("name" => "a", "type" => "STRING")])
            add_table!(server, "ds.dup", schema, ["{\"f\":[{\"v\":\"1\"},{\"v\":\"x\"}]}"])
            add_query!(server, "SELECT dup"; table = "ds.dup")
            df = bq_query(con, "SELECT dup")
            @test names(df) == ["a", "a_1"] && df.a == [1] && df.a_1 == ["x"]
        end

        @testset "bq_read_table over REST" begin
            df = bq_read_table(con, "proj.ds.large"; method = :rest, progress = false)
            check_table(df, large)
            server.max_page_rows = 900
            df = bq_read_table(con, "`ds.large`"; method = :rest, progress = false, columns = ["name", "id"])
            server.max_page_rows = typemax(Int)
            @test names(df) == ["name", "id"]
            @test isequal(df.name, large.name) && df.id == large.id
            @test nrow(bq_read_table(con, "ds.empty"; method = :rest)) == 0
            @test_throws ArgumentError bq_read_table(con, "ds.large"; columns = ["nope"], method = :rest)
            @test_throws ArgumentError bq_read_table(con, "just_a_table"; method = :rest)
            err = try
                bq_read_table(con, "ds.missing"; method = :rest)
            catch e
                e
            end
            @test err isa BigQueryError && err.status == 404 && err.reason == "notFound"

            # A filter cannot be applied by tabledata.list, and views have no stored rows: both go through a query.
            add_query!(server, "SELECT `id`, `x` FROM `proj.ds.large` WHERE id <= 50"; table = "ds.small")
            reset_log!(server)
            df = bq_read_table(con, "ds.large"; method = :rest, columns = ["id", "x"], where = "id <= 50")
            @test nrow(df) == 50
            @test any(r -> r.method == "POST", requests(server))
            add_table!(server, "ds.a_view", SCHEMA, String[]; kind = "VIEW")
            add_query!(server, "SELECT * FROM `proj.ds.a_view`"; table = "ds.small")
            check_table(bq_read_table(con, "ds.a_view"; method = :rest), small)

            with_default = bq_connect("proj"; token = "t", endpoint = base, dataset = "ds")
            check_table(bq_read_table(with_default, "small"; method = :rest), small)
        end
    finally
        stop!(server)
    end
end
