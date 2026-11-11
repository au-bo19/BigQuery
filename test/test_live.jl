# Tests against the real services. Off by default.
#
#   BIGQUERY_NETWORK_TESTS=1   reach Google's endpoints without credentials and
#                              check that they answer the way the client expects.
#   BIGQUERY_TEST_PROJECT=...  run real queries, billed to that project (a few
#                              MB at most), using your default credentials.

if get(ENV, "BIGQUERY_NETWORK_TESTS", "") != ""
    @testset "live endpoints, no credentials" begin
        # An existing project: requests naming a made-up one are rejected before authentication.
        con = bq_connect("bigquery-public-data"; token = "not-a-real-token", max_retries = 0)
        err = try
            bq_query(con, "SELECT 1")
        catch e
            e
        end
        @test err isa BigQueryError && err.status == 401
        # gRPC over HTTP/2 through this machine's network stack, status read from trailers.
        err = try
            B.create_read_session(con, B.TableRef("bigquery-public-data", "usa_names", "usa_1910_2013");
                                  max_streams = 1, compression = :zstd)
        catch e
            e
        end
        @test err isa B.GrpcError && err.code == 16
    end
end

if get(ENV, "BIGQUERY_TEST_PROJECT", "") != ""
    @testset "live BigQuery" begin
        con = bq_connect(ENV["BIGQUERY_TEST_PROJECT"])
        @info "running live tests" con

        df = bq_query(con, "SELECT 1 AS x, 'a' AS s")
        @test df.x == [1] && df.s == ["a"]

        types_sql(n) = """
            SELECT
              id,
              IF(MOD(id, 7) = 0, NULL, id / 4) AS x,
              IF(MOD(id, 11) = 0, NULL, CONCAT('näme "', CAST(id AS STRING), '"\\n')) AS name,
              MOD(id, 2) = 1 AS flag,
              TIMESTAMP_ADD(TIMESTAMP '2020-01-01 00:00:00.123456+00', INTERVAL id SECOND) AS ts,
              DATE_ADD(DATE '2000-01-01', INTERVAL MOD(id, 9000) DAY) AS d,
              DATETIME_ADD(DATETIME '1999-12-31 23:59:59.987654', INTERVAL id MINUTE) AS dt,
              TIME_ADD(TIME '00:00:00.5', INTERVAL MOD(id, 80000) SECOND) AS t,
              CAST(id AS NUMERIC) / 8 AS num,
              CAST(id AS BIGNUMERIC) * 2 AS bignum,
              CAST(CONCAT('b', CAST(id AS STRING)) AS BYTES) AS by,
              GENERATE_ARRAY(1, MOD(id, 3)) AS arr,
              IF(MOD(id, 5) = 0, NULL, STRUCT(id AS a, IF(MOD(id, 2) = 0, NULL, 'b') AS b, ['p', 'q'] AS c)) AS st,
              [STRUCT(id AS k, 1.5 AS v)] AS sts,
              ST_GEOGPOINT(1, 2) AS geo,
              JSON '{"a": 1}' AS js,
              CAST(NULL AS INT64) AS nothing_here
            FROM UNNEST(GENERATE_ARRAY(1, $n)) AS id
            ORDER BY id"""

        small = bq_query(con, types_sql(100))
        @test nrow(small) == 100 && small.id == 1:100
        @test small.x[1] == 0.25 && ismissing(small.x[7])
        @test small.name[1] == "näme \"1\"\n" && ismissing(small.name[11])
        @test small.ts[1] == DateTime(2020, 1, 1, 0, 0, 1, 123)
        @test small.d[1] == Date(2000, 1, 2)
        @test small.dt[1] == DateTime(2000, 1, 1, 0, 0, 59, 987)
        @test small.t[1] == Time(0, 0, 1, 500)
        @test small.num[1] == 0.125 && small.bignum[1] == 2
        @test small.by[1] == Vector{UInt8}("b1")
        @test small.arr[1] == [1] && small.arr[3] == Int[]
        @test small.st[1].a == 1 && small.st[1].b == "b" && small.st[1].c == ["p", "q"] && ismissing(small.st[5])
        @test small.sts[1][1].k == 1 && small.sts[1][1].v == 1.5
        @test small.geo[1] == "POINT(1 2)"
        @test all(ismissing, small.nothing_here)

        # A result big enough for the bulk paths; both must agree exactly.
        n = 300_000
        rest = B.run_query(con, types_sql(n); method = :rest)
        @test rest.method == :rest && rest.total_rows == n
        arrow = B.run_query(con, types_sql(n); method = :arrow)
        @test arrow.method == :arrow && arrow.total_rows == n
        df_rest, df_arrow = DataFrame(rest), DataFrame(arrow)
        @test df_rest.id == 1:n && df_arrow.id == 1:n            # ORDER BY is preserved
        for name in names(df_rest)
            @test typeof(df_rest[!, name]) == typeof(df_arrow[!, name])
            @test isequal(df_rest[!, name], df_arrow[!, name])
        end
        @test isequal(df_rest[1:100, :], small)

        auto = B.run_query(con, replace(types_sql(n), "ORDER BY id" => ""))     # parallel streams
        @test auto.total_rows == n && sort(DataFrame(auto).id) == 1:n
        @info "bulk download used" method = auto.method

        public = bq_read_table(con, "bigquery-public-data.usa_names.usa_1910_current";
                               columns = ["name", "number"], where = "state = 'RI' AND year = 2000")
        @test nrow(public) > 100 && names(public) == ["name", "number"] && eltype(public.number) == Int64

        @test_throws BigQueryError bq_query(con, "SELEC 1")
        @test bq_query(con, "SELECT @a + 1 AS b, @names AS names"; params = (a = 41, names = ["x", "y"])).b == [42]
    end

    # Uploads need somewhere to write: a dataset you can create tables in. One
    # table is created there and dropped again.
    if get(ENV, "BIGQUERY_TEST_DATASET", "") == ""
        @info "live upload tests skipped: set BIGQUERY_TEST_DATASET to a dataset you can create tables in"
    else
        @testset "live BigQuery uploads" begin
            con = bq_connect(ENV["BIGQUERY_TEST_PROJECT"])
            table = ENV["BIGQUERY_TEST_DATASET"] * ".bqjl_upload_test_" * string(rand(100_000:999_999))
            sql_name = "`" * replace(string(B.parse_table(con, table)), "`" => "") * "`"
            n = 2_000
            truth = truth_table(n)
            extra = (
                odd = Float64[i % 4 == 0 ? NaN : i % 4 == 1 ? Inf : i % 4 == 2 ? -Inf : i * 1.0e300 for i in 1:n],
                money = Float64[i % 2 == 0 ? i * 1.5e6 : i * 1.0e-7 for i in 1:n],       # printed with an exponent
                bignum = BigFloat[parse(BigFloat, "$(i)234567890123456789.$(i)23456789") for i in 1:n],
                exact = String["$i.123456789" for i in 1:n],
                js = Union{Missing,String}[i % 3 == 0 ? missing : "{\"a\": $i, \"b\": [\"x\", null]}" for i in 1:n],
                geo = String["POINT($(i % 90) 2)" for i in 1:n],
                nothing_here = fill(missing, n),
            )
            data = merge(truth, extra)
            try
                res = bq_write_table(con, table, data; types = (dt = "DATETIME", num = "NUMERIC", money = "NUMERIC",
                                                                 exact = "NUMERIC", js = "JSON", geo = "GEOGRAPHY"))
                @info "uploaded" res
                @test res.rows == n
                schema = bq_query(con, "SELECT * FROM $sql_name LIMIT 0")
                @test names(schema) == [string(k) for k in keys(data)]

                back = bq_query(con, "SELECT * FROM $sql_name ORDER BY id")
                @test nrow(back) == n
                for name in keys(truth)                 # every type comes back as it went in
                    @test isequal(back[!, name], narrow(truth[name]))
                end
                @test isequal(back.odd, extra.odd)
                @test back.money ≈ extra.money
                @test back.bignum == extra.bignum
                @test back.exact ≈ [parse(Float64, s) for s in extra.exact]
                @test ismissing(back.js[3]) && JSON.parse(back.js[1]) == Dict("a" => 1, "b" => ["x", nothing])
                @test back.geo[1] == "POINT(1 2)"
                @test all(ismissing, back.nothing_here)
                kinds = bq_query(con, """
                    SELECT column_name, data_type FROM `$(replace(ENV["BIGQUERY_TEST_DATASET"], "`" => ""))`.INFORMATION_SCHEMA.COLUMNS
                    WHERE table_name = @t"""; params = (t = last(split(table, '.')),))
                kind = Dict(zip(kinds.column_name, kinds.data_type))
                @test kind["dt"] == "DATETIME" && kind["ts"] == "TIMESTAMP" && kind["num"] == "NUMERIC"
                @test kind["bignum"] == "BIGNUMERIC" && kind["js"] == "JSON" && kind["tags"] == "ARRAY<STRING>"
                @test kind["exact"] == "NUMERIC" && kind["by"] == "BYTES" && startswith(kind["st"], "STRUCT<")

                # What was read can be appended as it is: the table's schema decides how it is written.
                @test_throws BigQueryError bq_write_table(con, table, back)
                @test bq_write_table(con, table, back; if_exists = :append).rows == n
                @test bq_query(con, "SELECT COUNT(*) AS c, COUNT(DISTINCT id) AS d FROM $sql_name") ==
                      DataFrame(c = [2n], d = [n])
                twice = bq_query(con, "SELECT * FROM $sql_name WHERE id <= 5 ORDER BY id")
                @test isequal(twice[1:2:end, :], twice[2:2:end, :])

                # A row BigQuery rejects: nothing is added, and the error says why.
                err = try
                    bq_write_table(con, table, (id = ["1", "not a number"],); if_exists = :append)
                catch e
                    e
                end
                @test err isa BigQueryError
                @info "a rejected upload reads" err
                @test bq_query(con, "SELECT COUNT(*) AS c FROM $sql_name").c == [2n]

                # Uploads are gzipped unless asked otherwise; this one is not.
                @test bq_write_table(con, table, (a = [1, 2, 3], b = ["x", missing, "z"]); if_exists = :replace,
                                     compress = false).rows == 3
                small = bq_query(con, "SELECT * FROM $sql_name ORDER BY a")
                @test names(small) == ["a", "b"] && small.a == [1, 2, 3] && isequal(small.b, ["x", missing, "z"])

                @test bq_write_table(con, table, (a = Int[], b = String[]); if_exists = :replace).rows == 0
                @test size(bq_read_table(con, table)) == (0, 2)

                # Enough rows for the upload to take several requests, compressed and not.
                m = 600_000                             # about 19 MB of JSON, a few MB gzipped
                big = (id = collect(1:m), s = [string("row ", i) for i in 1:m])
                chunk = B.UPLOAD_CHUNK_BYTES[]
                try
                    for (compress, chunk_bytes) in ((true, 1024 * 1024), (false, 4 * 1024 * 1024))
                        B.UPLOAD_CHUNK_BYTES[] = chunk_bytes
                        seconds = @elapsed res = bq_write_table(con, table, big; if_exists = :replace, compress = compress)
                        @info "uploaded 600,000 rows" compress seconds
                        @test res.rows == m
                        sums = bq_query(con, "SELECT COUNT(*) AS c, SUM(id) AS total, COUNTIF(s = CONCAT('row ', CAST(id AS STRING))) AS ok FROM $sql_name")
                        @test sums.c == [m] && sums.total == [sum(1:m)] && sums.ok == [m]
                    end
                finally
                    B.UPLOAD_CHUNK_BYTES[] = chunk
                end
            finally
                bq_query(con, "DROP TABLE IF EXISTS $sql_name")
            end
        end
    end
end
