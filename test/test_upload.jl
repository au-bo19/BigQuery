@testset "uploads: choosing BigQuery types" begin
    field(col) = B.infer_column("c", col)
    kind(col) = B.describe(field(col))
    @test kind([1, 2]) == "INT64"
    @test kind(UInt8[1, 2]) == "INT64"
    @test kind([true, false]) == "BOOL"
    @test kind(Union{Missing,Float32}[1.5f0, missing]) == "FLOAT64"
    @test kind(Union{Nothing,Int}[nothing, 1]) == "INT64"
    @test kind([big"1.5"]) == "BIGNUMERIC"
    @test kind(["a"]) == "STRING" && kind([:a]) == "STRING" && kind(['a']) == "STRING"
    @test kind([Date(2024)]) == "DATE" && kind([DateTime(2024)]) == "TIMESTAMP" && kind([Time(1)]) == "TIME"
    @test kind([UInt8[1]]) == "BYTES"
    @test kind([[1, 2]]) == "ARRAY<INT64>" && kind([[UInt8[1]]]) == "ARRAY<BYTES>"
    @test kind(1:3) == "INT64"
    st = field([(a = 1, b = ["x"], c = (d = 1.5,))])
    @test st.type == "RECORD" && !st.repeated
    @test [f.name => B.describe(f) for f in st.fields] == ["a" => "INT64", "b" => "ARRAY<STRING>", "c" => "RECORD"]
    @test kind([[(a = 1,)]]) == "ARRAY<RECORD>"

    # General element types: the values give the type.
    @test kind(Any[1, 2]) == "INT64"
    @test kind(Any[1, 2.5, missing]) == "FLOAT64" && kind(Real[2.5, 1]) == "FLOAT64"
    @test kind(Any["a", missing]) == "STRING"
    @test kind(Any[Any[], [1], Any[2, 3]]) == "ARRAY<INT64>"
    @test kind([missing, missing]) == "STRING" && kind(Any[]) == "STRING"
    @test_throws ArgumentError field(Any[1, "a"])
    @test_throws ArgumentError field(Any[[1], 1])
    @test_throws ArgumentError field([1 + 2im])
    @test_throws ArgumentError field([[[1]]])               # BigQuery has no arrays of arrays.
    @test_throws ArgumentError field([(a = 1, b = missing)])

    # Types from the user.
    f = B.field_from_spec("c", "datetime")
    @test f.type == "DATETIME" && !f.repeated && f.kind == B.K_DATETIME
    f = B.field_from_spec("c", " array< date > ")
    @test f.type == "DATE" && f.repeated
    @test B.field_from_spec("c", "json").isjson
    f = B.field_from_spec("c", Dict(:type => "numeric", :precision => 10, "scale" => 2))
    @test f.kind == B.K_NUMERIC
    @test B.schema_json(f) == Dict("name" => "c", "type" => "numeric", "precision" => 10, "scale" => 2)
    f = B.field_from_spec("c", Dict("type" => "RECORD", "mode" => "REPEATED",
                                    "fields" => [Dict("name" => "a", "type" => "INT64")]))
    @test f.repeated && f.kind == B.K_RECORD && f.fields[1].name == "a" && f.fields[1].kind == B.K_INT
    for bad in ("STRUCT<a INT64>", "NUMERIC(10, 2)", "ARRAY<ARRAY<INT64>>", "ARRAY", "", 5, Dict("mode" => "REPEATED"))
        @test_throws ArgumentError B.field_from_spec("c", bad)
    end

    names = ["a", "b"]
    cols = AbstractVector[[1], [DateTime(2024)]]
    @test [B.describe(f) for f in B.infer_fields(names, cols, nothing)] == ["INT64", "TIMESTAMP"]
    @test [B.describe(f) for f in B.infer_fields(names, cols, (b = "DATETIME",))] == ["INT64", "DATETIME"]
    @test [B.describe(f) for f in B.infer_fields(names, cols, Dict("a" => "NUMERIC"))] == ["NUMERIC", "TIMESTAMP"]
    @test_throws ArgumentError B.infer_fields(names, cols, (nope = "STRING",))
    @test_throws ArgumentError B.infer_fields(names, cols, ["a" => "STRING"])

    # The description of a new table uses the type names of the API.
    described = [B.schema_json(B.infer_column("c", col)) for col in ([1], [1.5], [true], [[(a = "x",)]])]
    @test [d["type"] for d in described] == ["INTEGER", "FLOAT", "BOOLEAN", "RECORD"]
    @test [d["mode"] for d in described] == ["NULLABLE", "NULLABLE", "NULLABLE", "REPEATED"]
    @test described[4]["fields"] == [Dict("name" => "a", "type" => "STRING", "mode" => "NULLABLE")]
end

@testset "uploads: encoding values" begin
    enc(type, v; repeated = false) = sprint(io -> B.write_value(io, B.WriteField("c", type, repeated), v))

    @test enc("INT64", 5) == "5" && enc("INT64", typemin(Int64)) == "-9223372036854775808"
    @test enc("INT64", true) == "1" && enc("INT64", 3.0) == "3" && enc("INT64", 0x05) == "5"
    @test_throws ArgumentError enc("INT64", 3.5)
    @test_throws ArgumentError enc("INT64", Date(2024))

    @test enc("FLOAT64", 1.5) == "1.5" && enc("FLOAT64", 1) == "1.0" && enc("FLOAT64", 1.5f0) == "1.5"
    @test enc("FLOAT64", 1.0e22) == "1.0e22" && enc("FLOAT64", -2.5e-9) == "-2.5e-9"
    @test enc("FLOAT64", NaN) == "\"NaN\"" && enc("FLOAT64", Inf) == "\"inf\"" && enc("FLOAT64", -Inf) == "\"-inf\""
    @test_throws ArgumentError enc("FLOAT64", true)

    @test enc("BOOL", true) == "true" && enc("BOOL", false) == "false"
    @test_throws ArgumentError enc("BOOL", 1)

    @test enc("STRING", "a\"b\\c\n\t\r\0\x1f é😀") == "\"a\\\"b\\\\c\\n\\t\\r\\u0000\\u001f é😀\""
    for s in ("", "plain", "with \"quotes\"", "tab\tand\nnewline", "ünïcödé", "emoji 😀", "back\\slash", "\x01\x7f", "\\", "\"")
        @test JSON.parse(enc("STRING", s)) == s
        @test JSON.parse(enc("STRING", chop("xx" * s * "yy"; head = 2, tail = 2))) == s
    end
    @test enc("STRING", :sym) == "\"sym\"" && enc("STRING", 'c') == "\"c\"" && enc("STRING", 12) == "\"12\""
    @test_throws ArgumentError enc("STRING", [1])

    @test enc("BYTES", UInt8[1, 2, 255]) == "\"AQL/\"" && enc("BYTES", UInt8[]) == "\"\""
    @test_throws ArgumentError enc("BYTES", "text")

    @test enc("TIMESTAMP", DateTime(2024, 1, 31, 12, 34, 56, 789)) == "\"2024-01-31T12:34:56.789Z\""
    @test enc("TIMESTAMP", Date(2024, 1, 31)) == "\"2024-01-31T00:00:00.000Z\""
    @test enc("DATETIME", DateTime(1, 2, 3, 4, 5, 6)) == "\"0001-02-03T04:05:06.000\""
    @test enc("DATE", Date(2024, 1, 31)) == "\"2024-01-31\""
    @test enc("TIME", Time(23, 59, 59, 999, 999, 999)) == "\"23:59:59.999999\"" && enc("TIME", Time(0)) == "\"00:00:00.000000\""
    @test_throws ArgumentError enc("DATE", DateTime(2024, 1, 31))     # This must not remove the time of day without an error.
    @test_throws ArgumentError enc("TIMESTAMP", DateTime(10_000))
    @test_throws ArgumentError enc("TIME", 5)

    @test enc("NUMERIC", 0.1) == "0.1" && enc("NUMERIC", 12) == "12" && enc("NUMERIC", -99.75) == "-99.75"
    @test enc("NUMERIC", 1.5e6) == "1500000" && enc("NUMERIC", 1.0e-7) == "0.00000010" && enc("NUMERIC", -1.2345e10) == "-12345000000"
    @test enc("NUMERIC", big"0.1") == "0.1" && enc("NUMERIC", 1 // 3) == "0.333333333" && enc("NUMERIC", big"-0.05") == "-0.05"
    # The values are rounded to the digits that the type keeps: 9 for NUMERIC, 38 for BIGNUMERIC.
    @test enc("NUMERIC", 0.1 + 0.2) == "0.3" && enc("NUMERIC", 3.0000000000000004e-7) == "0.0000003"
    @test enc("NUMERIC", 2 / 3) == "0.666666667" && enc("NUMERIC", -2 / 3) == "-0.666666667" && enc("NUMERIC", 1.0e-12) == "0"
    @test enc("NUMERIC", 1.0e25) == "1" * "0"^25 && enc("NUMERIC", 123456789.123456789) == "123456789.12345679"
    @test enc("BIGNUMERIC", 0.1 + 0.2) == "0.30000000000000004" && enc("BIGNUMERIC", 1.0e-40) == "0"
    @test enc("BIGNUMERIC", parse(BigFloat, "-12345678901234567890.123456789")) == "-12345678901234567890.123456789"
    @test enc("BIGNUMERIC", big"2.0") == "2" && enc("BIGNUMERIC", big"0.0") == "0"
    @test_throws ArgumentError enc("NUMERIC", NaN)
    @test_throws ArgumentError enc("NUMERIC", big"Inf")
    @test B.plain_decimal("1.2345e2") == "123.45" && B.plain_decimal("12.5") == "12.5" && B.plain_decimal("5.0e-324") == "0." * "0"^323 * "50"

    @test enc("JSON", "{\"a\": [1,\n 2]}") == "{\"a\":[1,2]}" && enc("JSON", Dict("a" => 1)) == "{\"a\":1}"
    @test enc("JSON", "\"text\"") == "\"text\"" && enc("JSON", [1, 2]) == "[1,2]"
    @test_throws ArgumentError enc("JSON", "{not json")
    # The text of a JSON document does not change, except the whitespace between the tokens.
    @test enc("JSON", "{\"price\": 0.12345678901234567890, \"id\": 1.50, \"n\": 1e400}") ==
          "{\"price\":0.12345678901234567890,\"id\":1.50,\"n\":1e400}"
    @test enc("JSON", " { \"s\" : \"a b\\t\\\"c\\\\\" ,\r\n \"k\": [ 1 , 2 ] } ") == "{\"s\":\"a b\\t\\\"c\\\\\",\"k\":[1,2]}"
    @test enc("JSON", "{\"u\": \"ünï 😀\"}") == "{\"u\":\"ünï 😀\"}"
    @test enc("GEOGRAPHY", "POINT(1 2)") == "\"POINT(1 2)\""
    @test enc("RANGE", "[2024-01-01, 2024-02-01)") == "\"[2024-01-01, 2024-02-01)\""
    @test_throws ArgumentError enc("RANGE", 5)

    # The text goes to BigQuery, and BigQuery parses it.
    @test enc("INT64", "42") == "\"42\"" && enc("NUMERIC", "123.456789012") == "\"123.456789012\""
    @test enc("TIMESTAMP", "2024-01-01 00:00:00.123456 UTC") == "\"2024-01-01 00:00:00.123456 UTC\""

    @test enc("INT64", missing) == "null" && enc("STRING", nothing) == "null" && enc("JSON", missing) == "null"
    @test enc("INT64", [1, 2]; repeated = true) == "[1,2]" && enc("STRING", String[]; repeated = true) == "[]"
    @test enc("INT64", missing; repeated = true) == "[]"
    @test enc("DATE", [Date(2024), Date(2025)]; repeated = true) == "[\"2024-01-01\",\"2025-01-01\"]"
    @test_throws ArgumentError enc("INT64", 1; repeated = true)

    record = B.WriteField("r", "RECORD", false, [B.WriteField("a", "INT64", false), B.WriteField("b", "STRING", true),
                                                 B.WriteField("c", "RECORD", false, [B.WriteField("d", "BOOL", false)])])
    rec(v) = sprint(io -> B.write_value(io, record, v))
    @test rec((a = 1, b = ["x", "y"], c = (d = true,))) == "{\"a\":1,\"b\":[\"x\",\"y\"],\"c\":{\"d\":true}}"
    @test rec((b = String[], a = missing)) == "{\"a\":null,\"b\":[],\"c\":null}"
    @test rec(Dict("a" => 2, :b => ["z"])) == "{\"a\":2,\"b\":[\"z\"],\"c\":null}"
    @test rec(missing) == "null"
    @test_throws ArgumentError rec(5)

    # Rows: the typed encoders (a tuple) and the encoders for wide tables (a vector) write the same text.
    fields = [B.WriteField("id", "INT64", false), B.WriteField("we\"ird", "STRING", false)]
    cols = AbstractVector[[1, 2], Union{Missing,String}["a", missing]]
    typed = B.row_encoders(fields, cols)
    @test typed isa Tuple
    for encoders in (typed, collect(Any, typed))
        io = IOBuffer()
        B.write_row(io, encoders, 1)
        B.write_row(io, encoders, 2)
        @test String(take!(io)) == "{\"id\":1,\"we\\\"ird\":\"a\"}\n{\"id\":2,\"we\\\"ird\":null}\n"
    end
end

@testset "uploads: gzip" begin
    data = Vector{UInt8}(join(("{\"id\":$i,\"name\":\"row $i\"}\n" for i in 1:50_000)))
    packed = B.gzip_bytes(data)
    @test is_gzip(packed) && length(packed) < length(data) ÷ 3
    @test gunzip(packed) == data                 # a different implementation reads it
    @test gunzip(B.gzip_bytes(UInt8[])) == UInt8[]

    # If the package writes the data in parts, as an upload does, the result is also one stream.
    gz = B.GzipWriter()
    out = IOBuffer()
    for a in 1:100_000:length(data)
        B.deflate!(out, gz, data[a:min(a + 99_999, end)])
    end
    B.deflate!(out, gz, UInt8[]; finish = true)
    close(gz)
    close(gz)                                    # a second close causes no problem
    @test gunzip(take!(out)) == data
    @test_throws ErrorException B.deflate!(out, gz, data)

    @test B.sample_rows(5) == 1:5 && B.sample_rows(0) == Int[]
    sample = B.sample_rows(1_000_000)
    @test length(sample) == B.GZIP_SAMPLE_ROWS && first(sample) == 1 && last(sample) == 1_000_000 && issorted(sample)
end

@testset "uploads (mock BigQuery)" begin
    server = MockBQ.Server()
    base = MockBQ.url(server)
    con = bq_connect("proj"; token = "t", endpoint = base, max_retries = 5)
    small = truth_table(50)
    read_back(name) = bq_read_table(con, name; method = :rest, progress = false)
    upload_requests() = filter(r -> startswith(r.path, "/upload/"), requests(server))
    load_config() = JSON.parse(first(upload_requests()).body)["configuration"]["load"]
    last_upload() = server.uploads["upload_$(length(server.uploads))"]
    chunk_bytes = B.UPLOAD_CHUNK_BYTES[]
    gzip_limit = B.GZIP_LIMIT_BYTES[]
    try
        @testset "a new table" begin
            reset_log!(server)
            res = bq_write_table(con, "ds.up", DataFrame(small); progress = false)
            @test res.table == "proj.ds.up" && res.rows == 50 && startswith(res.job_id, "bqjl_")
            log = requests(server)
            check_table(read_back("ds.up"), small)

            @test length(log) == 3           # find the table, open the upload, send the rows
            @test log[1].method == "GET" && log[1].path == "/bigquery/v2/projects/proj/datasets/ds/tables/up"
            @test log[2].method == "POST" && log[2].path == "/upload/bigquery/v2/projects/proj/jobs"
            @test log[2].params["uploadType"] == "resumable"
            job = JSON.parse(log[2].body)
            @test job["jobReference"] == Dict("projectId" => "proj", "jobId" => res.job_id)
            load = job["configuration"]["load"]
            @test load["destinationTable"] == Dict("projectId" => "proj", "datasetId" => "ds", "tableId" => "up")
            @test load["sourceFormat"] == "NEWLINE_DELIMITED_JSON"
            @test load["writeDisposition"] == "WRITE_EMPTY" && load["createDisposition"] == "CREATE_IF_NEEDED"
            sent = load["schema"]["fields"]
            @test [f["name"] for f in sent] == [string(k) for k in keys(small)]
            @test [f["type"] for f in sent] == ["INTEGER", "FLOAT", "STRING", "BOOLEAN", "TIMESTAMP", "DATE",
                                                "TIMESTAMP", "TIME", "FLOAT", "BYTES", "STRING", "RECORD"]
            @test [f["mode"] for f in sent] == [f["name"] == "tags" ? "REPEATED" : "NULLABLE" for f in sent]
            @test [(f["name"], f["type"]) for f in sent[end]["fields"]] == [("a", "INTEGER"), ("b", "STRING")]
            @test log[3].method == "PUT" && log[3].params["upload_id"] == "upload_1" && log[3].auth == "Bearer t"
            @test log[3].range == "bytes 0-$(ncodeunits(log[3].body) - 1)/$(ncodeunits(log[3].body))"
            @test is_gzip(log[3].body)                       # compressed, if the caller does not ask for a different behaviour
            text = String(gunzip(log[3].body))
            @test count(==('\n'), text) == 50 && endswith(text, "}\n")
            @test ncodeunits(log[3].body) < ncodeunits(text) ÷ 2

            reset_log!(server)
            bq_write_table(con, "ds.up", small; if_exists = :replace, compress = false)
            @test last(requests(server)).body == text
            check_table(read_back("ds.up"), small)
        end

        @testset "types for a new table" begin
            reset_log!(server)
            bq_write_table(con, "ds.typed", small; progress = false,
                           types = (dt = "DATETIME", num = Dict("type" => "NUMERIC", "precision" => 20, "scale" => 4)))
            sent = load_config()["schema"]["fields"]
            @test [f["type"] for f in sent] == [f["type"] for f in SCHEMA["fields"]]
            @test sent[9] == Dict("name" => "num", "type" => "NUMERIC", "precision" => 20, "scale" => 4)
            check_table(read_back("ds.typed"), small)
            @test_throws ArgumentError bq_write_table(con, "ds.typed2", small; types = (nope = "STRING",))
        end

        @testset "an existing table" begin
            add_table!(server, "ds.target", SCHEMA, rest_rows(small))
            reset_log!(server)
            err = try
                bq_write_table(con, "ds.target", small)
            catch e
                e
            end
            @test err isa BigQueryError && err.reason == "duplicate" && occursin("if_exists = :append", err.message)
            @test isempty(upload_requests())

            # Append: the schema of the table (DATETIME, NUMERIC) controls how the package writes the values.
            more = truth_table(80)
            res = bq_write_table(con, "ds.target", more; if_exists = :append, progress = false)
            @test res.rows == 80
            @test load_config()["writeDisposition"] == "WRITE_APPEND" && !haskey(load_config(), "schema")
            df = read_back("ds.target")
            @test nrow(df) == 130
            check_table(df[1:50, :], small)
            check_table(df[51:130, :], more)

            # The package matches the columns by name, in all sequences and letter cases. The other columns stay NULL.
            reset_log!(server)
            bq_write_table(con, "ds.target", (NAME = ["only"], id = [999]); if_exists = :append)
            @test String(gunzip(last(requests(server)).body)) == "{\"name\":\"only\",\"id\":999}\n"
            df = read_back("ds.target")
            @test nrow(df) == 131 && df.id[end] == 999 && df.name[end] == "only"
            @test ismissing(df.flag[end]) && ismissing(df.st[end]) && df.tags[end] == String[]

            reset_log!(server)
            err = try
                bq_write_table(con, "ds.target", (id = [1], nope = [2], Nada = [3]); if_exists = :append)
            catch e
                e
            end
            @test err isa ArgumentError && occursin("`nope`, `Nada`", err.msg) && occursin("proj.ds.target", err.msg)
            @test isempty(upload_requests())

            # A value that the column cannot accept: BigQuery loads nothing, and the error shows which row and why.
            err = try
                bq_write_table(con, "ds.target", (id = ["12", "abc"],); if_exists = :append)
            catch e
                e
            end
            @test err isa BigQueryError && err.reason == "invalid" && err.status == 0
            @test occursin("too many errors", err.message) && occursin("row starting at position 12", err.message)
            @test occursin("Could not convert value", err.message)
            @test nrow(read_back("ds.target")) == 131
            @test_throws ArgumentError bq_write_table(con, "ds.target", (flag = [1, 0],); if_exists = :append)

            # A replace does not examine the current table.
            reset_log!(server)
            res = bq_write_table(con, "ds.target", (a = [1, 2, 3], b = ["x", missing, "z"]); if_exists = :replace)
            @test res.rows == 3 && !any(r -> r.method == "GET", requests(server))
            @test load_config()["writeDisposition"] == "WRITE_TRUNCATE"
            @test [f["name"] for f in load_config()["schema"]["fields"]] == ["a", "b"]
            df = read_back("ds.target")
            @test names(df) == ["a", "b"] && df.a == [1, 2, 3] && isequal(df.b, ["x", missing, "z"])
        end

        @testset "a dataset that does not exist" begin
            # The upload stops before it sends rows, because the load job can only fail.
            push!(server.missing_datasets, "no_such_ds")
            reset_log!(server)
            err = try
                bq_write_table(con, "no_such_ds.t", small; progress = false)
            catch e
                e
            end
            @test err isa BigQueryError && err.status == 404 && occursin("Dataset", err.message)
            @test occursin("create the dataset first", err.message)
            @test isempty(upload_requests())
            delete!(server.missing_datasets, "no_such_ds")
        end

        @testset "large uploads go in chunks" begin
            large = truth_table(25_000)
            B.UPLOAD_CHUNK_BYTES[] = 512 * 1024
            reset_log!(server)
            res = bq_write_table(con, "ds.big", large; progress = false, compress = false)
            @test res.rows == 25_000
            puts = filter(r -> r.method == "PUT", requests(server))
            sizes = [ncodeunits(r.body) for r in puts]
            @test length(puts) > 3 && all(==(512 * 1024), sizes[1:end-1]) && 0 < sizes[end] <= 512 * 1024
            starts = cumsum(sizes) .- sizes
            @test [r.range for r in puts[1:end-1]] == ["bytes $a-$(a + 512 * 1024 - 1)/*" for a in starts[1:end-1]]
            @test puts[end].range == "bytes $(starts[end])-$(sum(sizes) - 1)/$(sum(sizes))"
            check_table(read_back("ds.big"), large)

            # The server keeps only a part of the bytes of each request.
            server.upload_accept_bytes = 256 * 1024
            reset_log!(server)
            bq_write_table(con, "ds.big", large; if_exists = :replace, progress = false, compress = false)
            server.upload_accept_bytes = typemax(Int)
            @test count(r -> r.method == "PUT", requests(server)) >= 2 * (length(puts) - 1)
            check_table(read_back("ds.big"), large)

            # Requests fail before the server keeps bytes ...
            server.fail_upload_requests = 2
            bq_write_table(con, "ds.big", large; if_exists = :replace, progress = false, compress = false)
            @test server.fail_upload_requests == 0
            check_table(read_back("ds.big"), large)

            # ... or after: the bytes arrived, but the response did not. The package must not send bytes two times.
            server.lose_upload_responses = 2
            bq_write_table(con, "ds.big", large; if_exists = :replace, progress = false, compress = false)
            @test server.lose_upload_responses == 0
            check_table(read_back("ds.big"), large)

            # With compression, the same table uses fewer chunks. The chunk limits can be at all positions in the gzip stream.
            B.UPLOAD_CHUNK_BYTES[] = 256 * 1024
            reset_log!(server)
            bq_write_table(con, "ds.big", large; if_exists = :replace, progress = false)
            packed = filter(r -> r.method == "PUT", requests(server))
            @test 3 <= length(packed) < length(puts)
            @test all(r -> ncodeunits(r.body) == 256 * 1024, packed[1:end-1]) && is_gzip(packed[1].body)
            @test last_upload()[:gzip] && length(last_upload()[:data]) == sum(ncodeunits(r.body) for r in packed)
            check_table(read_back("ds.big"), large)
            B.UPLOAD_CHUNK_BYTES[] = 512 * 1024
            server.upload_accept_bytes = 256 * 1024
            server.lose_upload_responses = 2
            bq_write_table(con, "ds.big", large; if_exists = :replace, progress = false)
            server.upload_accept_bytes = typemax(Int)
            @test server.lose_upload_responses == 0 && last_upload()[:gzip]
            check_table(read_back("ds.big"), large)

            # Do the same test for the request that completes the upload and starts the job.
            B.UPLOAD_CHUNK_BYTES[] = chunk_bytes
            server.lose_upload_responses = 1
            res = bq_write_table(con, "ds.once", small; progress = false)
            @test res.rows == 50
            check_table(read_back("ds.once"), small)

            impatient = bq_connect("proj"; token = "t", endpoint = base, max_retries = 0)
            server.fail_upload_requests = 1
            err = try
                bq_write_table(impatient, "ds.never", small; progress = false)
            catch e
                e
            end
            server.fail_upload_requests = 0
            @test err isa BigQueryError && err.status == 503
            @test_throws BigQueryError read_back("ds.never")
        end

        @testset "no rows" begin
            reset_log!(server)
            res = bq_write_table(con, "ds.none", (a = Int[], b = String[]))
            @test res.rows == 0
            put = last(requests(server))
            @test put.method == "PUT" && put.range == "bytes */0" && isempty(put.body)      # also no gzip header
            df = read_back("ds.none")
            @test size(df) == (0, 2) && eltype(df.a) == Int64 && eltype(df.b) == String
        end

        @testset "tables too large to upload compressed" begin
            # BigQuery accepts a gzip file of 4 GB maximum. For this test, the limit is much lower.
            B.GZIP_LIMIT_BYTES[] = server.max_gzip_bytes = 1_000
            reset_log!(server)
            res = @test_logs min_level = Base.CoreLogging.Warn bq_write_table(con, "ds.limit", small; progress = false)
            @test res.rows == 50 && !last_upload()[:gzip]
            @test count(r -> r.method == "POST", upload_requests()) == 1     # The sample gives this result before the upload.
            check_table(read_back("ds.limit"), small)

            # The sample can give an incorrect estimate. Here, each row in the sample becomes
            # almost nothing after compression, and the other rows become only a little smaller.
            # The package starts the upload again without compression. This occurs during
            # the upload (the larger table) or at the end.
            for (n, limit) in ((3_000, 100_000), (40_000, 1_000_000))
                sampled = Set(B.sample_rows(n))
                text = [i in sampled ? "a"^200 : String(rand(UInt8('a'):UInt8('z'), 200)) for i in 1:n]
                B.GZIP_LIMIT_BYTES[] = server.max_gzip_bytes = limit
                B.UPLOAD_CHUNK_BYTES[] = 256 * 1024
                before = length(server.uploads)
                reset_log!(server)
                res = @test_logs (:warn, r"4 GB.*without compression") bq_write_table(
                    con, "ds.limit", (id = 1:n, s = text); if_exists = :replace, progress = false)
                @test res.rows == n && length(server.uploads) == before + 2
                abandoned = server.uploads["upload_$(before + 1)"]
                @test abandoned[:done] === nothing && length(abandoned[:data]) <= limit
                @test !last_upload()[:gzip]
                df = read_back("ds.limit")
                @test df.id == 1:n && df.s == text
            end
            B.GZIP_LIMIT_BYTES[] = server.max_gzip_bytes = gzip_limit
            B.UPLOAD_CHUNK_BYTES[] = chunk_bytes
        end

        @testset "slow load jobs are waited for" begin
            server.load_polls = 2
            reset_log!(server)
            res = bq_write_table(con, "ds.slow", (a = [1, 2],))
            server.load_polls = 0
            @test res.rows == 2
            polls = filter(r -> r.method == "GET" && occursin("/jobs/", r.path), requests(server))
            @test length(polls) == 3                 # two "running" responses, then the result
            @test all(r -> r.path == "/bigquery/v2/projects/proj/jobs/$(res.job_id)", polls)
            @test all(r -> r.params["location"] == "US", polls)      # each poll sends the location of the job
        end

        @testset "request options and other tables" begin
            con2 = bq_connect("proj"; token = "t", endpoint = base, dataset = "ds", location = "EU")
            reset_log!(server)
            res = bq_write_table(con2, "opts", [(a = 1, b = "x"), (a = 2, b = "y")]; labels = Dict("team" => "data"))
            @test res.table == "proj.ds.opts" && res.rows == 2
            job = JSON.parse(first(upload_requests()).body)
            @test job["jobReference"]["location"] == "EU" && job["configuration"]["labels"] == Dict("team" => "data")
            @test job["configuration"]["load"]["destinationTable"]["datasetId"] == "ds"
            reset_log!(server)
            bq_write_table(con2, "other-proj.ds2.opts", (a = [1],); location = "US")
            job = JSON.parse(first(upload_requests()).body)
            @test job["jobReference"] == Dict("projectId" => "proj", "jobId" => job["jobReference"]["jobId"], "location" => "US")
            @test job["configuration"]["load"]["destinationTable"]["projectId"] == "other-proj"

            # More columns than the typed tuple encoders accept.
            wide = NamedTuple{Tuple(Symbol("c", i) for i in 1:40)}(Tuple([i, -i] for i in 1:40))
            bq_write_table(con, "ds.wide", wide)
            df = read_back("ds.wide")
            @test size(df) == (2, 40) && df.c1 == [1, -1] && df.c40 == [40, -40]

            # Columns that do not have a concrete element type.
            loose = DataFrame(n = Any[1, 2.5, missing], s = Any["a", missing, "c"], v = Any[[1], Any[], [2, 3]])
            bq_write_table(con, "ds.loose", loose)
            df = read_back("ds.loose")
            @test isequal(df.n, [1.0, 2.5, missing]) && isequal(df.s, ["a", missing, "c"]) && df.v == [[1], Int[], [2, 3]]
        end

        @testset "arguments" begin
            reset_log!(server)
            @test_throws ArgumentError bq_write_table(con, "ds.x", small; if_exists = :overwrite)
            @test_throws ArgumentError bq_write_table(con, "ds.x", 5)
            @test_throws ArgumentError bq_write_table(con, "ds.x", NamedTuple())
            @test_throws ArgumentError bq_write_table(con, "just_a_table", small)
            @test_throws ArgumentError bq_write_table(con, "ds.x", (a = [1 + 2im],))
            @test isempty(upload_requests())
            closed = bq_connect("proj"; token = "t", endpoint = base)
            close(closed)
            @test_throws ArgumentError bq_write_table(closed, "ds.x", small)
        end
    finally
        B.UPLOAD_CHUNK_BYTES[] = chunk_bytes
        B.GZIP_LIMIT_BYTES[] = gzip_limit
        stop!(server)
    end
end
