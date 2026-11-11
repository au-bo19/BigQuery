@testset "JSON scanning" begin
    # String unescaping must agree with a real JSON parser.
    samples = ["", "plain", "quote \" backslash \\ slash /", "ctrl \b\f\n\r\t \x01\x1f", "ünïcödé €", "😀 astral 𝄞",
               "ends with backslash \\", "\\\"", "{\"f\":[{\"v\":\"x\"}]}", "a,b]}"]
    for str in samples
        encoded = JSON.json(str)
        a, b, esc, next = B.scan_string(encoded, 1, ncodeunits(encoded))
        @test B.make_string(encoded, a, b, esc) == str
        @test next == ncodeunits(encoded) + 1
    end
    # \u escapes, including a surrogate pair and a lone surrogate.
    for (encoded, expected) in [("\"\\u0041\\u00e9\"", "Aé"), ("\"\\ud83d\\ude00!\"", "😀!"),
                                ("\"\\ud83d x\"", "� x"), ("\"\\/\"", "/")]
        a, b, esc, _ = B.scan_string(encoded, 1, ncodeunits(encoded))
        @test B.make_string(encoded, a, b, esc) == expected
    end

    doc = """ { "a" : {"x":[1,2,{"y":"}]"}]}, "rows":[ {"f":[]} ],"s":"q\\"}", "n": -1.5e3 ,"t":true,"z":null } """
    spans = B.top_level_spans(doc)
    @test sort(collect(keys(spans))) == ["a", "n", "rows", "s", "t", "z"]
    @test doc[spans["rows"]] == "[ {\"f\":[]} ]"
    @test JSON.parse(doc[spans["a"]])["x"][3]["y"] == "}]"
    @test doc[spans["n"]] == "-1.5e3"
    @test isempty(B.top_level_spans("{}"))
    @test_throws BigQueryError B.top_level_spans("[1,2]")
    @test_throws BigQueryError B.top_level_spans("{\"a\": \"unterminated")

    s, meta, range = B.split_response(Vector{UInt8}(doc))
    @test meta["s"] == "q\"}" && meta["n"] == -1500 && meta["t"] === true && meta["z"] === nothing
    @test !haskey(meta, "rows") && s[range] == "[ {\"f\":[]} ]"
end

@testset "scalar parsing" begin
    int(str) = B.parse_int(str, 1, ncodeunits(str))
    @test int("0") == 0 && int("-7") == -7 && int("123456789012") == 123456789012
    @test int(string(typemax(Int64))) == typemax(Int64)
    @test int(string(typemin(Int64))) == typemin(Int64)
    @test_throws BigQueryError int("12a")
    @test_throws BigQueryError int("-")

    float(str) = B.parse_float(str, 1, ncodeunits(str))
    @test float("1.5") == 1.5 && float("-2.5E-3") == -0.0025
    @test isnan(float("NaN")) && float("Infinity") == Inf && float("-Infinity") == -Inf

    ts(str) = B.parse_timestamp(str, 1, ncodeunits(str))
    @test ts("0") == DateTime(1970)
    @test ts("1700000000123456") == DateTime(2023, 11, 14, 22, 13, 20, 123)
    @test ts("-1") == DateTime(1969, 12, 31, 23, 59, 59, 999)        # floors, never rounds up
    @test ts("1.700000000123E9") == DateTime(2023, 11, 14, 22, 13, 20, 123)   # legacy float seconds

    date(str) = B.parse_date(str, 1, ncodeunits(str))
    @test date("2024-02-29") == Date(2024, 2, 29) && date("0001-01-01") == Date(1, 1, 1)
    @test_throws BigQueryError date("2024-2-29")

    time(str) = B.parse_time(str, 1, ncodeunits(str))
    @test time("00:00:00") == Time(0)
    @test time("23:59:59.999999") == Time(23, 59, 59, 999, 999)
    @test time("12:34:56.5") == Time(12, 34, 56, 500)

    datetime(str) = B.parse_datetime(str, 1, ncodeunits(str))
    @test datetime("2024-01-31T12:34:56") == DateTime(2024, 1, 31, 12, 34, 56)
    @test datetime("2024-01-31T12:34:56.789123") == DateTime(2024, 1, 31, 12, 34, 56, 789)
end

@testset "REST rows -> columns" begin
    fields = B.parse_schema(SCHEMA)
    truth = truth_table(300)
    rows = rest_rows(truth)
    body = "[\n" * join(rows, ",\n") * "\n]"
    cols = B.new_columns(fields)
    @test B.decode_rows!(cols, fields, body, 1, ncodeunits(body)) == 300
    check_table(B.to_dataframe(B.make_result(fields, cols)), truth)

    # Appending a second page continues the same columns.
    @test B.decode_rows!(cols, fields, body, 1, ncodeunits(body)) == 300
    @test length(cols[1]) == 600

    empty = B.new_columns(fields)
    @test B.decode_rows!(empty, fields, " [ ] ", 1, 5) == 0
    df = B.to_dataframe(B.make_result(fields, empty))
    @test nrow(df) == 0 && eltype(df.id) == Int64 && eltype(df.name) == String && eltype(df.tags) == Vector{String}

    # A schema/row mismatch is reported, not silently mis-read.
    @test_throws BigQueryError B.decode_rows!(B.new_columns(fields), fields, "[{\"f\":[{\"v\":\"1\"}]}]", 1, 19)
end

@testset "other types over REST" begin
    schema = JSON.parse("""{"fields":[
        {"name":"big","type":"BIGNUMERIC"},{"name":"geo","type":"GEOGRAPHY"},{"name":"js","type":"JSON"},
        {"name":"iv","type":"INTERVAL"},{"name":"rng","type":"RANGE"},{"name":"req","type":"INT64","mode":"REQUIRED"},
        {"name":"nested","type":"STRUCT","mode":"REPEATED","fields":[
            {"name":"k","type":"STRING"},{"name":"vals","type":"FLOAT64","mode":"REPEATED"},
            {"name":"inner","type":"RECORD","fields":[{"name":"when","type":"DATE"}]}]}]}""")
    fields = B.parse_schema(schema)
    body = """[{"f":[{"v":"123456789012345678901234567890.5"},{"v":"POINT(1 2)"},{"v":"{\\"a\\":1}"},{"v":"1-2 3 4:5:6"},
                     {"v":"[2024-01-01, 2024-02-01)"},{"v":"5"},
                     {"v":[{"v":{"f":[{"v":"k1"},{"v":[{"v":"1.5"},{"v":"2.5"}]},{"v":{"f":[{"v":"2024-05-06"}]}}]}},
                           {"v":{"f":[{"v":null},{"v":[]},{"v":null}]}}]}]}]"""
    cols = B.new_columns(fields)
    @test B.decode_rows!(cols, fields, body, 1, ncodeunits(body)) == 1
    df = B.to_dataframe(B.make_result(fields, cols))
    @test df.big[1] isa BigFloat && df.big[1] == parse(BigFloat, "123456789012345678901234567890.5")
    @test df.geo == ["POINT(1 2)"] && df.js == ["{\"a\":1}"] && df.iv == ["1-2 3 4:5:6"]
    @test df.rng == ["[2024-01-01, 2024-02-01)"]
    @test df.req == [5]
    nested = df.nested[1]
    @test length(nested) == 2
    @test nested[1].k == "k1" && nested[1].vals == [1.5, 2.5] && nested[1].inner.when == Date(2024, 5, 6)
    @test nested[2].k === missing && isempty(nested[2].vals) && nested[2].inner === missing
end

@testset "Arrow batches -> columns" begin
    fields = B.parse_schema(SCHEMA)
    truth = truth_table(1000)
    arrow = arrow_columns(truth)
    for compress in (nothing, :zstd, :lz4)
        schema, batches = arrow_messages(arrow, [1:400, 401:401, 402:1000]; compress = compress)
        names, cols = B.decode_arrow(vcat(schema, batches...))
        @test names == collect(keys(truth))
        check_table(B.to_dataframe(B.make_result(fields, B.align_columns(fields, names, cols))), truth)
    end

    # Both wire formats must give identical results, down to the element types.
    rows = rest_rows(truth)
    body = "[" * join(rows, ",") * "]"
    rest_cols = B.new_columns(fields)
    B.decode_rows!(rest_cols, fields, body, 1, ncodeunits(body))
    schema, batches = arrow_messages(arrow, [1:1000])
    names, cols = B.decode_arrow(vcat(schema, batches...))
    arrow_cols = B.align_columns(fields, names, cols)
    for (r, a) in zip(rest_cols, arrow_cols)
        @test typeof(r) == typeof(a)
        @test isequal(r, a)
    end

    # Column order follows the requested fields even if Arrow orders differently.
    reordered = B.align_columns(fields[[3, 1]], names, cols)
    @test isequal(reordered[1], truth.name) && reordered[2] == truth.id
end

@testset "Arrow edge cases" begin
    TU = Arrow.Meta.TimeUnit
    big = Arrow.Decimal{76,38,Arrow.Int256}
    tbl = (
        before_epoch = [Arrow.Timestamp{TU.MICROSECOND,:UTC}(-1), Arrow.Timestamp{TU.MICROSECOND,:UTC}(-1_000_001)],
        nanos = [Arrow.Timestamp{TU.NANOSECOND,nothing}(1_500_000_000), Arrow.Timestamp{TU.NANOSECOND,nothing}(0)],
        bignum = [big(Arrow.Int256(15) * Arrow.Int256(10)^37), big(-Arrow.Int256(10)^38)],
        num = [Arrow.Decimal{38,9,Int128}(Int128(100_000_000)), Arrow.Decimal{38,9,Int128}(Int128(-1))],
        nums = [Int64[1, 2], Int64[]],
    )
    io = IOBuffer()
    Arrow.write(io, tbl)
    names, cols = B.decode_arrow(take!(io))
    @test cols[1] == [DateTime(1969, 12, 31, 23, 59, 59, 999), DateTime(1969, 12, 31, 23, 59, 58, 999)]
    @test cols[2] == [DateTime(1970, 1, 1, 0, 0, 1, 500), DateTime(1970)]
    @test cols[3] isa Vector{BigFloat} && cols[3] == [BigFloat("1.5"), BigFloat(-1)]
    @test cols[4] == [0.1, -1.0e-9]
    @test cols[5] isa Vector{Vector{Int64}} && cols[5] == [[1, 2], Int64[]]
end

@testset "column assembly" begin
    @test B.narrow_column(Union{Missing,Int}[1, 2]) isa Vector{Int}
    @test B.narrow_column(Union{Missing,Int}[1, missing]) isa Vector{Union{Missing,Int}}
    @test B.narrow_column(Union{Missing,Int}[]) isa Vector{Int}
    @test B.narrow_column([missing, missing]) isa Vector{Missing}
    chunks = Vector{Any}[Any[[1, 2], ["a", "b"]], Any[[3], ["c"]], Any[Int[], String[]]]
    @test B.concat_chunks(chunks, 2) == Any[[1, 2, 3], ["a", "b", "c"]]
    @test B.unique_names(["a", "b", "a", "a", "a_1"]) == [:a, :b, :a_1, :a_2, :a_1_1]
end
