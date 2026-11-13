# Test data: one "truth" table, and independent encoders. The encoders write the
# table as BigQuery does in its two wire formats (REST JSON and Arrow).

module Fixtures

using Dates, Base64
import Arrow, JSON

export SCHEMA, truth_table, rest_rows, arrow_messages, arrow_columns

const SCHEMA = Dict{String,Any}("fields" => Any[
    Dict("name" => "id", "type" => "INTEGER", "mode" => "NULLABLE"),
    Dict("name" => "x", "type" => "FLOAT", "mode" => "NULLABLE"),
    Dict("name" => "name", "type" => "STRING", "mode" => "NULLABLE"),
    Dict("name" => "flag", "type" => "BOOLEAN", "mode" => "NULLABLE"),
    Dict("name" => "ts", "type" => "TIMESTAMP", "mode" => "NULLABLE"),
    Dict("name" => "d", "type" => "DATE", "mode" => "NULLABLE"),
    Dict("name" => "dt", "type" => "DATETIME", "mode" => "NULLABLE"),
    Dict("name" => "t", "type" => "TIME", "mode" => "NULLABLE"),
    Dict("name" => "num", "type" => "NUMERIC", "mode" => "NULLABLE"),
    Dict("name" => "by", "type" => "BYTES", "mode" => "NULLABLE"),
    Dict("name" => "tags", "type" => "STRING", "mode" => "REPEATED"),
    Dict("name" => "st", "type" => "RECORD", "mode" => "NULLABLE", "fields" => Any[
        Dict("name" => "a", "type" => "INTEGER", "mode" => "NULLABLE"),
        Dict("name" => "b", "type" => "STRING", "mode" => "NULLABLE"),
    ]),
])

const NAMES = ["plain", "with \"quotes\"", "tab\tand\nnewline", "ünïcödé", "emoji 😀", "back\\slash", "", "</script>"]

"The Julia columns that BigQuery.jl must return for `n` rows."
function truth_table(n::Integer)
    ts0 = DateTime(2020, 1, 1)
    id = collect(Int64, 1:n)
    x = Union{Missing,Float64}[i % 7 == 0 ? missing : i / 4 for i in 1:n]
    name = Union{Missing,String}[i % 11 == 0 ? missing : NAMES[i%length(NAMES)+1] * string(i) for i in 1:n]
    flag = Bool[isodd(i) for i in 1:n]
    ts = Union{Missing,DateTime}[i % 13 == 0 ? missing : ts0 + Millisecond(i * 86_400_123) for i in 1:n]
    d = Date[Date(2000, 1, 1) + Day(i % 9000) for i in 1:n]
    dt = DateTime[DateTime(1999, 12, 31, 23, 59, 59) + Millisecond(i * 1001) for i in 1:n]
    t = Time[Time(Nanosecond((i * 1_234_567_000) % 86_400_000_000_000)) for i in 1:n]
    num = Float64[i * 0.25 - 100 for i in 1:n]
    by = Vector{UInt8}[UInt8[i % 256, (i >> 8) % 256, 0xff] for i in 1:n]
    tags = Vector{String}[String["t$(i)_$k" for k in 1:(i%3)] for i in 1:n]
    ST = NamedTuple{(:a, :b),Tuple{Union{Missing,Int64},Union{Missing,String}}}
    st = Union{Missing,ST}[i % 5 == 0 ? missing : ST((i % 2 == 0 ? missing : Int64(i), "s$i")) for i in 1:n]
    return (; id, x, name, flag, ts, d, dt, t, num, by, tags, st)
end

# --- REST encoding ----------------------------------------------------------

unix_micros(x::DateTime) = Dates.value(x - DateTime(1970)) * 1000

function rest_scalar(x, typ::String)
    x === missing && return nothing
    typ == "INTEGER" && return string(x)
    typ == "FLOAT" && return isnan(x) ? "NaN" : isinf(x) ? (x > 0 ? "Infinity" : "-Infinity") : string(x)
    typ == "NUMERIC" && return string(x)
    typ == "BOOLEAN" && return x ? "true" : "false"
    typ == "STRING" && return x
    typ == "TIMESTAMP" && return string(unix_micros(x) + 456)   # the package removes the part below one millisecond
    typ == "DATE" && return Dates.format(x, dateformat"yyyy-mm-dd")
    typ == "DATETIME" && return Dates.format(x, dateformat"yyyy-mm-ddTHH:MM:SS.sss") * "000"
    typ == "TIME" && return Dates.format(x, dateformat"HH:MM:SS.sss") *
                            lpad(string(Dates.microsecond(x)), 3, '0')
    typ == "BYTES" && return base64encode(x)
    error("unsupported fixture type $typ")
end

function rest_value(x, field::AbstractDict)
    typ = field["type"]
    mode = get(field, "mode", "NULLABLE")
    if mode == "REPEATED"
        single = copy(field)
        single["mode"] = "NULLABLE"
        return Any[Dict("v" => rest_value(y, single)) for y in x]
    elseif typ == "RECORD"
        x === missing && return nothing
        return Dict("f" => Any[Dict("v" => rest_value(x[i], sub)) for (i, sub) in enumerate(field["fields"])])
    else
        return rest_scalar(x, typ)
    end
end

"Write a table as BigQuery REST rows: one JSON string `{\"f\":[{\"v\":...},...]}` for each row."
function rest_rows(table, schema = SCHEMA)
    fields = schema["fields"]
    cols = collect(values(table))
    n = length(first(cols))
    rows = Vector{String}(undef, n)
    for i in 1:n
        cells = Any[Dict("v" => rest_value(cols[j][i], fields[j])) for j in eachindex(fields)]
        rows[i] = JSON.json(Dict("f" => cells))
    end
    return rows
end

# --- Arrow encoding ---------------------------------------------------------

const TU = Arrow.Meta.TimeUnit
const ArrowTS = Arrow.Timestamp{TU.MICROSECOND,:UTC}
const ArrowDT = Arrow.Timestamp{TU.MICROSECOND,nothing}
const ArrowTime = Arrow.Time{TU.MICROSECOND,Int64}
const ArrowNumeric = Arrow.Decimal{38,9,Int128}

lift(f, x) = x === missing ? missing : f(x)

"The truth table as BigQuery's Arrow types (timestamp[us, UTC], decimal128(38, 9), binary, ...)."
function arrow_columns(table)
    return (
        id = table.id,
        x = table.x,
        name = table.name,
        flag = table.flag,
        ts = [lift(v -> ArrowTS(unix_micros(v) + 456), v) for v in table.ts],   # the package removes the part below one millisecond
        d = table.d,
        dt = [ArrowDT(unix_micros(v)) for v in table.dt],
        t = [ArrowTime(Dates.value(v) ÷ 1000) for v in table.t],
        num = [ArrowNumeric(Int128(round(BigInt, big(v) * 10^9))) for v in table.num],
        by = [codeunits(String(copy(v))) for v in table.by],
        tags = table.tags,
        st = table.st,
    )
end

rows_of(cols, range) = map(c -> c[range], cols)

"""
Serialize a table as the Storage Read API does: one schema message, and one record
batch message for each element of `ranges`.
"""
function arrow_messages(cols, ranges; compress = nothing)
    schema = UInt8[]
    batches = Vector{UInt8}[]
    for r in ranges
        io = IOBuffer()
        Arrow.write(io, rows_of(cols, r); compress = compress)
        bytes = take!(io)
        # The stream layout: [schema message][record batch message][8-byte end marker].
        # A schema message is 0xFFFFFFFF, an int32 metadata length, and then the metadata.
        meta_len = Int(reinterpret(Int32, bytes[5:8])[1])
        schema = bytes[1:8+meta_len]
        push!(batches, bytes[9+meta_len:end-8])
    end
    return schema, batches
end

end # module
