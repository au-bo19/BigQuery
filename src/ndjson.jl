# Preparing a table for upload: choosing BigQuery types for Julia columns and
# encoding rows as newline-delimited JSON, the format sent to load jobs.
#
#   Bool -> BOOL               Date     -> DATE
#   Integer -> INT64           DateTime -> TIMESTAMP (taken as UTC)
#   AbstractFloat -> FLOAT64   Time     -> TIME
#   BigFloat -> BIGNUMERIC     Vector{UInt8} -> BYTES
#   String, Symbol, Char -> STRING
#   Vector{T} -> ARRAY<T>      NamedTuple -> STRUCT
#
# `missing` and `nothing` become NULL. This is the reverse of the mapping in
# schema.jl, except where that one is not reversible: DATETIME and NUMERIC also
# arrive as DateTime and Float64, so writing those types has to be asked for.

"A column (or STRUCT field) of the table being written."
struct WriteField
    name::String
    type::String              # BigQuery type name, upper case
    kind::Kind
    isjson::Bool
    repeated::Bool
    fields::Vector{WriteField}
    # The REST definition to create the column with, when the user supplied one.
    definition::Union{Nothing,Dict{String,Any}}
end

function WriteField(name::AbstractString, type::AbstractString, repeated::Bool,
                    fields::Vector{WriteField} = WriteField[]; definition = nothing)
    t = uppercase(String(type))
    return WriteField(String(name), t, kind_of(t), t == "JSON", repeated, fields, definition)
end

"A field from its REST description, as found in a table's schema."
function WriteField(d::AbstractDict; definition = nothing)
    type = string(something(get(d, "type", nothing), "STRING"))
    mode = uppercase(string(something(get(d, "mode", nothing), "NULLABLE")))
    subs = WriteField[]
    raw = get(d, "fields", nothing)
    if raw isa AbstractVector
        for x in raw
            push!(subs, WriteField(x))
        end
    end
    return WriteField(string(d["name"]), type, mode == "REPEATED", subs; definition = definition)
end

describe(f::WriteField) = f.repeated ? "ARRAY<$(f.type)>" : f.type

function schema_json(f::WriteField)
    f.definition === nothing || return f.definition
    # The names the API itself uses when it describes a table.
    type = f.kind == K_INT ? "INTEGER" : f.kind == K_FLOAT ? "FLOAT" : f.kind == K_BOOL ? "BOOLEAN" :
           f.kind == K_RECORD ? "RECORD" : f.type
    d = Dict{String,Any}("name" => f.name, "type" => type, "mode" => f.repeated ? "REPEATED" : "NULLABLE")
    isempty(f.fields) || (d["fields"] = Any[schema_json(s) for s in f.fields])
    return d
end

# --- choosing types ---------------------------------------------------------

strip_null(T::Type) = Base.nonnothingtype(nonmissingtype(T))

function scalar_type_name(T::Type)
    T <: Bool && return "BOOL"
    T <: Integer && return "INT64"
    T <: BigFloat && return "BIGNUMERIC"
    T <: AbstractFloat && return "FLOAT64"
    T <: Union{AbstractString,Symbol,AbstractChar} && return "STRING"
    T <: Date && return "DATE"
    T <: DateTime && return "TIMESTAMP"
    T <: Time && return "TIME"
    T <: AbstractVector{UInt8} && return "BYTES"
    return nothing
end

"The field for values of Julia type `T`, or `nothing` when `T` does not determine one (`Any`, `Real`, ...)."
function infer_type(name::String, T::Type)
    T = strip_null(T)
    T === Union{} && return nothing
    scalar = scalar_type_name(T)
    scalar === nothing || return WriteField(name, scalar, false)
    if T <: AbstractVector
        el = infer_type(name, eltype(T))
        (el === nothing || el.repeated) && return nothing     # BigQuery has no arrays of arrays
        return WriteField(name, el.type, true, el.fields)
    elseif T <: NamedTuple && isconcretetype(T) && fieldcount(T) > 0
        subs = WriteField[]
        for (n, S) in zip(fieldnames(T), fieldtypes(T))
            sub = infer_type(String(n), S)
            sub === nothing && return nothing
            push!(subs, sub)
        end
        return WriteField(name, "RECORD", false, subs)
    end
    return nothing
end

@noinline cannot_infer(name::String, T::Type) = throw(ArgumentError(
    "cannot tell which BigQuery type column `$name` should have (it holds values of type $T); " *
    "convert the column to a concrete type, or give its BigQuery type with the `types` keyword"))

same_field(a::WriteField, b::WriteField) =
    a.name == b.name && a.type == b.type && a.repeated == b.repeated &&
    length(a.fields) == length(b.fields) && all(same_field(x, y) for (x, y) in zip(a.fields, b.fields))

function merge_fields(a::WriteField, b::WriteField)
    (a === b || same_field(a, b)) && return a
    if a.repeated == b.repeated && a.kind in (K_INT, K_FLOAT) && b.kind in (K_INT, K_FLOAT)
        return a.kind == K_FLOAT ? a : b
    end
    throw(ArgumentError("column `$(a.name)` mixes $(describe(a)) and $(describe(b)) values; " *
                        "convert it to a single type, or name its type with `types`"))
end

"The field that fits every value, for columns whose element type is too loose to say. `nothing` if all are NULL."
function infer_values(name::String, values)
    by_type = Dict{DataType,Union{Nothing,WriteField}}()
    found = nothing
    for v in values
        is_null(v) && continue
        T = typeof(v)
        f = get!(() -> infer_type(name, T), by_type, T)
        if f === nothing
            # The type alone does not say (a Vector{Any}, for example): look inside.
            v isa AbstractVector || cannot_infer(name, T)
            el = infer_values(name, v)
            el === nothing && continue
            el.repeated && cannot_infer(name, T)
            f = WriteField(name, el.type, true, el.fields)
        end
        found = found === nothing ? f : merge_fields(found, f)
    end
    return found
end

function infer_column(name::String, col::AbstractVector)
    f = infer_type(name, eltype(col))
    f === nothing || return f
    # A column of nothing but NULLs could be anything; STRING accepts later appends of any text.
    return something(infer_values(name, col), WriteField(name, "STRING", false))
end

"A field from a user's type override: a type name, or a REST field description."
function field_from_spec(name::String, spec)
    if spec isa AbstractString
        t = uppercase(String(strip(spec)))
        repeated = startswith(t, "ARRAY<") && endswith(t, ">")
        repeated && (t = String(strip(t[7:end-1])))
        (isempty(t) || occursin(r"[<(]", t) || t in ("ARRAY", "STRUCT", "RECORD")) && throw(ArgumentError(
            "cannot use \"$spec\" as the type of column `$name`: give a plain type such as \"DATETIME\" or " *
            "\"ARRAY<DATE>\", or describe the column with a Dict as the REST API does, " *
            "like Dict(\"type\" => \"NUMERIC\", \"precision\" => 10, \"scale\" => 2)"))
        return WriteField(name, t, repeated)
    elseif spec isa AbstractDict
        d = Dict{String,Any}(JSON.parse(JSON.json(spec)))      # string keys all the way down
        get(d, "type", nothing) isa AbstractString || throw(ArgumentError(
            "the description of column `$name` needs a \"type\" entry"))
        d["name"] = name
        return WriteField(d; definition = d)
    end
    throw(ArgumentError("the type of column `$name` must be a String or a Dict, got $(typeof(spec))"))
end

function infer_fields(names::Vector{String}, cols::Vector{AbstractVector}, types)
    overrides = Dict{String,Any}()
    if types !== nothing
        (types isa NamedTuple || types isa AbstractDict) || throw(ArgumentError(
            "`types` must be a NamedTuple or a Dict of column name => BigQuery type"))
        for (k, v) in pairs(types)
            overrides[string(k)] = v
        end
        unknown = sort!(collect(setdiff(keys(overrides), names)))
        isempty(unknown) || throw(ArgumentError(
            "`types` names columns that the data does not have: " * join(unknown, ", ")))
    end
    return WriteField[haskey(overrides, n) ? field_from_spec(n, overrides[n]) : infer_column(n, c)
                      for (n, c) in zip(names, cols)]
end

# --- encoding values --------------------------------------------------------

@noinline bad_value(f::WriteField, v) = throw(ArgumentError(
    "cannot write a value of type $(typeof(v)) to the $(describe(f)) column `$(f.name)`"))

const HEX_DIGITS = Vector{UInt8}("0123456789abcdef")

function write_json_string(io::IO, s::Union{String,SubString{String}})
    write(io, 0x22)
    n = ncodeunits(s)
    start = 1
    GC.@preserve s begin
        for i in 1:n
            b = codeunit(s, i)
            (b < 0x20 || b == 0x22 || b == 0x5c) || continue
            i > start && unsafe_write(io, pointer(s, start), UInt(i - start))
            if b == 0x22 || b == 0x5c
                write(io, 0x5c, b)
            elseif b == 0x0a
                write(io, 0x5c, UInt8('n'))
            elseif b == 0x0d
                write(io, 0x5c, UInt8('r'))
            elseif b == 0x09
                write(io, 0x5c, UInt8('t'))
            else
                write(io, "\\u00")
                write(io, HEX_DIGITS[(b>>4)+1], HEX_DIGITS[(b&0x0f)+1])
            end
            start = i + 1
        end
        n >= start && unsafe_write(io, pointer(s, start), UInt(n - start + 1))
    end
    write(io, 0x22)
    return nothing
end

write_json_string(io::IO, s::AbstractString) = write_json_string(io, String(s))

@inline function write_padded(io::IO, n::Integer, width::Int)
    p = 10^(width - 1)
    while p > 0
        write(io, (UInt8('0') + (n ÷ p) % 10) % UInt8)
        p ÷= 10
    end
    return nothing
end

function write_date(io::IO, f::WriteField, v::Union{Date,DateTime})
    y, m, d = Dates.yearmonthday(v)
    1 <= y <= 9999 || bad_value(f, v)          # the range BigQuery supports
    write_padded(io, y, 4)
    write(io, UInt8('-'))
    write_padded(io, m, 2)
    write(io, UInt8('-'))
    write_padded(io, d, 2)
    return nothing
end

function write_datetime(io::IO, f::WriteField, v::DateTime)
    write_date(io, f, v)
    write(io, UInt8('T'))
    write_padded(io, Dates.hour(v), 2)
    write(io, UInt8(':'))
    write_padded(io, Dates.minute(v), 2)
    write(io, UInt8(':'))
    write_padded(io, Dates.second(v), 2)
    write(io, UInt8('.'))
    write_padded(io, Dates.millisecond(v), 3)
    return nothing
end

function write_time(io::IO, v::Time)
    us = Dates.value(v) ÷ 1000                  # BigQuery keeps microseconds
    write_padded(io, us ÷ 3_600_000_000, 2)
    write(io, UInt8(':'))
    write_padded(io, us ÷ 60_000_000 % 60, 2)
    write(io, UInt8(':'))
    write_padded(io, us ÷ 1_000_000 % 60, 2)
    write(io, UInt8('.'))
    write_padded(io, us % 1_000_000, 6)
    return nothing
end

"`x` with `scale` decimal places and no exponent, trailing zeros dropped."
function fixed_decimal(x::Union{BigFloat,Rational}, scale::Int)
    n = round(BigInt, x * big(10)^scale)
    digits = string(abs(n))
    length(digits) <= scale && (digits = "0"^(scale - length(digits) + 1) * digits)
    whole = digits[1:end-scale]
    frac = rstrip(digits[end-scale+1:end], '0')
    out = isempty(frac) ? whole : whole * "." * frac
    return n < 0 ? "-" * out : out
end

"Rewrite Julia's float notation without an exponent: \"1.5e6\" -> \"1500000\"."
function plain_decimal(s::String)
    e = findfirst(==('e'), s)
    e === nothing && return s
    exponent = parse(Int, s[e+1:end])
    mantissa = s[1:e-1]
    negative = startswith(mantissa, '-')
    negative && (mantissa = mantissa[2:end])
    dot = findfirst(==('.'), mantissa)
    digits = dot === nothing ? mantissa : mantissa[1:dot-1] * mantissa[dot+1:end]
    point = (dot === nothing ? length(mantissa) : dot - 1) + exponent    # digits before the decimal point
    out = point <= 0 ? "0." * "0"^(-point) * digits :
          point >= length(digits) ? digits * "0"^(point - length(digits)) :
          digits[1:point] * "." * digits[point+1:end]
    return negative ? "-" * out : out
end

# NUMERIC keeps 9 decimal places and BIGNUMERIC 38. Values are rounded to that
# here, as a CAST would: a float such as 0.1 + 0.2 carries more digits than fit.
function write_decimal(io::IO, f::WriteField, v)
    scale = f.kind == K_NUMERIC ? 9 : 38
    if v isa Bool
        bad_value(f, v)
    elseif v isa Integer
        print(io, v)
    elseif v isa Union{BigFloat,Rational}
        (v isa BigFloat && !isfinite(v)) && bad_value(f, v)
        write(io, fixed_decimal(v, scale))
    elseif v isa AbstractFloat
        isfinite(v) || bad_value(f, v)
        text = plain_decimal(string(Float64(v)))
        dot = findfirst(==('.'), text)
        fits = dot === nothing || length(text) - dot <= scale
        write(io, fits ? text : fixed_decimal(big(Float64(v)), scale))
    else
        bad_value(f, v)
    end
    return nothing
end

# A JSON column takes any JSON value. Text is taken to be a JSON document (which
# is how this package returns JSON columns) and re-rendered onto a single line.
function write_json_document(io::IO, f::WriteField, v)
    if v isa AbstractString
        parsed = try
            JSON.parse(v)
        catch
            throw(ArgumentError("the JSON column `$(f.name)` was given text that is not valid JSON: " *
                                first(v, 80)))
        end
        write(io, JSON.json(parsed))
    else
        write(io, JSON.json(v))
    end
    return nothing
end

record_get(v::NamedTuple, name::String) = (s = Symbol(name); haskey(v, s) ? v[s] : missing)
record_get(v::AbstractDict, name::String) = get(() -> get(v, Symbol(name), missing), v, name)

function write_record(io::IO, f::WriteField, v)
    (v isa NamedTuple || v isa AbstractDict) || bad_value(f, v)
    write(io, UInt8('{'))
    for (j, sub) in enumerate(f.fields)
        j > 1 && write(io, UInt8(','))
        write_json_string(io, sub.name)
        write(io, UInt8(':'))
        write_value(io, sub, record_get(v, sub.name))
    end
    write(io, UInt8('}'))
    return nothing
end

"Write one value that is not an array. Branches on the value's type vanish when the column is typed."
function write_single(io::IO, f::WriteField, v)
    if is_null(v)
        write(io, "null")
        return nothing
    end
    k = f.kind
    if k == K_STRING
        if f.isjson
            write_json_document(io, f, v)
        elseif v isa AbstractString
            write_json_string(io, v)
        elseif v isa Union{Symbol,AbstractChar,Number,Dates.TimeType,UUID}
            write_json_string(io, string(v))
        else
            bad_value(f, v)
        end
    elseif k == K_RECORD
        write_record(io, f, v)
    elseif k == K_BYTES
        v isa AbstractVector{UInt8} || bad_value(f, v)
        write(io, 0x22)
        write(io, base64encode(v))
        write(io, 0x22)
    elseif v isa AbstractString
        # Text for a column of another type is passed on for BigQuery to parse. That
        # is the way to load exact NUMERIC values or timestamps with microseconds.
        write_json_string(io, v)
    elseif k == K_INT
        if v isa Bool
            write(io, v ? UInt8('1') : UInt8('0'))
        elseif v isa Integer
            print(io, v)
        elseif v isa AbstractFloat && isinteger(v)
            print(io, BigInt(v))
        else
            bad_value(f, v)
        end
    elseif k == K_FLOAT
        (v isa Real && !(v isa Bool)) || bad_value(f, v)
        x = Float64(v)
        if isfinite(x)
            print(io, x)
        else
            write(io, 0x22)
            write(io, format_float(x))
            write(io, 0x22)
        end
    elseif k == K_BOOL
        v isa Bool || bad_value(f, v)
        write(io, v ? "true" : "false")
    elseif k == K_TIMESTAMP
        v isa Union{Date,DateTime} || bad_value(f, v)
        write(io, 0x22)
        write_datetime(io, f, DateTime(v))
        write(io, UInt8('Z'), 0x22)
    elseif k == K_DATETIME
        v isa Union{Date,DateTime} || bad_value(f, v)
        write(io, 0x22)
        write_datetime(io, f, DateTime(v))
        write(io, 0x22)
    elseif k == K_DATE
        v isa Date || bad_value(f, v)
        write(io, 0x22)
        write_date(io, f, v)
        write(io, 0x22)
    elseif k == K_TIME
        v isa Time || bad_value(f, v)
        write(io, 0x22)
        write_time(io, v)
        write(io, 0x22)
    elseif k == K_NUMERIC || k == K_BIGNUMERIC
        write_decimal(io, f, v)
    else
        bad_value(f, v)       # a type this package does not model: only text is accepted
    end
    return nothing
end

"Write a value of field `f`, which may be an array."
function write_value(io::IO, f::WriteField, v)
    if !f.repeated
        write_single(io, f, v)
    elseif is_null(v)
        write(io, "[]")       # an array cannot be NULL in BigQuery, only empty
    else
        v isa AbstractVector || bad_value(f, v)
        write(io, UInt8('['))
        first = true
        for x in v
            first || write(io, UInt8(','))
            first = false
            write_single(io, f, x)
        end
        write(io, UInt8(']'))
    end
    return nothing
end

# --- encoding rows ----------------------------------------------------------

"One column ready for encoding. The column's type is a parameter, so each cell is written without dynamic dispatch."
struct ColumnEncoder{C<:AbstractVector}
    key::String               # `"name":`, already rendered
    field::WriteField
    col::C
end

ColumnEncoder(f::WriteField, col::AbstractVector) =
    ColumnEncoder(sprint(write_json_string, f.name) * ":", f, col)

function write_cell(io::IO, e::ColumnEncoder, i::Int)
    write(io, e.key)
    write_value(io, e.field, @inbounds e.col[i])
    return nothing
end

write_cells(io::IO, ::Tuple{}, i::Int) = nothing
@inline function write_cells(io::IO, encoders::Tuple, i::Int)
    write_cell(io, first(encoders), i)
    rest = Base.tail(encoders)
    rest === () || write(io, UInt8(','))
    return write_cells(io, rest, i)
end

# Very wide tables: a tuple that long would take ages to compile.
function write_cells(io::IO, encoders::Vector{Any}, i::Int)
    for (j, e) in enumerate(encoders)
        j > 1 && write(io, UInt8(','))
        write_cell(io, e, i)
    end
    return nothing
end

"Write row `i` as one line of JSON."
function write_row(io::IO, encoders, i::Int)
    write(io, UInt8('{'))
    write_cells(io, encoders, i)
    write(io, UInt8('}'), UInt8('\n'))
    return nothing
end

const MAX_TYPED_COLUMNS = 32

function row_encoders(fields::Vector{WriteField}, cols::Vector{AbstractVector})
    encoders = Any[ColumnEncoder(f, c) for (f, c) in zip(fields, cols)]
    return length(encoders) <= MAX_TYPED_COLUMNS ? Tuple(encoders) : encoders
end
