# BigQuery schemas and the related Julia types.
#
#   INT64 -> Int64             DATE      -> Date
#   FLOAT64 -> Float64         DATETIME  -> DateTime (the package keeps milliseconds)
#   BOOL -> Bool               TIME      -> Time
#   STRING -> String           TIMESTAMP -> DateTime in UTC (the package keeps milliseconds)
#   BYTES -> Vector{UInt8}     NUMERIC   -> Float64
#   STRUCT -> NamedTuple       BIGNUMERIC -> BigFloat
#   ARRAY<T> -> Vector{T}      GEOGRAPHY / JSON / INTERVAL -> String
#
# A NULL becomes `missing`. A column gets the element type `Union{Missing,T}` only
# if it contains a NULL.

@enum Kind::UInt8 begin
    K_INT
    K_FLOAT
    K_BOOL
    K_STRING
    K_BYTES
    K_TIMESTAMP
    K_DATE
    K_TIME
    K_DATETIME
    K_NUMERIC
    K_BIGNUMERIC
    K_RECORD
    K_RAW
end

struct Field
    name::String
    kind::Kind
    repeated::Bool
    nullable::Bool
    fields::Vector{Field}
    T::Type   # The Julia type of one value that is not NULL (the element type if the field is repeated)
end

function kind_of(typ::AbstractString)
    t = uppercase(typ)
    t in ("INTEGER", "INT64") && return K_INT
    t in ("FLOAT", "FLOAT64") && return K_FLOAT
    t in ("BOOLEAN", "BOOL") && return K_BOOL
    t in ("STRING", "GEOGRAPHY", "JSON", "INTERVAL") && return K_STRING
    t == "BYTES" && return K_BYTES
    t == "TIMESTAMP" && return K_TIMESTAMP
    t == "DATE" && return K_DATE
    t == "TIME" && return K_TIME
    t == "DATETIME" && return K_DATETIME
    t == "NUMERIC" && return K_NUMERIC
    t == "BIGNUMERIC" && return K_BIGNUMERIC
    t in ("RECORD", "STRUCT") && return K_RECORD
    return K_RAW   # For example RANGE. The package keeps the text.
end

function scalar_type(kind::Kind)
    kind == K_INT && return Int64
    kind == K_FLOAT && return Float64
    kind == K_BOOL && return Bool
    kind == K_BYTES && return Vector{UInt8}
    (kind == K_TIMESTAMP || kind == K_DATETIME) && return DateTime
    kind == K_DATE && return Date
    kind == K_TIME && return Time
    kind == K_NUMERIC && return Float64
    kind == K_BIGNUMERIC && return BigFloat
    return String
end

# The type of a value in a STRUCT.
nested_type(f::Field) = f.repeated ? Vector{f.T} : f.nullable ? Union{Missing,f.T} : f.T

# The element type of a top-level column while the package makes the column.
column_type(f::Field) = f.repeated ? Vector{f.T} : Union{Missing,f.T}

function Field(d::AbstractDict)
    name = string(d["name"])
    kind = kind_of(string(something(get(d, "type", nothing), "STRING")))
    mode = uppercase(string(something(get(d, "mode", nothing), "NULLABLE")))
    subs = Field[]
    raw = get(d, "fields", nothing)
    if raw isa AbstractVector
        for x in raw
            push!(subs, Field(x))
        end
    end
    T = if kind == K_RECORD
        names = Tuple(Symbol(f.name) for f in subs)
        allunique(names) || throw(BigQueryError(
            "STRUCT column `$name` has duplicate field names, which cannot be represented as a NamedTuple; " *
            "alias the fields in your query"))
        NamedTuple{names,Tuple{(nested_type(f) for f in subs)...}}
    else
        scalar_type(kind)
    end
    return Field(name, kind, mode == "REPEATED", mode != "REQUIRED", subs, T)
end

function parse_schema(schema)
    fields = Field[]
    schema isa AbstractDict || return fields
    raw = get(schema, "fields", nothing)
    raw isa AbstractVector || return fields
    for x in raw
        push!(fields, Field(x))
    end
    return fields
end

new_columns(fields::Vector{Field}) = Any[column_type(f)[] for f in fields]

# --- make columns -----------------------------------------------------------

function _concat(first::Vector{T}, parts::Vector{Any}) where {T}
    total = 0
    for p in parts
        total += length(p::Vector{T})
    end
    out = Vector{T}(undef, total)
    pos = 1
    for p in parts
        v = p::Vector{T}
        copyto!(out, pos, v, 1, length(v))
        pos += length(v)
    end
    return out
end

"Concatenate vectors of the same type. Repeated `vcat` calls have a quadratic cost, but this function does not."
function concat_parts(parts::Vector{Any})
    length(parts) == 1 && return parts[1]
    return _concat(parts[1], parts)
end

"Concatenate chunks (each chunk is a vector of columns), one column at a time."
function concat_chunks(chunks::Vector{Vector{Any}}, ncols::Integer)
    return Any[concat_parts(Any[c[j] for c in chunks]) for j in 1:ncols]
end

"Remove `Missing` from the element type if the column contains no NULL."
function narrow_column(col::AbstractVector)
    T = eltype(col)
    (T !== Missing && Missing <: T) || return col
    any(ismissing, col) && return col
    return convert(Vector{nonmissingtype(T)}, col)
end
