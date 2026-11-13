# Change Arrow record batches (from the Storage Read API) into the same Julia
# column types that the REST decoder makes.
#
# Arrow.jl does not convert the logical types here (`convert = false`). Its
# TIMESTAMP conversion uses `ZonedDateTime`. That conversion is slow, and its
# result is different from the REST result. This file converts the values.

const TimeUnit = Arrow.Meta.TimeUnit
const DateUnit = Arrow.Meta.DateUnit

"The Julia type for an Arrow element type."
function target_type(T::Type)
    T === Missing && return Missing
    if Missing <: T
        return Union{Missing,target_type(nonmissingtype(T))}
    end
    T <: Arrow.Timestamp && return DateTime
    T <: Arrow.Date && return T.parameters[1] === DateUnit.DAY ? Date : DateTime
    T <: Arrow.Time && return Time
    T <: Arrow.Decimal && return T.parameters[3] === Int128 ? Float64 : BigFloat
    T <: AbstractString && return String
    T <: Base.CodeUnits && return Vector{UInt8}
    if T <: NamedTuple && isconcretetype(T)
        return NamedTuple{fieldnames(T),Tuple{map(target_type, fieldtypes(T))...}}
    end
    # BigQuery arrays never contain NULL elements.
    T <: AbstractVector && return Vector{target_type(nonmissingtype(eltype(T)))}
    return T
end

@inline function timestamp_ms(::Type{Arrow.Timestamp{U,TZ}}, x::Int64) where {U,TZ}
    U === TimeUnit.MICROSECOND && return fld(x, 1_000)
    U === TimeUnit.MILLISECOND && return x
    U === TimeUnit.SECOND && return x * 1_000
    return fld(x, 1_000_000)
end

@inline function time_ns(::Type{Arrow.Time{U,T}}, x::Int64) where {U,T}
    U === TimeUnit.MICROSECOND && return x * 1_000
    U === TimeUnit.NANOSECOND && return x
    U === TimeUnit.MILLISECOND && return x * 1_000_000
    return x * 1_000_000_000
end

# The largest integer that a Float64 holds exactly, and all smaller integers also.
const EXACT_FLOAT_INT = 2^53

"""
The Float64 nearest to `value / 10^scale`, the same as the REST decoder gives
for the decimal text.

If `value` is small, the function divides directly. Both operands are exact, thus
one division gives the correctly rounded result. If `value` is large,
`Float64(value)` rounds, and the division then rounds a second time. Thus the
function parses the decimal text, as the REST decoder does.
"""
function decimal_to_float(value::Integer, scale::Integer)
    scale = Int(scale)
    (abs(value) <= EXACT_FLOAT_INT && scale <= 22) && return Float64(value) / exp10(scale)
    digits = string(abs(value))
    length(digits) <= scale && (digits = "0"^(scale - length(digits) + 1) * digits)
    text = string(value < 0 ? "-" : "", digits[1:end-scale], ".", digits[end-scale+1:end], "0")
    return parse(Float64, text)
end

# cv(R, x): convert the Arrow value `x` to the target type `R`.
@inline cv(::Type{R}, x) where {R} = convert(R, x)
@inline cv(::Type{Union{Missing,R}}, x) where {R} = x === missing ? missing : cv(R, x)
@inline cv(::Type{DateTime}, x::Arrow.Timestamp) = datetime_from_unix_ms(timestamp_ms(typeof(x), x.x))
@inline cv(::Type{Date}, x::Arrow.Date) = Date(Dates.UTD(UNIX_EPOCH_DAYS + Int64(x.x)))
@inline cv(::Type{DateTime}, x::Arrow.Date) = datetime_from_unix_ms(Int64(x.x))
@inline cv(::Type{Time}, x::Arrow.Time) = Time(Dates.Nanosecond(time_ns(typeof(x), Int64(x.x))))
@inline cv(::Type{Float64}, x::Arrow.Decimal{P,S,T}) where {P,S,T} = decimal_to_float(x.value, S)
@inline cv(::Type{BigFloat}, x::Arrow.Decimal{P,S,T}) where {P,S,T} =
    BigFloat(BigInt(x.value)) / BigFloat(10)^S
@inline cv(::Type{String}, x::AbstractString) = String(x)
@inline cv(::Type{Vector{UInt8}}, x::Base.CodeUnits) = Vector{UInt8}(x)

function cv(::Type{R}, x::NamedTuple) where {R<:NamedTuple}
    return R(ntuple(i -> cv(fieldtype(R, i), x[i]), fieldcount(R)))
end

function cv(::Type{Vector{E}}, x::AbstractVector) where {E}
    out = Vector{E}(undef, length(x))
    for (i, y) in enumerate(x)
        out[i] = cv(E, y)
    end
    return out
end
# BYTES values arrive as byte vectors. Do not use them as arrays of values.
cv(::Type{Vector{UInt8}}, x::AbstractVector{UInt8}) = Vector{UInt8}(x)

function _convert_column(::Type{R}, col::AbstractVector) where {R}
    out = Vector{R}(undef, length(col))
    i = 0
    for x in col
        i += 1
        @inbounds out[i] = cv(R, x)
    end
    return out
end

function convert_column(col::AbstractVector)
    R = target_type(eltype(col))
    return _convert_column(R, col)
end

"""
Decode an Arrow IPC stream (a schema message, then record batches). Return
`(names, columns)` with plain Julia vectors.
"""
function decode_arrow(bytes::Vector{UInt8})
    tbl = Arrow.Table(bytes; convert = false)
    names = collect(Symbol, Tables.columnnames(tbl))
    cols = Any[convert_column(Tables.getcolumn(tbl, i)) for i in 1:length(names)]
    return names, cols
end

"""
Put the Arrow columns in the sequence of the REST schema. Change their element
types to the types from the REST decoder. Thus the download method does not change
the shape of the result.
"""
function align_columns(fields::Vector{Field}, names::Vector{Symbol}, cols::Vector{Any})
    out = Vector{Any}(undef, length(fields))
    for (j, f) in enumerate(fields)
        idx = findfirst(==(Symbol(f.name)), names)
        if idx === nothing
            length(names) == length(fields) || throw(BigQueryError(
                "the Storage Read API did not return column `$(f.name)`"))
            idx = j
        end
        col = cols[idx]
        want = column_type(f)
        if eltype(col) !== want
            col = try
                convert(Vector{want}, col)
            catch
                col   # a type that the package does not model: keep the Arrow column
            end
        end
        out[j] = col
    end
    return out
end
