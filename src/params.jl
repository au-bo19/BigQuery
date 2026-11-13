# Query parameters (`@name` and `?` placeholders).

"""
    bq_param(value, type)

A query parameter with an explicit BigQuery type. Use it if the package cannot find
the type from the Julia value: a `missing`, an empty array, or a `DateTime` for a
`DATETIME` parameter (the default type is `TIMESTAMP`).

```julia
bq_query(con, "SELECT @d AS d, @x AS x"; params = (d = bq_param(now(), "DATETIME"), x = bq_param(missing, "INT64")))
```
"""
struct Param
    value::Any
    type::String
end

bq_param(value, type::AbstractString) = Param(value, uppercase(String(type)))

param_type(::Bool) = "BOOL"
param_type(::Integer) = "INT64"
param_type(::AbstractFloat) = "FLOAT64"
param_type(::AbstractString) = "STRING"
param_type(::Symbol) = "STRING"
param_type(::Date) = "DATE"
param_type(::DateTime) = "TIMESTAMP"
param_type(::Time) = "TIME"
param_type(::Vector{UInt8}) = "BYTES"
param_type(x) = throw(ArgumentError(
    "cannot infer a BigQuery type for a query parameter of type $(typeof(x)); use bq_param(value, \"TYPE\")"))

function format_float(x::AbstractFloat)
    isnan(x) && return "NaN"
    isinf(x) && return x > 0 ? "inf" : "-inf"
    return string(Float64(x))
end

function param_string(x, type::String)
    x isa Bool && return x ? "true" : "false"
    x isa AbstractFloat && return format_float(x)
    x isa Date && return Dates.format(x, dateformat"yyyy-mm-dd")
    if x isa DateTime
        return type == "DATETIME" ? Dates.format(x, dateformat"yyyy-mm-ddTHH:MM:SS.sss") :
               type == "DATE" ? Dates.format(x, dateformat"yyyy-mm-dd") :
               Dates.format(x, dateformat"yyyy-mm-dd HH:MM:SS.sss") * "+00:00"
    end
    x isa Time && return Dates.format(x, dateformat"HH:MM:SS.sss")
    x isa Vector{UInt8} && return base64encode(x)
    return string(x)
end

is_null(x) = x === missing || x === nothing

function param_json(x)
    type = ""
    if x isa Param
        type = x.type
        x = x.value
    end
    if x isa AbstractVector && !(x isa Vector{UInt8} && type in ("", "BYTES"))
        element_type = if startswith(type, "ARRAY<") && endswith(type, ">")
            String(strip(type[7:end-1]))
        elseif isempty(type)
            idx = findfirst(!is_null, x)
            idx === nothing && throw(ArgumentError(
                "cannot infer the element type of an empty array parameter; use bq_param(values, \"ARRAY<TYPE>\")"))
            param_type(x[idx])
        else
            throw(ArgumentError("an array parameter needs a type like \"ARRAY<INT64>\", got \"$type\""))
        end
        return Dict{String,Any}(
            "parameterType" => Dict{String,Any}("type" => "ARRAY",
                                                "arrayType" => Dict{String,Any}("type" => element_type)),
            "parameterValue" => Dict{String,Any}("arrayValues" => Any[
                Dict{String,Any}("value" => is_null(v) ? nothing : param_string(v, element_type)) for v in x]))
    end
    if is_null(x)
        isempty(type) && throw(ArgumentError(
            "a NULL query parameter needs an explicit type: bq_param(missing, \"INT64\")"))
        return Dict{String,Any}("parameterType" => Dict{String,Any}("type" => type),
                                "parameterValue" => Dict{String,Any}("value" => nothing))
    end
    isempty(type) && (type = param_type(x))
    return Dict{String,Any}("parameterType" => Dict{String,Any}("type" => type),
                            "parameterValue" => Dict{String,Any}("value" => param_string(x, type)))
end

"Returns `(mode, parameters)` for the jobs.query request, or `nothing` if there are no parameters."
function encode_params(params)
    params === nothing && return nothing
    named = params isa NamedTuple || params isa AbstractDict ||
            (params isa AbstractVector && !isempty(params) && all(p -> p isa Pair, params))
    if named
        isempty(params) && return nothing
        out = Any[]
        for (k, v) in pairs(params isa AbstractVector ? Dict(params) : params)
            d = param_json(v)
            d["name"] = lstrip(string(k), '@')
            push!(out, d)
        end
        return "NAMED", out
    elseif params isa Tuple || params isa AbstractVector
        isempty(params) && return nothing
        return "POSITIONAL", Any[param_json(v) for v in params]
    else
        throw(ArgumentError(
            "params must be a NamedTuple or Dict (for @name placeholders) or a Tuple or Vector (for ? placeholders)"))
    end
end
