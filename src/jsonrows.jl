# Decoding BigQuery's REST row format straight into typed columns.
#
# The REST API returns rows as  {"f":[{"v":"1"},{"v":null},{"v":{"f":[...]}}]}
# with every scalar encoded as a JSON string. Parsing that through a generic
# JSON library allocates an object per cell, which is what makes naive clients
# crawl on millions of rows. Here the bytes are scanned once, driven by the
# schema, and values are written directly into column vectors.

@inline cu(s::String, i::Int) = @inbounds codeunit(s, i)

@noinline function scan_fail(s::String, i::Int, what::AbstractString)
    lo = max(1, i - 20)
    hi = min(ncodeunits(s), i + 20)
    context = String(codeunits(s)[lo:hi])
    throw(BigQueryError("unexpected response format from BigQuery: $what at byte $i (near `$context`)",
                        "parseError", 0))
end

@inline function skip_ws(s::String, i::Int, n::Int)
    while i <= n
        c = cu(s, i)
        (c == 0x20 || c == 0x0a || c == 0x0d || c == 0x09) || break
        i += 1
    end
    return i
end

@inline function expect(s::String, i::Int, n::Int, c::UInt8)
    i = skip_ws(s, i, n)
    (i <= n && cu(s, i) == c) || scan_fail(s, i, "expected '$(Char(c))'")
    return i + 1
end

# `i` is at the opening quote. Returns (first, last, has_escapes, next).
@inline function scan_string(s::String, i::Int, n::Int)
    j = i + 1
    esc = false
    while j <= n
        c = cu(s, j)
        if c == UInt8('"')
            return (i + 1, j - 1, esc, j + 1)
        elseif c == UInt8('\\')
            esc = true
            j += 2
        else
            j += 1
        end
    end
    scan_fail(s, i, "unterminated string")
end

# Expects  "<key>":  where key is a single ASCII character (BigQuery uses "f" and "v").
@inline function expect_key(s::String, i::Int, n::Int, key::UInt8)
    i = skip_ws(s, i, n)
    (i + 2 <= n && cu(s, i) == UInt8('"') && cu(s, i + 1) == key && cu(s, i + 2) == UInt8('"')) ||
        scan_fail(s, i, "expected key \"$(Char(key))\"")
    return expect(s, i + 3, n, UInt8(':'))
end

@inline function expect_null(s::String, i::Int, n::Int)
    (i + 3 <= n && cu(s, i) == UInt8('n') && cu(s, i + 1) == UInt8('u') &&
     cu(s, i + 2) == UInt8('l') && cu(s, i + 3) == UInt8('l')) || scan_fail(s, i, "expected null")
    return i + 4
end

function skip_value(s::String, i::Int, n::Int)
    i = skip_ws(s, i, n)
    i <= n || scan_fail(s, i, "unexpected end of data")
    c = cu(s, i)
    if c == UInt8('"')
        return scan_string(s, i, n)[4]
    elseif c == UInt8('{') || c == UInt8('[')
        depth = 0
        while i <= n
            c = cu(s, i)
            if c == UInt8('"')
                i = scan_string(s, i, n)[4]
                continue
            elseif c == UInt8('{') || c == UInt8('[')
                depth += 1
            elseif c == UInt8('}') || c == UInt8(']')
                depth -= 1
                depth == 0 && return i + 1
            end
            i += 1
        end
        scan_fail(s, i, "unterminated value")
    else
        while i <= n
            c = cu(s, i)
            (c == UInt8(',') || c == UInt8('}') || c == UInt8(']') ||
             c == 0x20 || c == 0x0a || c == 0x0d || c == 0x09) && break
            i += 1
        end
        return i
    end
end

function hex4(s::String, i::Int, b::Int)
    i + 3 <= b || scan_fail(s, i, "truncated \\u escape")
    v = UInt32(0)
    for k in i:i+3
        c = cu(s, k)
        d = (UInt8('0') <= c <= UInt8('9')) ? c - UInt8('0') :
            (UInt8('a') <= c <= UInt8('f')) ? c - UInt8('a') + 0x0a :
            (UInt8('A') <= c <= UInt8('F')) ? c - UInt8('A') + 0x0a :
            scan_fail(s, k, "invalid \\u escape")
        v = (v << 4) | UInt32(d)
    end
    return v
end

function unescape_json(s::String, a::Int, b::Int)
    out = UInt8[]
    sizehint!(out, b - a + 1)
    i = a
    while i <= b
        c = cu(s, i)
        if c != UInt8('\\')
            push!(out, c)
            i += 1
            continue
        end
        i += 1
        i <= b || scan_fail(s, i, "truncated escape")
        e = cu(s, i)
        i += 1
        if e == UInt8('n')
            push!(out, 0x0a)
        elseif e == UInt8('t')
            push!(out, 0x09)
        elseif e == UInt8('r')
            push!(out, 0x0d)
        elseif e == UInt8('b')
            push!(out, 0x08)
        elseif e == UInt8('f')
            push!(out, 0x0c)
        elseif e == UInt8('u')
            cp = hex4(s, i, b)
            i += 4
            if 0xD800 <= cp <= 0xDBFF && i + 5 <= b && cu(s, i) == UInt8('\\') && cu(s, i + 1) == UInt8('u')
                lo = hex4(s, i + 2, b)
                if 0xDC00 <= lo <= 0xDFFF
                    cp = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00)
                    i += 6
                end
            end
            (0xD800 <= cp <= 0xDFFF) && (cp = 0xFFFD)   # lone surrogate
            append!(out, codeunits(string(Char(cp))))
        else
            push!(out, e)   # \" \\ \/
        end
    end
    return String(out)
end

function make_string(s::String, a::Int, b::Int, esc::Bool)
    b < a && return ""
    esc && return unescape_json(s, a, b)
    return GC.@preserve s unsafe_string(pointer(s, a), b - a + 1)
end

"Byte ranges of the values of a JSON object's top-level keys."
function top_level_spans(s::String)
    n = ncodeunits(s)
    spans = Dict{String,UnitRange{Int}}()
    i = expect(s, 1, n, UInt8('{'))
    i = skip_ws(s, i, n)
    (i <= n && cu(s, i) == UInt8('}')) && return spans
    while true
        i = skip_ws(s, i, n)
        (i <= n && cu(s, i) == UInt8('"')) || scan_fail(s, i, "expected an object key")
        a, b, esc, i = scan_string(s, i, n)
        key = make_string(s, a, b, esc)
        i = expect(s, i, n, UInt8(':'))
        i = skip_ws(s, i, n)
        j = skip_value(s, i, n)
        spans[key] = i:(j-1)
        i = skip_ws(s, j, n)
        i <= n || scan_fail(s, i, "unterminated object")
        c = cu(s, i)
        if c == UInt8(',')
            i += 1
        elseif c == UInt8('}')
            break
        else
            scan_fail(s, i, "expected ',' or '}'")
        end
    end
    return spans
end

"""
Split a REST response into its metadata (everything except `rows`, parsed
normally) and the byte range of the `rows` array, which is decoded separately.
"""
function split_response(raw::Vector{UInt8})
    s = String(raw)
    spans = top_level_spans(s)
    meta = Dict{String,Any}()
    for (k, r) in spans
        k == "rows" && continue
        meta[k] = JSON.parse(String(codeunits(s)[r]))
    end
    return s, meta, get(spans, "rows", nothing)
end

# --- scalar parsers ---------------------------------------------------------

@inline function digit(s::String, i::Int)
    d = cu(s, i) - UInt8('0')
    d <= 0x09 || scan_fail(s, i, "expected a digit")
    return Int(d)
end

@inline function parse_int(s::String, a::Int, b::Int)
    a <= b || scan_fail(s, a, "empty integer")
    neg = cu(s, a) == UInt8('-')
    i = neg ? a + 1 : a
    i <= b || scan_fail(s, a, "invalid integer")
    x = Int64(0)
    # Accumulate as a negative number so that typemin(Int64) parses.
    while i <= b
        x = x * 10 - digit(s, i)
        i += 1
    end
    return neg ? x : -x
end

@inline function parse_float(s::String, a::Int, b::Int)
    x = tryparse(Float64, SubString(s, a, b))
    x === nothing && scan_fail(s, a, "invalid number")
    return x
end

const UNIX_EPOCH_MS = Dates.value(DateTime(1970))
const UNIX_EPOCH_DAYS = Dates.value(Date(1970))

@inline datetime_from_unix_ms(ms::Int64) = DateTime(Dates.UTM(UNIX_EPOCH_MS + ms))

@inline function is_integer_text(s::String, a::Int, b::Int)
    for i in a:b
        c = cu(s, i)
        ((UInt8('0') <= c <= UInt8('9')) || (c == UInt8('-') && i == a)) || return false
    end
    return true
end

# TIMESTAMP arrives as integer microseconds since the epoch (we ask for that
# with formatOptions.useInt64Timestamp); the legacy float-seconds form is
# handled as well in case a server ignores the option.
@inline function parse_timestamp(s::String, a::Int, b::Int)
    us = is_integer_text(s, a, b) ? parse_int(s, a, b) : round(Int64, parse_float(s, a, b) * 1.0e6)
    return datetime_from_unix_ms(fld(us, 1000))
end

@inline function parse_ymd(s::String, a::Int)
    y = digit(s, a) * 1000 + digit(s, a + 1) * 100 + digit(s, a + 2) * 10 + digit(s, a + 3)
    m = digit(s, a + 5) * 10 + digit(s, a + 6)
    d = digit(s, a + 8) * 10 + digit(s, a + 9)
    return y, m, d
end

@inline function parse_date(s::String, a::Int, b::Int)
    b - a == 9 || scan_fail(s, a, "invalid DATE")
    y, m, d = parse_ymd(s, a)
    return Date(y, m, d)
end

# "HH:MM:SS[.ffffff]" starting at `a` -> nanoseconds since midnight
@inline function parse_hms_ns(s::String, a::Int, b::Int)
    b - a >= 7 || scan_fail(s, a, "invalid time of day")
    h = digit(s, a) * 10 + digit(s, a + 1)
    mi = digit(s, a + 3) * 10 + digit(s, a + 4)
    sec = digit(s, a + 6) * 10 + digit(s, a + 7)
    frac = 0
    scale = 100_000_000
    i = a + 9          # first fractional digit, when present
    while i <= b && scale > 0
        c = cu(s, i)
        (UInt8('0') <= c <= UInt8('9')) || break
        frac += Int(c - UInt8('0')) * scale
        scale ÷= 10
        i += 1
    end
    return ((h * 60 + mi) * 60 + sec) * 1_000_000_000 + frac
end

@inline parse_time(s::String, a::Int, b::Int) = Time(Dates.Nanosecond(parse_hms_ns(s, a, b)))

@inline function parse_datetime(s::String, a::Int, b::Int)
    b - a >= 18 || scan_fail(s, a, "invalid DATETIME")
    y, m, d = parse_ymd(s, a)
    ns = parse_hms_ns(s, a + 11, b)
    return DateTime(y, m, d) + Dates.Millisecond(ns ÷ 1_000_000)
end

function scalar_value(kind::Kind, s::String, a::Int, b::Int, esc::Bool)
    kind == K_INT && return parse_int(s, a, b)
    kind == K_FLOAT && return parse_float(s, a, b)
    kind == K_BOOL && return (cu(s, a) | 0x20) == UInt8('t')
    kind == K_BYTES && return base64decode(make_string(s, a, b, esc))
    kind == K_TIMESTAMP && return parse_timestamp(s, a, b)
    kind == K_DATE && return parse_date(s, a, b)
    kind == K_TIME && return parse_time(s, a, b)
    kind == K_DATETIME && return parse_datetime(s, a, b)
    kind == K_NUMERIC && return parse_float(s, a, b)
    kind == K_BIGNUMERIC && return parse(BigFloat, make_string(s, a, b, esc))
    return make_string(s, a, b, esc)
end

# --- nested values (slow path: STRUCT, ARRAY, unknown types) -----------------

# One non-repeated value, or `missing`. `i` is at the start of the value.
function read_single(f::Field, s::String, i::Int, n::Int)
    i <= n || scan_fail(s, i, "unexpected end of data")
    c = cu(s, i)
    c == UInt8('n') && return (missing, expect_null(s, i, n))
    if f.kind == K_RECORD
        i = expect(s, i, n, UInt8('{'))
        i = expect_key(s, i, n, UInt8('f'))
        i = expect(s, i, n, UInt8('['))
        vals = Vector{Any}(undef, length(f.fields))
        for (j, sub) in enumerate(f.fields)
            j > 1 && (i = expect(s, i, n, UInt8(',')))
            i = expect(s, i, n, UInt8('{'))
            i = expect_key(s, i, n, UInt8('v'))
            i = skip_ws(s, i, n)
            vals[j], i = read_value(sub, s, i, n)
            i = expect(s, i, n, UInt8('}'))
        end
        i = expect(s, i, n, UInt8(']'))
        i = expect(s, i, n, UInt8('}'))
        return (f.T(Tuple(vals)), i)
    elseif c != UInt8('"')
        # Not a string: keep the raw JSON text (types this package does not model).
        j = skip_value(s, i, n)
        return (String(codeunits(s)[i:j-1]), j)
    else
        a, b, esc, i = scan_string(s, i, n)
        return (scalar_value(f.kind, s, a, b, esc), i)
    end
end

# A value of field `f`, which may be repeated. `i` is at the start of the value.
function read_value(f::Field, s::String, i::Int, n::Int)
    f.repeated || return read_single(f, s, i, n)
    out = Vector{f.T}()
    i <= n || scan_fail(s, i, "unexpected end of data")
    cu(s, i) == UInt8('n') && return (out, expect_null(s, i, n))
    i = expect(s, i, n, UInt8('['))
    i = skip_ws(s, i, n)
    (i <= n && cu(s, i) == UInt8(']')) && return (out, i + 1)
    while true
        i = expect(s, i, n, UInt8('{'))
        i = expect_key(s, i, n, UInt8('v'))
        i = skip_ws(s, i, n)
        v, i = read_single(f, s, i, n)
        push!(out, v)
        i = expect(s, i, n, UInt8('}'))
        i = skip_ws(s, i, n)
        i <= n || scan_fail(s, i, "unterminated array")
        c = cu(s, i)
        if c == UInt8(',')
            i += 1
        elseif c == UInt8(']')
            return (out, i + 1)
        else
            scan_fail(s, i, "expected ',' or ']'")
        end
    end
end

# --- rows -------------------------------------------------------------------

# Reads one cell `{"v": ...}` into column `j`. The branches assert each
# column's concrete type so the hot path involves no dynamic dispatch.
@inline function read_cell!(cols::Vector{Any}, j::Int, f::Field, s::String, i::Int, n::Int)
    i = expect(s, i, n, UInt8('{'))
    i = expect_key(s, i, n, UInt8('v'))
    i = skip_ws(s, i, n)
    i <= n || scan_fail(s, i, "unexpected end of data")
    k = f.kind
    if f.repeated || k == K_RECORD || k == K_RAW
        v, i = read_value(f, s, i, n)::Tuple{Any,Int}
        push!(cols[j], v)
        return expect(s, i, n, UInt8('}'))
    end
    isnull = cu(s, i) == UInt8('n')
    a = 0
    b = 0
    esc = false
    if isnull
        i = expect_null(s, i, n)
    else
        cu(s, i) == UInt8('"') || scan_fail(s, i, "expected a string value")
        a, b, esc, i = scan_string(s, i, n)
    end
    if k == K_INT
        col = cols[j]::Vector{Union{Missing,Int64}}
        isnull ? push!(col, missing) : push!(col, parse_int(s, a, b))
    elseif k == K_FLOAT || k == K_NUMERIC
        col = cols[j]::Vector{Union{Missing,Float64}}
        isnull ? push!(col, missing) : push!(col, parse_float(s, a, b))
    elseif k == K_STRING
        col = cols[j]::Vector{Union{Missing,String}}
        isnull ? push!(col, missing) : push!(col, make_string(s, a, b, esc))
    elseif k == K_BOOL
        col = cols[j]::Vector{Union{Missing,Bool}}
        isnull ? push!(col, missing) : push!(col, (cu(s, a) | 0x20) == UInt8('t'))
    elseif k == K_TIMESTAMP
        col = cols[j]::Vector{Union{Missing,DateTime}}
        isnull ? push!(col, missing) : push!(col, parse_timestamp(s, a, b))
    elseif k == K_DATE
        col = cols[j]::Vector{Union{Missing,Date}}
        isnull ? push!(col, missing) : push!(col, parse_date(s, a, b))
    elseif k == K_DATETIME
        col = cols[j]::Vector{Union{Missing,DateTime}}
        isnull ? push!(col, missing) : push!(col, parse_datetime(s, a, b))
    elseif k == K_TIME
        col = cols[j]::Vector{Union{Missing,Time}}
        isnull ? push!(col, missing) : push!(col, parse_time(s, a, b))
    elseif k == K_BYTES
        col = cols[j]::Vector{Union{Missing,Vector{UInt8}}}
        isnull ? push!(col, missing) : push!(col, base64decode(make_string(s, a, b, esc)))
    else # K_BIGNUMERIC
        col = cols[j]::Vector{Union{Missing,BigFloat}}
        isnull ? push!(col, missing) : push!(col, parse(BigFloat, make_string(s, a, b, esc)))
    end
    return expect(s, i, n, UInt8('}'))
end

"""
Decode the JSON `rows` array occupying bytes `lo:hi` of `s`, appending to
`cols` (as created by `new_columns(fields)`). Returns the number of rows read.
"""
function decode_rows!(cols::Vector{Any}, fields::Vector{Field}, s::String, lo::Int, hi::Int)
    n = hi
    i = expect(s, lo, n, UInt8('['))
    i = skip_ws(s, i, n)
    (i <= n && cu(s, i) == UInt8(']')) && return 0
    nf = length(fields)
    count = 0
    while true
        i = expect(s, i, n, UInt8('{'))
        i = expect_key(s, i, n, UInt8('f'))
        i = expect(s, i, n, UInt8('['))
        for j in 1:nf
            j > 1 && (i = expect(s, i, n, UInt8(',')))
            i = read_cell!(cols, j, fields[j], s, i, n)
        end
        i = expect(s, i, n, UInt8(']'))
        i = expect(s, i, n, UInt8('}'))
        count += 1
        i = skip_ws(s, i, n)
        i <= n || scan_fail(s, i, "unterminated rows array")
        c = cu(s, i)
        if c == UInt8(',')
            i += 1
        elseif c == UInt8(']')
            break
        else
            scan_fail(s, i, "expected ',' or ']'")
        end
    end
    return count
end

decode_rows!(cols::Vector{Any}, fields::Vector{Field}, s::String, r::UnitRange{Int}) =
    decode_rows!(cols, fields, s, first(r), last(r))
