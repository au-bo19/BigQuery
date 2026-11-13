# The protobuf messages that the Storage Read API uses. The functions in this file
# encode and decode them directly. The field numbers are from
# google/cloud/bigquery/storage/v1 (storage.proto, stream.proto, arrow.proto) in
# github.com/googleapis/googleapis.
#
# Thus the package does not use a protobuf code generator or a gRPC library. The
# wire format of these messages is small and stable.

function pb_varint(io::IO, x::UInt64)
    while x >= 0x80
        write(io, UInt8(x & 0x7f) | 0x80)
        x >>= 7
    end
    write(io, UInt8(x))
    return nothing
end

pb_tag(io::IO, field::Integer, wire::Integer) = pb_varint(io, (UInt64(field) << 3) | UInt64(wire))

function pb_bytes(io::IO, field::Integer, data::AbstractVector{UInt8})
    pb_tag(io, field, 2)
    pb_varint(io, UInt64(length(data)))
    write(io, data)
    return nothing
end

pb_string(io::IO, field::Integer, s::AbstractString) =
    isempty(s) ? nothing : pb_bytes(io, field, codeunits(String(s)))

function pb_int(io::IO, field::Integer, v::Integer)
    v == 0 && return nothing
    pb_tag(io, field, 0)
    pb_varint(io, reinterpret(UInt64, Int64(v)))
    return nothing
end

function pb_read_varint(buf::AbstractVector{UInt8}, pos::Int, hi::Int)
    x = UInt64(0)
    shift = 0
    while true
        pos <= hi || throw(BigQueryError("truncated protobuf message from the Storage Read API"))
        b = buf[pos]
        pos += 1
        x |= UInt64(b & 0x7f) << shift
        b < 0x80 && break
        shift += 7
        shift > 63 && throw(BigQueryError("malformed protobuf varint from the Storage Read API"))
    end
    return x, pos
end

"""
Call `f(field, wiretype, value, range)` for each field in `buf[lo:hi]`. For a
varint, `value` contains the value. For length-delimited and fixed-width fields,
`range` is the byte range of the payload.
"""
function pb_foreach(f, buf::AbstractVector{UInt8}, lo::Int, hi::Int)
    pos = lo
    while pos <= hi
        key, pos = pb_read_varint(buf, pos, hi)
        field = Int(key >> 3)
        wire = Int(key & 0x07)
        if wire == 0
            v, pos = pb_read_varint(buf, pos, hi)
            f(field, wire, v, 0:-1)
        elseif wire == 2
            len, pos = pb_read_varint(buf, pos, hi)
            stop = pos + Int(len) - 1
            stop <= hi || throw(BigQueryError("truncated protobuf message from the Storage Read API"))
            f(field, wire, UInt64(0), pos:stop)
            pos = stop + 1
        elseif wire == 1
            f(field, wire, UInt64(0), pos:pos+7)
            pos += 8
        elseif wire == 5
            f(field, wire, UInt64(0), pos:pos+3)
            pos += 4
        else
            throw(BigQueryError("unsupported protobuf wire type $wire from the Storage Read API"))
        end
    end
    return nothing
end

const ARROW_COMPRESSION = Dict(:none => 0, :lz4 => 1, :zstd => 2)

"""
CreateReadSessionRequest {
  1: parent
  2: ReadSession { 3: data_format (ARROW = 2), 6: table,
                   8: TableReadOptions { 1: selected_fields*, 2: row_restriction,
                                         3: ArrowSerializationOptions { 2: buffer_compression } } }
  3: max_stream_count
}
"""
function encode_create_read_session(parent::AbstractString, table::AbstractString;
                                    max_streams::Integer = 0, compression::Symbol = :none,
                                    selected_fields = String[], row_restriction::AbstractString = "")
    haskey(ARROW_COMPRESSION, compression) ||
        throw(ArgumentError("compression must be :zstd, :lz4 or :none"))
    options = IOBuffer()
    for name in selected_fields
        pb_bytes(options, 1, codeunits(String(name)))
    end
    pb_string(options, 2, row_restriction)
    codec = ARROW_COMPRESSION[compression]
    if codec != 0
        arrow_options = IOBuffer()
        pb_int(arrow_options, 2, codec)
        pb_bytes(options, 3, take!(arrow_options))
    end
    options_bytes = take!(options)

    session = IOBuffer()
    pb_int(session, 3, 2)
    pb_string(session, 6, table)
    isempty(options_bytes) || pb_bytes(session, 8, options_bytes)

    request = IOBuffer()
    pb_string(request, 1, parent)
    pb_bytes(request, 2, take!(session))
    pb_int(request, 3, max_streams)
    return take!(request)
end

"ReadRowsRequest { 1: read_stream, 2: offset }"
function encode_read_rows(stream::AbstractString, offset::Integer)
    request = IOBuffer()
    pb_string(request, 1, stream)
    pb_int(request, 2, offset)
    return take!(request)
end

struct ReadSession
    name::String
    arrow_schema::Vector{UInt8}
    streams::Vector{String}
    estimated_rows::Int
end

"ReadSession { 1: name, 5: ArrowSchema { 1: serialized_schema }, 10: ReadStream { 1: name }*, 14: estimated_row_count }"
function decode_read_session(buf::Vector{UInt8})
    name = ""
    schema = UInt8[]
    streams = String[]
    estimated = 0
    pb_foreach(buf, 1, length(buf)) do field, wire, value, range
        if field == 1 && wire == 2
            name = String(buf[range])
        elseif field == 5 && wire == 2
            pb_foreach(buf, first(range), last(range)) do f2, w2, v2, r2
                (f2 == 1 && w2 == 2) && (schema = buf[r2])
            end
        elseif field == 10 && wire == 2
            pb_foreach(buf, first(range), last(range)) do f2, w2, v2, r2
                (f2 == 1 && w2 == 2) && push!(streams, String(buf[r2]))
            end
        elseif field == 14 && wire == 0
            estimated = Int(reinterpret(Int64, value))
        end
    end
    return ReadSession(name, schema, streams, estimated)
end

"""
ReadRowsResponse { 4: ArrowRecordBatch { 1: serialized_record_batch, 2: row_count (deprecated) },
                   6: row_count }

Returns `(the byte range of the record batch in buf, the row count)`. The range is
empty if the message has no rows.
"""
function decode_read_rows_response(buf::Vector{UInt8})
    batch = 0:-1
    rows = 0
    legacy_rows = 0
    pb_foreach(buf, 1, length(buf)) do field, wire, value, range
        if field == 4 && wire == 2
            pb_foreach(buf, first(range), last(range)) do f2, w2, v2, r2
                if f2 == 1 && w2 == 2
                    batch = r2
                elseif f2 == 2 && w2 == 0
                    legacy_rows = Int(reinterpret(Int64, v2))
                end
            end
        elseif field == 6 && wire == 0
            rows = Int(reinterpret(Int64, value))
        end
    end
    return batch, (rows == 0 ? legacy_rows : rows)
end
