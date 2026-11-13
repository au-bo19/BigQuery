# Gzip compression for uploads, with the zlib library that Julia includes. This file
# has only the functions that an upload uses: one stream that the package writes in parts.

const Z_NO_FLUSH = Cint(0)
const Z_FINISH = Cint(4)
const Z_OK = Cint(0)
const Z_STREAM_END = Cint(1)
const Z_BUF_ERROR = Cint(-5)
const Z_DEFLATED = Cint(8)

# Fast compression, not the smallest output. At this level, the compression is faster
# than most connections. The higher levels make JSON rows only a little smaller.
const GZIP_LEVEL = Cint(1)

# The `z_stream` of zlib. zlib keeps a pointer to it, thus it must not move in memory.
# A mutable struct does not move, because the Julia garbage collector does not move objects.
mutable struct ZStream
    next_in::Ptr{UInt8}
    avail_in::Cuint
    total_in::Culong
    next_out::Ptr{UInt8}
    avail_out::Cuint
    total_out::Culong
    msg::Ptr{UInt8}
    state::Ptr{Cvoid}
    zalloc::Ptr{Cvoid}
    zfree::Ptr{Cvoid}
    opaque::Ptr{Cvoid}
    data_type::Cint
    adler::Culong
    reserved::Culong
    ZStream() = new(C_NULL, 0, 0, C_NULL, 0, 0, C_NULL, C_NULL, C_NULL, C_NULL, C_NULL, 0, 0, 0)
end

mutable struct GzipWriter
    stream::ZStream
    scratch::Vector{UInt8}
    isopen::Bool
end

function GzipWriter(level::Integer = GZIP_LEVEL)
    stream = ZStream()
    version = unsafe_string(ccall((:zlibVersion, libz), Cstring, ()))
    # Window bits 15 + 16: the largest window, with a gzip wrapper (not a zlib wrapper).
    code = ccall((:deflateInit2_, libz), Cint, (Ref{ZStream}, Cint, Cint, Cint, Cint, Cint, Cstring, Cint),
                 stream, level, Z_DEFLATED, 15 + 16, 8, 0, version, sizeof(ZStream))
    code == Z_OK || error("zlib could not start compressing (code $code)")
    return GzipWriter(stream, Vector{UInt8}(undef, 64 * 1024), true)
end

function Base.close(gz::GzipWriter)
    gz.isopen || return nothing
    gz.isopen = false
    ccall((:deflateEnd, libz), Cint, (Ref{ZStream},), gz.stream)
    return nothing
end

"""
Compress `input` and add the zlib output to `out`. zlib keeps some bytes until it
gets more input. `finish = true` ends the stream and writes all the remaining bytes.
"""
function deflate!(out::IO, gz::GzipWriter, input::Vector{UInt8}; finish::Bool = false)
    gz.isopen || error("this gzip stream has been closed")
    z = gz.stream
    scratch = gz.scratch
    GC.@preserve input scratch begin
        z.next_in = pointer(input)
        z.avail_in = length(input)
        while true
            z.next_out = pointer(scratch)
            z.avail_out = length(scratch)
            code = ccall((:deflate, libz), Cint, (Ref{ZStream}, Cint), z, finish ? Z_FINISH : Z_NO_FLUSH)
            (code == Z_OK || code == Z_STREAM_END || code == Z_BUF_ERROR) ||
                error("zlib failed while compressing (code $code)")
            produced = length(scratch) - Int(z.avail_out)
            produced > 0 && unsafe_write(out, pointer(scratch), UInt(produced))
            if finish
                code == Z_STREAM_END && break
                produced == 0 && error("zlib stopped before the end of the stream")
            elseif z.avail_out != 0
                break       # Space remains in the buffer, thus zlib has no more output now.
            end
        end
    end
    return nothing
end

"`data` as a complete gzip file."
function gzip_bytes(data::Vector{UInt8})
    gz = GzipWriter()
    out = IOBuffer()
    try
        deflate!(out, gz, data; finish = true)
    finally
        close(gz)
    end
    return take!(out)
end
