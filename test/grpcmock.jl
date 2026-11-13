# A scripted gRPC server. It tests the Storage Read API client of BigQuery.jl over
# real HTTP/2 (plain text, "prior knowledge"). The HTTP/2 protocol comes from
# libnghttp2. Julia includes this library (libcurl uses it for HTTP/2). The server
# does not decode protobuf. The responses are byte files that the tests make, and
# the server keeps each request in a log for the tests.
#
# A plan sets the responses: Dict("<key>" => [step, ...]).
#   key  = "CreateReadSession"  or  "ReadRows:<stream name>"
#   step = Dict("file" => "x.bin" or nothing, "status" => 0, "message" => "", "chunk" => 1000)
# Call n for a key sends step n (after the last step, the last step repeats).
#
# The server sends the response body in parts of "chunk" bytes. After each part, it
# writes to the socket and lets the other tasks run. Thus the messages arrive in
# parts, divided at random positions.

module GrpcMock

using Sockets
import HTTP
using nghttp2_jll: libnghttp2

export set_plan!, requests, clear_requests!

const FRAME_DATA = 0x00
const FRAME_HEADERS = 0x01
const FLAG_END_STREAM = 0x01
const DATA_FLAG_EOF = UInt32(0x01)
const DATA_FLAG_NO_END_STREAM = UInt32(0x02)
const ERR_DEFERRED = -508
const ERR_CALLBACK_FAILURE = -902

# nghttp2_nv: one header field.
struct NV
    name::Ptr{UInt8}
    value::Ptr{UInt8}
    namelen::Csize_t
    valuelen::Csize_t
    flags::UInt8
end

# nghttp2_data_provider: the source of a response body.
struct DataProvider
    source::Ptr{Cvoid}
    read_callback::Ptr{Cvoid}
end

mutable struct Stream
    id::Int32
    headers::Dict{String,String}
    body::Vector{UInt8}       # the request body
    data::Vector{UInt8}       # the response body
    sent::Int                 # the bytes of `data` that the server has sent
    chunk::Int
    status::Int
    message::String
    round_done::Bool          # the stream has sent one part in this round
    deferred::Bool            # the stream waits for the next round
end

Stream(id::Int32) = Stream(id, Dict{String,String}(), UInt8[], UInt8[], 0, 4096, 0, "", false, false)

mutable struct Connection
    server::Any
    socket::TCPSocket
    session::Ptr{Cvoid}
    streams::Dict{Int32,Stream}
    complete::Vector{Int32}   # the streams with a complete request that has no response yet
    wake::Base.Condition      # the writer task waits on this condition
    closed::Bool
    error::Any
end

mutable struct Server
    dir::String
    listener::Sockets.TCPServer
    port::Int
    plan::Dict{String,Any}
    counters::Dict{String,Int}
    log::Vector{Dict{String,Any}}
    connections::Vector{Connection}
    callbacks::Ptr{Cvoid}
end

"Set a new plan, start the call counts again, and clear the log."
function set_plan!(s::Server, calls::AbstractDict)
    s.plan = Dict{String,Any}(String(k) => v for (k, v) in calls)
    empty!(s.counters)
    empty!(s.log)
    return nothing
end

"A copy of the request log."
requests(s::Server) = copy(s.log)

clear_requests!(s::Server) = (empty!(s.log); nothing)

# --- nghttp2 callbacks ---------------------------------------------------------
#
# nghttp2 calls these functions in nghttp2_session_mem_recv and nghttp2_session_mem_send.
# They must not throw an error and must not let other tasks run, because a different
# task can then use the same session. Thus they only record data. The tasks of the
# connection do the remaining work after the nghttp2 call.

const READ_CALLBACK = Ref{Ptr{Cvoid}}(C_NULL)

# The frame header (nghttp2_frame_hd) is at the start of each frame:
# size_t length; int32_t stream_id; uint8_t type; uint8_t flags.
frame_stream_id(frame::Ptr{Cvoid}) = unsafe_load(Ptr{Int32}(frame + sizeof(Csize_t)))
frame_type(frame::Ptr{Cvoid}) = unsafe_load(Ptr{UInt8}(frame + sizeof(Csize_t) + 4))
frame_flags(frame::Ptr{Cvoid}) = unsafe_load(Ptr{UInt8}(frame + sizeof(Csize_t) + 5))

connection(user_data::Ptr{Cvoid}) = unsafe_pointer_to_objref(user_data)::Connection

function callback_failure(user_data::Ptr{Cvoid}, e)
    connection(user_data).error = e
    return Cint(ERR_CALLBACK_FAILURE)
end

function on_header(session::Ptr{Cvoid}, frame::Ptr{Cvoid}, name::Ptr{UInt8}, namelen::Csize_t,
                   value::Ptr{UInt8}, valuelen::Csize_t, flags::UInt8, user_data::Ptr{Cvoid})::Cint
    try
        c = connection(user_data)
        id = frame_stream_id(frame)
        stream = get!(() -> Stream(id), c.streams, id)
        stream.headers[unsafe_string(name, namelen)] = unsafe_string(value, valuelen)
        return 0
    catch e
        return callback_failure(user_data, e)
    end
end

function on_data_chunk(session::Ptr{Cvoid}, flags::UInt8, id::Int32, data::Ptr{UInt8}, len::Csize_t,
                       user_data::Ptr{Cvoid})::Cint
    try
        stream = get(connection(user_data).streams, id, nothing)
        stream === nothing || append!(stream.body, unsafe_wrap(Array, data, len))
        return 0
    catch e
        return callback_failure(user_data, e)
    end
end

function on_frame_recv(session::Ptr{Cvoid}, frame::Ptr{Cvoid}, user_data::Ptr{Cvoid})::Cint
    try
        c = connection(user_data)
        t = frame_type(frame)
        id = frame_stream_id(frame)
        if (t == FRAME_DATA || t == FRAME_HEADERS) && (frame_flags(frame) & FLAG_END_STREAM) != 0 &&
           haskey(c.streams, id)
            push!(c.complete, id)
        end
        return 0
    catch e
        return callback_failure(user_data, e)
    end
end

function on_stream_close(session::Ptr{Cvoid}, id::Int32, error_code::UInt32, user_data::Ptr{Cvoid})::Cint
    try
        delete!(connection(user_data).streams, id)
        return 0
    catch e
        return callback_failure(user_data, e)
    end
end

# Give nghttp2 the next part of a response body. Each stream gives one part in each
# round. After that, it defers until the writer task starts the next round.
function read_data(session::Ptr{Cvoid}, id::Int32, buf::Ptr{UInt8}, maxlen::Csize_t, data_flags::Ptr{UInt32},
                   source::Ptr{Cvoid}, user_data::Ptr{Cvoid})::Cssize_t
    try
        stream = connection(user_data).streams[id]
        if stream.round_done
            stream.deferred = true
            return ERR_DEFERRED
        end
        n = min(Int(maxlen), stream.chunk, length(stream.data) - stream.sent)
        n > 0 && GC.@preserve stream unsafe_copyto!(buf, pointer(stream.data, stream.sent + 1), n)
        stream.sent += n
        stream.round_done = true
        if stream.sent >= length(stream.data)
            unsafe_store!(data_flags, DATA_FLAG_EOF | DATA_FLAG_NO_END_STREAM)
            with_nv(["grpc-status" => string(stream.status), "grpc-message" => HTTP.escapeuri(stream.message)]) do nv
                ccall((:nghttp2_submit_trailer, libnghttp2), Cint, (Ptr{Cvoid}, Int32, Ptr{NV}, Csize_t),
                      session, id, nv, length(nv))
            end
        end
        return n
    catch e
        return callback_failure(user_data, e)
    end
end

function make_callbacks()
    ptr = Ref{Ptr{Cvoid}}(C_NULL)
    ccall((:nghttp2_session_callbacks_new, libnghttp2), Cint, (Ptr{Ptr{Cvoid}},), ptr) == 0 ||
        error("nghttp2_session_callbacks_new failed")
    cbs = ptr[]
    ccall((:nghttp2_session_callbacks_set_on_header_callback, libnghttp2), Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), cbs,
          @cfunction(on_header, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Ptr{UInt8}, Csize_t, UInt8, Ptr{Cvoid})))
    ccall((:nghttp2_session_callbacks_set_on_data_chunk_recv_callback, libnghttp2), Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), cbs,
          @cfunction(on_data_chunk, Cint, (Ptr{Cvoid}, UInt8, Int32, Ptr{UInt8}, Csize_t, Ptr{Cvoid})))
    ccall((:nghttp2_session_callbacks_set_on_frame_recv_callback, libnghttp2), Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), cbs,
          @cfunction(on_frame_recv, Cint, (Ptr{Cvoid}, Ptr{Cvoid}, Ptr{Cvoid})))
    ccall((:nghttp2_session_callbacks_set_on_stream_close_callback, libnghttp2), Cvoid, (Ptr{Cvoid}, Ptr{Cvoid}), cbs,
          @cfunction(on_stream_close, Cint, (Ptr{Cvoid}, Int32, UInt32, Ptr{Cvoid})))
    READ_CALLBACK[] = @cfunction(read_data, Cssize_t,
        (Ptr{Cvoid}, Int32, Ptr{UInt8}, Csize_t, Ptr{UInt32}, Ptr{Cvoid}, Ptr{Cvoid}))
    return cbs
end

"Call `f` with the header fields as a vector of `NV`. nghttp2 copies the fields."
function with_nv(f, fields::Vector{Pair{String,String}})
    nv = NV[NV(pointer(k), pointer(v), sizeof(k), sizeof(v), 0x00) for (k, v) in fields]
    return GC.@preserve fields nv f(nv)
end

function submit_response(c::Connection, id::Int32, fields::Vector{Pair{String,String}}, with_body::Bool)
    provider = Ref(DataProvider(C_NULL, READ_CALLBACK[]))
    rv = with_nv(fields) do nv
        ccall((:nghttp2_submit_response, libnghttp2), Cint, (Ptr{Cvoid}, Int32, Ptr{NV}, Csize_t, Ptr{DataProvider}),
              c.session, id, nv, length(nv), with_body ? provider : Ptr{DataProvider}(C_NULL))
    end
    rv == 0 || error("nghttp2_submit_response failed ($rv)")
    return nothing
end

# --- requests and responses ------------------------------------------------------

"Record a complete request in the log, and send the response that the plan gives."
function answer!(c::Connection, stream::Stream)
    s = c.server
    h = stream.headers
    path = get(h, ":path", "")
    method = String(last(split(path, '/')))
    routing = get(h, "x-goog-request-params", "")
    key = method == "ReadRows" ? "ReadRows:" * HTTP.unescapeuri(replace(routing, r"^read_stream=" => "")) : method
    body = stream.body
    push!(s.log, Dict{String,Any}(
        "method" => method,
        "path" => path,
        "contentType" => get(h, "content-type", nothing),
        "te" => get(h, "te", nothing),
        "authorization" => get(h, "authorization", nothing),
        "userProject" => get(h, "x-goog-user-project", nothing),
        "routing" => routing,
        "flag" => isempty(body) ? nothing : Int(body[1]),
        "declaredLength" => length(body) >= 5 ? Int(ntoh(reinterpret(UInt32, body[2:5])[1])) : nothing,
        "message" => length(body) >= 5 ? body[6:end] : UInt8[],
    ))

    steps = get(s.plan, key, nothing)
    if steps === nothing
        # A response with only headers: the status is in the first and only HEADERS frame.
        submit_response(c, stream.id, [":status" => "200", "content-type" => "application/grpc",
                                       "grpc-status" => "5", "grpc-message" => HTTP.escapeuri("no plan for " * key)], false)
        return nothing
    end
    n = get(s.counters, key, 0)
    s.counters[key] = n + 1
    step = steps[min(n + 1, length(steps))]
    file = get(step, "file", nothing)
    stream.data = file === nothing ? UInt8[] : read(joinpath(s.dir, file))
    stream.chunk = something(get(step, "chunk", nothing), 4096)
    stream.status = something(get(step, "status", nothing), 0)
    stream.message = something(get(step, "message", nothing), "")
    submit_response(c, stream.id, [":status" => "200", "content-type" => "application/grpc"], true)
    return nothing
end

"Send all the bytes that nghttp2 has ready to the socket."
function flush!(c::Connection)
    out = IOBuffer()
    ptr = Ref{Ptr{UInt8}}(C_NULL)
    while true
        n = ccall((:nghttp2_session_mem_send, libnghttp2), Cssize_t, (Ptr{Cvoid}, Ptr{Ptr{UInt8}}), c.session, ptr)
        n < 0 && error("nghttp2_session_mem_send failed ($n)")
        n == 0 && break
        unsafe_write(out, ptr[], n)
    end
    bytes = take!(out)
    isempty(bytes) || write(c.socket, bytes)
    return nothing
end

want_write(c::Connection) = ccall((:nghttp2_session_want_write, libnghttp2), Cint, (Ptr{Cvoid},), c.session) != 0

"The writer task: send the ready bytes, then start the next round of the deferred streams."
function writer(c::Connection)
    try
        while !c.closed
            flush!(c)
            more = false
            for stream in collect(values(c.streams))
                stream.round_done = false
                if stream.deferred
                    stream.deferred = false
                    ccall((:nghttp2_session_resume_data, libnghttp2), Cint, (Ptr{Cvoid}, Int32), c.session, stream.id)
                    more = true
                end
            end
            if more
                yield()
            elseif !want_write(c) && !c.closed
                wait(c.wake)
            end
        end
    catch e
        c.closed || (c.error = e)
    finally
        c.closed = true
    end
end

"The reader task: give the received bytes to nghttp2, then answer the complete requests."
function reader(c::Connection)
    try
        while !eof(c.socket)
            data = readavailable(c.socket)
            n = GC.@preserve data ccall((:nghttp2_session_mem_recv, libnghttp2), Cssize_t,
                                        (Ptr{Cvoid}, Ptr{UInt8}, Csize_t), c.session, data, length(data))
            n < 0 && break
            while !isempty(c.complete)
                stream = get(c.streams, popfirst!(c.complete), nothing)
                stream === nothing || answer!(c, stream)
            end
            notify(c.wake)
        end
    catch e
        e isa Base.IOError || c.closed || (c.error = e)
    finally
        c.closed = true
        notify(c.wake)
    end
end

function serve(s::Server, socket::TCPSocket)
    c = Connection(s, socket, C_NULL, Dict{Int32,Stream}(), Int32[], Base.Condition(), false, nothing)
    push!(s.connections, c)
    ptr = Ref{Ptr{Cvoid}}(C_NULL)
    ccall((:nghttp2_session_server_new, libnghttp2), Cint, (Ptr{Ptr{Cvoid}}, Ptr{Cvoid}, Ptr{Cvoid}),
          ptr, s.callbacks, pointer_from_objref(c)) == 0 || error("nghttp2_session_server_new failed")
    c.session = ptr[]
    try
        ccall((:nghttp2_submit_settings, libnghttp2), Cint, (Ptr{Cvoid}, UInt8, Ptr{Cvoid}, Csize_t),
              c.session, 0x00, C_NULL, 0)
        w = @async writer(c)
        reader(c)
        wait(w)
    finally
        close(socket)
        ccall((:nghttp2_session_del, libnghttp2), Cvoid, (Ptr{Cvoid},), c.session)
        c.session = C_NULL
    end
    c.error === nothing || @error "gRPC mock server: connection failed" exception = c.error
    return nothing
end

"""
    Server(dir)

Start a server on a free port of 127.0.0.1. The files of the plan steps are in `dir`.
The tasks of the server run on the thread of the caller.
"""
function Server(dir::AbstractString)
    listener = listen(Sockets.localhost, 0)
    port = Int(getsockname(listener)[2])
    s = Server(String(dir), listener, port, Dict{String,Any}(), Dict{String,Int}(), Dict{String,Any}[],
               Connection[], make_callbacks())
    @async begin
        while isopen(s.listener)
            socket = try
                accept(s.listener)
            catch
                break
            end
            @async try
                serve(s, socket)
            catch e
                @error "gRPC mock server failed" exception = (e, catch_backtrace())
            end
        end
    end
    return s
end

"Stop the server and close its connections."
function stop!(s::Server)
    close(s.listener)
    for c in s.connections
        c.closed = true
        close(c.socket)
        notify(c.wake)
    end
    return nothing
end

end # module
