# A minimal gRPC client: unary calls and server-stream calls over the HTTP/2 of libcurl.
#
# A gRPC call is an HTTP/2 POST request. Its body contains protobuf messages, each
# with a length prefix. The status of the call is in the trailers. libcurl (in Julia,
# through the Downloads standard library) supports HTTP/2 and trailers. Thus this
# file only adds and removes the 5-byte message frames.

struct GrpcError <: Exception
    code::Int
    message::String
end

const GRPC_CODE_NAMES = Dict(
    1 => "CANCELLED", 2 => "UNKNOWN", 3 => "INVALID_ARGUMENT", 4 => "DEADLINE_EXCEEDED",
    5 => "NOT_FOUND", 6 => "ALREADY_EXISTS", 7 => "PERMISSION_DENIED", 8 => "RESOURCE_EXHAUSTED",
    9 => "FAILED_PRECONDITION", 10 => "ABORTED", 11 => "OUT_OF_RANGE", 12 => "UNIMPLEMENTED",
    13 => "INTERNAL", 14 => "UNAVAILABLE", 15 => "DATA_LOSS", 16 => "UNAUTHENTICATED")

grpc_code_name(code::Integer) = get(GRPC_CODE_NAMES, Int(code), "code $code")

Base.showerror(io::IO, e::GrpcError) =
    print(io, "BigQuery Storage API error (", grpc_code_name(e.code), "): ", e.message)

# Temporary conditions. After one of these errors, the client can try the call again.
grpc_retryable(e::GrpcError) = e.code in (4, 8, 10, 13, 14)

"""
Gets the response body from libcurl and cuts it into gRPC messages. Gives each
complete message to `on_message`.
"""
mutable struct GrpcSink <: IO
    buf::Vector{UInt8}
    on_message::Any
    error::Any
    interrupt::Base.Event
end
GrpcSink(on_message) = GrpcSink(UInt8[], on_message, nothing, Base.Event())

function Base.unsafe_write(sink::GrpcSink, p::Ptr{UInt8}, nb::UInt)
    sink.error === nothing || return nb      # a failure occurred before: accept the data and ignore it
    old = length(sink.buf)
    resize!(sink.buf, old + nb)
    GC.@preserve sink unsafe_copyto!(pointer(sink.buf, old + 1), p, nb)
    try
        consume_frames!(sink)
    catch e
        sink.error = e
        empty!(sink.buf)
        notify(sink.interrupt)               # stop the transfer
    end
    return nb
end

Base.write(sink::GrpcSink, data::Vector{UInt8}) =
    GC.@preserve data Base.unsafe_write(sink, pointer(data), UInt(length(data)))
Base.write(sink::GrpcSink, b::UInt8) = write(sink, UInt8[b])

function consume_frames!(sink::GrpcSink)
    buf = sink.buf
    pos = 1
    n = length(buf)
    while n - pos + 1 >= 5
        buf[pos] == 0x00 || throw(GrpcError(13, "received a compressed gRPC message, which was not requested"))
        len = (Int(buf[pos+1]) << 24) | (Int(buf[pos+2]) << 16) | (Int(buf[pos+3]) << 8) | Int(buf[pos+4])
        n - pos + 1 >= 5 + len || break
        sink.on_message(buf[pos+5:pos+4+len])
        pos += 5 + len
    end
    pos > 1 && deleteat!(buf, 1:pos-1)
    return nothing
end

function grpc_frame(message::Vector{UInt8})
    len = length(message)
    frame = Vector{UInt8}(undef, 5 + len)
    frame[1] = 0x00
    frame[2] = (len >> 24) % UInt8
    frame[3] = (len >> 16) % UInt8
    frame[4] = (len >> 8) % UInt8
    frame[5] = len % UInt8
    copyto!(frame, 6, message, 1, len)
    return frame
end

# Downloads 1.7 added `interrupt` (cancel a request that is in progress).
const DOWNLOADS_HAS_INTERRUPT =
    any(m -> :interrupt in Base.kwarg_decl(m), methods(Downloads.request))

"""
A set of calls in progress that the package can cancel together. If one read
stream fails, or if the user pushes Ctrl-C, the package uses it to stop the other
streams.
"""
mutable struct CancelGroup
    lock::ReentrantLock
    events::Vector{Base.Event}
    cancelled::Bool
end
CancelGroup() = CancelGroup(ReentrantLock(), Base.Event[], false)

function register!(group::CancelGroup, ev::Base.Event)
    lock(group.lock)
    try
        push!(group.events, ev)
        group.cancelled && notify(ev)
    finally
        unlock(group.lock)
    end
    return nothing
end

function unregister!(group::CancelGroup, ev::Base.Event)
    lock(group.lock)
    try
        filter!(e -> e !== ev, group.events)
    finally
        unlock(group.lock)
    end
    return nothing
end

function cancel!(group::CancelGroup)
    lock(group.lock)
    try
        group.cancelled = true
        foreach(notify, group.events)
    finally
        unlock(group.lock)
    end
    return nothing
end

function header_value(headers, name::AbstractString)
    value = nothing
    for (k, v) in headers
        k == name && (value = v)     # the trailers come last, thus use the last value
    end
    return value
end

"""
    grpc_call(on_message, dl, url, token, request; routing, user_project, cancel)

Send one protobuf `request` to a gRPC method with a POST request. Call
`on_message(bytes)` for each response message. Throw `GrpcError` if the call does
not end with the status OK. If `user_project` is not empty, the request sends it in
the `x-goog-user-project` header.
"""
function grpc_call(on_message, dl::Downloads.Downloader, url::AbstractString, token::AbstractString,
                   request::Vector{UInt8}; routing::AbstractString = "",
                   user_project::AbstractString = "",
                   cancel::Union{Nothing,CancelGroup} = nothing)
    sink = GrpcSink(on_message)
    headers = Pair{String,String}[
        "content-type" => GRPC_CONTENT_TYPE,
        "te" => "trailers",
        "grpc-accept-encoding" => "identity",
        "user-agent" => USER_AGENT,
        "authorization" => "Bearer " * token,
    ]
    isempty(routing) || push!(headers, "x-goog-request-params" => routing)
    isempty(user_project) || push!(headers, "x-goog-user-project" => String(user_project))
    cancel === nothing || register!(cancel, sink.interrupt)
    kwargs = DOWNLOADS_HAS_INTERRUPT ? (; interrupt = sink.interrupt) : (;)
    resp = try
        with_request_body(grpc_frame(request)) do
            Downloads.request(url; method = "POST", output = sink, headers = headers,
                              downloader = dl, throw = false, kwargs...)
        end
    catch e
        e isa InterruptException && rethrow()
        sink.error === nothing || throw(sink.error)
        (cancel !== nothing && cancel.cancelled) && throw(GrpcError(1, "cancelled"))
        throw(GrpcError(14, "transport failure: " * sprint(showerror, e)))
    finally
        cancel === nothing || unregister!(cancel, sink.interrupt)
    end
    sink.error === nothing || throw(sink.error)
    (cancel !== nothing && cancel.cancelled) && throw(GrpcError(1, "cancelled"))
    if resp isa Downloads.RequestError
        throw(GrpcError(14, "transport failure: " * resp.message))
    end
    status = header_value(resp.headers, "grpc-status")
    if status === nothing
        code = resp.status == 401 ? 16 : resp.status == 403 ? 7 : resp.status == 404 ? 12 : 14
        throw(GrpcError(code, "the server did not answer with gRPC (HTTP $(resp.status)); " *
                              "a proxy that does not support HTTP/2 can cause this"))
    end
    code = something(tryparse(Int, status), 2)
    if code != 0
        message = something(header_value(resp.headers, "grpc-message"), "")
        throw(GrpcError(code, unescapeuri(message)))
    end
    isempty(sink.buf) || throw(GrpcError(14, "the response stream ended in the middle of a message"))
    return nothing
end
