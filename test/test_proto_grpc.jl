# Protobuf encoders for the server messages, for the tests only (the package only decodes these messages).
function encode_read_session(name, schema, streams, estimated_rows)
    io = IOBuffer()
    B.pb_string(io, 1, name)
    inner = IOBuffer()
    B.pb_bytes(inner, 1, schema)
    B.pb_bytes(io, 5, take!(inner))
    for stream in streams
        inner = IOBuffer()
        B.pb_string(inner, 1, stream)
        B.pb_bytes(io, 10, take!(inner))
    end
    B.pb_int(io, 14, estimated_rows)
    return take!(io)
end

function encode_read_rows_response(batch, row_count)
    io = IOBuffer()
    # First a stats message (field 2), as the real server sends. The decoder must ignore it.
    B.pb_bytes(io, 2, UInt8[0x12, 0x09, 0x09, 0, 0, 0, 0, 0, 0, 0xe0, 0x3f])
    inner = IOBuffer()
    B.pb_bytes(inner, 1, batch)
    B.pb_bytes(io, 4, take!(inner))
    B.pb_int(io, 6, row_count)
    return take!(io)
end

@testset "protobuf" begin
    # The expected bytes, written manually from the wire format specification.
    @test B.encode_read_rows("s", 0) == UInt8[0x0a, 0x01, 0x73]
    @test B.encode_read_rows("s", 300) == UInt8[0x0a, 0x01, 0x73, 0x10, 0xac, 0x02]
    @test B.encode_create_read_session("p", "t"; max_streams = 3, compression = :none) ==
          UInt8[0x0a, 0x01, 0x70, 0x12, 0x05, 0x18, 0x02, 0x32, 0x01, 0x74, 0x18, 0x03]
    @test B.encode_create_read_session("p", "t"; max_streams = 0, compression = :zstd,
                                       selected_fields = ["a"], row_restriction = "x>1") ==
          UInt8[0x0a, 0x01, 0x70,
                0x12, 0x13,
                  0x18, 0x02,
                  0x32, 0x01, 0x74,
                  0x42, 0x0c,
                    0x0a, 0x01, 0x61,
                    0x12, 0x03, 0x78, 0x3e, 0x31,
                    0x1a, 0x02, 0x10, 0x02]
    @test_throws ArgumentError B.encode_create_read_session("p", "t"; compression = :gzip)

    schema = rand(UInt8, 300)
    session = B.decode_read_session(encode_read_session("projects/p/locations/us/sessions/x", schema, ["s/1", "s/2"], 123456))
    @test session.name == "projects/p/locations/us/sessions/x"
    @test session.arrow_schema == schema
    @test session.streams == ["s/1", "s/2"]
    @test session.estimated_rows == 123456
    empty = B.decode_read_session(UInt8[])
    @test isempty(empty.streams) && isempty(empty.arrow_schema)

    batch = rand(UInt8, 70_000)
    msg = encode_read_rows_response(batch, 4321)
    range, rows = B.decode_read_rows_response(msg)
    @test msg[range] == batch && rows == 4321
    range, rows = B.decode_read_rows_response(UInt8[0x30, 0x00])     # no record batch
    @test isempty(range)
    @test_throws BigQueryError B.decode_read_rows_response(msg[1:end-10])   # the message is not complete

    # Negative int64 values use the 10-byte varint format.
    io = IOBuffer()
    B.pb_int(io, 2, -1)
    @test take!(io) == vcat(UInt8[0x10], fill(0xff, 9), UInt8[0x01])
end

@testset "gRPC framing" begin
    messages = [rand(UInt8, n) for n in (0, 1, 5, 4096, 70_000)]
    wire = reduce(vcat, [B.grpc_frame(m) for m in messages])
    @test B.grpc_frame(UInt8[1, 2, 3]) == UInt8[0, 0, 0, 0, 3, 1, 2, 3]
    for chunk in (1, 2, 5, 7, 4096, 16_384, length(wire))
        got = Vector{UInt8}[]
        sink = B.GrpcSink(m -> push!(got, m))
        for start in 1:chunk:length(wire)
            write(sink, wire[start:min(start + chunk - 1, length(wire))])
        end
        @test got == messages
        @test isempty(sink.buf) && sink.error === nothing
    end

    # A message that is not complete at the end stays in the buffer. The caller finds it.
    sink = B.GrpcSink(m -> nothing)
    write(sink, wire[1:end-1])
    @test !isempty(sink.buf)

    # If the consumer fails, the stream stops, and the sink keeps the error.
    sink = B.GrpcSink(m -> error("boom"))
    write(sink, wire)
    @test sink.error isa ErrorException
    @test sink.interrupt.set
    # The client did not ask for compressed messages.
    sink = B.GrpcSink(m -> nothing)
    write(sink, UInt8[1, 0, 0, 0, 1, 9])
    @test sink.error isa B.GrpcError

    @test B.unescapeuri("Permission%20denied%3A%20x%2Fy 100%") == "Permission denied: x/y 100%"
    @test B.escapeuri("projects/p-1/datasets/d_2/tables/t.3 x") == "projects%2Fp-1%2Fdatasets%2Fd_2%2Ftables%2Ft.3%20x"
end
