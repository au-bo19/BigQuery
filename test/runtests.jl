using Test
using Dates, Base64
using DataFrames
using BigQuery
import Arrow, JSON, Tables, DBInterface, Downloads, CodecZlib

const B = BigQuery

include("fixtures.jl")
using .Fixtures
include("mockbq.jl")
using .MockBQ
include("grpcmock.jl")
using .GrpcMock: GrpcMock

# The expected form of a returned column. If the column has no missing value, its
# element type does not include Missing.
narrow(c) = any(ismissing, c) ? c : convert(Vector{nonmissingtype(eltype(c))}, c)

# Gzip decompression with a different implementation (not the gzip code of the package).
gunzip(data) = transcode(CodecZlib.GzipDecompressor, Vector{UInt8}(data))
is_gzip(data) = ncodeunits(data) >= 2 && codeunit(data, 1) == 0x1f && codeunit(data, 2) == 0x8b
is_gzip(data::Vector{UInt8}) = length(data) >= 2 && data[1] == 0x1f && data[2] == 0x8b

function check_table(df, truth)
    @test names(df) == [string(k) for k in keys(truth)]
    for (name, expected) in pairs(truth)
        expected = narrow(expected)
        actual = df[!, name]
        @test typeof(actual) == typeof(expected)
        @test isequal(actual, expected)
    end
end

@testset "BigQuery.jl" begin
    include("test_decoding.jl")
    include("test_proto_grpc.jl")
    include("test_auth.jl")
    include("test_rest.jl")
    include("test_upload.jl")
    include("test_storage.jl")
    include("test_live.jl")
end
