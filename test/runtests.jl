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

# What a column should look like once returned: Missing dropped from the element
# type when the column holds no missing value.
narrow(c) = any(ismissing, c) ? c : convert(Vector{nonmissingtype(eltype(c))}, c)

# Gzip, undone by an implementation other than the package's own.
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
