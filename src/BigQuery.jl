"""
    BigQuery

Query Google BigQuery from Julia and get a `DataFrame` back, or upload one to a
table. Written entirely in Julia: no external tool or other language runtime is
needed.

```julia
using BigQuery

con = bq_connect("my-billing-project")
df  = bq_query(con, "SELECT * FROM `my-project.my_dataset.my_table`")
bq_write_table(con, "my_dataset.my_copy", df)
```

Large results are downloaded through the BigQuery Storage Read API (Arrow over
gRPC), falling back to parallel REST paging when that API is not available.
Uploads are load jobs fed by a gzipped, resumable upload.
"""
module BigQuery

using Dates
using Base64
using UUIDs
import Downloads
using Downloads: Curl
import JSON
import Arrow
import Tables
import DBInterface
using DataFrames: DataFrame
using OpenSSL_jll: libcrypto
using Zlib_jll: libz

export bq_connect, bq_query, bq_read_table, bq_write_table, bq_param, BigQueryError

include("http.jl")
include("auth.jl")
include("connection.jl")
include("schema.jl")
include("jsonrows.jl")
include("arrowrows.jl")
include("proto.jl")
include("grpc.jl")
include("progress.jl")
include("storage.jl")
include("restread.jl")
include("params.jl")
include("query.jl")
include("ndjson.jl")
include("gzip.jl")
include("upload.jl")
include("dbinterface.jl")

end # module
