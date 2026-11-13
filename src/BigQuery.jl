"""
    BigQuery

Use this module to query Google BigQuery from Julia. A query gives a `DataFrame`.
You can also upload a table to BigQuery. The module is written only in Julia. It
does not use an external tool or a runtime of a different language.

```julia
using BigQuery

con = bq_connect("my-billing-project")
df  = bq_query(con, "SELECT * FROM `my-project.my_dataset.my_table`")
bq_write_table(con, "my_dataset.my_copy", df)
```

The Storage Read API (Arrow through gRPC) downloads large results. If that API is
not available, the module downloads the results as REST pages, in parallel. An
upload is a load job. The module sends the rows to the load job through a
resumable upload, with gzip compression.
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
