# BigQuery.jl

## Overview

BigQuery.jl is a client for Google BigQuery, providing a small set of
functions that take you from SQL to a `DataFrame`, and from a `DataFrame` back
to a table:

- `bq_connect()` opens a connection to a billing project using your Google
  credentials.
- `bq_query()` runs a query and returns the whole result as a `DataFrame`.
- `bq_read_table()` downloads a table directly, without running a query.
- `bq_write_table()` uploads a `DataFrame`, or any other table, to a BigQuery
  table.
- `bq_param()` gives a query parameter an explicit BigQuery type.

These work the same way whether a table has ten rows or ten million. The
package also implements
[DBInterface.jl](https://github.com/JuliaDatabases/DBInterface.jl), so
`DBInterface.connect()` and `DBInterface.execute()` work as they do for other
databases.

BigQuery.jl is written entirely in Julia. It needs no other language runtime,
no command-line tool and no storage bucket; the only compiled libraries it
uses, libcurl, OpenSSL and zlib, ship with Julia. Managing datasets is not covered.

This is version 0.1. Its test suite runs the whole client against local
imitations of BigQuery's APIs, but it has not yet been run against a real
BigQuery project; see [Checking your setup](#checking-your-setup) before
relying on it.

## Download methods

Results of up to 10,000 rows come back with the query itself. Larger results
are downloaded in bulk, by one of two methods:

- [Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage)
  (`method = :arrow`) streams rows as compressed Arrow record batches over
  several parallel gRPC connections. It is the fast path, and needs the
  `bigquery.readsessions.create` permission on the billing project plus HTTP/2
  access to `bigquerystorage.googleapis.com`.

- [REST paging](https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list)
  (`method = :rest`) fetches and decodes JSON pages on several tasks at once.
  It is slower, but needs nothing beyond the permission to run the query.

The default, `method = :auto`, uses the Storage Read API and switches to REST
paging with a warning when that is not possible. Both return identical data
frames.

## Uploads

`bq_write_table()` sends a table to BigQuery as a
[load job](https://cloud.google.com/bigquery/docs/loading-data-local). Rows are
encoded as newline-delimited JSON, gzipped, and uploaded in chunks of 8 MB, so
a table of any size can be sent without holding its encoded form in memory,
and an interrupted connection resumes where it stopped. Load jobs are free,
and they are atomic: either every row arrives or the table is left as it was.

Compression makes the upload several times smaller: a million rows of four
columns are 90 MB as JSON and 14 MB on the wire. BigQuery accepts a compressed
upload of at most 4 GB, against 5 TB uncompressed, so a table that would be
larger than that even when compressed is sent uncompressed instead; you do not
need to do anything. `compress = false` turns compression off.

It needs the `bigquery.jobs.create` permission on the billing project and
permission to create or update the table (the "BigQuery Data Editor" role on
the dataset covers the latter). The dataset must already exist.

## Installation

``` julia
# BigQuery.jl is not in the General registry yet. Install it from its folder:
using Pkg
Pkg.develop(path = "path/to/bq-jl")

# Or from a Git repository:
# Pkg.add(url = "https://github.com/<user>/BigQuery.jl")
```

Julia 1.10 or newer is required. Development and testing so far have been on
Julia 1.13 on Windows.

### Checking your setup

The tests need no Google account. To also run them against the real service,
name a project you can run queries in; this bills a few megabytes of queries to
it and compares both download methods on a 300,000 row result. To test uploads
as well, also name a dataset you can create tables in; one table is created
there, about 25 MB is uploaded to it, and it is dropped again.

``` julia
# ENV["BIGQUERY_TEST_PROJECT"] = "my-project"
# ENV["BIGQUERY_TEST_DATASET"] = "my_dataset"      # optional: test uploads too
using Pkg
Pkg.test("BigQuery")
```

## Usage

``` julia
using BigQuery

# Credentials are discovered automatically: the GOOGLE_APPLICATION_CREDENTIALS
# variable, a `gcloud auth application-default login`, or the service account
# of a Google Cloud VM.
con = bq_connect("my-project")
con
#> BigQuery.Connection(project = "my-project", auth = user credentials (refresh token))

# Or point to a service account key, or pass an access token:
# con = bq_connect("my-project"; credentials = "path/to/key.json")
# con = bq_connect("my-project"; token = "ya29...")

bq_query(con, """
    SELECT country, COUNT(*) AS orders, ROUND(AVG(amount), 2) AS avg_amount
    FROM shop.orders
    GROUP BY country
    ORDER BY orders DESC
""")
#> 4×3 DataFrame
#>  Row │ country  orders  avg_amount
#>      │ String   Int64   Float64
#> ─────┼─────────────────────────────
#>    1 │ DE       400000      500.01
#>    2 │ PL       300000      500.0
#>    3 │ FR       200000      499.98
#>    4 │ SE       100000      499.96

orders = bq_query(con, "SELECT * FROM shop.orders")
#> 1000000×4 DataFrame
#>      Row │ order_id  country  amount   placed_at
#>          │ Int64     String   Float64  DateTime
#> ─────────┼─────────────────────────────────────────────────
#>        1 │        1  DE         79.19  2024-01-01T00:00:31
#>        2 │        2  DE        158.38  2024-01-01T00:01:02
#>        3 │        3  DE        237.57  2024-01-01T00:01:33
#>        4 │        4  PL        316.76  2024-01-01T00:02:04
#>     ⋮    │    ⋮         ⋮        ⋮              ⋮
#>   999998 │   999998  FR        841.62  2024-12-24T19:05:38
#>   999999 │   999999  SE        920.81  2024-12-24T19:06:09
#>  1000000 │  1000000  SE          0.0   2024-12-24T19:06:40
#>                                         999993 rows omitted

using Dates

bq_query(con, """
    SELECT order_id, country, amount
    FROM shop.orders
    WHERE country = @country AND placed_at >= @since
    ORDER BY amount DESC
    LIMIT 3
"""; params = (country = "FR", since = DateTime(2024, 12, 1)))
#> 3×3 DataFrame
#>  Row │ order_id  country  amount
#>      │ Int64     String   Float64
#> ─────┼────────────────────────────
#>    1 │   976247  FR        999.93
#>    2 │   958568  FR        999.92
#>    3 │   999457  FR        999.83

bq_read_table(con, "shop.orders"; columns = ["order_id", "amount"], where = "amount > 999.98")
#> 10×2 DataFrame
#>  Row │ order_id  amount
#>      │ Int64     Float64
#> ─────┼───────────────────
#>    1 │    82321   999.99
#>    2 │   182321   999.99
#>    3 │   282321   999.99
#>    4 │   382321   999.99
#>   ⋮  │    ⋮         ⋮
#>    8 │   782321   999.99
#>    9 │   882321   999.99
#>   10 │   982321   999.99
#>            3 rows omitted

bq_query(con, "SELEC 1")
#> ERROR: BigQueryError: Syntax error: Unexpected identifier "SELEC" at [1:1] [reason: invalidQuery, HTTP 400]

# Upload a DataFrame. The table is created, with types taken from the columns.
french = bq_query(con, "SELECT * FROM shop.orders WHERE country = 'FR'")
bq_write_table(con, "shop.orders_fr", french)
#> (table = "my-project.shop.orders_fr", rows = 200000, job_id = "bqjl_0b9d3a52-6c1e-4b7f-9d67-2f4a8e1c5b30")

# The table exists now, so say what should happen to it: add rows or start over.
bq_write_table(con, "shop.orders_fr", more_orders; if_exists = :append)
bq_write_table(con, "shop.orders_fr", french; if_exists = :replace, types = (placed_at = "DATETIME",))
```

Query parameters take a `NamedTuple` or `Dict` for `@name` placeholders and a
`Tuple` or `Vector` for `?` placeholders. Types are inferred from the Julia
values; use `bq_param(value, "TYPE")` where they cannot be, as in
`bq_param(missing, "INT64")` or `bq_param(Int[], "ARRAY<INT64>")`.

The same through DBInterface.jl, which returns a Tables.jl table that also
carries the job's id, bytes processed and rows affected:

``` julia
using DBInterface, DataFrames

con = DBInterface.connect(BigQuery.Connection, "my-project")

DBInterface.execute(con, """
    SELECT order_id, country, amount
    FROM shop.orders
    WHERE country = @country AND placed_at >= @since
    ORDER BY amount DESC
    LIMIT 3
""", (country = "SE", since = DateTime(2024, 12, 1))) |> DataFrame
#> 3×3 DataFrame
#>  Row │ order_id  country  amount
#>      │ Int64     String   Float64
#> ─────┼────────────────────────────
#>    1 │   976247  SE        999.93
#>    2 │   958568  SE        999.92
#>    3 │   999457  SE        999.83

DBInterface.close!(con)
```

See the docstrings (`?bq_connect`, `?bq_query`, `?bq_read_table`,
`?bq_write_table`) for the remaining options, such as `max_bytes_billed`,
`location` and `progress`.

## Data types

BigQuery values arrive as the closest Julia type:

- `INT64`, `FLOAT64`, `BOOL` and `STRING` become `Int64`, `Float64`, `Bool` and
  `String`.
- `DATE`, `TIME` and `DATETIME` become `Date`, `Time` and `DateTime`.
- `TIMESTAMP` becomes a `DateTime` in UTC.
- `NUMERIC` becomes `Float64` and `BIGNUMERIC` becomes `BigFloat`.
- `BYTES` becomes `Vector{UInt8}`.
- `ARRAY<T>` becomes `Vector{T}` and `STRUCT` becomes a `NamedTuple`.
- `GEOGRAPHY`, `JSON` and `INTERVAL` become `String`.
- `NULL` becomes `missing`.

A column's element type is `Union{Missing, T}` only when the result contains a
`NULL` in that column. `DateTime` holds milliseconds, so the microsecond digits
of `TIMESTAMP` and `DATETIME` values are dropped, and `NUMERIC` is no longer
exact as a `Float64`; cast to `STRING` in the query to keep either in full.

### When uploading

A table created by `bq_write_table()` gets its column types from the Julia
types, the same mapping in reverse:

- `Int64` and other integers become `INT64`; `Float64` and other floats become
  `FLOAT64`; `Bool` becomes `BOOL`.
- `String`, `Symbol` and `Char` become `STRING`.
- `Date` and `Time` become `DATE` and `TIME`; `DateTime` becomes `TIMESTAMP`
  and is taken to be in UTC.
- `BigFloat` becomes `BIGNUMERIC`.
- `Vector{UInt8}` becomes `BYTES`.
- Other vectors become `ARRAY<T>` and a `NamedTuple` becomes a `STRUCT`.
- `missing` and `nothing` become `NULL`. Every column is created nullable.

Because `DATETIME` and `NUMERIC` are read as `DateTime` and `Float64`, a table
that is downloaded and uploaded again would come back with `TIMESTAMP` and
`FLOAT64` columns. Name the types to get instead with `types`, as in
`types = (placed_at = "DATETIME", amount = "NUMERIC")`; `JSON` and `GEOGRAPHY`
columns, which are read as `String`, are asked for the same way.

A column whose element type does not say what it holds, such as `Any`, gets its
type from its values, and a column of nothing but `missing` becomes `STRING`.

When rows are appended to an existing table, that table's schema decides how
each value is written and `types` is not used. Columns are matched by name, in
any order, and columns the data does not have are left `NULL`. Text is
accepted for a column of any type and parsed by BigQuery, which is how to
write exact `NUMERIC` values or timestamps with microseconds.
