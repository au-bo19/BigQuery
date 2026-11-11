# DBInterface.jl integration, Julia's common database interface:
#
#   con = DBInterface.connect(BigQuery.Connection, "my-project")
#   df  = DBInterface.execute(con, "SELECT ...") |> DataFrame
#   DBInterface.execute(con, "SELECT * FROM t WHERE id = @id", (id = 42,))
#   DBInterface.close!(con)

struct Statement <: DBInterface.Statement
    conn::Connection
    sql::String
end

DBInterface.connect(::Type{Connection}, args...; kwargs...) = bq_connect(args...; kwargs...)

function DBInterface.close!(conn::Connection)
    conn.isopen = false
    return nothing
end

Base.close(conn::Connection) = DBInterface.close!(conn)
Base.isopen(conn::Connection) = conn.isopen

DBInterface.prepare(conn::Connection, sql::AbstractString) = Statement(conn, String(sql))
DBInterface.getconnection(stmt::Statement) = stmt.conn
DBInterface.close!(::Statement) = nothing

function DBInterface.execute(stmt::Statement, params::DBInterface.StatementParams)
    return run_query(stmt.conn, stmt.sql; params = isempty(params) ? nothing : params)
end
