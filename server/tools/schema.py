from server.config import mcp
from server.logging_config import get_logger

logger = get_logger("pg-mcp.tools.schema")

def register_schema_tools():
    @mcp.tool()
    async def pg_list_tables(conn_id: str):
        """
        List all table names in the connected PostgreSQL database.
        """
        db = mcp.state["db"]
        async with db.get_connection(conn_id) as conn:
            rows = await conn.fetch("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            return [r["table_name"] for r in rows]

    @mcp.tool()
    async def pg_describe_table(conn_id: str, table: str):
        """
        Return column names + types for the specified table.
        """
        db = mcp.state["db"]
        async with db.get_connection(conn_id) as conn:
            rows = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = $1
                ORDER BY ordinal_position
            """, table)
            return [{"column": r["column_name"], "type": r["data_type"]} for r in rows]