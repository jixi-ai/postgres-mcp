# server/tools/connection.py
from server.config import mcp
from server.logging_config import get_logger

logger = get_logger("pg-mcp.tools.connection")

def register_connection_tools():
    """Register the database connection tools with the MCP server."""
    logger.debug("Registering database connection tools")

    @mcp.tool()
    async def connect(connection_string: str):
        """
        Register a database connection string and return its connection ID.
        """
        db = mcp.state["db"]  # global_db from config.py
        conn_id = db.register_connection(connection_string)
        logger.info(f"Registered database connection with ID: {conn_id}")
        return {"conn_id": conn_id}

    @mcp.tool()
    async def disconnect(conn_id: str):
        """
        Close a specific database connection and remove it from the pool.
        """
        db = mcp.state["db"]

        if conn_id not in db._connection_map:
            logger.warning(f"Attempted to disconnect unknown connection ID: {conn_id}")
            return {"success": False, "error": "Unknown connection ID"}

        try:
            await db.close(conn_id)
            connection_string = db._connection_map.pop(conn_id, None)
            if connection_string in db._reverse_map:
                del db._reverse_map[connection_string]
            logger.info(f"Successfully disconnected database connection with ID: {conn_id}")
            return {"success": True}
        except Exception as e:
            logger.error(f"Error disconnecting connection {conn_id}: {e}")
            return {"success": False, "error": str(e)}