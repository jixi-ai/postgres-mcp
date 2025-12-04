# server/config.py
from mcp.server.fastmcp import FastMCP
from server.database import Database
from server.logging_config import get_logger

logger = get_logger("instance")

# Global DB manager
global_db = Database()
logger.info("Global database manager initialized")

# IMPORTANT: stateless_http=True enables the HTTP transport
mcp = FastMCP(
    name="pg-mcp-server",
    debug=True,
    stateless_http=True,
    dependencies=["asyncpg", "mcp"],
)

# Attach shared state once
mcp.state = {"db": global_db}
logger.info("FastMCP instance initialized with global DB in state")