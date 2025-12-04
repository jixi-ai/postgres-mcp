# server/config.py
from mcp.server.fastmcp import FastMCP
from server.database import Database
from server.logging_config import get_logger

logger = get_logger("instance")

# Create a global DB manager once per process
global_db = Database()
logger.info("Global database manager initialized")

# Note: json_response=True is recommended for HTTP transport
mcp = FastMCP(
    "pg-mcp-server",
    debug=True,
    json_response=True,
    dependencies=["asyncpg", "mcp"],
)

# Attach shared state once at import time
mcp.state = {"db": global_db}
logger.info("FastMCP instance initialized with global DB in state")