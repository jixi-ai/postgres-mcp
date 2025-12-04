# server/config.py
from mcp.server.fastmcp import FastMCP
from server.database import Database
from server.logging_config import get_logger

logger = get_logger("instance")

# Create a global DB manager once per process
global_db = Database()
logger.info("Global database manager initialized")

# Create the MCP instance with NO lifespan; let it be a simple ASGI app
mcp = FastMCP(
    "pg-mcp-server",
    debug=True,
    dependencies=["asyncpg", "mcp"],
)

# Attach shared state once at import time
mcp.state = {"db": global_db}
logger.info("FastMCP instance initialized with global DB in state")