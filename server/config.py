# server/config.py
from mcp.server.fastmcp import FastMCP
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from server.database import Database
from server.logging_config import get_logger

logger = get_logger("instance")

global_db = Database()
logger.info("Global database manager initialized")

# Create MCP instance first
mcp = FastMCP(
    "pg-mcp-server",
    debug=True,
    dependencies=["asyncpg", "mcp"]
)

# Eagerly set state so tools can *always* find db
mcp.state = {"db": global_db}

@asynccontextmanager
async def app_lifespan(app: FastMCP) -> AsyncIterator[dict]:
    """Manage application lifecycle."""
    # optional: reinforce state, but it's already set above
    logger.info("Application startup - using global database manager")
    try:
        yield {"db": global_db}
    finally:
        # Don't close connections on individual session end
        pass