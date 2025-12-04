# server/config.py
from mcp.server.fastmcp import FastMCP
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from server.database import Database
from server.logging_config import get_logger

logger = get_logger("instance")

global_db = Database()
logger.info("Global database manager initialized")

@asynccontextmanager
async def app_lifespan(app: FastMCP) -> AsyncIterator[dict]:
    """Manage application lifecycle."""
    mcp.state = {"db": global_db}
    logger.info("Application startup - using global database manager")

    try:
        # anything you want to expose in ctx.request_context.lifespan_context
        yield {"db": global_db}
    finally:
        logger.info("Application shutdown - closing all database connections")
        await global_db.close()

# Create the MCP instance
mcp = FastMCP(
    "pg-mcp-server",
    debug=True,
    lifespan=app_lifespan,
    dependencies=["asyncpg", "mcp"]
)