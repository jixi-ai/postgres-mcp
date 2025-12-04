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
    """
    FastMCP lifespan: set up shared state once per app.
    IMPORTANT: do NOT close the DB pools here, because FastMCP may
    end this lifespan when a session ends while SSE writer is still active.
    """
    mcp.state = {"db": global_db}
    logger.info("Application startup - using global database manager")
    try:
        yield {"db": global_db}
    finally:
        # Let the process shutdown handle DB cleanup instead.
        logger.info("Application shutdown - leaving DB pools open (process-level cleanup)")
        # no global_db.close() here

mcp = FastMCP(
    "pg-mcp-server",
    debug=True,
    lifespan=app_lifespan,
    dependencies=["asyncpg", "mcp"],
)