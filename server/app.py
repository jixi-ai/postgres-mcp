# server/app.py
import os
from server.logging_config import configure_logging, get_logger, configure_uvicorn_logging
from server.config import mcp  # this also initializes global_db + tools/resources

log_level = os.environ.get("LOG_LEVEL", "DEBUG")
configure_logging(level=log_level)
logger = get_logger("app")

import uvicorn

if __name__ == "__main__":
    logger.info("Starting MCP server with SSE transport")

    app = mcp.sse_app()

    uvicorn_log_config = configure_uvicorn_logging(log_level)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level=log_level.lower(),
        log_config=uvicorn_log_config,
    )