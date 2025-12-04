# server/app.py
import os
import uvicorn

from server.logging_config import (
    configure_logging,
    get_logger,
    configure_uvicorn_logging,
)
from server.config import mcp

# Import + register all tools/resources (same as before)
from server.resources.schema import register_schema_resources
from server.resources.data import register_data_resources
from server.resources.extensions import register_extension_resources
from server.tools.connection import register_connection_tools
from server.tools.query import register_query_tools
from server.tools.viz import register_viz_tools
from server.prompts.natural_language import register_natural_language_prompts
from server.prompts.data_visualization import register_data_visualization_prompts

log_level = os.environ.get("LOG_LEVEL", "DEBUG")
configure_logging(level=log_level)
logger = get_logger("app")

logger.info("Registering resources and tools")
register_schema_resources()
register_extension_resources()
register_data_resources()
register_connection_tools()
register_query_tools()
register_viz_tools()
register_natural_language_prompts()
register_data_visualization_prompts()

# ⬇️ THIS is the HTTP JSON transport app
app = mcp.streamable_http_app()

if __name__ == "__main__":
    logger.info("Starting MCP server with streamable HTTP transport")

    uvicorn_log_config = configure_uvicorn_logging(log_level)

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level=log_level.lower(),
        log_config=uvicorn_log_config,
    )