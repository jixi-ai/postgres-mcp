#!/usr/bin/env python
# example-clients/ollama_cli.py
"""
Ollama client for the PostgreSQL MCP server.
Translates natural language queries to SQL using Ollama and executes them.
"""
import asyncio
import os
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

import dotenv
import httpx
from mcp import ClientSession
from mcp.client.sse import sse_client
from tabulate import tabulate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('ollama_cli')

# Load environment variables from .env file
dotenv.load_dotenv()

# Configuration with defaults
MCP_URL = os.getenv('PG_MCP_URL', 'http://localhost:8000/sse')
DB_URL = os.getenv('DATABASE_URL')
OLLAMA_URL = os.getenv('OLLAMA_URL', 'http://localhost:11434')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL')

async def generate_sql_with_ollama(user_query: str, conn_id: str, session: ClientSession) -> Dict[str, Any]:
    """
    Generate SQL using Ollama with the server's generate_sql prompt.

    Args:
        user_query: Natural language query from the user
        conn_id: Database connection ID
        session: MCP client session

    Returns:
        Dictionary with results containing:
        - success: Boolean indicating success or failure
        - sql: The extracted SQL query (if successful)
        - explanation: Human-readable explanation (if successful)
        - error: Error message (if failure)
    """
    try:
        logger.info("Fetching SQL generation prompt from server")
        prompt_response = await session.get_prompt('generate_sql', {
            'conn_id': conn_id,
            'nl_query': user_query
        })

        if not hasattr(prompt_response, 'messages') or not prompt_response.messages:
            logger.error(f"Invalid prompt response from server: {prompt_response}")
            return {
                "success": False,
                "error": "Invalid prompt response from server",
                "raw": str(prompt_response)
            }

        # Concatenate all messages into a single prompt
        prompt = "\n".join([
            (msg.content.text if hasattr(msg.content, 'text') else str(msg.content))
            for msg in prompt_response.messages
        ])

        logger.debug(f"Prompt sent to Ollama:\n{prompt}")

        # Call Ollama API
        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                logger.info(f"Sending request to Ollama API at {OLLAMA_URL}")
                response = await client.post(
                    f"{OLLAMA_URL}/api/generate",
                    json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False}
                )
                response.raise_for_status()
                data = response.json()
                response_text = data.get('response', '')
                logger.debug(f"Ollama raw response: {data}")

            except httpx.HTTPStatusError as e:
                logger.error(f"HTTP error from Ollama API: {e.response.status_code} - {e.response.text}")
                return {
                    "success": False,
                    "error": f"Ollama API HTTP error: {e.response.status_code}",
                    "details": e.response.text
                }
            except httpx.RequestError as e:
                logger.error(f"Request error to Ollama API: {e}")
                return {
                    "success": False,
                    "error": f"Ollama API connection error: {str(e)}"
                }
            except Exception as e:
                logger.error(f"Exception calling Ollama API: {e}", exc_info=True)
                return {
                    "success": False,
                    "error": f"Ollama API error: {str(e)}"
                }

        # Extract SQL query from response using multiple strategies
        sql_query = extract_sql_from_response(response_text)

        if not sql_query:
            logger.error(f"Could not extract SQL from Ollama's response: {response_text}")
            return {
                "success": False,
                "error": "Could not extract SQL from Ollama's response",
                "response": response_text
            }

        # Ensure SQL query ends with semicolon
        sql_query = sql_query.strip()
        if not sql_query.endswith(';'):
            sql_query += ';'

        logger.info("SQL query successfully generated")
        return {
            "success": True,
            "sql": sql_query,
            "explanation": "SQL generated using Ollama",
            "ollama_response": response_text
        }

    except Exception as e:
        logger.error(f"Exception in generate_sql_with_ollama: {e}", exc_info=True)
        return {
            "success": False,
            "error": f"Error: {str(e)}"
        }


def extract_sql_from_response(response_text: str) -> Optional[str]:
    """
    Extract SQL query from Ollama's response text using multiple strategies.

    Args:
        response_text: The raw text response from Ollama

    Returns:
        Extracted SQL query or None if no SQL could be extracted
    """
    sql_query = None

    # Strategy 1: Extract SQL from code blocks
    if "```sql" in response_text and "```" in response_text.split("```sql", 1)[1]:
        sql_start = response_text.find("```sql") + 6
        remaining_text = response_text[sql_start:]
        sql_end = remaining_text.find("```")
        if sql_end > 0:
            sql_query = remaining_text[:sql_end].strip()
            return sql_query

    # Strategy 2: Try with just markdown code block
    if "```" in response_text:
        code_blocks = response_text.split("```")
        if len(code_blocks) >= 3:  # At least one complete code block
            for i in range(1, len(code_blocks), 2):  # Check each code block
                block = code_blocks[i].strip()
                if block.startswith("sql\n"):  # Remove sql language marker if present
                    block = block[4:].strip()

                # Check if block contains SQL keywords
                for keyword in ["SELECT", "WITH", "CREATE", "INSERT", "UPDATE", "DELETE"]:
                    if keyword in block.upper():
                        return block

    # Strategy 3: Look for SQL keywords directly in text
    if not sql_query:
        for keyword in ["WITH", "SELECT", "CREATE", "INSERT", "UPDATE", "DELETE"]:
            if keyword in response_text.upper():
                keyword_pos = response_text.upper().find(keyword)
                sql_query = response_text[keyword_pos:].strip()

                # Find end of SQL statement
                for end_marker in ["\n\n", "```", ".\n"]:
                    if end_marker in sql_query:
                        sql_query = sql_query[:sql_query.find(end_marker)].strip()

                return sql_query

    return None

async def connect_to_database(session: ClientSession) -> Optional[str]:
    """
    Connect to the database using the MCP server.

    Args:
        session: Active MCP client session

    Returns:
        Connection ID if successful, None otherwise
    """
    try:
        logger.info(f"Registering connection with server...")
        connect_result = await session.call_tool(
            "connect", {"connection_string": DB_URL}
        )

        if not hasattr(connect_result, 'content') or not connect_result.content:
            logger.error("Connection response missing content")
            return None

        content = connect_result.content[0]
        if not hasattr(content, 'text'):
            logger.error("Connection response missing text content")
            return None

        result_data = json.loads(content.text)
        conn_id = result_data.get('conn_id')
        if not conn_id:
            logger.error("Connection ID not found in response")
            return None

        logger.info(f"Connection registered with ID: {conn_id}")
        return conn_id

    except Exception as e:
        logger.error(f"Error connecting to database: {e}", exc_info=True)
        return None


async def execute_query(session: ClientSession, sql_query: str, conn_id: str) -> List[Dict[str, Any]]:
    """
    Execute SQL query and parse results.

    Args:
        session: Active MCP client session
        sql_query: SQL query to execute
        conn_id: Database connection ID

    Returns:
        List of result rows as dictionaries
    """
    logger.info("Executing SQL query...")
    result = await session.call_tool(
        "pg_query", {"query": sql_query, "conn_id": conn_id}
    )

    if not hasattr(result, 'content') or not result.content:
        logger.warning("Query returned no content")
        return []

    query_results = []
    for item in result.content:
        if hasattr(item, 'text') and item.text:
            try:
                row_data = json.loads(item.text)
                if isinstance(row_data, list):
                    query_results.extend(row_data)
                else:
                    query_results.append(row_data)
            except json.JSONDecodeError:
                logger.warning(f"Could not parse result: {item.text}")

    return query_results


async def main() -> None:
    """Main function that coordinates the entire workflow."""
    # Validate required environment variables
    if not DB_URL:
        logger.error("ERROR: DATABASE_URL environment variable is not set.")
        print("ERROR: DATABASE_URL environment variable is not set.")
        print("Please set this in your .env file or environment.")
        sys.exit(1)

    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python ollama_cli.py 'your natural language query'")
        print("Example: python ollama_cli.py 'Show me the top 5 customers'")
        sys.exit(1)

    user_query = sys.argv[1]
    print(f"Processing query: {user_query}")

    try:
        logger.info(f"Connecting to MCP server at {MCP_URL}...")
        print(f"Connecting to MCP server at {MCP_URL}...")

        async with sse_client(url=MCP_URL) as streams:
            async with ClientSession(*streams) as session:
                # Initialize session
                await session.initialize()
                logger.info("Connection initialized!")
                print("Connection initialized!")

                # Connect to database
                conn_id = await connect_to_database(session)
                if not conn_id:
                    logger.error("Failed to connect to database")
                    print("Error: Failed to connect to database")
                    sys.exit(1)

                # Generate SQL query
                print("Generating SQL query with Ollama...")
                response_data = await generate_sql_with_ollama(user_query, conn_id, session)

                # Handle SQL generation failure
                if not response_data.get("success"):
                    error_msg = response_data.get("error", "Unknown error")
                    logger.error(f"SQL generation failed: {error_msg}")
                    print(f"ERROR: SQL generation failed: {error_msg}")

                    # Print debug information if available
                    for key in ["response", "details", "raw"]:
                        if key in response_data:
                            logger.debug(f"{key}: {response_data[key]}")

                    # Ensure clean disconnect before exiting
                    await session.call_tool("disconnect", {"conn_id": conn_id})
                    sys.exit(1)

                # Extract and show SQL query
                sql_query = response_data.get("sql", "")
                explanation = response_data.get("explanation", "")

                if explanation:
                    print(f"\nExplanation:\n------------\n{explanation}")

                print(f"\nGenerated SQL query:\n------------------\n{sql_query}\n------------------\n")

                if not sql_query:
                    logger.error("No SQL query was generated")
                    print("No SQL query was generated. Exiting.")
                    await session.call_tool("disconnect", {"conn_id": conn_id})
                    sys.exit(1)

                # Execute the query
                query_results = await execute_query(session, sql_query, conn_id)

                # Display results
                print("\nQuery Results:\n==============")
                if query_results:
                    # Use pretty table format
                    table = tabulate(query_results, headers="keys", tablefmt="pretty")
                    print(table)
                    print(f"\nTotal rows: {len(query_results)}")
                else:
                    print("Query executed successfully but returned no results.")

                # Clean disconnect
                logger.info("Disconnecting from database...")
                print("Disconnecting from database...")
                await session.call_tool("disconnect", {"conn_id": conn_id})
                print("Successfully disconnected.")

    except httpx.ConnectError as e:
        logger.error(f"Connection error: {e}")
        print(f"ERROR: Could not connect to MCP server at {MCP_URL}")
        print(f"Please make sure the server is running and accessible.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Exception in main: {type(e).__name__}: {e}", exc_info=True)
        print(f"ERROR: {type(e).__name__}: {e}")
        sys.exit(1)

def print_help() -> None:
    """Print CLI usage instructions."""
    print("""
PostgreSQL MCP - Ollama CLI
---------------------------
This tool translates natural language queries to SQL using Ollama
and executes them against a PostgreSQL database.

Required environment variables:
- DATABASE_URL: PostgreSQL connection string
- OLLAMA_MODEL: Ollama model name (default: llama3)
- OLLAMA_URL: URL to Ollama API (default: http://localhost:11434)
- PG_MCP_URL: URL to MCP server (default: http://localhost:8000/sse)

Usage:
  python ollama_cli.py 'your natural language query'

Example:
  python ollama_cli.py 'Show me the top 5 customers by total purchases'
""")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print_help()
        sys.exit(0)

    # Set higher event loop debug if DEBUG environment variable is set
    if os.getenv("DEBUG"):
        logging.getLogger().setLevel(logging.DEBUG)
        os.environ["PYTHONASYNCIODEBUG"] = "1"

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0)
