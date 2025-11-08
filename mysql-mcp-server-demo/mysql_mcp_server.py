#! /usr/bin/env python3
import os
import traceback
from typing import Any, Dict, List

import pymysql
from fastmcp import FastMCP
from pymysql import Connection

mcp = FastMCP("mysql-mcp-server")


def get_db_config() -> Dict[str, Any]:
    """Read database config from environment variables and return a dictionary."""
    return {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DATABASE", ""),
        "cursorclass": pymysql.cursors.DictCursor
    }


def get_connection() -> Connection[Any]:
    """Get a database connection"""
    return pymysql.connect(**get_db_config())


@mcp.tool("list_tables", description="List all the tables in the database")
def list_tables() -> List[str]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW TABLES")
            tables = [row[next(iter(row))] for row in cur.fetchall()]
            return tables
    except Exception as e:
        print("Error in list tables:", e)
        traceback.print_exc()
        raise RuntimeError(f"list tables failed: {e}")
    finally:
        conn.close()


@mcp.tool("describe_table", description="Query the table structure for a specified table")
def describe_table(table_name: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE {table_name}")
            columns = cur.fetchall()
            return list(columns) if columns else []
    except Exception as e:
        print("Error in describe table:", e)
        traceback.print_exc()
        raise RuntimeError(f"describe table failed: {e}")
    finally:
        conn.close()


@mcp.tool("explain_sql", description="Explain a SQL query")
def explain_sql(query: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN {query}")
            result = cur.fetchall()
            return result
    except Exception as e:
        print("Error in explain sql:", e)
        traceback.print_exc()
        raise RuntimeError(f"explain sql failed: {e}")
    finally:
        conn.close()


@mcp.tool("run_sql", description="Run SELECT SQL query")
def run_sql(query: str, limit: int = 1000) -> List[Dict[str, Any]]:
    if not query.strip().lower().startswith("select"):
        raise RuntimeError("Only SELECT queries are supported")

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"{query} LIMIT {limit}")
            result = cur.fetchall()
            return result
    except Exception as e:
        print("Error in run sql:", e)
        traceback.print_exc()
        raise RuntimeError(f"run sql failed: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    mcp.run()
    # mcp.run(transport="http", host="0.0.0.0", port=8000)
