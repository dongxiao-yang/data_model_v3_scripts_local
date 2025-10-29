"""
Database connection utilities for ClickHouse.
"""

import logging
from contextlib import contextmanager
from typing import Iterator, Any, List, Dict
import clickhouse_connect
from clickhouse_connect.driver import Client

from src.config.settings import DatabaseConfig


logger = logging.getLogger(__name__)


class ClickHouseConnection:
    """Wrapper class for ClickHouse database connections."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._client: Client = None

    def connect(self) -> Client:
        """Establish connection to ClickHouse."""
        if self._client is None:
            logger.info(f"Connecting to ClickHouse at {self.config.host}:{self.config.port}")
            self._client = clickhouse_connect.get_client(**self.config.connection_params)
        return self._client

    def disconnect(self):
        """Close the connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Disconnected from ClickHouse")

    @contextmanager
    def get_connection(self) -> Iterator[Client]:
        """Context manager for database connections."""
        try:
            client = self.connect()
            yield client
        finally:
            # Keep connection alive for reuse
            pass

    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> Any:
        """Execute a query and return results."""
        logger.debug(f"Executing query: {query[:100]}...")
        with self.get_connection() as client:
            return client.query(query, parameters=parameters or {})

    def execute_command(self, command: str, parameters: Dict[str, Any] = None) -> None:
        """Execute a command (DDL, INSERT, etc.)."""
        logger.info(f"Executing command: {command[:100]}...")
        with self.get_connection() as client:
            client.command(command, parameters=parameters or {})

    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            result = self.execute_query("SELECT 1")
            return len(result.result_rows) > 0 and result.result_rows[0][0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get table schema information."""
        query = f"DESCRIBE TABLE {table_name}"
        result = self.execute_query(query)

        schema = []
        for row in result.result_rows:
            schema.append({
                'name': row[0],
                'type': row[1],
                'default_type': row[2] if len(row) > 2 else '',
                'default_expression': row[3] if len(row) > 3 else '',
                'comment': row[4] if len(row) > 4 else ''
            })

        return schema

    def get_table_count(self, table_name: str, where_clause: str = "") -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(*) FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self.execute_query(query)
        return result.result_rows[0][0] if result.result_rows else 0