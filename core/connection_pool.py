"""
Database Connection Management for FCST Application
Provides connection pooling, retry logic, health checks, and session management for multiple users.
"""
import os
import threading
import time
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from contextlib import contextmanager
from queue import Queue, Empty
import random

from databricks import sql
from databricks.sdk.core import Config

logger = logging.getLogger(__name__)

class ConnectionConfig:
    """Configuration for database connections"""

    def __init__(self,
                 host: str = None,
                 http_path: str = "/sql/1.0/warehouses/62d47c983bb6df91",
                 client_id: str = None,
                 client_secret: str = None,
                 pool_size: int = 10,
                 max_overflow: int = 20,
                 pool_timeout: int = 30,
                 pool_recycle: int = 3600,
                 retry_attempts: int = 3,
                 retry_delay: float = 1.0,
                 health_check_interval: int = 300,
                 session_timeout: int = 60):
        # Use environment variables if not provided, but respect None values
        self.host = host if host is not None else os.getenv("DATABRICKS_HOST")
        self.http_path = http_path  # Use same hardcoded path as dashboard.py
        self.client_id = client_id if client_id is not None else os.getenv("DATABRICKS_CLIENT_ID")
        self.client_secret = client_secret if client_secret is not None else os.getenv("DATABRICKS_CLIENT_SECRET")

        # Connection pool settings
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self.health_check_interval = health_check_interval
        self.session_timeout = session_timeout

class ConnectionWrapper:
    """Wrapper for database connections with metadata"""
    def __init__(self, connection, created_at: datetime, last_used: datetime = None):
        self.connection = connection
        self.created_at = created_at
        self.last_used = last_used or created_at
        self.is_healthy = True
        self.thread_id = threading.get_ident()

    def mark_used(self):
        """Mark connection as recently used"""
        self.last_used = datetime.now()

    def is_expired(self, max_age: int) -> bool:
        """Check if connection has expired"""
        return (datetime.now() - self.created_at).total_seconds() > max_age

    def needs_health_check(self, check_interval: int) -> bool:
        """Check if connection needs health validation"""
        return (datetime.now() - self.last_used).total_seconds() > check_interval

class ConnectionPool:
    """Thread-safe connection pool for Databricks SQL"""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._pool: Queue = Queue(maxsize=config.pool_size)
        self._overflow: List[ConnectionWrapper] = []
        self._lock = threading.RLock()
        self._created_connections = 0
        self._closed = False

        # Initialize pool with minimum connections
        self._initialize_pool()

    def _initialize_pool(self):
        """Initialize the connection pool"""
        for _ in range(self.config.pool_size // 2):  # Start with half the pool size
            try:
                conn_wrapper = self._create_connection()
                self._pool.put(conn_wrapper)
                self._created_connections += 1
            except Exception as e:
                logger.warning(f"Failed to create initial connection: {e}")

    def _create_connection(self) -> ConnectionWrapper:
        """Create a new database connection using databricks-cli authentication"""
        try:
            if not self.config.host:
                raise ValueError("DATABRICKS_HOST environment variable must be set")

            # Check if we should use databricks-cli authentication
            # Use CLI auth if no client_id/client_secret are set OR if they're explicitly None
            use_cli_auth = (
                not self.config.client_id or
                not self.config.client_secret or
                self.config.client_id is None or
                self.config.client_secret is None
            )

            if use_cli_auth:
                logger.info("Using databricks-cli authentication")
                connection = sql.connect(
                    server_hostname=self.config.host,
                    http_path=self.config.http_path
                )
            else:
                # Only use Config authentication when valid credentials are provided
                logger.info("Using Config authentication with credentials")
                databricks_config = Config(
                    host=self.config.host,
                    client_id=self.config.client_id,
                    client_secret=self.config.client_secret
                )

                connection = sql.connect(
                    server_hostname=databricks_config.host,
                    http_path=self.config.http_path,
                    credentials_provider=lambda: databricks_config.authenticate
                )

            conn_wrapper = ConnectionWrapper(
                connection=connection,
                created_at=datetime.now()
            )

            logger.debug(f"Created new database connection (total: {self._created_connections + 1})")
            return conn_wrapper

        except Exception as e:
            logger.error(f"Failed to create database connection: {e}")
            logger.error(f"Host: {self.config.host}, HTTP Path: {self.config.http_path}")
            if self.config.client_id:
                logger.error("Using Config authentication - make sure credentials are valid")
            else:
                logger.error("Using databricks-cli authentication - make sure 'databricks auth login' was run")
            raise

    def get_connection(self) -> ConnectionWrapper:
        """Get a connection from the pool"""
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        with self._lock:
            # Try to get from main pool first
            try:
                conn_wrapper = self._pool.get_nowait()
                if self._is_connection_valid(conn_wrapper):
                    conn_wrapper.mark_used()
                    return conn_wrapper
                else:
                    # Connection is invalid, close it and try again
                    self._close_connection(conn_wrapper)
                    return self.get_connection()
            except Empty:
                pass

            # Try to get from overflow pool
            if self._overflow:
                conn_wrapper = self._overflow.pop()
                if self._is_connection_valid(conn_wrapper):
                    conn_wrapper.mark_used()
                    return conn_wrapper
                else:
                    self._close_connection(conn_wrapper)

            # Create new connection if under limits
            if self._created_connections < (self.config.pool_size + self.config.max_overflow):
                try:
                    conn_wrapper = self._create_connection()
                    self._created_connections += 1
                    conn_wrapper.mark_used()
                    return conn_wrapper
                except Exception as e:
                    logger.error(f"Failed to create overflow connection: {e}")

            # Wait for available connection
            try:
                conn_wrapper = self._pool.get(timeout=self.config.pool_timeout)
                if self._is_connection_valid(conn_wrapper):
                    conn_wrapper.mark_used()
                    return conn_wrapper
                else:
                    self._close_connection(conn_wrapper)
                    return self.get_connection()
            except Empty:
                raise RuntimeError(f"No connection available within {self.config.pool_timeout} seconds")

    def return_connection(self, conn_wrapper: ConnectionWrapper):
        """Return a connection to the pool"""
        if self._closed or conn_wrapper.connection is None:
            return

        with self._lock:
            if not self._is_connection_valid(conn_wrapper):
                self._close_connection(conn_wrapper)
                self._created_connections -= 1
                return

            # Return to appropriate pool
            if self._pool.qsize() < self.config.pool_size:
                try:
                    self._pool.put_nowait(conn_wrapper)
                except:
                    # Pool is full, add to overflow
                    self._overflow.append(conn_wrapper)
            else:
                self._overflow.append(conn_wrapper)

    def _is_connection_valid(self, conn_wrapper: ConnectionWrapper) -> bool:
        """Check if a connection is still valid"""
        if conn_wrapper.connection is None:
            return False

        # Check if connection has expired
        if conn_wrapper.is_expired(self.config.pool_recycle):
            logger.debug("Connection expired")
            return False

        # Check if connection needs health validation
        if conn_wrapper.needs_health_check(self.config.health_check_interval):
            try:
                # Perform a simple health check
                with conn_wrapper.connection.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    if result and result[0] == 1:
                        conn_wrapper.is_healthy = True
                        return True
                    else:
                        logger.debug("Health check failed - no valid result")
                        return False
            except Exception as e:
                logger.debug(f"Health check failed: {e}")
                return False

        return conn_wrapper.is_healthy

    def _close_connection(self, conn_wrapper: ConnectionWrapper):
        """Close a connection"""
        try:
            if conn_wrapper.connection:
                conn_wrapper.connection.close()
                logger.debug("Closed database connection")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")

    def close_all(self):
        """Close all connections in the pool"""
        with self._lock:
            self._closed = True

            # Close connections in main pool
            while not self._pool.empty():
                try:
                    conn_wrapper = self._pool.get_nowait()
                    self._close_connection(conn_wrapper)
                except Empty:
                    break

            # Close connections in overflow pool
            for conn_wrapper in self._overflow:
                self._close_connection(conn_wrapper)

            self._overflow.clear()
            self._created_connections = 0

    @property
    def pool_size(self) -> int:
        """Get current pool size"""
        return self._pool.qsize() + len(self._overflow)

    @property
    def active_connections(self) -> int:
        """Get number of active connections"""
        return self._created_connections - self._pool.qsize() - len(self._overflow)
