"""
Database Session Manager for Multi-User Applications
Provides user-isolated database sessions with proper resource management
"""
import os
import threading
import time
import uuid
import logging
from typing import Dict, Optional, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from contextlib import contextmanager
from weakref import WeakValueDictionary

from databricks.sdk.core import Config
from .connection_pool import ConnectionWrapper
from .retry_handler import RetryHandler
from .health_checker import ConnectionHealthChecker

logger = logging.getLogger(__name__)

class SimpleConnectionPool:
    """Simple connection pool that works with Databricks SDK Config"""

    def __init__(self, config: Config, pool_size: int = 10):
        self.config = config
        self.pool_size = pool_size
        self._pool = []
        self._lock = threading.RLock()

    def get_connection(self):
        """Get a connection from the pool"""
        with self._lock:
            if self._pool:
                return self._pool.pop()
            else:
                # Create new connection
                return self._create_connection()

    def return_connection(self, conn_wrapper):
        """Return a connection to the pool"""
        with self._lock:
            if len(self._pool) < self.pool_size:
                self._pool.append(conn_wrapper)
            else:
                # Pool is full, close the connection
                conn_wrapper.connection.close()

    def _create_connection(self):
        """Create a new database connection using Config"""
        from databricks import sql

        connection = sql.connect(
            server_hostname=self.config.host,
            http_path="/sql/1.0/warehouses/62d47c983bb6df91",  # Same as dashboard.py
            credentials_provider=lambda: self.config.authenticate
        )

        conn_wrapper = ConnectionWrapper(
            connection=connection,
            created_at=datetime.now()
        )

        logger.debug("Created new database connection")
        return conn_wrapper

    def close_all(self):
        """Close all connections in the pool"""
        with self._lock:
            for conn_wrapper in self._pool:
                conn_wrapper.connection.close()
            self._pool.clear()

@dataclass
class UserSession:
    """Represents a user database session"""
    session_id: str
    user_id: str
    created_at: datetime
    last_activity: datetime
    connection: Optional[ConnectionWrapper] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True

    def update_activity(self):
        """Update the last activity timestamp"""
        self.last_activity = datetime.now()

    def is_expired(self, timeout: int) -> bool:
        """Check if session has expired"""
        return (datetime.now() - self.last_activity).total_seconds() > timeout

    @property
    def age(self) -> float:
        """Get session age in seconds"""
        return (datetime.now() - self.created_at).total_seconds()

    @property
    def idle_time(self) -> float:
        """Get idle time in seconds"""
        return (datetime.now() - self.last_activity).total_seconds()

@dataclass
class SessionStats:
    """Statistics for session management"""
    total_sessions: int = 0
    active_sessions: int = 0
    expired_sessions: int = 0
    avg_session_age: float = 0.0
    max_concurrent_sessions: int = 0
    total_connections_used: int = 0

class DatabaseSessionManager:
    """Manages database sessions for multiple users with proper isolation"""

    def __init__(self, config: Config):
        self.config = config
        # Create a simple connection pool using the Config
        self.connection_pool = SimpleConnectionPool(config)
        self.retry_handler = RetryHandler(
            max_attempts=3,  # Default values since Config doesn't have these
            base_delay=1.0
        )

        # Session management
        self._sessions: Dict[str, UserSession] = {}
        self._user_sessions: Dict[str, List[str]] = {}  # user_id -> [session_ids]
        self._lock = threading.RLock()
        self._cleanup_thread: Optional[threading.Thread] = None
        self._cleanup_interval = 300  # 5 minutes
        self._cleanup_running = False

        # Statistics
        self.stats = SessionStats()

        # Health checker with simple connection factory
        self.health_checker = ConnectionHealthChecker(
            connection_factory=self._create_test_connection,
            interval=300  # 5 minutes
        )

        # Start cleanup thread
        self._start_cleanup_thread()

    def create_session(self, user_id: str, metadata: Dict[str, Any] = None) -> str:
        """Create a new database session for a user"""
        with self._lock:
            session_id = str(uuid.uuid4())
            session = UserSession(
                session_id=session_id,
                user_id=user_id,
                created_at=datetime.now(),
                last_activity=datetime.now(),
                metadata=metadata or {}
            )

            self._sessions[session_id] = session

            # Track user sessions
            if user_id not in self._user_sessions:
                self._user_sessions[user_id] = []
            self._user_sessions[user_id].append(session_id)

            # Update statistics
            self.stats.total_sessions += 1
            self.stats.active_sessions += 1
            self.stats.max_concurrent_sessions = max(
                self.stats.max_concurrent_sessions,
                self.stats.active_sessions
            )

            logger.info(f"Created session {session_id} for user {user_id}")
            return session_id

    def get_session(self, session_id: str) -> Optional[UserSession]:
        """Get a session by ID"""
        with self._lock:
            return self._sessions.get(session_id)

    def get_user_sessions(self, user_id: str) -> List[UserSession]:
        """Get all sessions for a user"""
        with self._lock:
            session_ids = self._user_sessions.get(user_id, [])
            return [self._sessions[sid] for sid in session_ids if sid in self._sessions]

    @contextmanager
    def get_connection(self, session_id: str, user_id: str = None):
        """Get a database connection for a session"""
        session = None
        connection = None

        try:
            with self._lock:
                # Get or create session
                if session_id not in self._sessions:
                    if not user_id:
                        raise ValueError("user_id required for new session")
                    session_id = self.create_session(user_id)

                session = self._sessions[session_id]
                session.update_activity()

                # Get connection from pool
                connection = self.connection_pool.get_connection()

                # Associate connection with session
                session.connection = connection

            yield connection.connection

        except Exception as e:
            logger.error(f"Error in session {session_id}: {e}")
            raise
        finally:
            if connection:
                with self._lock:
                    # Return connection to pool
                    self.connection_pool.return_connection(connection)

                    # Clear session connection reference
                    if session:
                        session.connection = None

                    self.stats.total_connections_used += 1

    def execute_in_session(self, session_id: str, func: Callable, user_id: str = None, *args, **kwargs) -> Any:
        """Execute a function within a database session with retry logic"""
        def _execute():
            with self.get_connection(session_id, user_id) as conn:
                return func(conn, *args, **kwargs)

        return self.retry_handler.execute_with_retry(_execute)

    def close_session(self, session_id: str):
        """Close a specific session"""
        with self._lock:
            if session_id in self._sessions:
                session = self._sessions[session_id]
                session.is_active = False

                # Close associated connection if any
                if session.connection:
                    self.connection_pool.return_connection(session.connection)
                    session.connection = None

                # Remove from user sessions
                user_id = session.user_id
                if user_id in self._user_sessions:
                    self._user_sessions[user_id] = [
                        sid for sid in self._user_sessions[user_id] if sid != session_id
                    ]
                    if not self._user_sessions[user_id]:
                        del self._user_sessions[user_id]

                # Remove session
                del self._sessions[session_id]
                self.stats.active_sessions -= 1

                logger.info(f"Closed session {session_id} for user {user_id}")

    def close_user_sessions(self, user_id: str):
        """Close all sessions for a user"""
        with self._lock:
            session_ids = self._user_sessions.get(user_id, []).copy()
            for session_id in session_ids:
                self.close_session(session_id)

    def cleanup_expired_sessions(self):
        """Clean up expired sessions"""
        with self._lock:
            expired_sessions = []
            now = datetime.now()

            for session_id, session in self._sessions.items():
                if session.is_expired(self.config.session_timeout):
                    expired_sessions.append(session_id)

            for session_id in expired_sessions:
                self.close_session(session_id)
                self.stats.expired_sessions += 1

            if expired_sessions:
                logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")

    def get_session_stats(self) -> Dict[str, Any]:
        """Get session statistics"""
        with self._lock:
            # Calculate average session age
            if self.stats.active_sessions > 0:
                total_age = sum(
                    (datetime.now() - session.created_at).total_seconds()
                    for session in self._sessions.values()
                    if session.is_active
                )
                self.stats.avg_session_age = total_age / self.stats.active_sessions

            return {
                'total_sessions': self.stats.total_sessions,
                'active_sessions': self.stats.active_sessions,
                'expired_sessions': self.stats.expired_sessions,
                'avg_session_age_seconds': self.stats.avg_session_age,
                'max_concurrent_sessions': self.stats.max_concurrent_sessions,
                'total_connections_used': self.stats.total_connections_used,
                'connection_pool_size': self.connection_pool.pool_size,
                'connection_pool_active': self.connection_pool.active_connections,
                'health_status': self.health_checker.get_health_status()
            }

    def _create_test_connection(self):
        """Create a test connection for health checking"""
        return self.connection_pool._create_connection().connection

    def _start_cleanup_thread(self):
        """Start the session cleanup thread"""
        self._cleanup_running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
            name="Session-Cleanup"
        )
        self._cleanup_thread.start()
        logger.info("Session cleanup thread started")

    def _cleanup_loop(self):
        """Background cleanup loop for expired sessions"""
        while self._cleanup_running:
            try:
                self.cleanup_expired_sessions()
                time.sleep(self._cleanup_interval)
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                time.sleep(60)  # Wait a minute before retrying

    def shutdown(self):
        """Shutdown the session manager"""
        with self._lock:
            self._cleanup_running = False

            # Close all sessions
            session_ids = list(self._sessions.keys())
            for session_id in session_ids:
                self.close_session(session_id)

            # Close connection pool
            self.connection_pool.close_all()

            # Stop health monitoring
            self.health_checker.stop_monitoring()

            # Wait for cleanup thread
            if self._cleanup_thread and self._cleanup_thread.is_alive():
                self._cleanup_thread.join(timeout=5.0)

            logger.info("Database session manager shutdown complete")

# Global session manager instance
_session_manager: Optional[DatabaseSessionManager] = None

def get_session_manager(config: Config = None) -> DatabaseSessionManager:
    """Get the global session manager instance"""
    global _session_manager
    if _session_manager is None:
        if config is None:
            # Initialize Config the same way as working dashboard.py
            config_kwargs = {
                "host": os.getenv("DATABRICKS_HOST")
            }

            # Only add client credentials if they exist and are not empty
            client_id = os.getenv("DATABRICKS_CLIENT_ID")
            client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

            if client_id and client_id.strip():
                config_kwargs["client_id"] = client_id
            if client_secret and client_secret.strip():
                config_kwargs["client_secret"] = client_secret

            config = Config(**config_kwargs)
        _session_manager = DatabaseSessionManager(config)
    return _session_manager

def create_user_session(user_id: str, metadata: Dict[str, Any] = None) -> str:
    """Create a new session for a user"""
    return get_session_manager().create_session(user_id, metadata)

def execute_in_user_session(session_id: str, func: Callable, user_id: str = None, *args, **kwargs) -> Any:
    """Execute a function in a user session"""
    return get_session_manager().execute_in_session(session_id, func, user_id, *args, **kwargs)

@contextmanager
def user_session(session_id: str, user_id: str = None):
    """Context manager for user database sessions"""
    with get_session_manager().get_connection(session_id, user_id) as conn:
        yield conn
