"""
Database Configuration for FCST Application
Centralized configuration management for database connections
"""
from dotenv import load_dotenv
import os

# Load environment variables from .env file if not already loaded
load_dotenv()

from typing import Dict, Any
from .connection_pool import ConnectionConfig

# Database configuration
DATABASE_CONFIG = ConnectionConfig(
    # Databricks connection settings
    host=os.getenv("DATABRICKS_HOST"),
    http_path=os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/62d47c983bb6df91"),
    client_id=os.getenv("DATABRICKS_CLIENT_ID"),
    client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),  # Optional - may not be needed for some auth methods

    # Connection pool settings
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
    pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
    pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "3600")),  # 1 hour

    # Retry settings
    retry_attempts=int(os.getenv("DB_RETRY_ATTEMPTS", "3")),
    retry_delay=float(os.getenv("DB_RETRY_DELAY", "1.0")),

    # Health check settings
    health_check_interval=int(os.getenv("DB_HEALTH_CHECK_INTERVAL", "300")),  # 5 minutes

    # Session settings
    session_timeout=int(os.getenv("DB_SESSION_TIMEOUT", "1800"))  # 30 minutes
)

# Environment validation
def validate_database_config() -> Dict[str, Any]:
    """Validate database configuration and return status"""
    issues = []

    if not DATABASE_CONFIG.host:
        issues.append("DATABRICKS_HOST environment variable not set")

    if not DATABASE_CONFIG.client_id:
        issues.append("DATABRICKS_CLIENT_ID environment variable not set")

    if not DATABASE_CONFIG.client_secret:
        issues.append("DATABRICKS_CLIENT_SECRET environment variable not set")

    if not DATABASE_CONFIG.http_path:
        issues.append("DATABRICKS_HTTP_PATH environment variable not set")

    return {
        "is_valid": len(issues) == 0,
        "issues": issues,
        "config": {
            "host": DATABASE_CONFIG.host,
            "http_path": DATABASE_CONFIG.http_path,
            "pool_size": DATABASE_CONFIG.pool_size,
            "max_overflow": DATABASE_CONFIG.max_overflow,
            "retry_attempts": DATABASE_CONFIG.retry_attempts,
            "health_check_interval": DATABASE_CONFIG.health_check_interval,
            "session_timeout": DATABASE_CONFIG.session_timeout
        }
    }

# Logging configuration for database operations
DATABASE_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "database": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "database_file": {
            "class": "logging.FileHandler",
            "filename": "logs/database.log",
            "formatter": "database",
            "level": "INFO"
        },
        "database_console": {
            "class": "logging.StreamHandler",
            "formatter": "database",
            "level": "WARNING"
        }
    },
    "loggers": {
        "database": {
            "handlers": ["database_file", "database_console"],
            "level": "INFO",
            "propagate": False
        },
        "connection_pool": {
            "handlers": ["database_file", "database_console"],
            "level": "INFO",
            "propagate": False
        },
        "session_manager": {
            "handlers": ["database_file", "database_console"],
            "level": "INFO",
            "propagate": False
        }
    }
}

# Performance monitoring settings
PERFORMANCE_CONFIG = {
    "enable_metrics": os.getenv("DB_ENABLE_METRICS", "true").lower() == "true",
    "metrics_interval": int(os.getenv("DB_METRICS_INTERVAL", "60")),  # 1 minute
    "slow_query_threshold": float(os.getenv("DB_SLOW_QUERY_THRESHOLD", "5.0")),  # 5 seconds
    "enable_query_logging": os.getenv("DB_ENABLE_QUERY_LOGGING", "false").lower() == "true"
}

def get_database_config() -> ConnectionConfig:
    """Get the database configuration"""
    return DATABASE_CONFIG

def setup_database_logging():
    """Setup logging for database operations"""
    import logging.config

    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Configure logging
    logging.config.dictConfig(DATABASE_LOGGING_CONFIG)

    # Set up specific loggers
    logging.getLogger("database").info("Database logging initialized")
    logging.getLogger("connection_pool").info("Connection pool logging initialized")
    logging.getLogger("session_manager").info("Session manager logging initialized")
