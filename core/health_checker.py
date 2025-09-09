"""
Connection Health Checker for Database Connections
Provides comprehensive health validation for database connections
"""
import threading
import time
import logging
from typing import Optional, Callable, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class HealthCheckResult:
    """Result of a health check operation"""
    is_healthy: bool
    response_time: float
    error_message: Optional[str] = None
    timestamp: datetime = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.metadata is None:
            self.metadata = {}

class ConnectionHealthChecker:
    """Comprehensive health checker for database connections"""

    def __init__(self,
                 connection_factory: Callable,
                 health_check_query: str = "SELECT 1",
                 timeout: float = 5.0,
                 interval: int = 300,  # 5 minutes
                 max_failures: int = 3):
        self.connection_factory = connection_factory
        self.health_check_query = health_check_query
        self.timeout = timeout
        self.interval = interval
        self.max_failures = max_failures

        self._lock = threading.RLock()
        self._last_check = datetime.min
        self._consecutive_failures = 0
        self._is_healthy = True
        self._last_result: Optional[HealthCheckResult] = None
        self._monitoring = False
        self._monitor_thread: Optional[threading.Thread] = None

    def check_health(self) -> HealthCheckResult:
        """Perform a health check on the database connection"""
        start_time = time.time()

        try:
            # Create a test connection
            connection = self.connection_factory()

            try:
                # Execute health check query
                with connection.cursor() as cursor:
                    cursor.execute(self.health_check_query)
                    result = cursor.fetchone()

                    response_time = time.time() - start_time

                    if result and result[0] == 1:
                        # Health check passed
                        return HealthCheckResult(
                            is_healthy=True,
                            response_time=response_time,
                            metadata={
                                'query_result': result[0],
                                'connection_type': type(connection).__name__
                            }
                        )
                    else:
                        # Health check failed - unexpected result
                        return HealthCheckResult(
                            is_healthy=False,
                            response_time=response_time,
                            error_message=f"Unexpected health check result: {result}",
                            metadata={'query_result': result}
                        )

            finally:
                # Always close the test connection
                try:
                    connection.close()
                except Exception as e:
                    logger.warning(f"Error closing test connection: {e}")

        except Exception as e:
            response_time = time.time() - start_time
            error_msg = f"Health check failed: {str(e)}"

            logger.warning(f"Database health check failed: {error_msg}")

            return HealthCheckResult(
                is_healthy=False,
                response_time=response_time,
                error_message=error_msg,
                metadata={
                    'exception_type': type(e).__name__,
                    'connection_attempted': True
                }
            )

    def is_healthy(self, force_check: bool = False) -> bool:
        """Check if the database is healthy, with optional forced recheck"""
        with self._lock:
            now = datetime.now()

            # If we don't need to force a check and the last check is recent, return cached result
            if not force_check and (now - self._last_check).total_seconds() < self.interval:
                return self._is_healthy

            # Perform health check
            result = self.check_health()
            self._last_result = result
            self._last_check = now

            if result.is_healthy:
                self._consecutive_failures = 0
                self._is_healthy = True
            else:
                self._consecutive_failures += 1

                if self._consecutive_failures >= self.max_failures:
                    self._is_healthy = False
                    logger.error(f"Database marked as unhealthy after {self._consecutive_failures} consecutive failures")
                else:
                    # Still consider healthy until we hit the threshold
                    self._is_healthy = True

            return self._is_healthy

    def get_health_status(self) -> Dict[str, Any]:
        """Get comprehensive health status information"""
        with self._lock:
            return {
                'is_healthy': self._is_healthy,
                'last_check': self._last_check.isoformat() if self._last_check != datetime.min else None,
                'consecutive_failures': self._consecutive_failures,
                'last_result': {
                    'is_healthy': self._last_result.is_healthy if self._last_result else None,
                    'response_time': self._last_result.response_time if self._last_result else None,
                    'error_message': self._last_result.error_message if self._last_result else None,
                    'timestamp': self._last_result.timestamp.isoformat() if self._last_result else None,
                } if self._last_result else None,
                'monitoring_active': self._monitoring
            }

    def start_monitoring(self):
        """Start background health monitoring"""
        with self._lock:
            if self._monitoring:
                return

            self._monitoring = True
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                daemon=True,
                name="DB-Health-Monitor"
            )
            self._monitor_thread.start()
            logger.info("Database health monitoring started")

    def stop_monitoring(self):
        """Stop background health monitoring"""
        with self._lock:
            if not self._monitoring:
                return

            self._monitoring = False
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=5.0)

            logger.info("Database health monitoring stopped")

    def _monitor_loop(self):
        """Background monitoring loop"""
        while self._monitoring:
            try:
                # Perform health check
                is_healthy = self.is_healthy(force_check=True)

                if not is_healthy:
                    logger.warning("Database health check failed in monitoring loop")

                # Sleep until next check
                time.sleep(self.interval)

            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                time.sleep(min(self.interval, 60))  # Sleep for at least 1 minute on error

class DatabaseHealthMonitor:
    """Multi-database health monitoring system"""

    def __init__(self):
        self._checkers: Dict[str, ConnectionHealthChecker] = {}
        self._lock = threading.RLock()

    def register_checker(self, name: str, checker: ConnectionHealthChecker):
        """Register a health checker for a database"""
        with self._lock:
            self._checkers[name] = checker
            logger.info(f"Registered health checker for database: {name}")

    def unregister_checker(self, name: str):
        """Unregister a health checker"""
        with self._lock:
            if name in self._checkers:
                self._checkers[name].stop_monitoring()
                del self._checkers[name]
                logger.info(f"Unregistered health checker for database: {name}")

    def get_health_status(self, name: Optional[str] = None) -> Dict[str, Any]:
        """Get health status for all or specific database"""
        with self._lock:
            if name:
                checker = self._checkers.get(name)
                if checker:
                    return {name: checker.get_health_status()}
                else:
                    return {name: None}
            else:
                return {
                    db_name: checker.get_health_status()
                    for db_name, checker in self._checkers.items()
                }

    def check_all_health(self) -> Dict[str, bool]:
        """Check health of all registered databases"""
        with self._lock:
            return {
                db_name: checker.is_healthy(force_check=True)
                for db_name, checker in self._checkers.items()
            }

    def start_all_monitoring(self):
        """Start monitoring for all registered databases"""
        with self._lock:
            for name, checker in self._checkers.items():
                checker.start_monitoring()
                logger.info(f"Started health monitoring for database: {name}")

    def stop_all_monitoring(self):
        """Stop monitoring for all registered databases"""
        with self._lock:
            for name, checker in self._checkers.items():
                checker.stop_monitoring()
                logger.info(f"Stopped health monitoring for database: {name}")

# Global health monitor instance
health_monitor = DatabaseHealthMonitor()
