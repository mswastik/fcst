"""
Retry Handler with Exponential Backoff for Database Operations
"""
import time
import random
import logging
from typing import Callable, Any, Optional, Type, Tuple
from functools import wraps

logger = logging.getLogger(__name__)

class RetryError(Exception):
    """Exception raised when all retry attempts are exhausted"""
    def __init__(self, message: str, last_exception: Exception):
        super().__init__(message)
        self.last_exception = last_exception

class RetryHandler:
    """Handles retry logic with exponential backoff for database operations"""

    def __init__(self,
                 max_attempts: int = 3,
                 base_delay: float = 1.0,
                 max_delay: float = 60.0,
                 backoff_factor: float = 2.0,
                 jitter: bool = True):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt using exponential backoff"""
        delay = min(self.base_delay * (self.backoff_factor ** attempt), self.max_delay)

        if self.jitter:
            # Add random jitter to prevent thundering herd
            delay = delay * (0.5 + random.random() * 0.5)

        return delay

    def _should_retry(self, exception: Exception) -> bool:
        """Determine if the exception should trigger a retry"""
        # Retry on common database connection errors
        retry_exceptions = (
            ConnectionError,
            TimeoutError,
            OSError,  # Includes network-related errors
        )

        # Also retry on specific Databricks/SQL errors
        error_message = str(exception).lower()
        retry_messages = [
            'connection',
            'timeout',
            'network',
            'temporary',
            'unavailable',
            'overload',
            'rate limit'
        ]

        return (
            isinstance(exception, retry_exceptions) or
            any(msg in error_message for msg in retry_messages)
        )

    def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function with retry logic"""
        last_exception = None

        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e

                if attempt == self.max_attempts - 1:
                    # Last attempt failed
                    break

                if not self._should_retry(e):
                    # Exception is not retryable
                    break

                delay = self._calculate_delay(attempt)
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f} seconds...")
                time.sleep(delay)

        # All attempts failed
        raise RetryError(
            f"Operation failed after {self.max_attempts} attempts",
            last_exception
        )

    def __call__(self, func: Callable) -> Callable:
        """Decorator version of retry logic"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.execute_with_retry(func, *args, **kwargs)
        return wrapper

# Convenience functions for common retry scenarios
def retry_on_connection_error(max_attempts: int = 3, base_delay: float = 1.0):
    """Decorator for retrying on connection errors"""
    def decorator(func):
        retry_handler = RetryHandler(max_attempts=max_attempts, base_delay=base_delay)
        return retry_handler(func)
    return decorator

def execute_with_retry(func: Callable,
                      max_attempts: int = 3,
                      base_delay: float = 1.0,
                      *args, **kwargs) -> Any:
    """Convenience function for executing with retry"""
    retry_handler = RetryHandler(max_attempts=max_attempts, base_delay=base_delay)
    return retry_handler.execute_with_retry(func, *args, **kwargs)

class CircuitBreaker:
    """Circuit breaker pattern for handling repeated failures"""

    def __init__(self,
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 expected_exception: Type[Exception] = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN

    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit"""
        if self.state != 'OPEN':
            return True

        if self.last_failure_time is None:
            return True

        elapsed = time.time() - self.last_failure_time
        return elapsed >= self.recovery_timeout

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker"""
        if self.state == 'OPEN':
            if not self._should_attempt_reset():
                raise CircuitBreakerOpenException("Circuit breaker is OPEN")

            self.state = 'HALF_OPEN'
            logger.info("Circuit breaker moving to HALF_OPEN state")

        try:
            result = func(*args, **kwargs)

            if self.state == 'HALF_OPEN':
                self._reset()
                logger.info("Circuit breaker reset to CLOSED state")

            return result

        except self.expected_exception as e:
            self._record_failure()
            raise

    def _record_failure(self):
        """Record a failure and potentially open the circuit"""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'
            logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

    def _reset(self):
        """Reset the circuit breaker to closed state"""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'

class CircuitBreakerOpenException(Exception):
    """Exception raised when circuit breaker is open"""
    pass
