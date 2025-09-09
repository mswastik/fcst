# Utilities API Documentation

The `utils.py` module provides common utilities and helper classes for the FCST application, centralizing frequently used patterns to reduce code duplication and improve maintainability.

## Constants

### COLUMN_MAPPING

Standard column name mapping dictionary for data transformation.

```python
COLUMN_MAPPING = {
    'act_orders_rev': 'Act Orders Rev',
    'fcst_stat_prelim_rev': 'Fcst Stat Prelim Rev',
    'fcst_stat_final_rev': 'Fcst Stat Final Rev',
    'l2_stat_final_rev': 'L2 Stat Final Rev',
    'fcst_df_final_rev': 'Fcst DF Final Rev',
    'l2_df_final_rev': 'L2 DF Final Rev',
    'sales_date': 'SALES_DATE',
    'catalog_number': 'CatalogNumber',
    'region': 'Region',
    'country': 'Country',
    'area': 'Area',
    'business_unit': 'Business Unit',
    'franchise': 'Franchise',
    'ibp_level_5': 'IBP Level 5',
    'ibp_level_6': 'IBP Level 6'
}
```

### NUMERIC_COLUMNS

List of numeric column names used in data processing.

```python
NUMERIC_COLUMNS = [
    'Act Orders Rev', 'Fcst Stat Prelim Rev', 'Fcst Stat Final Rev',
    'L2 Stat Final Rev', 'Fcst DF Final Rev', 'L2 DF Final Rev'
]
```

## Classes

### DataUtils

Utility class for data manipulation operations.

```python
class DataUtils:
    """Utility class for data manipulation operations."""
```

#### Methods

##### `apply_column_mapping(df: pl.DataFrame) -> pl.DataFrame`

Applies standard column name mapping to a Polars DataFrame.

```python
@staticmethod
def apply_column_mapping(df: pl.DataFrame) -> pl.DataFrame:
    """Apply standard column name mapping to dataframe."""
```

**Parameters:**
- `df` (pl.DataFrame): Input dataframe

**Returns:**
- `pl.DataFrame`: DataFrame with standardized column names

**Usage:**
```python
from core.utils import DataUtils

# Apply column mapping
df_mapped = DataUtils.apply_column_mapping(raw_df)
print(df_mapped.columns)  # ['Act Orders Rev', 'SALES_DATE', ...]
```

##### `ensure_numeric_columns(df: pl.DataFrame) -> pl.DataFrame`

Ensures numeric columns are properly typed as Float32.

```python
@staticmethod
def ensure_numeric_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Ensure numeric columns are properly typed."""
```

**Parameters:**
- `df` (pl.DataFrame): Input dataframe

**Returns:**
- `pl.DataFrame`: DataFrame with properly typed numeric columns

##### `validate_data_integrity(df: pl.DataFrame) -> Dict`

Performs comprehensive data validation checks.

```python
@staticmethod
def validate_data_integrity(df: pl.DataFrame) -> Dict:
    """Validate data integrity and return validation results."""
```

**Parameters:**
- `df` (pl.DataFrame): DataFrame to validate

**Returns:**
- `Dict`: Validation results including:
  - `total_rows`: Number of rows
  - `null_counts`: Null count per column
  - `duplicate_rows`: Number of duplicate rows
  - `date_range`: Min/max dates
  - `numeric_stats`: Statistics for numeric columns

### DatabaseUtils

Utility class for database operations and connections.

```python
class DatabaseUtils:
    """Utility class for database operations."""
```

#### Methods

##### `get_database_service() -> DatabaseService`

Returns the global database service instance (singleton pattern).

```python
@staticmethod
def get_database_service() -> DatabaseService:
    """Get the global database service instance."""
```

**Returns:**
- `DatabaseService`: Global database service instance

**Usage:**
```python
from core.utils import DatabaseUtils

db = DatabaseUtils.get_database_service()
data = db.get_sales_actuals()
```

##### `test_database_connection() -> bool`

Tests database connectivity.

```python
@staticmethod
def test_database_connection() -> bool:
    """Test database connection."""
```

**Returns:**
- `bool`: True if connection successful, False otherwise

##### `get_table_info(table_name: str) -> Dict`

Retrieves information about a database table.

```python
@staticmethod
def get_table_info(table_name: str) -> Dict:
    """Get information about a database table."""
```

**Parameters:**
- `table_name` (str): Name of the table

**Returns:**
- `Dict`: Table information including column names, types, and row count

### UIUtils

Utility class for UI-related helper functions.

```python
class UIUtils:
    """Utility class for UI operations."""
```

#### Methods

##### `show_loading_spinner(message: str = "Loading...") -> ui.spinner`

Displays a loading spinner with custom message.

```python
@staticmethod
def show_loading_spinner(message: str = "Loading...") -> ui.spinner:
    """Show loading spinner with message."""
```

**Parameters:**
- `message` (str): Loading message to display

**Returns:**
- `ui.spinner`: NiceGUI spinner element

**Usage:**
```python
from core.utils import UIUtils

spinner = UIUtils.show_loading_spinner("Processing data...")
try:
    # Long running operation
    process_data()
finally:
    spinner.delete()
```

##### `create_notification(message: str, type: str = "info", duration: int = 3000) -> None`

Creates a user notification.

```python
@staticmethod
def create_notification(message: str, type: str = "info", duration: int = 3000) -> None:
    """Create a user notification."""
```

**Parameters:**
- `message` (str): Notification message
- `type` (str): Notification type ('info', 'success', 'warning', 'error')
- `duration` (int): Display duration in milliseconds

##### `format_number(value: float, decimals: int = 2) -> str`

Formats numeric values for display.

```python
@staticmethod
def format_number(value: float, decimals: int = 2) -> str:
    """Format numeric value for display."""
```

**Parameters:**
- `value` (float): Numeric value to format
- `decimals` (int): Number of decimal places

**Returns:**
- `str`: Formatted string representation

**Usage:**
```python
formatted = UIUtils.format_number(1234.567, decimals=1)  # "1,234.6"
```

##### `create_confirm_dialog(message: str, on_confirm: Callable, on_cancel: Callable = None) -> None`

Creates a confirmation dialog.

```python
@staticmethod
def create_confirm_dialog(message: str, on_confirm: Callable, on_cancel: Callable = None) -> None:
    """Create a confirmation dialog."""
```

**Parameters:**
- `message` (str): Confirmation message
- `on_confirm` (Callable): Function to call on confirmation
- `on_cancel` (Callable, optional): Function to call on cancellation

### ErrorHandler

Utility class for error handling and logging.

```python
class ErrorHandler:
    """Utility class for error handling and logging."""
```

#### Methods

##### `handle_error(error: Exception, context: str = "", show_ui: bool = True) -> None`

Handles exceptions with logging and optional UI notification.

```python
@staticmethod
def handle_error(error: Exception, context: str = "", show_ui: bool = True) -> None:
    """Handle error with logging and optional UI notification."""
```

**Parameters:**
- `error` (Exception): The exception to handle
- `context` (str): Context information about where the error occurred
- `show_ui` (bool): Whether to show UI notification

**Usage:**
```python
from core.utils import ErrorHandler

try:
    risky_operation()
except Exception as e:
    ErrorHandler.handle_error(e, "Failed to load data")
```

##### `log_operation(operation: str, duration: float = None, success: bool = True) -> None`

Logs operation completion with timing information.

```python
@staticmethod
def log_operation(operation: str, duration: float = None, success: bool = True) -> None:
    """Log operation completion with timing."""
```

**Parameters:**
- `operation` (str): Name of the operation
- `duration` (float, optional): Operation duration in seconds
- `success` (bool): Whether the operation was successful

##### `create_error_report(error: Exception, context: Dict = None) -> Dict`

Creates a comprehensive error report.

```python
@staticmethod
def create_error_report(error: Exception, context: Dict = None) -> Dict:
    """Create comprehensive error report."""
```

**Parameters:**
- `error` (Exception): The exception
- `context` (Dict, optional): Additional context information

**Returns:**
- `Dict`: Error report with timestamp, error type, message, and context

## Usage Examples

### Data Processing Pipeline

```python
from core.utils import DataUtils, ErrorHandler
import polars as pl

def process_data_pipeline(raw_df: pl.DataFrame) -> pl.DataFrame:
    """Complete data processing pipeline using utilities."""
    try:
        # Apply column mapping
        df = DataUtils.apply_column_mapping(raw_df)

        # Ensure proper data types
        df = DataUtils.ensure_numeric_columns(df)

        # Validate data integrity
        validation = DataUtils.validate_data_integrity(df)
        print(f"Validation results: {validation}")

        # Log successful operation
        ErrorHandler.log_operation("data_processing", success=True)

        return df

    except Exception as e:
        ErrorHandler.handle_error(e, "Data processing pipeline failed")
        raise
```

### Database Operations with Error Handling

```python
from core.utils import DatabaseUtils, ErrorHandler

def safe_database_operation():
    """Safe database operation with error handling."""
    db = None
    try:
        db = DatabaseUtils.get_database_service()

        # Test connection
        if not DatabaseUtils.test_database_connection():
            raise ConnectionError("Database connection failed")

        # Perform operation
        data = db.get_sales_actuals()
        return data

    except Exception as e:
        ErrorHandler.handle_error(e, "Database operation failed")
        return None
```

### UI Operations with Loading States

```python
from core.utils import UIUtils
from nicegui import ui

def load_data_with_ui_feedback():
    """Load data with UI feedback."""
    # Show loading spinner
    spinner = UIUtils.show_loading_spinner("Loading data...")

    try:
        # Perform data loading
        data = load_data_from_database()

        # Show success notification
        UIUtils.create_notification(
            f"Successfully loaded {len(data)} records",
            type="success"
        )

        return data

    except Exception as e:
        # Show error notification
        UIUtils.create_notification(
            f"Failed to load data: {str(e)}",
            type="error"
        )
        return None

    finally:
        # Always remove spinner
        spinner.delete()
```

### Comprehensive Error Handling

```python
from core.utils import ErrorHandler, UIUtils

def robust_operation():
    """Example of robust operation with comprehensive error handling."""
    try:
        # Log operation start
        start_time = time.time()

        # Perform operation
        result = perform_complex_operation()

        # Calculate duration
        duration = time.time() - start_time

        # Log success
        ErrorHandler.log_operation("complex_operation", duration=duration)

        # Show success notification
        UIUtils.create_notification("Operation completed successfully", "success")

        return result

    except ValueError as e:
        ErrorHandler.handle_error(e, "Invalid input data")
        UIUtils.create_notification("Invalid input data provided", "warning")

    except ConnectionError as e:
        ErrorHandler.handle_error(e, "Network connectivity issue")
        UIUtils.create_notification("Network connection failed", "error")

    except Exception as e:
        # Create detailed error report
        error_report = ErrorHandler.create_error_report(e, {
            'operation': 'complex_operation',
            'timestamp': datetime.now().isoformat()
        })

        ErrorHandler.handle_error(e, "Unexpected error in complex operation")
        UIUtils.create_notification("An unexpected error occurred", "error")

        # Could send error report to logging service
        # send_error_report_to_service(error_report)
```

## Best Practices

### When to Use Each Utility Class

- **DataUtils**: For any data transformation, validation, or cleaning operations
- **DatabaseUtils**: For database connection management and table operations
- **UIUtils**: For user interface feedback, notifications, and formatting
- **ErrorHandler**: For consistent error handling and logging across the application

### Performance Considerations

- Use static methods for stateless operations to avoid object creation overhead
- Implement caching for frequently accessed data in DatabaseUtils
- Use ErrorHandler for centralized logging configuration
- UIUtils methods are optimized for NiceGUI's reactive system

### Error Handling Strategy

1. **Catch specific exceptions** first (ValueError, ConnectionError, etc.)
2. **Use ErrorHandler.handle_error()** for consistent logging and UI feedback
3. **Create error reports** for complex operations with context
4. **Always clean up resources** in finally blocks
5. **Provide user-friendly messages** through UIUtils notifications

This utilities module provides a solid foundation for maintainable, error-resistant code throughout the FCST application.
