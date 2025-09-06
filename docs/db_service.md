# Database Service API Documentation

The `db_service.py` module provides a comprehensive database abstraction layer for DuckDB operations, handling all data persistence and retrieval for the FCST application.

## Classes

### DatabaseService

Main database service class that handles all database operations.

```python
class DatabaseService:
    """Database service for DuckDB operations."""
```

#### Initialization

```python
def __init__(self, db_path: str = "forecasting.duckdb"):
    """Initialize database service with connection."""
```

**Parameters:**
- `db_path` (str): Path to DuckDB database file

#### Core Methods

##### `get_sales_actuals(filters: Dict = None) -> pl.DataFrame`

Retrieves sales actuals data with optional filtering.

```python
def get_sales_actuals(self, filters: Dict = None) -> pl.DataFrame:
    """Get sales actuals data with joined hierarchy information."""
```

**Parameters:**
- `filters` (Dict, optional): Filter criteria for data retrieval

**Returns:**
- `pl.DataFrame`: Sales actuals with hierarchy data

**Usage:**
```python
db = get_database_service()
data = db.get_sales_actuals()
filtered_data = db.get_sales_actuals(filters={'region': 'North America'})
```

##### `get_filter_options() -> Dict[str, List[str]]`

Returns available filter options from hierarchy tables.

```python
def get_filter_options(self) -> Dict[str, List[str]]:
    """Get available filter options from hierarchy tables."""
```

**Returns:**
- `Dict[str, List[str]]`: Dictionary with filter options:
  - `franchises`: List of available franchises
  - `ibp_level_5s`: List of IBP Level 5 values
  - `ibp_level_6s`: List of IBP Level 6 values
  - `catalog_numbers`: List of catalog numbers
  - `regions`: List of regions
  - `countries`: List of countries
  - `areas`: List of areas

**Usage:**
```python
db = get_database_service()
options = db.get_filter_options()
print(f"Available regions: {options['regions']}")
```

##### `save_forecast_results(results: pl.DataFrame) -> None`

Saves forecast results to the database.

```python
def save_forecast_results(self, results: pl.DataFrame) -> None:
    """Save forecast results to database."""
```

**Parameters:**
- `results` (pl.DataFrame): Forecast results dataframe

**Expected DataFrame Schema:**
```python
{
    'product_id': str,
    'location_id': str,
    'forecast_date': datetime,
    'model_name': str,
    'forecast_value': float,
    'confidence_lower': float,
    'confidence_upper': float
}
```

**Usage:**
```python
db = get_database_service()
forecast_df = pl.DataFrame({
    'product_id': ['P001', 'P002'],
    'location_id': ['L001', 'L001'],
    'forecast_date': [datetime(2024, 1, 1), datetime(2024, 1, 1)],
    'model_name': ['NHITS', 'NHITS'],
    'forecast_value': [1000.0, 1500.0],
    'confidence_lower': [900.0, 1350.0],
    'confidence_upper': [1100.0, 1650.0]
})
db.save_forecast_results(forecast_df)
```

##### `get_model_validation_results() -> pl.DataFrame`

Retrieves model validation results from database.

```python
def get_model_validation_results(self) -> pl.DataFrame:
    """Get model validation results."""
```

**Returns:**
- `pl.DataFrame`: Validation results with metrics

**Usage:**
```python
db = get_database_service()
validation_results = db.get_model_validation_results()
```

##### `save_validation_results(results: Dict) -> None`

Saves model validation results to database.

```python
def save_validation_results(self, results: Dict) -> None:
    """Save model validation results."""
```

**Parameters:**
- `results` (Dict): Validation results dictionary

**Expected Results Format:**
```python
{
    'model_name': str,
    'validation_date': datetime,
    'mae': float,
    'mape': float,
    'rmse': float,
    'accuracy_percentage': float,
    'forecast_bias': float,
    'validation_period_months': int
}
```

#### Transaction Management

##### `begin_transaction() -> None`

Begins a database transaction.

```python
def begin_transaction(self) -> None:
    """Begin database transaction."""
```

##### `commit_transaction() -> None`

Commits the current transaction.

```python
def commit_transaction(self) -> None:
    """Commit current transaction."""
```

##### `rollback_transaction() -> None`

Rolls back the current transaction.

```python
def rollback_transaction(self) -> None:
    """Rollback current transaction."""
```

**Transaction Usage:**
```python
db = get_database_service()
try:
    db.begin_transaction()
    db.save_forecast_results(forecast_data)
    db.save_validation_results(validation_data)
    db.commit_transaction()
except Exception as e:
    db.rollback_transaction()
    raise e
```

#### Migration and Maintenance

##### `migrate_from_parquet(parquet_path: str) -> None`

Migrates data from parquet files to DuckDB.

```python
def migrate_from_parquet(self, parquet_path: str) -> None:
    """Migrate data from parquet files to DuckDB."""
```

**Parameters:**
- `parquet_path` (str): Path to parquet files directory

##### `create_indexes() -> None`

Creates database indexes for performance optimization.

```python
def create_indexes(self) -> None:
    """Create database indexes for performance."""
```

##### `vacuum_database() -> None`

Optimizes database storage and performance.

```python
def vacuum_database() -> None:
    """Vacuum database for optimization."""
```

##### `get_database_stats() -> Dict`

Returns database statistics and metadata.

```python
def get_database_stats(self) -> Dict:
    """Get database statistics."""
```

**Returns:**
- `Dict`: Database statistics including table sizes, row counts, etc.

## Global Functions

### `get_database_service() -> DatabaseService`

Returns the global database service instance (singleton pattern).

```python
def get_database_service() -> DatabaseService:
    """Get the global database service instance."""
```

**Returns:**
- `DatabaseService`: Global database service instance

### `close_database_service() -> None`

Closes the database connection and cleans up resources.

```python
def close_database_service() -> None:
    """Close database service and cleanup resources."""
```

## Database Schema

### Tables

#### `sales_actuals`
Main fact table containing historical sales data.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `product_id` | VARCHAR | Foreign key to product_hierarchy |
| `location_id` | VARCHAR | Foreign key to location_hierarchy |
| `sales_date` | DATE | Date of sales record |
| `act_orders_rev` | DECIMAL | Actual orders revenue |
| `fcst_stat_prelim_rev` | DECIMAL | Statistical preliminary forecast |
| `fcst_stat_final_rev` | DECIMAL | Statistical final forecast |
| `l2_stat_final_rev` | DECIMAL | L2 statistical final forecast |
| `fcst_df_final_rev` | DECIMAL | DF final forecast |
| `l2_df_final_rev` | DECIMAL | L2 DF final forecast |

#### `product_hierarchy`
Product hierarchy and catalog information.

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | VARCHAR | Primary key |
| `catalog_number` | VARCHAR | Product catalog number |
| `business_unit` | VARCHAR | Business unit |
| `franchise` | VARCHAR | Product franchise |
| `ibp_level_5` | VARCHAR | IBP Level 5 classification |
| `ibp_level_6` | VARCHAR | IBP Level 6 classification |

#### `location_hierarchy`
Geographic hierarchy information.

| Column | Type | Description |
|--------|------|-------------|
| `location_id` | VARCHAR | Primary key |
| `area` | VARCHAR | Geographic area |
| `region` | VARCHAR | Geographic region |
| `country` | VARCHAR | Country |

#### `forecasts`
Model forecast results storage.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `product_id` | VARCHAR | Product identifier |
| `location_id` | VARCHAR | Location identifier |
| `forecast_date` | DATE | Date of forecast |
| `model_name` | VARCHAR | Name of forecasting model |
| `forecast_value` | DECIMAL | Predicted value |
| `confidence_lower` | DECIMAL | Lower confidence bound |
| `confidence_upper` | DECIMAL | Upper confidence bound |
| `created_at` | TIMESTAMP | Record creation timestamp |

#### `model_validation`
Model validation results and metrics.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Primary key |
| `model_name` | VARCHAR | Name of validated model |
| `validation_date` | DATE | Date of validation |
| `mae` | DECIMAL | Mean Absolute Error |
| `mape` | DECIMAL | Mean Absolute Percentage Error |
| `rmse` | DECIMAL | Root Mean Square Error |
| `accuracy_percentage` | DECIMAL | Accuracy percentage |
| `forecast_bias` | DECIMAL | Forecast bias metric |
| `validation_period_months` | INTEGER | Validation period in months |

## Performance Optimization

### Query Optimization
- Use appropriate WHERE clauses for filtering
- Leverage indexes on frequently queried columns
- Use LIMIT for large result sets
- Batch operations when possible

### Memory Management
```python
# Configure DuckDB memory settings
db.connection.execute("SET memory_limit='4GB'")
db.connection.execute("SET threads=4")
```

### Connection Pooling
The service uses a singleton pattern for connection management:
- Single connection per application instance
- Automatic connection cleanup on exit
- Thread-safe operations

## Error Handling

### Common Exceptions
- `DatabaseConnectionError`: Connection issues
- `QueryExecutionError`: SQL execution failures
- `DataValidationError`: Invalid data format
- `TransactionError`: Transaction management issues

### Error Recovery
```python
try:
    db = get_database_service()
    data = db.get_sales_actuals()
except DatabaseConnectionError:
    # Retry connection or use fallback
    pass
except QueryExecutionError as e:
    # Log error and handle gracefully
    logger.error(f"Query failed: {e}")
```

## Migration Guide

### From Parquet to DuckDB
1. Run migration script: `python migrate_to_duckdb.py`
2. Verify data integrity
3. Update application configuration
4. Test all functionality

### Schema Updates
1. Create migration scripts for schema changes
2. Use transactions for atomic updates
3. Backup database before migrations
4. Test migrations on development environment first
