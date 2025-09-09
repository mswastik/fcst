# Data Processor API Documentation

The `data_processor.py` module provides comprehensive data processing utilities for the forecasting pipeline, including data cleaning, validation, hierarchy management, and forecast integration.

## DataCleaner

Handles data cleaning and preprocessing operations for forecasting models.

```python
class DataCleaner:
    """Handles data cleaning and preprocessing operations."""
```

### Methods

#### `prepare_data_for_forecasting(df: pl.DataFrame) -> pl.DataFrame`

Prepares data with proper handling of missing values and outliers using IQR method.

```python
@staticmethod
def prepare_data_for_forecasting(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare data with proper handling of missing values and outliers."""
```

**Parameters:**
- `df` (pl.DataFrame): Input dataframe with time series data

**Returns:**
- `pl.DataFrame`: Cleaned dataframe with outliers handled

**Outlier Detection Process:**
1. Calculate Q1 (25th percentile) and Q3 (75th percentile) per unique_id
2. Compute IQR (Interquartile Range) = Q3 - Q1
3. Define bounds: Lower = Q1 - 1.5*IQR, Upper = Q3 + 1.5*IQR
4. Cap outliers instead of removing them (preserves data continuity)

**Usage:**
```python
from forecasting.data_processor import DataCleaner

# Clean data before forecasting
clean_df = DataCleaner.prepare_data_for_forecasting(raw_df)
```

#### `filter_last_n_months(df: pl.DataFrame, months: int = 36) -> pl.DataFrame`

Filters data to include only the last N months of historical data.

```python
@staticmethod
def filter_last_n_months(df: pl.DataFrame, months: int = 36) -> pl.DataFrame:
    """Filter data to last N months."""
```

**Parameters:**
- `df` (pl.DataFrame): Input dataframe with SALES_DATE column
- `months` (int): Number of months to include (default: 36)

**Returns:**
- `pl.DataFrame`: Filtered dataframe with recent data only

**Date Filtering Logic:**
- Calculates last full month from current date
- Includes data from (last_full_month - months + 1) to last_full_month
- Uses date boundaries for precise filtering

#### `prepare_training_data(df: pl.DataFrame) -> pl.DataFrame`

Prepares data for training by cleaning, filtering, and adding required columns.

```python
@staticmethod
def prepare_training_data(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare data for training by cleaning and filtering."""
```

**Parameters:**
- `df` (pl.DataFrame): Raw input dataframe

**Returns:**
- `pl.DataFrame`: Prepared dataframe ready for model training

**Processing Steps:**
1. **Column Removal**: Drops irrelevant columns like UOM, NPI Flag, Pack Content, etc.
2. **Missing Value Handling**: Fills NaN values with 0
3. **Unique ID Creation**: Creates unique_id from Country + CatalogNumber if missing
4. **Date Filtering**: Applies 36-month filter using filter_last_n_months()

**Columns Removed:**
```python
columns_to_drop = [
    'UOM', 'NPI Flag', 'Pack Content', '`L0 ASP Final Rev', 'Act Orders Rev Val',
    'L2 DF Final Rev', 'L1 DF Final Rev', 'L0 DF Final Rev', 'L2 Stat Final Rev',
    '`Fcst DF Final Rev', '`Fcst Stat Final Rev', '`Fcst Stat Prelim Rev',
    'Fcst DF Final Rev Val'
]
```

## ForecastDataProcessor

Processes forecast data for integration with original dataset.

```python
class ForecastDataProcessor:
    """Processes forecast data for integration with original dataset."""
```

### Initialization

```python
def __init__(self):
    """Initialize ForecastDataProcessor with hierarchy loader."""
    self.hierarchy_loader = HierarchyLoader()
```

### Methods

#### `process_forecasts(forecasts: pl.DataFrame, original_df: pl.DataFrame, file_path: str) -> pl.DataFrame`

Process and integrate forecasts with original data.

```python
def process_forecasts(self, forecasts: pl.DataFrame, original_df: pl.DataFrame,
                     file_path: str) -> pl.DataFrame:
    """Process and integrate forecasts with original data."""
```

**Parameters:**
- `forecasts` (pl.DataFrame): Raw forecast dataframe from models
- `original_df` (pl.DataFrame): Original historical data
- `file_path` (str): File path for saving results

**Returns:**
- `pl.DataFrame`: Merged dataframe with forecasts integrated

**Processing Pipeline:**
1. **Column Renaming**: Rename 'ds' to 'SALES_DATE'
2. **Unique ID Splitting**: Split unique_id into Country and CatalogNumber
3. **Hierarchy Joining**: Join with product and location hierarchies
4. **Model Column Detection**: Identify forecast prediction columns
5. **Data Merging**: Merge forecasts with original data
6. **Result Saving**: Save to parquet file

### Private Methods

#### `_split_unique_id(forecasts: pl.DataFrame) -> pl.DataFrame`

Splits unique_id column into separate Country and CatalogNumber columns.

```python
def _split_unique_id(self, forecasts: pl.DataFrame) -> pl.DataFrame:
    """Split unique_id into Country and CatalogNumber columns."""
```

**Format:** "Country,CatalogNumber" → separate columns

#### `_join_hierarchy_data(forecasts: pl.DataFrame) -> pl.DataFrame`

Joins forecasts with product and location hierarchy data.

```python
def _join_hierarchy_data(self, forecasts: pl.DataFrame) -> pl.DataFrame:
    """Join forecasts with product and location hierarchy data."""
```

**Hierarchy Data Sources:**
- Product Hierarchy: `data/phierarchy.parquet`
- Location Hierarchy: `data/lhierarchy.parquet`

**Join Keys:**
- Product: CatalogNumber
- Location: Country

#### `_merge_with_original(original_df: pl.DataFrame, forecasts: pl.DataFrame, model_cols: List[str]) -> pl.DataFrame`

Merges forecast results with original dataframe using intelligent column matching.

```python
def _merge_with_original(self, original_df: pl.DataFrame, forecasts: pl.DataFrame,
                        model_cols: List[str]) -> pl.DataFrame:
```

**Smart Merge Logic:**
1. **Dynamic Join Columns**: Finds common columns between datasets
2. **Fallback Strategy**: Uses unique_id as minimum join key
3. **Column Filtering**: Only includes forecast-specific columns
4. **Error Handling**: Graceful fallback on merge failures

## HierarchyLoader

Loads and manages hierarchy data from parquet files.

```python
class HierarchyLoader:
    """Loads and manages hierarchy data."""
```

### Methods

#### `load_product_hierarchy() -> pl.DataFrame`

Loads product hierarchy data from parquet file.

```python
def load_product_hierarchy(self) -> pl.DataFrame:
    """Load product hierarchy data."""
```

**Returns:**
- `pl.DataFrame`: Product hierarchy with catalog numbers and classifications

**File:** `data/phierarchy.parquet`

#### `load_location_hierarchy() -> pl.DataFrame`

Loads location hierarchy data from parquet file.

```python
def load_location_hierarchy(self) -> pl.DataFrame:
    """Load location hierarchy data."""
```

**Returns:**
- `pl.DataFrame`: Location hierarchy with countries and regions

**File:** `data/lhierarchy.parquet`

**Processing:**
- Removes 'Selling Division' column if present
- Returns unique records only

## ValidationProcessor

Handles forecast validation operations.

```python
class ValidationProcessor:
    """Handles forecast validation operations."""
```

### Methods

#### `validate_forecasts(df: pl.DataFrame, forecast_df: pl.DataFrame, validation_months: int = 6) -> dict`

Validates forecasts using walk-forward validation methodology.

```python
@staticmethod
def validate_forecasts(df: pl.DataFrame, forecast_df: pl.DataFrame,
                      validation_months: int = 6) -> dict:
    """Validate forecasts using walk-forward validation."""
```

**Parameters:**
- `df` (pl.DataFrame): Historical data for validation
- `forecast_df` (pl.DataFrame): Forecast predictions to validate
- `validation_months` (int): Number of months for validation window

**Returns:**
- `dict`: Validation results with metrics and status

**Validation Structure:**
```python
{
    'validation_months': int,
    'status': str,  # 'completed', 'failed', etc.
    'metrics': dict  # Validation metrics (MAE, MAPE, RMSE, etc.)
}
```

## Usage Examples

### Complete Data Processing Pipeline

```python
from forecasting.data_processor import DataCleaner, ForecastDataProcessor

# 1. Clean and prepare data
clean_df = DataCleaner.prepare_training_data(raw_df)

# 2. Process forecasts (after model training)
processor = ForecastDataProcessor()
merged_df = processor.process_forecasts(
    forecasts=forecast_results,
    original_df=original_df,
    file_path="forecasts_2024.parquet"
)

# 3. Validate results
from forecasting.data_processor import ValidationProcessor
validation_results = ValidationProcessor.validate_forecasts(
    df=clean_df,
    forecast_df=merged_df,
    validation_months=6
)
```

### Data Cleaning Workflow

```python
# Handle outliers
clean_df = DataCleaner.prepare_data_for_forecasting(raw_df)

# Filter to recent data
recent_df = DataCleaner.filter_last_n_months(clean_df, months=24)

# Final preparation
training_df = DataCleaner.prepare_training_data(recent_df)
```

### Forecast Integration

```python
# Load hierarchies
loader = HierarchyLoader()
products = loader.load_product_hierarchy()
locations = loader.load_location_hierarchy()

# Process and merge forecasts
processor = ForecastDataProcessor()
result_df = processor.process_forecasts(forecasts, historical_df, "results")
```

## Error Handling

### File Loading Errors
```python
try:
    hierarchy_df = loader.load_product_hierarchy()
    if hierarchy_df.is_empty():
        print("Warning: Product hierarchy file not found or empty")
except Exception as e:
    print(f"Hierarchy loading failed: {e}")
```

### Data Merge Issues
```python
try:
    merged_df = processor._merge_with_original(original_df, forecasts, model_cols)
except Exception as e:
    print(f"Merge failed, using fallback: {e}")
    # Fallback merge logic
    merged_df = original_df.join(forecasts, on='unique_id', how='outer')
```

### Validation Failures
```python
validation = ValidationProcessor.validate_forecasts(df, forecasts)
if validation.get('status') != 'completed':
    print(f"Validation issues: {validation}")
    # Handle validation failures
```

## Performance Considerations

### Memory Management
- **Lazy Loading**: Hierarchy data loaded only when needed
- **Column Selection**: Only necessary columns included in operations
- **Streaming Operations**: Large datasets processed in chunks

### Data Quality
- **Outlier Handling**: IQR method preserves data continuity
- **Missing Values**: Consistent NaN handling across pipeline
- **Date Filtering**: Efficient temporal filtering for large datasets

### Processing Efficiency
- **Batch Operations**: Polars operations optimized for performance
- **Index Usage**: Proper column selection for joins
- **Error Recovery**: Graceful handling of processing failures

This data processing module provides a robust foundation for the forecasting pipeline, ensuring data quality, proper integration, and reliable validation throughout the entire workflow.
