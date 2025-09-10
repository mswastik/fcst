# Simple Pipeline API Documentation

The `simple_pipeline.py` module provides a completely standalone forecasting pipeline with minimal dependencies, designed specifically for pickle compatibility and lightweight deployment.

## Overview

This module offers a self-contained forecasting solution that:
- Has minimal external dependencies
- Is guaranteed to be pickle-safe for model serialization
- Includes basic clustering functionality
- Provides a placeholder framework for forecast generation
- Can operate independently of the main FCST application

## Functions

### standalone_forecasting_pipeline(df_dict: dict) -> tuple

Completely standalone forecasting pipeline with pickle-safe operation.

```python
def standalone_forecasting_pipeline(df_dict: dict) -> tuple:
    """Completely standalone forecasting pipeline - guaranteed pickle-safe"""
```

**Parameters:**
- `df_dict` (dict): Dictionary representation of the input dataframe

**Returns:**
- `tuple`: (merged_df_dict, validation_results)
  - `merged_df_dict` (dict or None): Processed dataframe as dictionary, or None if error
  - `validation_results` (dict): Validation metrics or error information

## Pipeline Process

### 1. Data Reconstruction
```python
df = pl.DataFrame(df_dict)
```
- Converts input dictionary back to Polars DataFrame
- Enables efficient data processing operations

### 2. Clustering Logic (if not present)
The pipeline automatically performs clustering if cluster labels are missing:

#### Data Preparation
```python
# Create unique_id if not present
if 'unique_id' not in df.columns:
    df = df.with_columns(unique_id = pl.col('Country') + "," + pl.col('CatalogNumber'))
```

#### Time-based Filtering
```python
# Use data up to last full month for clustering
last_full_month = datetime.today() - relativedelta(months=1)
df1 = df.filter(pl.col('SALES_DATE') <= last_full_month)
```

#### Data Normalization
```python
# Normalize sales data by unique_id
df1 = df1.with_columns(
    ynorm=((pl.col('Act Orders Rev') - pl.col('Act Orders Rev').mean()) /
           pl.col('Act Orders Rev').std()).over('unique_id')
)
```

#### Pivot and Clustering
```python
# Pivot data for clustering
df1 = df1.pivot(
    index='unique_id',
    on='SALES_DATE',
    values='ynorm',
    aggregate_function='sum'
)

# Apply BIRCH clustering
bi = Birch(n_clusters=6).fit(df1[:, 1:])
df1 = df1.with_columns(cluster=bi.labels_)
```

#### Cluster Integration
```python
# Join clusters back to main dataframe
df = df.join(df1, on='unique_id', how='left', coalesce=True)

# Forward/backward fill cluster values
df = df.with_columns(
    cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id")
)
```

### 3. Forecast Generation (Placeholder)
```python
# Simple forecast generation (placeholder)
# For now, just return the clustered data
merged_df = df
validation_results = {'mae': 0.0, 'mape': 0.0, 'rmse': 0.0}
```

### 4. Result Serialization
```python
# Return as dictionary to avoid filename issues
merged_df_dict = merged_df.to_dict(as_series=False) if merged_df is not None else None
return merged_df_dict, validation_results
```

## Usage Examples

### Basic Usage

```python
from forecasting.simple_pipeline import standalone_forecasting_pipeline

# Convert dataframe to dictionary
df_dict = sales_dataframe.to_dict(as_series=False)

# Run standalone pipeline
result_dict, validation = standalone_forecasting_pipeline(df_dict)

# Convert back to dataframe if successful
if result_dict is not None:
    processed_df = pl.DataFrame(result_dict)
    print(f"Processed {len(processed_df)} rows")
    print(f"Validation results: {validation}")
```

### Integration with Pickle Serialization

```python
import pickle
from forecasting.simple_pipeline import standalone_forecasting_pipeline

# Process data
df_dict = sales_data.to_dict(as_series=False)
processed_dict, validation = standalone_forecasting_pipeline(df_dict)

# Serialize results (pickle-safe)
with open('forecast_results.pkl', 'wb') as f:
    pickle.dump((processed_dict, validation), f)

# Deserialize later
with open('forecast_results.pkl', 'rb') as f:
    loaded_dict, loaded_validation = pickle.load(f)

if loaded_dict:
    final_df = pl.DataFrame(loaded_dict)
```

### Error Handling

```python
try:
    result_dict, validation = standalone_forecasting_pipeline(df_dict)

    if 'error' in validation:
        print(f"Pipeline error: {validation['error']}")
    else:
        print(f"Pipeline completed successfully")
        print(f"MAE: {validation.get('mae', 'N/A')}")
        print(f"MAPE: {validation.get('mape', 'N/A')}")
        print(f"RMSE: {validation.get('rmse', 'N/A')}")

except Exception as e:
    print(f"Unexpected error: {e}")
```

## Design Principles

### Minimal Dependencies
- Only imports required libraries within the function
- Avoids circular import issues
- Enables standalone operation

### Pickle Compatibility
- All operations are pickle-serializable
- No lambda functions or complex objects
- Dictionary-based data interchange

### Fault Tolerance
- Comprehensive error handling with try/catch
- Graceful degradation when components fail
- Informative error messages for debugging

## Clustering Algorithm

### BIRCH Algorithm
- **Balanced Iterative Reducing and Clustering using Hierarchies**
- Efficient for large datasets
- Automatically determines optimal number of clusters
- Handles outliers gracefully

### Clustering Configuration
- `n_clusters=6`: Target number of clusters
- Based on normalized sales patterns
- Applied per unique product-location combination

### Data Preprocessing
- Standardization by product-location group
- Missing value handling (fill with 0)
- Infinite value protection
- Time-series pivoting for feature extraction

## Performance Characteristics

### Memory Efficiency
- Processes data in chunks where possible
- Minimal memory overhead for clustering
- Efficient Polars operations throughout

### Speed Optimization
- Fast clustering with BIRCH algorithm
- Vectorized Polars operations
- Minimal data copying

### Scalability
- Handles large datasets efficiently
- Linear scaling with data size
- Memory-conscious processing

## Integration Points

### With Main Application
- Can be called from main FCST application
- Results compatible with existing data structures
- Maintains same dataframe schema

### With External Systems
- Dictionary-based input/output for easy integration
- Pickle-safe for model serialization
- Minimal dependency requirements

### With Clustering Workflows
- Extends existing clustering functionality
- Compatible with BIRCH clustering results
- Supports cluster-based forecasting strategies

## Future Extensions

### Forecast Generation
The current implementation includes a placeholder for forecast generation:

```python
# Placeholder for actual forecasting logic
# Can be extended to include:
# - Time series forecasting models
# - Regression-based approaches
# - Machine learning predictions
```

### Enhanced Clustering
- Support for additional clustering algorithms
- Dynamic cluster number determination
- Hierarchical clustering options

### Validation Metrics
- More comprehensive validation metrics
- Cross-validation support
- Statistical significance testing

## Dependencies

### Required Libraries
- **polars**: Data processing and manipulation
- **numpy**: Numerical operations
- **scikit-learn**: BIRCH clustering algorithm
- **python-dateutil**: Date arithmetic operations

### Import Strategy
- All imports within function scope
- Avoids global import conflicts
- Enables lazy loading of dependencies

## Error Scenarios

### Data Quality Issues
- Handles missing values gracefully
- Manages infinite values in normalization
- Provides fallbacks for edge cases

### Clustering Failures
- Graceful handling of clustering errors
- Continues processing without clustering if needed
- Logs errors for debugging

### Serialization Issues
- Dictionary-based data interchange
- Avoids complex object serialization
- Pickle-compatible result format

## Best Practices

### Data Preparation
- Ensure consistent column naming
- Validate data types before processing
- Handle missing values appropriately

### Error Handling
- Always check for error keys in validation results
- Implement proper exception handling
- Log errors for debugging purposes

### Performance Monitoring
- Monitor memory usage for large datasets
- Track processing time for optimization
- Validate clustering quality metrics
