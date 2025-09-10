# Simple Forecaster API Documentation

The `simple_forecaster.py` module provides streamlined forecasting capabilities for single NHITS model training, extracted from the original `create_models_action` function for better separation of concerns and maintainability.

## Classes

### SimpleModelConfiguration

Configuration class for simple NHITS model parameters.

```python
class SimpleModelConfiguration:
    """Configuration for simple NHITS model."""
```

#### Initialization

```python
def __init__(self, horizon: int = 56, input_size: int = 56, stacks: int = 3):
    """Initialize simple model configuration."""
```

**Parameters:**
- `horizon` (int): Forecast horizon in time steps (default: 56)
- `input_size` (int): Input sequence length for training (default: 56)
- `stacks` (int): Number of NHITS stacks (default: 3)

**Configuration Attributes:**
| Attribute | Default | Description |
|-----------|---------|-------------|
| `max_steps` | 700 | Maximum training steps |
| `learning_rate` | 1e-3 | Learning rate for optimization |
| `random_seed` | 1 | Random seed for reproducibility |
| `batch_size` | 36 | Batch size for training |
| `windows_batch_size` | 35 | Window batch size |
| `val_check_steps` | 100 | Validation check frequency |

### SimpleNHITSForecaster

Main class for creating and training simple NHITS forecasting models.

```python
class SimpleNHITSForecaster:
    """Simple NHITS forecaster for single model training."""
```

#### Initialization

```python
def __init__(self, config: SimpleModelConfiguration = None):
    """Initialize the simple NHITS forecaster."""
```

**Parameters:**
- `config` (SimpleModelConfiguration, optional): Model configuration (uses defaults if None)

#### Methods

##### `create_nhits_model() -> NHITS`

Creates and configures a single NHITS model instance.

```python
def create_nhits_model(self) -> NHITS:
    """Create and configure a single NHITS model."""
```

**Returns:**
- `NHITS`: Configured NeuralForecast NHITS model

**Model Configuration:**
- Uses identity stacks with 3 blocks each
- MLP units: [256, 256, 128] per stack
- Pool kernel sizes: [2, 4, 6]
- Frequency downsampling: [2, 4, 6]
- ReLU activation with 0.3 dropout
- Robust scaling and RMSE loss

##### `train_and_forecast(df_fr: pl.DataFrame) -> pl.DataFrame`

Trains the NHITS model and generates forecasts.

```python
def train_and_forecast(self, df_fr: pl.DataFrame) -> pl.DataFrame:
    """Train NHITS model and generate forecasts."""
```

**Parameters:**
- `df_fr` (pl.DataFrame): Prepared training data with columns ['unique_id', 'ds', 'y']

**Returns:**
- `pl.DataFrame`: Forecast results

**Process:**
1. Creates NHITS model instance
2. Initializes NeuralForecast with monthly frequency
3. Fits model on training data (with NaN/null handling)
4. Generates predictions for forecast horizon

### SimpleModelPipeline

Complete end-to-end pipeline for simple model forecasting.

```python
class SimpleModelPipeline:
    """Complete pipeline for simple model training and forecasting."""
```

#### Initialization

```python
def __init__(self, config: SimpleModelConfiguration = None):
    """Initialize the simple model pipeline."""
```

**Parameters:**
- `config` (SimpleModelConfiguration, optional): Model configuration

#### Methods

##### `run_pipeline(df: pl.DataFrame, file_path: str) -> pl.DataFrame`

Runs the complete forecasting pipeline from raw data to integrated results.

```python
def run_pipeline(self, df: pl.DataFrame, file_path: str) -> pl.DataFrame:
    """Run the complete simple forecasting pipeline."""
```

**Parameters:**
- `df` (pl.DataFrame): Raw sales data
- `file_path` (str): Path to original parquet file for region filtering

**Returns:**
- `pl.DataFrame`: Processed data with integrated forecasts

**Pipeline Steps:**
1. **Data Preparation**: Clean and format data for training
2. **Model Training**: Train NHITS model and generate forecasts
3. **Result Integration**: Process and merge forecasts with original data

#### Private Methods

##### `_prepare_data(df: pl.DataFrame) -> pl.DataFrame`

Prepares raw data for NHITS training.

```python
def _prepare_data(self, df: pl.DataFrame) -> pl.DataFrame:
    """Prepare data for training."""
```

**Data Transformations:**
- Uses `DataCleaner.prepare_training_data()` for initial cleaning
- Renames columns: 'SALES_DATE' → 'ds', 'Act Orders Rev' → 'y'
- Selects required columns: ['unique_id', 'ds', 'y']

##### `_process_and_integrate_results(forecasts: pl.DataFrame, original_df: pl.DataFrame, file_path: str) -> pl.DataFrame`

Processes forecasts and integrates them with original data.

```python
def _process_and_integrate_results(self, forecasts: pl.DataFrame,
                                 original_df: pl.DataFrame, file_path: str) -> pl.DataFrame:
    """Process forecasts and integrate with original data."""
```

**Integration Steps:**
1. Split unique_id and add hierarchy data
2. Filter by region (matching original data)
3. Merge forecasts with original dataframe
4. Handle non-forecasted data
5. Forward-fill cluster columns if present
6. Save results back to parquet file

##### `_split_unique_id_and_add_hierarchy(forecasts: pl.DataFrame) -> pl.DataFrame`

Splits the composite unique_id and joins with hierarchy data.

```python
def _split_unique_id_and_add_hierarchy(self, forecasts: pl.DataFrame) -> pl.DataFrame:
    """Split unique_id and join with hierarchy data."""
```

**Hierarchy Integration:**
- Splits 'unique_id' into 'Country' and 'CatalogNumber'
- Renames 'ds' to 'SALES_DATE'
- Joins with product hierarchy (phierarchy.parquet)
- Joins with location hierarchy (lhierarchy.parquet)

##### `_filter_by_region(forecasts: pl.DataFrame, file_path: str) -> pl.DataFrame`

Filters forecasts to match the region of the original data.

```python
def _filter_by_region(self, forecasts: pl.DataFrame, file_path: str) -> pl.DataFrame:
    """Filter forecasts by the same region as original data."""
```

**Region Filtering:**
- Reads original data from parquet file
- Extracts 'Stryker Group Region' from original data
- Filters forecasts to matching region

##### `_merge_with_original_data(forecasts: pl.DataFrame, original_df: pl.DataFrame, file_path: str) -> pl.DataFrame`

Merges forecasts with the original dataframe.

```python
def _merge_with_original_data(self, forecasts: pl.DataFrame,
                            original_df: pl.DataFrame, file_path: str) -> pl.DataFrame:
    """Merge forecasts with original dataframe."""
```

**Merge Process:**
1. Filter original data to forecasted unique_ids
2. Join forecasts with original data
3. Combine with non-forecasted data
4. Handle cluster column forward-filling
5. Save updated data back to parquet file

## Usage Examples

### Basic Usage

```python
from forecasting.simple_forecaster import SimpleModelPipeline, SimpleModelConfiguration

# Create pipeline with default configuration
pipeline = SimpleModelPipeline()

# Run forecasting pipeline
result_df = pipeline.run_pipeline(sales_data, "data_file.parquet")
```

### Custom Configuration

```python
from forecasting.simple_forecaster import SimpleModelConfiguration, SimpleModelPipeline

# Create custom configuration
config = SimpleModelConfiguration(
    horizon=84,      # 84 months ahead
    input_size=84,   # Use 84 months of history
    stacks=4         # Use 4 NHITS stacks
)

# Create pipeline with custom config
pipeline = SimpleModelPipeline(config)

# Run pipeline
results = pipeline.run_pipeline(data, "forecast_data.parquet")
```

### Direct Model Usage

```python
from forecasting.simple_forecaster import SimpleNHITSForecaster
import polars as pl

# Create forecaster
forecaster = SimpleNHITSForecaster()

# Prepare data manually
df_fr = pl.DataFrame({
    'unique_id': ['US,ABC123', 'US,DEF456'],
    'ds': [datetime(2024, 1, 1), datetime(2024, 1, 1)],
    'y': [1000.0, 1500.0]
})

# Train and forecast
forecasts = forecaster.train_and_forecast(df_fr)
```

## Architecture Benefits

### Separation of Concerns
- **SimpleModelConfiguration**: Centralized parameter management
- **SimpleNHITSForecaster**: Focused on model creation and training
- **SimpleModelPipeline**: Orchestrates the complete workflow

### Maintainability
- Extracted from monolithic `create_models_action` function
- Clear interfaces between components
- Easier testing and debugging

### Flexibility
- Configurable model parameters
- Extensible pipeline architecture
- Support for different data sources and formats

## Dependencies

- **neuralforecast**: For NHITS model implementation
- **polars**: For efficient data processing
- **forecasting.data_processor**: For data cleaning utilities

## Performance Considerations

### Memory Usage
- Efficient Polars operations for data processing
- Streaming data handling for large datasets
- Minimal memory footprint compared to ensemble methods

### Training Time
- Single model training (faster than ensemble methods)
- Optimized NHITS configuration for convergence
- Configurable training parameters for performance tuning

### Data Processing
- Lazy evaluation where possible
- Efficient joins with hierarchy data
- Optimized parquet file operations

## Error Handling

The module includes robust error handling for common scenarios:

- **Data Quality Issues**: NaN/null value handling in training data
- **File System Errors**: Graceful handling of missing hierarchy files
- **Region Filtering**: Fallback behavior when region data unavailable
- **Merge Conflicts**: Safe merging with conflict resolution

## Integration Points

### With Data Processor
- Uses `DataCleaner.prepare_training_data()` for data preparation
- Integrates with `ForecastDataProcessor` for additional processing

### With File System
- Reads from and writes to parquet files in `data/` directory
- Supports both phierarchy.parquet and lhierarchy.parquet for hierarchy data

### With UI Components
- Results integrate seamlessly with existing dashboard components
- Maintains compatibility with existing data structures and schemas
