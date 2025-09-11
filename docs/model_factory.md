# Forecasting Module API Documentation

The `forecasting/` module provides comprehensive forecasting capabilities for the FCST application, including model creation, data processing, and validation.

## model_factory.py

### ModelConfiguration

Configuration class for model parameters and hyperparameters.

```python
class ModelConfiguration:
    """Configuration class for model parameters."""
```

#### Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `horizon` | `int` | 60 | Forecast horizon in time steps |
| `input_size` | `int` | horizon | Input sequence length |
| `max_steps` | `int` | 180 | Maximum training steps |
| `learning_rate` | `float` | 1e-3 | Learning rate for optimization |
| `random_seed` | `int` | 42 | Random seed for reproducibility |
| `val_check_steps` | `int` | 25 | Validation check frequency |

#### Methods

##### `calculate_batch_sizes(n_series: int) -> Tuple[int, int]`

Calculates appropriate batch sizes based on number of time series.

```python
def calculate_batch_sizes(self, n_series: int) -> Tuple[int, int]:
    """Calculate appropriate batch sizes based on number of series."""
```

**Parameters:**
- `n_series` (int): Number of time series in dataset

**Returns:**
- `Tuple[int, int]`: (batch_size, windows_batch_size)

### NeuralModelFactory

Factory for creating neural forecasting models.

```python
class NeuralModelFactory:
    """Factory for creating neural forecasting models."""
```

#### Methods

##### `create_nhits_model(batch_size: int, windows_batch_size: int) -> NHITS`

Creates and configures an NHITS neural forecasting model with current parameters.

```python
def create_nhits_model(self, batch_size: int, windows_batch_size: int) -> NHITS:
    """Create and configure NHITS model."""
```

**Parameters:**
- `batch_size` (int): Training batch size
- `windows_batch_size` (int): Window batch size for sliding window training

**Returns:**
- `NHITS`: Configured NHITS model instance

**Current Configuration:**
- Stack types: Identity stacks (2 stacks)
- Network architecture: [64, 32] → [32, 16] units per stack
- Pooling: Kernel size 2, frequency downsampling [2, 2]
- Interpolation: 'nearest' mode
- Activation: ReLU with 0.1 dropout
- Loss: RMSE with robust scaling
- Training: Early stopping with validation checks
- Random seed: Configured value for reproducibility

##### `create_lstm_model(batch_size: int) -> LSTM`

Creates and configures an LSTM neural forecasting model.

```python
def create_lstm_model(self, batch_size: int) -> LSTM:
    """Create and configure LSTM model."""
```

**Parameters:**
- `batch_size` (int): Training batch size

**Returns:**
- `LSTM`: Configured LSTM model instance

**Configuration:**
- Encoder: 32 hidden units, 1 layer, 0.1 dropout
- Loss: RMSE with robust scaling
- Scaler: Robust scaling for outlier handling
- Training: Standard LSTM architecture for time series

### StatisticalModelFactory

Factory for creating statistical forecasting models.

```python
class StatisticalModelFactory:
    """Factory for creating statistical forecasting models."""
```

#### Methods

##### `create_statistical_models(season_length: int = 12) -> List`

Creates a suite of statistical forecasting models.

```python
@staticmethod
def create_statistical_models(season_length: int = 12) -> List:
    """Create statistical forecasting models."""
```

**Parameters:**
- `season_length` (int): Seasonal period length (default: 12 for monthly data)

**Returns:**
- `List`: List of configured statistical models:
  - AutoARIMA: Automatic ARIMA model selection
  - AutoETS: Automatic Exponential Smoothing
  - SeasonalNaive: Seasonal naive method

### ForecastProcessor

Handles forecast processing and model combination for individual clusters.

```python
class ForecastProcessor:
    """Handles forecast processing and combination."""
```

#### Initialization

```python
def __init__(self, config: ModelConfiguration):
    """Initialize ForecastProcessor with configuration."""
```

**Parameters:**
- `config` (ModelConfiguration): Model configuration instance

#### Methods

##### `process_cluster(cluster_data: pl.DataFrame, cluster_id: str) -> pl.DataFrame`

Process a single cluster and generate forecasts using ensemble methods.

```python
def process_cluster(self, cluster_data: pl.DataFrame, cluster_id: str) -> pl.DataFrame:
    """Process a single cluster and generate forecasts."""
```

**Parameters:**
- `cluster_data` (pl.DataFrame): Time series data for a specific cluster
- `cluster_id` (str): Identifier for the cluster being processed

**Returns:**
- `pl.DataFrame`: Combined forecasts for the cluster with ensemble predictions

**Process:**
1. Prepare data for forecasting (select required columns)
2. Calculate appropriate batch sizes based on number of series
3. Create neural and statistical models
4. Generate forecasts from both model types
5. Combine forecasts using ensemble methods
6. Return combined results with cluster identifier

### EnsembleForecaster

Main class for ensemble forecasting across all clusters in the dataset.

```python
class EnsembleForecaster:
    """Main class for ensemble forecasting across clusters."""
```

#### Initialization

```python
def __init__(self, horizon: int = 60):
    """Initialize EnsembleForecaster with forecast horizon."""
```

**Parameters:**
- `horizon` (int): Forecast horizon in time steps (default: 60)

**Attributes:**
- `config` (ModelConfiguration): Model configuration with calculated input size
- `processor` (ForecastProcessor): Forecast processor for individual clusters

#### Methods

##### `generate_forecasts(df_fr: pl.DataFrame) -> pl.DataFrame`

Generate forecasts for all clusters in the dataset using ensemble methods.

```python
def generate_forecasts(self, df_fr: pl.DataFrame) -> pl.DataFrame:
    """Generate forecasts for all clusters in the dataset."""
```

**Parameters:**
- `df_fr` (pl.DataFrame): Prepared forecasting data with columns ['unique_id', 'ds', 'y', 'cluster']

**Returns:**
- `pl.DataFrame`: Combined forecasts from all clusters, or None if no forecasts generated

**Process:**
1. Calculate optimal input size based on available historical data
2. Process each cluster separately using ForecastProcessor
3. Collect forecasts from all clusters
4. Concatenate results into unified dataframe
5. Handle clusters with insufficient data gracefully

**Forecast Output Columns:**
- `unique_id`: Product-location identifier
- `ds`: Forecast date
- `NHITS`: NHITS model predictions
- `LSTM`: LSTM model predictions  
- `AutoARIMA`: AutoARIMA model predictions
- `AutoETS`: AutoETS model predictions
- `SeasonalNaive`: SeasonalNaive model predictions
- `ensemble`: Ensemble average of all models
- `cluster`: Cluster identifier

## Usage Examples

### Basic Ensemble Forecasting

```python
from forecasting.model_factory import EnsembleForecaster

# Create ensemble forecaster for 60-month horizon
forecaster = EnsembleForecaster(horizon=60)

# Generate forecasts for clustered data
forecasts = forecaster.generate_forecasts(df_fr)

# Forecasts will contain predictions from all models for each cluster
print(f"Generated forecasts for {len(forecasts)} time series")
```

### Advanced Configuration

```python
from forecasting.model_factory import ModelConfiguration, EnsembleForecaster

# Custom configuration for short-term forecasting
config = ModelConfiguration(
    horizon=12,           # 12 months ahead
    input_size=24,        # Use 24 months of history
    max_steps=100,        # Faster training
    learning_rate=5e-4    # Conservative learning rate
)

# Create forecaster with custom config
forecaster = EnsembleForecaster(horizon=12)
forecaster.config = config  # Override default config

# Generate forecasts
forecasts = forecaster.generate_forecasts(df_fr)
```

### Cluster-by-Cluster Processing

```python
from forecasting.model_factory import ModelConfiguration, ForecastProcessor

# Create processor for individual cluster processing
config = ModelConfiguration(horizon=60)
processor = ForecastProcessor(config)

# Process specific cluster
cluster_data = df_fr.filter(pl.col('cluster') == 'cluster_0')
cluster_forecasts = processor.process_cluster(cluster_data, 'cluster_0')

# Access individual model predictions
nhits_predictions = cluster_forecasts['NHITS']
ensemble_predictions = cluster_forecasts['ensemble']
```

### Statistical Models Only

```python
from forecasting.model_factory import StatisticalModelFactory
from statsforecast import StatsForecast

# Create statistical models for comparison
stat_models = StatisticalModelFactory.create_statistical_models(season_length=12)

# Use with StatsForecast for traditional forecasting
sf = StatsForecast(models=stat_models, freq='1mo')
sf.fit(df)
predictions = sf.predict(h=12)
```

## Performance Considerations

### Neural Models
- **Memory Usage**: NHITS and LSTM models require significant GPU/CPU memory
- **Training Time**: Neural models take longer to train than statistical models
- **Scalability**: Batch size affects both memory usage and training speed
- **Data Requirements**: Minimum data length validation prevents training failures
- **Parallel Processing**: Cluster-based processing allows parallel training

### Statistical Models
- **Speed**: Statistical models train and predict much faster than neural models
- **Memory**: Lower memory footprint than neural models
- **Robustness**: Less sensitive to data quality issues and missing values
- **Fallback Option**: Used when neural models fail due to insufficient data

### Cluster-Based Processing
- **Parallelization**: Each cluster processed independently, enabling parallel execution
- **Memory Management**: Smaller datasets per cluster reduce memory requirements
- **Fault Isolation**: Failures in one cluster don't affect others
- **Scalability**: Linear scaling with number of clusters
- **Resource Optimization**: Dynamic batch size calculation based on cluster size

### Ensemble Processing
- **Computational Cost**: Running multiple models increases total computation time
- **Memory Management**: Efficient handling of large forecast datasets
- **Error Handling**: Graceful degradation when individual models fail
- **Prediction Diversity**: Combining neural and statistical models improves robustness
- **Result Consolidation**: Automatic merging of forecasts from all clusters

## Model Architecture Details

### NHITS Model Structure
```
Input Sequence → Hierarchical Interpolation → Stacks → Output
                     ↓
             Identity/Trend/Seasonal Components
                     ↓
             Multi-layer Perceptron Blocks
                     ↓
             Forecast Combination
```

### LSTM Model Structure
```
Input Sequence → LSTM Encoder → Hidden States → Output Projection
                     ↓
             Temporal Dependencies → Forecast Generation
```

### Statistical Models
- **AutoARIMA**: Automatic order selection for ARIMA models
- **AutoETS**: Automatic error, trend, seasonal component selection
- **SeasonalNaive**: Last season's values as forecast

## Error Handling

### Model Training Failures
```python
try:
    forecasts = processor.generate_forecasts(df, models)
except Exception as e:
    print(f"Model training failed: {e}")
    # Fallback to statistical models only
    stat_forecasts = generate_statistical_forecasts(df)
```

### Memory Issues
```python
# Monitor memory usage during model training
import psutil
memory = psutil.virtual_memory()
if memory.percent > 80:
    # Reduce batch size or use CPU instead of GPU
    config.batch_size = config.batch_size // 2
```

### Data Quality Issues
```python
# Validate data before model creation
if len(df) < config.input_size:
    raise ValueError("Insufficient data for training")

if df['y'].null_count() > len(df) * 0.1:
    print("Warning: High proportion of missing values")
```

This forecasting module provides a comprehensive, production-ready solution for time series forecasting with support for multiple model types, ensemble methods, and robust error handling.
