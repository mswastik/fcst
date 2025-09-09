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

Creates and configures an NHITS neural forecasting model.

```python
def create_nhits_model(self, batch_size: int, windows_batch_size: int) -> NHITS:
    """Create and configure NHITS model."""
```

**Parameters:**
- `batch_size` (int): Training batch size
- `windows_batch_size` (int): Window batch size for sliding window training

**Returns:**
- `NHITS`: Configured NHITS model instance

**Configuration:**
- Stack types: Identity stacks for hierarchical forecasting
- Network architecture: 2-layer MLP with 64→32→16 units
- Pooling: Kernel size 2 with frequency downsampling
- Loss: RMSE with robust scaling
- Training: Early stopping with validation checks

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

Handles forecast processing and model combination.

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

##### `create_ensemble_models(df: pl.DataFrame) -> Tuple[List, Tuple[int, int]]`

Creates an ensemble of neural and statistical forecasting models.

```python
def create_ensemble_models(self, df: pl.DataFrame) -> Tuple[List, Tuple[int, int]]:
    """Create ensemble of neural and statistical models."""
```

**Parameters:**
- `df` (pl.DataFrame): Input dataframe with time series data

**Returns:**
- `Tuple[List, Tuple[int, int]]`: (models_list, batch_sizes)

**Models Created:**
1. NHITS neural network model
2. LSTM neural network model
3. Statistical models (AutoARIMA, AutoETS, SeasonalNaive)

##### `generate_forecasts(df: pl.DataFrame, models: List) -> pl.DataFrame`

Generates forecasts using the ensemble of models.

```python
def generate_forecasts(self, df: pl.DataFrame, models: List) -> pl.DataFrame:
    """Generate forecasts using ensemble models."""
```

**Parameters:**
- `df` (pl.DataFrame): Prepared time series dataframe
- `models` (List): List of trained models

**Returns:**
- `pl.DataFrame`: Forecast results with predictions from all models

**Process:**
1. Fits each model on the training data
2. Generates predictions for forecast horizon
3. Combines results into unified dataframe
4. Handles model failures gracefully

##### `combine_forecasts(forecasts: Dict[str, pl.DataFrame]) -> pl.DataFrame`

Combines forecasts from multiple models using ensemble methods.

```python
def combine_forecasts(self, forecasts: Dict[str, pl.DataFrame]) -> pl.DataFrame:
    """Combine forecasts from multiple models."""
```

**Parameters:**
- `forecasts` (Dict[str, pl.DataFrame]): Dictionary of model forecasts

**Returns:**
- `pl.DataFrame`: Combined ensemble forecast

**Ensemble Methods:**
- Simple averaging of model predictions
- Weighted averaging based on historical performance
- Confidence interval calculation from prediction variance

## Usage Examples

### Basic Model Creation

```python
from forecasting.model_factory import ModelConfiguration, ForecastProcessor

# Configure models
config = ModelConfiguration(horizon=60, input_size=24)
processor = ForecastProcessor(config)

# Create ensemble models
models, batch_sizes = processor.create_ensemble_models(df)

# Generate forecasts
forecasts = processor.generate_forecasts(df, models)
```

### Custom Model Configuration

```python
# Custom configuration for short-term forecasting
config = ModelConfiguration(
    horizon=12,           # 12 months ahead
    input_size=24,        # Use 24 months of history
    max_steps=100,        # Faster training
    learning_rate=5e-4    # Conservative learning rate
)

processor = ForecastProcessor(config)
```

### Statistical Models Only

```python
from forecasting.model_factory import StatisticalModelFactory

# Create statistical models for comparison
stat_models = StatisticalModelFactory.create_statistical_models(season_length=12)

# Use with StatsForecast
from statsforecast import StatsForecast
sf = StatsForecast(models=stat_models, freq='M')
sf.fit(df)
predictions = sf.predict(h=12)
```

## Performance Considerations

### Neural Models
- **Memory Usage**: NHITS and LSTM models require significant GPU/CPU memory
- **Training Time**: Neural models take longer to train than statistical models
- **Scalability**: Batch size affects both memory usage and training speed
- **Hyperparameters**: Learning rate and architecture affect convergence

### Statistical Models
- **Speed**: Statistical models train and predict much faster
- **Memory**: Lower memory footprint than neural models
- **Robustness**: Less sensitive to data quality issues
- **Interpretability**: Model parameters are more interpretable

### Ensemble Processing
- **Diversity**: Combining neural and statistical models improves robustness
- **Computational Cost**: Ensemble methods increase total computation time
- **Memory Management**: Handle large forecast datasets efficiently
- **Error Handling**: Graceful degradation when individual models fail

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
