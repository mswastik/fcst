# Model Validator API Documentation

The `model_validator.py` module provides comprehensive model validation and comparison functionality for forecasting models, implementing a 3-month rolling validation approach to assess real-world forecast accuracy.

## Classes

### ModelValidator

Main class for validating forecasting models with rolling window approach.

```python
class ModelValidator:
    """Validates forecasting models with rolling window approach."""
```

#### Initialization

```python
def __init__(self, validation_months: int = 3):
    """Initialize ModelValidator with validation parameters."""
```

**Parameters:**
- `validation_months` (int): Number of months for rolling validation window (default: 3)

#### Core Methods

##### `validate_models(data: pl.DataFrame, models: Dict) -> ValidationResults`

Performs comprehensive model validation using rolling window approach.

```python
def validate_models(self, data: pl.DataFrame, models: Dict) -> ValidationResults:
    """Validate models using 3-month rolling window approach."""
```

**Parameters:**
- `data` (pl.DataFrame): Historical data for validation
- `models` (Dict): Dictionary of models to validate

**Returns:**
- `ValidationResults`: Comprehensive validation results

**Usage:**
```python
validator = ModelValidator(validation_months=3)
models = {
    'ensemble': ensemble_model,
    'simple_nhits': simple_model
}
results = validator.validate_models(historical_data, models)
```

**Validation Process:**
1. Splits data into training and validation periods
2. For each validation month, trains models on data up to 3 months before
3. Generates forecasts for the validation month
4. Calculates accuracy metrics
5. Aggregates results across all validation periods

##### `calculate_accuracy_metrics(actual: List, predicted: List) -> Dict`

Calculates comprehensive accuracy metrics for model predictions.

```python
def calculate_accuracy_metrics(self, actual: List, predicted: List) -> Dict:
    """Calculate comprehensive accuracy metrics."""
```

**Parameters:**
- `actual` (List): List of actual values
- `predicted` (List): List of predicted values

**Returns:**
- `Dict`: Dictionary containing accuracy metrics:
  - `mae`: Mean Absolute Error
  - `mape`: Mean Absolute Percentage Error
  - `rmse`: Root Mean Square Error
  - `accuracy_percentage`: Percentage of predictions within acceptable range
  - `forecast_bias`: Systematic over/under-forecasting tendency

**Usage:**
```python
validator = ModelValidator()
metrics = validator.calculate_accuracy_metrics(
    actual=[100, 150, 200],
    predicted=[110, 140, 190]
)
print(f"MAE: {metrics['mae']:.2f}")
```

**Metric Definitions:**
- **MAE**: Average absolute difference between actual and predicted
- **MAPE**: Average percentage error (handles zero values gracefully)
- **RMSE**: Square root of mean squared errors (penalizes large errors)
- **Accuracy %**: Percentage within ±10% of actual values
- **Forecast Bias**: (Sum of errors) / (Sum of actuals) * 100

##### `generate_validation_report(results: ValidationResults) -> str`

Generates a comprehensive validation report in markdown format.

```python
def generate_validation_report(self, results: ValidationResults) -> str:
    """Generate comprehensive validation report."""
```

**Parameters:**
- `results` (ValidationResults): Validation results from validate_models()

**Returns:**
- `str`: Markdown-formatted validation report

**Usage:**
```python
validator = ModelValidator()
results = validator.validate_models(data, models)
report = validator.generate_validation_report(results)
print(report)
```

### ValidationResults

Data class containing comprehensive validation results.

```python
@dataclass
class ValidationResults:
    """Container for model validation results."""
```

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `model_comparisons` | `Dict[str, Dict]` | Model-by-model comparison metrics |
| `summary_stats` | `Dict` | Overall validation summary statistics |
| `detailed_results` | `List[Dict]` | Month-by-month detailed results |
| `validation_period` | `Tuple[datetime, datetime]` | Start and end dates of validation |
| `models_validated` | `List[str]` | List of model names validated |

#### Methods

##### `get_best_model() -> str`

Returns the name of the best performing model based on overall accuracy.

```python
def get_best_model(self) -> str:
    """Get the best performing model name."""
```

##### `get_model_metrics(model_name: str) -> Dict`

Returns detailed metrics for a specific model.

```python
def get_model_metrics(self, model_name: str) -> Dict:
    """Get detailed metrics for specific model."""
```

##### `export_to_csv(filepath: str) -> None`

Exports validation results to CSV format.

```python
def export_to_csv(self, filepath: str) -> None:
    """Export validation results to CSV."""
```

### ValidationReportGenerator

Utility class for generating various types of validation reports.

```python
class ValidationReportGenerator:
    """Generates various types of validation reports."""
```

#### Methods

##### `generate_summary_report(results: ValidationResults) -> str`

Generates a concise summary report.

```python
def generate_summary_report(self, results: ValidationResults) -> str:
    """Generate summary validation report."""
```

##### `generate_detailed_report(results: ValidationResults) -> str`

Generates a detailed technical report.

```python
def generate_detailed_report(self, results: ValidationResults) -> str:
    """Generate detailed validation report."""
```

##### `generate_comparison_chart_data(results: ValidationResults) -> Dict`

Generates data formatted for comparison charts.

```python
def generate_comparison_chart_data(self, results: ValidationResults) -> Dict:
    """Generate data for model comparison charts."""
```

## Validation Methodology

### 3-Month Rolling Window

The validation uses a sophisticated rolling window approach:

1. **Historical Split**: Data is split to reserve last 3 months for validation
2. **Training Window**: For each validation month, models are trained on data available up to 3 months before that month
3. **Forecast Generation**: Models generate 3-month ahead forecasts
4. **Accuracy Assessment**: Forecasts are compared against actual values
5. **Aggregation**: Results are aggregated across all validation periods

### Example Timeline
```
Historical Data: Jan 2023 - Dec 2023
Validation Period: Oct 2023 - Dec 2023

Validation Month: Oct 2023
- Training Data: Jan 2023 - Jun 2023 (up to 3 months before)
- Forecast: Oct 2023 (3 months ahead)

Validation Month: Nov 2023  
- Training Data: Jan 2023 - Jul 2023
- Forecast: Nov 2023

Validation Month: Dec 2023
- Training Data: Jan 2023 - Aug 2023  
- Forecast: Dec 2023
```

## Usage Examples

### Basic Validation

```python
from forecasting.model_validator import ModelValidator
from forecasting.model_factory import ModelFactory

# Initialize validator
validator = ModelValidator(validation_months=3)

# Create models
factory = ModelFactory()
models = {
    'ensemble': factory.create_ensemble_model(),
    'simple_nhits': factory.create_simple_model()
}

# Load historical data
data = load_historical_data()

# Validate models
results = validator.validate_models(data, models)

# Get best model
best_model = results.get_best_model()
print(f"Best performing model: {best_model}")

# Generate report
report = validator.generate_validation_report(results)
print(report)
```

### Advanced Validation with Custom Metrics

```python
# Custom validation with specific parameters
validator = ModelValidator(validation_months=6)  # 6-month validation

# Validate with custom accuracy threshold
results = validator.validate_models(
    data=historical_data,
    models=model_dict,
    accuracy_threshold=0.15  # 15% tolerance
)

# Export results
results.export_to_csv('validation_results.csv')

# Generate comparison chart data
chart_data = ValidationReportGenerator().generate_comparison_chart_data(results)
```

### Integration with UI Components

```python
# In UI component
def validate_models_action():
    try:
        # Get current data and models
        state = get_global_state()
        data = state.filtered_df
        models = get_current_models()
        
        # Validate models
        validator = ModelValidator()
        results = validator.validate_models(data, models)
        
        # Show results dialog
        show_validation_results_dialog(results)
        
    except Exception as e:
        show_error_dialog(f"Validation failed: {e}")
```

## Performance Considerations

### Memory Management
- Validation processes large datasets efficiently using Polars
- Streaming operations for memory-constrained environments
- Garbage collection between validation iterations

### Computational Optimization
- Parallel model training when possible
- Efficient data splitting and aggregation
- Caching of intermediate results

### Progress Tracking
```python
# Validation with progress callback
def progress_callback(current_month: int, total_months: int, model_name: str):
    print(f"Validating {model_name}: {current_month}/{total_months}")

results = validator.validate_models(
    data=data,
    models=models,
    progress_callback=progress_callback
)
```

## Error Handling

### Common Validation Errors

#### Insufficient Data
```python
try:
    results = validator.validate_models(data, models)
except InsufficientDataError as e:
    print(f"Not enough data for validation: {e}")
    # Handle gracefully - perhaps reduce validation window
```

#### Model Training Failures
```python
try:
    results = validator.validate_models(data, models)
except ModelTrainingError as e:
    print(f"Model training failed: {e}")
    # Remove problematic model and continue
```

#### Data Quality Issues
```python
try:
    results = validator.validate_models(data, models)
except DataQualityError as e:
    print(f"Data quality issues detected: {e}")
    # Clean data or apply filters
```

## Validation Metrics Deep Dive

### Mean Absolute Error (MAE)
- **Formula**: `Σ|actual - predicted| / n`
- **Interpretation**: Average absolute difference in original units
- **Good Values**: Lower is better, context-dependent

### Mean Absolute Percentage Error (MAPE)
- **Formula**: `Σ|actual - predicted|/|actual| * 100 / n`
- **Interpretation**: Average percentage error
- **Good Values**: < 10% excellent, 10-20% good, > 20% poor

### Root Mean Square Error (RMSE)
- **Formula**: `√(Σ(actual - predicted)² / n)`
- **Interpretation**: Penalizes large errors more heavily
- **Good Values**: Lower is better, compare relative to data scale

### Accuracy Percentage
- **Formula**: `Count(|error| <= threshold) / total_count * 100`
- **Interpretation**: Percentage of predictions within acceptable range
- **Good Values**: > 80% excellent, 60-80% good, < 60% poor

### Forecast Bias
- **Formula**: `Σ(predicted - actual) / Σ(actual) * 100`
- **Interpretation**: Systematic over/under-forecasting
- **Good Values**: Close to 0%, positive = over-forecast, negative = under-forecast

## Best Practices

### Data Preparation
- Ensure data quality and completeness
- Handle missing values appropriately
- Validate date ranges and continuity

### Model Selection
- Include diverse model types for comparison
- Ensure models are properly configured
- Test with different hyperparameters

### Validation Configuration
- Choose appropriate validation window length
- Consider seasonality in validation periods
- Use consistent evaluation criteria

### Result Interpretation
- Consider business context when evaluating metrics
- Look for patterns in validation results
- Validate findings with domain experts
