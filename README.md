# FCST - Forecasting Application

A comprehensive forecasting application built with NiceGUI that provides advanced time series forecasting capabilities using machine learning models, with a focus on sales and revenue prediction.

## 🚀 Features

- **Interactive Web Dashboard**: Modern web-based UI built with NiceGUI
- **Multiple Forecasting Models**: Support for NHITS, ensemble models, and statistical forecasting
- **Model Validation**: 3-month rolling validation with comprehensive accuracy metrics
- **DuckDB Integration**: High-performance database backend for fast data processing
- **Hierarchical Data Support**: Product and location hierarchies for multi-level forecasting
- **Real-time Visualization**: Interactive charts and dashboards for data exploration
- **Model Comparison**: Side-by-side comparison of different forecasting approaches

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Core Components](#core-components)
- [API Documentation](#api-documentation)
- [Debugging Guide](#debugging-guide)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Contributing](#contributing)

## 🛠 Installation

### Prerequisites

- Python 3.8 or higher
- Windows/Linux/macOS
- At least 4GB RAM (8GB recommended for large datasets)

### Setup

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd fcst
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   
   # Windows
   venv\Scripts\activate
   
   # Linux/macOS
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Initialize database** (if migrating from parquet files)
   ```bash
   python migrate_to_duckdb.py
   ```

## 🚀 Quick Start

### Running the Application

```bash
python main.py
```

The application will start on `http://localhost:8000`

### Basic Usage

1. **Load Data**: Click "Load Sample Data" to load your forecasting dataset
2. **Apply Filters**: Use the product and location filters to focus on specific segments
3. **Generate Forecasts**: Click "Create Models" to generate forecasting models
4. **Validate Models**: Use "Validate Models" to assess forecast accuracy
5. **View Results**: Explore charts and metrics in the dashboard

## 🏗 Architecture

The application follows a modular architecture with clear separation of concerns:

```
fcst/
├── main.py                 # Application entry point
├── state_manager.py        # Centralized state management
├── db_service.py          # Database operations
├── data_service.py        # Data processing utilities
├── forecasting/           # Forecasting models and logic
│   ├── model_factory.py   # Model creation and configuration
│   ├── data_processor.py  # Data cleaning and validation
│   ├── model_validator.py # Model validation and metrics
│   └── simple_forecaster.py # Simple NHITS pipeline
├── ui/                    # User interface components
│   ├── dashboard.py       # Main dashboard
│   ├── components.py      # Reusable UI components
│   └── charts.py          # Chart generation
└── docs/                  # Documentation
```

### Key Design Principles

- **State Management**: Centralized state using `DataState` class
- **Dependency Injection**: Modular components with clear interfaces
- **Database Abstraction**: Clean separation between data access and business logic
- **Component-Based UI**: Reusable UI components for maintainability

## 🔧 Core Components

### State Manager (`state_manager.py`)
Manages application state including data, filters, and UI state.

**Key Classes:**
- `DataState`: Central state container
- Functions: `get_global_state()`, `initialize_global_state()`

### Database Service (`db_service.py`)
Handles all database operations with DuckDB backend.

**Key Features:**
- CRUD operations for sales data
- Hierarchy management
- Transaction support
- Migration utilities

### Forecasting Module (`forecasting/`)
Contains all forecasting-related functionality.

**Key Components:**
- `ModelFactory`: Creates and configures ensemble models
- `DataProcessor`: Handles data cleaning and validation
- `ModelValidator`: Provides model validation and accuracy metrics
- `SimpleForecaster`: Simple NHITS model pipeline

### UI Components (`ui/`)
Modular UI components for the web interface.

**Key Components:**
- `Dashboard`: Main application dashboard
- `Components`: Reusable UI elements (filters, dialogs, charts)
- `Charts`: Chart generation and visualization

## 📚 API Documentation

### DataState Class

```python
class DataState:
    """Centralized state management for application data."""
    
    # Main dataframes
    df: Optional[pl.DataFrame] = None
    filtered_df: Optional[pl.DataFrame] = None
    
    # Methods
    def load_sample_data(self, path: str = None) -> pl.DataFrame
    def get_filter_options(self, prod: str = None, loc: str = None) -> Dict[str, Any]
    def update_filtered_data(self, new_filtered_df: pl.DataFrame) -> None
    def get_chart_data(self, chart_type: str) -> Optional[Dict[str, Any]]
```

### Database Service

```python
class DatabaseService:
    """Database operations for DuckDB backend."""
    
    def get_sales_actuals(self, filters: Dict = None) -> pl.DataFrame
    def get_filter_options(self) -> Dict[str, List[str]]
    def save_forecast_results(self, results: pl.DataFrame) -> None
    def get_model_validation_results(self) -> pl.DataFrame
```

### Model Validator

```python
class ModelValidator:
    """Validates forecasting models with rolling window approach."""
    
    def validate_models(self, data: pl.DataFrame, models: Dict) -> ValidationResults
    def calculate_accuracy_metrics(self, actual: List, predicted: List) -> Dict
    def generate_validation_report(self, results: ValidationResults) -> str
```

## 🐛 Debugging Guide

### Common Issues and Solutions

#### 1. Database Connection Issues

**Symptoms:**
- "Failed to load data from database" error
- Empty data in dashboard

**Solutions:**
```python
# Check database file exists
import os
print(os.path.exists('forecasting.duckdb'))

# Verify database service
from db_service import get_database_service
db = get_database_service()
print(db.connection.execute("SELECT COUNT(*) FROM sales_actuals").fetchone())
```

#### 2. Memory Issues with Large Datasets

**Symptoms:**
- Application crashes or becomes unresponsive
- "Out of memory" errors

**Solutions:**
- Increase system memory allocation
- Use data filtering to reduce dataset size
- Enable lazy loading in Polars:
```python
df = pl.scan_parquet("data.parquet").collect(streaming=True)
```

#### 3. Model Training Failures

**Symptoms:**
- "Model creation failed" errors
- NaN values in forecasts

**Solutions:**
```python
# Check data quality
print(df.null_count())
print(df.describe())

# Verify date column format
print(df['SALES_DATE'].dtype)
print(df['SALES_DATE'].min(), df['SALES_DATE'].max())
```

#### 4. UI Component Issues

**Symptoms:**
- Charts not displaying
- Filters not working

**Solutions:**
```python
# Check state management
from state_manager import get_global_state
state = get_global_state()
print(f"Data loaded: {state.df is not None}")
print(f"Filtered data: {state.filtered_df is not None}")
```

### Logging and Debugging

Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Performance Monitoring

Monitor key metrics:
```python
import time
import psutil

# Memory usage
print(f"Memory usage: {psutil.virtual_memory().percent}%")

# Execution time
start_time = time.time()
# ... your code ...
print(f"Execution time: {time.time() - start_time:.2f}s")
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file:
```env
# Database configuration
DATABASE_PATH=forecasting.duckdb
DATABASE_MEMORY_LIMIT=4GB

# Application settings
HOST=0.0.0.0
PORT=8000
DEBUG=False

# Model settings
DEFAULT_FORECAST_HORIZON=12
VALIDATION_WINDOW_MONTHS=3
```

### Application Configuration

Key configuration options in `main.py`:
```python
ui.run(
    reload=True,                    # Enable hot reload in development
    title="ML Integration",         # Application title
    reconnect_timeout=7000,         # WebSocket reconnection timeout
    storage_secret='my_secret_key', # Session storage encryption
    host="0.0.0.0",                # Bind to all interfaces
    port=8000                       # Port number
)
```

## 🚀 Deployment

### Development Deployment

```bash
python main.py
```

### Production Deployment

1. **Using PyInstaller** (for standalone executable):
   ```bash
   pip install pyinstaller
   pyinstaller main.spec
   ```

2. **Using Docker**:
   ```dockerfile
   FROM python:3.9-slim
   WORKDIR /app
   COPY requirements.txt .
   RUN pip install -r requirements.txt
   COPY . .
   EXPOSE 8000
   CMD ["python", "main.py"]
   ```

3. **Using systemd** (Linux):
   ```ini
   [Unit]
   Description=FCST Forecasting Application
   After=network.target
   
   [Service]
   Type=simple
   User=fcst
   WorkingDirectory=/opt/fcst
   ExecStart=/opt/fcst/venv/bin/python main.py
   Restart=always
   
   [Install]
   WantedBy=multi-user.target
   ```

### Performance Optimization

- **Database**: Use appropriate indexes and query optimization
- **Memory**: Configure DuckDB memory limits based on available RAM
- **Caching**: Implement caching for frequently accessed data
- **Load Balancing**: Use reverse proxy (nginx) for production deployments

## 📊 Model Validation

The application includes comprehensive model validation:

### Validation Process

1. **3-Month Rolling Window**: Tests forecast accuracy using historical data
2. **Multiple Metrics**: MAE, MAPE, RMSE, accuracy percentage, forecast bias
3. **Model Comparison**: Side-by-side comparison of different models
4. **Interactive Results**: Detailed validation reports with charts

### Validation Metrics

- **MAE (Mean Absolute Error)**: Average absolute difference between actual and predicted values
- **MAPE (Mean Absolute Percentage Error)**: Average percentage error
- **RMSE (Root Mean Square Error)**: Square root of average squared errors
- **Accuracy Percentage**: Percentage of predictions within acceptable range
- **Forecast Bias**: Systematic over/under-forecasting tendency

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Run tests: `python -m pytest tests/`
5. Submit a pull request

### Code Style

- Follow PEP 8 guidelines
- Use type hints where possible
- Add docstrings to all functions and classes
- Write unit tests for new functionality

### Testing

```bash
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_model_validator.py

# Run with coverage
python -m pytest --cov=forecasting tests/
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For support and questions:
- Check the [Debugging Guide](#debugging-guide)
- Review the [API Documentation](#api-documentation)
- Create an issue in the repository
- Contact the development team

---

**Last Updated**: January 2025
**Version**: 2.0.0
