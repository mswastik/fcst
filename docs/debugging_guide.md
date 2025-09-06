# FCST Debugging Guide

This guide provides comprehensive debugging information for the FCST forecasting application, covering common issues, diagnostic techniques, and troubleshooting steps.

## Quick Diagnostic Checklist

Before diving into specific issues, run this quick diagnostic:

```python
# Basic health check
from state_manager import get_global_state
from db_service import get_database_service
import polars as pl

# Check state
state = get_global_state()
print(f"✓ State initialized: {state is not None}")
print(f"✓ Data loaded: {state.df is not None}")
print(f"✓ Filtered data: {state.filtered_df is not None}")

# Check database
try:
    db = get_database_service()
    stats = db.get_database_stats()
    print(f"✓ Database connected: {stats is not None}")
except Exception as e:
    print(f"✗ Database error: {e}")

# Check dependencies
try:
    import neuralforecast
    import nicegui
    print("✓ Core dependencies available")
except ImportError as e:
    print(f"✗ Missing dependency: {e}")
```

## Common Issues and Solutions

### 1. Application Won't Start

#### Symptoms
- Application crashes on startup
- "Module not found" errors
- Port binding errors

#### Diagnostic Steps
```python
# Check Python version
import sys
print(f"Python version: {sys.version}")

# Check required packages
required_packages = [
    'nicegui', 'polars', 'neuralforecast', 'scikit-learn',
    'pandas', 'mlforecast', 'statsforecast', 'duckdb'
]

for package in required_packages:
    try:
        __import__(package)
        print(f"✓ {package}")
    except ImportError:
        print(f"✗ {package} - MISSING")
```

#### Solutions
1. **Missing Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Port Already in Use**:
   ```python
   # In main.py, change port
   ui.run(port=8001)  # or any available port
   ```

3. **Python Version Issues**:
   - Ensure Python 3.8+ is installed
   - Use virtual environment to avoid conflicts

### 2. Database Connection Issues

#### Symptoms
- "Failed to load data from database" error
- Empty data in dashboard
- Database file not found

#### Diagnostic Steps
```python
import os
from db_service import get_database_service

# Check database file
db_path = "forecasting.duckdb"
print(f"Database exists: {os.path.exists(db_path)}")
print(f"Database size: {os.path.getsize(db_path) if os.path.exists(db_path) else 'N/A'} bytes")

# Test connection
try:
    db = get_database_service()
    result = db.connection.execute("SELECT COUNT(*) FROM sales_actuals").fetchone()
    print(f"Sales records: {result[0] if result else 'None'}")
except Exception as e:
    print(f"Connection error: {e}")
```

#### Solutions
1. **Database File Missing**:
   ```bash
   # Run migration from parquet files
   python migrate_to_duckdb.py
   ```

2. **Corrupted Database**:
   ```python
   # Recreate database
   import os
   if os.path.exists("forecasting.duckdb"):
       os.remove("forecasting.duckdb")
   # Then run migration again
   ```

3. **Permission Issues**:
   - Ensure write permissions in application directory
   - Run as administrator if necessary (Windows)

### 3. Data Loading Problems

#### Symptoms
- Data appears empty in UI
- Filter dropdowns are empty
- Charts show no data

#### Diagnostic Steps
```python
from state_manager import get_global_state

state = get_global_state()

# Check data loading
if state.df is not None:
    print(f"Main data shape: {state.df.shape}")
    print(f"Columns: {state.df.columns}")
    print(f"Date range: {state.df['SALES_DATE'].min()} to {state.df['SALES_DATE'].max()}")
else:
    print("No data loaded")

# Check filtered data
if state.filtered_df is not None:
    print(f"Filtered data shape: {state.filtered_df.shape}")
else:
    print("No filtered data")

# Check filter options
options = state.get_filter_options()
print(f"Available products: {len(options['products_filt'])}")
print(f"Available locations: {len(options['locations_filt'])}")
```

#### Solutions
1. **Data Not Loading**:
   ```python
   # Force reload data
   state = get_global_state()
   try:
       df = state.load_sample_data()
       print(f"Loaded {len(df)} records")
   except Exception as e:
       print(f"Load error: {e}")
   ```

2. **Column Name Mismatches**:
   ```python
   # Check expected vs actual columns
   expected_cols = ['SALES_DATE', 'Act Orders Rev', 'CatalogNumber', 'Region']
   actual_cols = state.df.columns if state.df is not None else []
   
   for col in expected_cols:
       if col not in actual_cols:
           print(f"Missing column: {col}")
           # Find similar columns
           similar = [c for c in actual_cols if col.lower() in c.lower()]
           print(f"Similar columns: {similar}")
   ```

3. **Data Type Issues**:
   ```python
   # Check data types
   if state.df is not None:
       print(state.df.dtypes)
       
       # Fix common type issues
       numeric_cols = ['Act Orders Rev', 'Fcst Stat Prelim Rev']
       for col in numeric_cols:
           if col in state.df.columns:
               state.df = state.df.with_columns(pl.col(col).cast(pl.Float32))
   ```

### 4. Model Training Failures

#### Symptoms
- "Model creation failed" errors
- NaN values in forecasts
- Training takes too long or hangs

#### Diagnostic Steps
```python
# Check data quality for modeling
if state.filtered_df is not None:
    df = state.filtered_df
    
    # Check for missing values
    null_counts = df.null_count()
    print("Null counts per column:")
    for col, count in zip(df.columns, null_counts.row(0)):
        if count > 0:
            print(f"  {col}: {count}")
    
    # Check date continuity
    dates = df['SALES_DATE'].sort()
    date_gaps = dates.diff().dt.total_days()
    large_gaps = date_gaps.filter(date_gaps > 31)  # More than 31 days
    print(f"Large date gaps: {len(large_gaps)}")
    
    # Check for zero/negative values
    numeric_cols = ['Act Orders Rev']
    for col in numeric_cols:
        if col in df.columns:
            zero_count = df.filter(pl.col(col) <= 0).height
            print(f"Zero/negative values in {col}: {zero_count}")
```

#### Solutions
1. **Insufficient Data**:
   ```python
   # Check minimum data requirements
   min_periods = 24  # 2 years of monthly data
   if len(state.filtered_df) < min_periods:
       print(f"Warning: Only {len(state.filtered_df)} periods, need at least {min_periods}")
       # Reduce filters or use different aggregation level
   ```

2. **Data Quality Issues**:
   ```python
   # Clean data before modeling
   df_clean = state.filtered_df.filter(
       pl.col('Act Orders Rev').is_not_null() &
       (pl.col('Act Orders Rev') > 0) &
       pl.col('SALES_DATE').is_not_null()
   )
   state.update_filtered_data(df_clean)
   ```

3. **Memory Issues**:
   ```python
   # Monitor memory usage
   import psutil
   memory = psutil.virtual_memory()
   print(f"Memory usage: {memory.percent}%")
   print(f"Available memory: {memory.available / (1024**3):.1f} GB")
   
   # Reduce data size if needed
   if memory.percent > 80:
       # Sample data or increase aggregation level
       sampled_df = state.filtered_df.sample(fraction=0.5)
       state.update_filtered_data(sampled_df)
   ```

### 5. UI Component Issues

#### Symptoms
- Charts not displaying
- Filters not working
- UI elements not responding

#### Diagnostic Steps
```python
# Check UI state
from ui.components import get_ui_state

# Verify chart data
chart_data = state.get_chart_data('column')
if chart_data:
    print(f"Chart data available: {len(chart_data.get('series', []))} series")
else:
    print("No chart data available")

# Check filter state
print(f"Filtered products: {len(state.filtered_products)}")
print(f"By month toggle: {state.by_month}")
```

#### Solutions
1. **Chart Display Issues**:
   ```python
   # Verify chart data format
   chart_data = state.get_chart_data('column')
   if chart_data:
       # Check required fields
       required_fields = ['months', 'series']
       for field in required_fields:
           if field not in chart_data:
               print(f"Missing chart field: {field}")
   ```

2. **Filter Problems**:
   ```python
   # Reset filters
   state.filtered_products = []
   state.filtered_models = []
   state.filtered_df = state.df.clone() if state.df is not None else None
   ```

3. **UI Refresh Issues**:
   ```python
   # Force UI refresh
   from nicegui import ui
   ui.update()
   ```

### 6. Performance Issues

#### Symptoms
- Slow data loading
- UI becomes unresponsive
- High memory usage

#### Diagnostic Steps
```python
import time
import psutil

# Measure data loading time
start_time = time.time()
state.load_sample_data()
load_time = time.time() - start_time
print(f"Data loading time: {load_time:.2f} seconds")

# Check memory usage
process = psutil.Process()
memory_mb = process.memory_info().rss / (1024 * 1024)
print(f"Process memory usage: {memory_mb:.1f} MB")

# Check data size
if state.df is not None:
    data_size_mb = state.df.estimated_size() / (1024 * 1024)
    print(f"Data size in memory: {data_size_mb:.1f} MB")
```

#### Solutions
1. **Optimize Data Loading**:
   ```python
   # Use lazy loading
   df_lazy = pl.scan_parquet("data.parquet")
   df = df_lazy.filter(pl.col("Region") == "North America").collect()
   ```

2. **Reduce Memory Usage**:
   ```python
   # Use more efficient data types
   df = df.with_columns([
       pl.col("Act Orders Rev").cast(pl.Float32),  # Instead of Float64
       pl.col("Region").cast(pl.Categorical),      # For string columns with few unique values
   ])
   ```

3. **Database Optimization**:
   ```python
   # Create indexes for better query performance
   db = get_database_service()
   db.create_indexes()
   ```

## Advanced Debugging Techniques

### 1. Logging Configuration

```python
import logging

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('fcst_debug.log'),
        logging.StreamHandler()
    ]
)

# Add logger to modules
logger = logging.getLogger(__name__)
logger.debug("Debug message")
```

### 2. Data Validation

```python
def validate_data_integrity(df: pl.DataFrame) -> Dict[str, Any]:
    """Comprehensive data validation."""
    validation_results = {
        'total_rows': len(df),
        'null_counts': df.null_count().to_dict(),
        'duplicate_rows': df.is_duplicated().sum(),
        'date_range': (df['SALES_DATE'].min(), df['SALES_DATE'].max()) if 'SALES_DATE' in df.columns else None,
        'numeric_stats': {}
    }
    
    # Check numeric columns
    numeric_cols = [col for col in df.columns if df[col].dtype in [pl.Float32, pl.Float64, pl.Int32, pl.Int64]]
    for col in numeric_cols:
        stats = df[col].describe()
        validation_results['numeric_stats'][col] = {
            'min': stats['min'][0],
            'max': stats['max'][0],
            'mean': stats['mean'][0],
            'null_count': stats['null_count'][0]
        }
    
    return validation_results
```

### 3. Performance Profiling

```python
import cProfile
import pstats

def profile_function(func, *args, **kwargs):
    """Profile function execution."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    result = func(*args, **kwargs)
    
    profiler.disable()
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(10)  # Top 10 functions
    
    return result

# Usage
result = profile_function(state.load_sample_data)
```

### 4. Memory Profiling

```python
from memory_profiler import profile

@profile
def memory_intensive_function():
    """Function to profile memory usage."""
    state = get_global_state()
    df = state.load_sample_data()
    filtered_df = df.filter(pl.col('Region') == 'North America')
    chart_data = state.get_chart_data('column')
    return chart_data

# Run with: python -m memory_profiler script.py
```

## Environment-Specific Issues

### Windows-Specific Issues

1. **Path Separators**:
   ```python
   import os
   db_path = os.path.join("data", "forecasting.duckdb")  # Use os.path.join
   ```

2. **PowerShell Execution Policy**:
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

3. **Long Path Names**:
   - Enable long path support in Windows
   - Use shorter directory names

### Linux/macOS-Specific Issues

1. **Permission Issues**:
   ```bash
   chmod +x main.py
   sudo chown -R $USER:$USER /path/to/fcst
   ```

2. **Virtual Environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

## Testing and Validation

### Unit Testing

```python
import pytest
from state_manager import DataState

def test_data_loading():
    """Test data loading functionality."""
    state = DataState()
    df = state.load_sample_data()
    
    assert df is not None
    assert len(df) > 0
    assert 'SALES_DATE' in df.columns
    assert 'Act Orders Rev' in df.columns

def test_filter_options():
    """Test filter options generation."""
    state = DataState()
    state.load_sample_data()
    
    options = state.get_filter_options()
    assert 'products_filt' in options
    assert 'locations_filt' in options
    assert len(options['products_filt']) > 0
```

### Integration Testing

```python
def test_end_to_end_workflow():
    """Test complete workflow."""
    # Initialize state
    state = get_global_state()
    
    # Load data
    df = state.load_sample_data()
    assert df is not None
    
    # Apply filters
    filtered_df = df.filter(pl.col('Region') == 'North America')
    state.update_filtered_data(filtered_df)
    
    # Generate chart data
    chart_data = state.get_chart_data('column')
    assert chart_data is not None
    assert 'series' in chart_data
```

## Emergency Recovery

### Database Recovery

```python
def recover_database():
    """Emergency database recovery."""
    import shutil
    import os
    
    # Backup corrupted database
    if os.path.exists("forecasting.duckdb"):
        shutil.copy("forecasting.duckdb", "forecasting_corrupted.duckdb")
        os.remove("forecasting.duckdb")
    
    # Recreate from parquet files
    from migrate_to_duckdb import main as migrate
    migrate()
    
    print("Database recovered successfully")
```

### State Recovery

```python
def recover_application_state():
    """Recover application state."""
    from state_manager import initialize_global_state
    
    # Reset global state
    initialize_global_state()
    
    # Reload data
    state = get_global_state()
    state.load_sample_data()
    
    print("Application state recovered")
```

## Getting Help

### Debug Information Collection

```python
def collect_debug_info():
    """Collect comprehensive debug information."""
    import sys
    import platform
    import pkg_resources
    
    debug_info = {
        'system': {
            'platform': platform.platform(),
            'python_version': sys.version,
            'architecture': platform.architecture()
        },
        'packages': {
            pkg.project_name: pkg.version 
            for pkg in pkg_resources.working_set
        },
        'application': {
            'data_loaded': get_global_state().df is not None,
            'database_connected': True  # Add actual check
        }
    }
    
    return debug_info

# Save debug info
import json
debug_info = collect_debug_info()
with open('debug_info.json', 'w') as f:
    json.dump(debug_info, f, indent=2)
```

### Support Channels

1. **Check Documentation**: Review API documentation and architecture guides
2. **Search Issues**: Look for similar issues in the repository
3. **Create Issue**: Provide debug information and steps to reproduce
4. **Contact Team**: Reach out to development team with debug_info.json

Remember to always include:
- Error messages (full stack trace)
- Steps to reproduce
- Environment information
- Debug information from collect_debug_info()
