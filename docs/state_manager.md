# State Manager API Documentation

The `state_manager.py` module provides centralized state management for the FCST application, replacing global variables with a proper state management system.

## Classes

### DataState

The main state container that manages all application data and UI state.

```python
@dataclass
class DataState:
    """Centralized state management for application data."""
```

#### Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `df` | `Optional[pl.DataFrame]` | Main dataframe containing all loaded data |
| `full_df` | `Optional[pl.DataFrame]` | Store original full dataset (backup of complete data) |
| `filtered_df` | `Optional[pl.DataFrame]` | Filtered subset of main dataframe |
| `filtered_products` | `List[str]` | List of currently filtered products |
| `filtered_models` | `List[str]` | List of available models for filtered data |
| `by_month` | `bool` | Toggle for monthly aggregation in charts |
| `loading_charts` | `bool` | Loading state indicator for chart components |
| `loading_table` | `bool` | Loading state indicator for table components |
| `loading_data` | `bool` | Loading state indicator for data loading operations |
| `loading_message` | `str` | Current loading message for UI feedback |
| `products` | `List[str]` | Available product hierarchy levels |
| `locations` | `List[str]` | Available location hierarchy levels |
| `levels` | `List[str]` | Available aggregation levels |
| `products_filt` | `Optional[pl.DataFrame]` | Product hierarchy data |
| `locations_filt` | `Optional[pl.DataFrame]` | Location hierarchy data |

#### Methods

##### `initialize_data() -> None`

Initializes the application data by resetting dataframes.

```python
def initialize_data(self) -> None:
    """Initialize the application data."""
```

**Usage:**
```python
state = get_global_state()
state.initialize_data()
```

##### `set_loading_state(component: str, loading: bool, message: str = "") -> None`

Sets the loading state for a specific UI component.

```python
def set_loading_state(self, component: str, loading: bool, message: str = "") -> None:
    """Set loading state for a specific component."""
```

**Parameters:**
- `component` (str): Component name ('charts', 'table', 'data')
- `loading` (bool): Loading state (True for loading, False for complete)
- `message` (str, optional): Loading message for UI feedback

**Usage:**
```python
state = get_global_state()
state.set_loading_state('charts', True, "Loading chart data...")
# ... perform chart operations ...
state.set_loading_state('charts', False)
```

##### `is_loading(component: str = None) -> bool`

Checks if a component is in loading state.

```python
def is_loading(self, component: str = None) -> bool:
    """Check if a component or any component is loading."""
```

**Parameters:**
- `component` (str, optional): Specific component to check ('charts', 'table', 'data')

**Returns:**
- `bool`: True if loading, False if not. If no component specified, returns True if any component is loading.

**Usage:**
```python
state = get_global_state()
if state.is_loading('charts'):
    print("Charts are loading...")
if state.is_loading():  # Check if any component is loading
    print("Some component is loading...")
```

##### `load_sample_data(path: str = None) -> pl.DataFrame`

Loads sample data from the DuckDB database and performs necessary transformations.

```python
def load_sample_data(self, path: str = None) -> pl.DataFrame:
    """Load sample data from DuckDB database."""
```

**Parameters:**
- `path` (str, optional): Path to data file (currently unused, loads from database)

**Returns:**
- `pl.DataFrame`: Loaded and transformed dataframe

**Raises:**
- `ValueError`: If data loading fails

**Usage:**
```python
state = get_global_state()
df = state.load_sample_data()
print(f"Loaded {len(df)} rows")
```

**Data Transformations:**
- Renames columns to match expected format
- Casts numeric columns to Float32
- Sets up filtered_df as a clone of main df

##### `get_filter_options(prod: str = None, loc: str = None) -> Dict[str, Any]`

Returns filter options for UI dropdowns based on current data or database.

```python
def get_filter_options(self, prod: str = None, loc: str = None) -> Dict[str, Any]:
    """Return filter options for UI dropdowns."""
```

**Parameters:**
- `prod` (str, optional): Product hierarchy level to get options for
- `loc` (str, optional): Location hierarchy level to get options for

**Returns:**
- `Dict[str, Any]`: Dictionary containing filter options with keys:
  - `products_filt`: List of available products
  - `locations_filt`: List of available locations
  - `products`: List of product hierarchy levels
  - `locations`: List of location hierarchy levels
  - `levels`: List of aggregation levels

**Usage:**
```python
state = get_global_state()
options = state.get_filter_options(prod="Franchise", loc="Region")
print(f"Available franchises: {options['products_filt']}")
```

##### `update_filtered_data(new_filtered_df: pl.DataFrame) -> None`

Updates the filtered dataframe and related state based on new filter criteria.

```python
def update_filtered_data(self, new_filtered_df: pl.DataFrame) -> None:
    """Update the filtered dataframe and related state."""
```

**Parameters:**
- `new_filtered_df` (pl.DataFrame): New filtered dataframe

**Side Effects:**
- Updates `filtered_df` attribute
- Updates `filtered_products` list
- Updates `filtered_models` list

**Usage:**
```python
state = get_global_state()
filtered_data = state.df.filter(pl.col("Region") == "North America")
state.update_filtered_data(filtered_data)
```

##### `get_chart_data(chart_type: str) -> Optional[Dict[str, Any]]`

Gets data formatted for specific chart types.

```python
def get_chart_data(self, chart_type: str) -> Optional[Dict[str, Any]]:
    """Get data formatted for charts."""
```

**Parameters:**
- `chart_type` (str): Type of chart ('column' or 'line')

**Returns:**
- `Optional[Dict[str, Any]]`: Chart data dictionary or None if no data available

**Chart Types:**
- `'column'`: Returns data for column/bar charts with monthly aggregation
- `'line'`: Returns data for line charts with time series

**Usage:**
```python
state = get_global_state()
column_data = state.get_chart_data('column')
line_data = state.get_chart_data('line')
```

**Column Chart Data Format:**
```python
{
    'months': ['Jan', 'Feb', 'Mar', ...],
    'series': [
        {
            'name': '2024 - Actual',
            'type': 'bar',
            'data': [100, 150, 200, ...],
            'color': '#5470C6'
        },
        {
            'name': '2024 - Forecast',
            'type': 'line',
            'data': [110, 160, 190, ...],
            'color': '#5470C6',
            'lineStyle': {'type': 'dashed'}
        }
    ]
}
```

**Line Chart Data Format:**
```python
{
    'categories': ['2024-01-01', '2024-02-01', ...],
    'values': [100, 150, 200, ...],
    'forecast_values': [110, 160, 190, ...]
}
```

## Global Functions

### `get_global_state() -> DataState`

Returns the global state instance for backward compatibility.

```python
def get_global_state() -> DataState:
    """Get the global state instance."""
```

**Returns:**
- `DataState`: The global state instance

**Usage:**
```python
from state_manager import get_global_state

state = get_global_state()
df = state.load_sample_data()
```

### `initialize_global_state() -> None`

Initializes the global state instance.

```python
def initialize_global_state() -> None:
    """Initialize the global state."""
```

**Usage:**
```python
from state_manager import initialize_global_state

initialize_global_state()
```

## Data Flow

### Loading Data
1. `load_sample_data()` called
2. Database service retrieves sales actuals
3. Column names normalized to expected format
4. Numeric columns cast to Float32
5. `filtered_df` initialized as clone of main `df`

### Filtering Data
1. UI applies filters to main dataframe
2. `update_filtered_data()` called with filtered result
3. `filtered_products` and `filtered_models` updated
4. Charts and UI components refresh with new data

### Chart Generation
1. `get_chart_data()` called with chart type
2. Data aggregated by month or date based on `by_month` toggle
3. Series data formatted for specific chart library
4. Color schemes and styling applied

## Error Handling

The state manager includes comprehensive error handling:

- **Database Connection Errors**: Graceful fallback to empty data
- **Column Mapping Errors**: Flexible column name matching
- **Data Type Errors**: Safe casting with error recovery
- **Filter Errors**: Default to empty filter lists

## Performance Considerations

- **Lazy Loading**: Hierarchy data loaded only when needed
- **Memory Management**: Filtered dataframes are views when possible
- **Caching**: Filter options cached until data changes
- **Efficient Aggregation**: Uses Polars for fast group operations

## Thread Safety

The current implementation uses a global singleton pattern. For multi-threaded applications, consider:

- Using thread-local storage
- Implementing proper locking mechanisms
- Creating separate state instances per session
