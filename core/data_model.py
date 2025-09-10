import polars as pl
from core.state_manager import get_global_state
from typing import Optional, Dict, Any

# Remove early state access - make it lazy
# state = get_global_state()  # This causes import-time error

# Backward compatibility - expose state properties as module-level variables via lazy access
def get_df():
    return get_global_state().df

def get_filtered_df():
    return get_global_state().filtered_df

def get_filtered_products():
    return get_global_state().filtered_products

def get_filtered_models():
    return get_global_state().filtered_models

def get_by_month():
    return get_global_state().by_month

# For backward compatibility, create module-level references (lazy)
@property
def df():
    return get_global_state().df

@property
def filtered_df():
    return get_global_state().filtered_df

@property
def filtered_products():
    return get_global_state().filtered_products

@property
def filtered_models():
    return get_global_state().filtered_models

@property
def by_month():
    return get_global_state().by_month

def initialize_data():
    """Initialize the application data"""
    state = get_global_state()
    state.initialize_data()

def generate_sample_data(path: str = None) -> pl.DataFrame:
    """Generate sample data for the application from DuckDB with lazy loading"""
    if path is None:
        # Lazy loading mode - return empty dataframe until filters are applied
        return pl.DataFrame()
    else:
        # Legacy mode for backward compatibility
        state = get_global_state()
        return state.load_sample_data(path)

def get_filter_options(prod: Optional[str] = None, loc: Optional[str] = None) -> Dict[str, Any]:
    """Return filter options for UI dropdowns"""
    from core.utils import DatabaseUtils
    
    # Get options directly from database for better reliability
    db_service = DatabaseUtils.get_database_service()
    if db_service is None:
        return {
            'products_filt': [],
            'locations_filt': [],
            'products': ['Franchise', 'IBP Level 5', 'IBP Level 6', 'CatalogNumber'],
            'locations': ['Area', 'Region', 'Country'],
            'levels': ['Franchise', 'IBP Level 5', 'IBP Level 6', 'CatalogNumber']
        }
    
    filter_options = db_service.get_filter_options()
    
    # Map the requested product/location to appropriate database fields
    prod_key = 'catalog_numbers'  # Default
    if prod == 'Franchise':
        prod_key = 'franchises'
    elif prod == 'IBP Level 5':
        prod_key = 'ibp_level_5s'
    elif prod == 'IBP Level 6':
        prod_key = 'ibp_level_6s'
    elif prod == 'CatalogNumber':
        prod_key = 'catalog_numbers'
    
    loc_key = 'countries'  # Default
    if loc == 'Region':
        loc_key = 'regions'
    elif loc == 'Area':
        loc_key = 'areas'
    elif loc == 'Country':
        loc_key = 'countries'
    
    return {
        'products_filt': filter_options.get(prod_key, []),
        'locations_filt': filter_options.get(loc_key, []),
        'products': ['Franchise', 'IBP Level 5', 'IBP Level 6', 'CatalogNumber'],
        'locations': ['Area', 'Region', 'Country'],
        'levels': ['Franchise', 'IBP Level 5', 'IBP Level 6', 'CatalogNumber']
    }

def get_chart_data(chart_type: str, filtered_df: pl.DataFrame) -> Optional[Dict[str, Any]]:
    """Get data formatted for charts"""
    print(f"DEBUG: get_chart_data called with chart_type='{chart_type}', filtered_df={filtered_df is not None}")
    if filtered_df is not None:
        print(f"DEBUG: filtered_df has {len(filtered_df)} rows")
        print(f"DEBUG: filtered_df columns: {list(filtered_df.columns) if hasattr(filtered_df, 'columns') else 'No columns attr'}")

    # Update state with the provided filtered_df
    state = get_global_state()
    state.update_filtered_data(filtered_df)
    result = state.get_chart_data(chart_type)
    print(f"DEBUG: get_chart_data returning: {result is not None}")
    return result
