import pandas as pd
import polars as pl
import random
from datetime import datetime, timedelta
from state_manager import get_global_state, DataState
from typing import Optional, Dict, Any

# Get the global state instance
state = get_global_state()

# Backward compatibility - expose state properties as module-level variables
def get_df():
    return state.df

def get_filtered_df():
    return state.filtered_df

def get_filtered_products():
    return state.filtered_products

def get_filtered_models():
    return state.filtered_models

def get_by_month():
    return state.by_month

# For backward compatibility, create module-level references
df = state.df
filtered_df = state.filtered_df
filtered_products = state.filtered_products
filtered_models = state.filtered_models
by_month = state.by_month

def initialize_data():
    """Initialize the application data"""
    state.initialize_data()

def generate_sample_data(path: str) -> pl.DataFrame:
    """Generate sample data for the application"""
    return state.load_sample_data(path)
def get_filter_options(prod: Optional[str] = None, loc: Optional[str] = None) -> Dict[str, Any]:
    """Return filter options for UI dropdowns"""
    return state.get_filter_options(prod, loc)

def get_chart_data(chart_type: str, filtered_df: pl.DataFrame) -> Optional[Dict[str, Any]]:
    """Get data formatted for charts"""
    # Update state with the provided filtered_df
    state.update_filtered_data(filtered_df)
    return state.get_chart_data(chart_type)
