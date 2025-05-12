import pandas as pd
import polars as pl
from models.data_model import df, filtered_df, filtered_products, filtered_models, by_month

def apply_filters(filters):
    """Apply filters to the dataset"""
    global filtered_df, filtered_products, filtered_models
    from models.data_model import df, filtered_df, filtered_products, filtered_models
    
    #mask = pd.Series(True, index=df.index)
    #mask=pl.Series([True])
    
    if filters.get('data_files'):
        # Apply data files filter (placeholder)
        pass
        
    if filters.get('location2') and filters.get('location1'):
       df = df.filter(pl.col(filters['location1']) == filters['location2'])
        
    if filters.get('product2') and filters.get('product1'):
        df = df.filter(pl.col(filters['product1']) == filters['product2'])
    
    # Update global filtered dataframe
    if filters.get('level'):
        df=df.group_by(['SALES_DATE',filters['level'],filters['location1']]).sum()
    else:
        df=df.group_by(['SALES_DATE',filters['product1'],filters['location1']]).sum()
    filtered_df = df
    
    # Update filtered products list
    try:
        filtered_products = filtered_df[filters['product2']].unique().tolist()
    except:
        pass
    # Update models list (placeholder for real model data)
    filtered_models = [f"Model for {product}" for product in filtered_products]
    
    return {
        'filtered_df': filtered_df,
        'filtered_products': filtered_products,
        'filtered_models': filtered_models
    }

def toggle_month_view(state):
    """Toggle between normal and month view"""
    global by_month
    from models.data_model import by_month
    by_month = state
    return by_month

def create_models_action():
    """Business logic for creating models"""
    # Actual model creation logic would go here
    return f"Creating {len(filtered_products)} models"

def generate_fc_action():
    """Business logic for generating forecast"""
    # Actual forecast generation logic would go here
    return f"Generating forecasts for {len(filtered_products)} products"

def change_fc_action():
    """Business logic for changing forecast settings"""
    # Actual forecast settings logic would go here
    return "Changing forecast settings"

def download_data_action():
    """Business logic for downloading data"""
    # Actual download implementation would go here
    return f"Downloading data for {len(filtered_df)} records"

def export_data_action():
    """Business logic for exporting data"""
    # Actual export implementation would go here
    return f"Exporting data for {len(filtered_df)} records"