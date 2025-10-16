#!/usr/bin/env python3
"""
Test script to verify the forecast insertion fixes
"""
import polars as pl
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from core.db_service import get_database_service
from core.utils import DatabaseUtils

def test_forecast_insertion():
    """Test the forecast insertion with sample data"""
    print("Testing forecast insertion with sample data...")
    
    # Create more comprehensive sample forecast data with 60 months of data for multiple series
    import numpy as np
    
    # Generate 60 months of forecast data for multiple unique_ids
    unique_ids = ['123_456', '789_012', '345_678']
    base_date = datetime.today()
    
    all_unique_ids = []
    all_forecast_dates = []
    all_fcst_values = []
    all_item_skeys = []
    all_location_skeys = []
    
    for unique_id in unique_ids:
        # Extract skeys from unique_id (item_skey_location_skey format)
        item_skey = int(unique_id.split('_')[0])
        location_skey = int(unique_id.split('_')[1])
        
        # Generate 60 months of forecast data for this series
        for i in range(60):  # 60 months horizon
            all_unique_ids.append(unique_id)
            forecast_date = base_date + relativedelta(months=i+1)  # Next 60 months
            all_forecast_dates.append(forecast_date)
            
            # Generate some realistic forecast values with trend
            base_value = (item_skey % 1000) + (location_skey % 100)  # Base value
            trend = i * 2  # Slight upward trend
            seasonal = 50 * np.sin(2 * np.pi * i / 12)  # Annual seasonality
            forecast_value = base_value + trend + seasonal + np.random.normal(0, 10)
            all_fcst_values.append(forecast_value)
            
            all_item_skeys.append(item_skey)
            all_location_skeys.append(location_skey)
    
    # Create forecast DataFrame
    forecast_df = pl.DataFrame({
        'unique_id': all_unique_ids,
        'forecast_date': all_forecast_dates,
        'Fcst Ensemble Rev': all_fcst_values,
        'item_skey': all_item_skeys,
        'location_skey': all_location_skeys
    })
    
    print(f"Sample forecast DataFrame created with {len(forecast_df)} records across {forecast_df['unique_id'].n_unique()} series")
    print("DataFrame schema:")
    print(forecast_df.schema)
    print("\nFirst few records:")
    print(forecast_df.head())
    print(f"\nDate range: {forecast_df['forecast_date'].min()} to {forecast_df['forecast_date'].max()}")
    print(f"Unique IDs: {forecast_df['unique_id'].unique().to_list()}")
    
    # Test the database insertion
    db_service = DatabaseUtils.get_database_service()
    if db_service is None:
        print("ERROR: Could not get database service")
        return False
    
    print("\nStarting database insertion test...")
    try:
        result = db_service.insert_forecasts(forecast_df, model_type="TestModel")
        print(f"Successfully inserted {result} forecast records")
        return True
    except Exception as e:
        print(f"Error during database insertion: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_forecast_insertion()
    if success:
        print("\n✓ Test passed: Forecast insertion is working correctly!")
    else:
        print("\n✗ Test failed: There are issues with forecast insertion.")