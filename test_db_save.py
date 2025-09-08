#!/usr/bin/env python3
"""
Simple test to verify database forecast saving
"""
import sys
import os

# Add current directory to path
sys.path.append('.')

def test_db_forecast_save():
    """Test saving a single forecast record to database"""
    try:
        import polars as pl
        from db_service import get_database_service

        print("[TEST] Testing database forecast saving...")

        # Create a simple forecast record
        forecast_df = pl.DataFrame({
            'forecast_id': [1],
            'item_skey': [1],
            'location_skey': [1],
            'forecast_date': [None],
            'forecast_horizon': [60],
            'model_type': ['Test'],
            'forecast_value': [100.0]
        })

        print(f"[TEST] Created forecast data: {forecast_df}")

        # Get database service
        db_service = get_database_service()

        # Try to save the forecast
        print("[TEST] Attempting to save forecast to database...")
        saved_count = db_service.insert_forecasts(forecast_df, model_type="Test")

        print(f"[TEST] Successfully saved {saved_count} forecast records to database")
        return True

    except Exception as e:
        print(f"[TEST] Error saving forecast to database: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_db_forecast_save()
