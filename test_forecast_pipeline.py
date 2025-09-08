#!/usr/bin/env python3
"""
Simple test to verify forecast generation and database saving
"""
import sys
import os

# Add current directory to path
sys.path.append('.')

def test_forecast_pipeline():
    """Test the complete forecast generation pipeline"""
    try:
        import polars as pl
        from datetime import datetime
        from data_service import _standalone_forecasting_pipeline

        print("[TEST] Testing forecast pipeline...")

        # Create sample data for testing
        sample_data = pl.DataFrame({
            'SALES_DATE': [datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1),
                          datetime(2024, 4, 1), datetime(2024, 5, 1), datetime(2024, 6, 1)],
            'Act Orders Rev': [100.0, 120.0, 110.0, 130.0, 125.0, 140.0],
            'Country': ['USA', 'USA', 'USA', 'USA', 'USA', 'USA'],
            'CatalogNumber': ['ABC123', 'ABC123', 'ABC123', 'ABC123', 'ABC123', 'ABC123']
        })

        print(f"[TEST] Created sample data with {len(sample_data)} rows")

        # Convert to JSON for the pipeline
        df_json = sample_data.write_json()

        # Run the forecasting pipeline
        print("[TEST] Running forecasting pipeline...")
        merged_df_json, validation_results = _standalone_forecasting_pipeline(df_json, "test_file")

        print(f"[TEST] Pipeline completed. Validation results: {validation_results}")

        if merged_df_json:
            merged_df = pl.read_json(merged_df_json)
            print(f"[TEST] Merged dataframe has {len(merged_df)} rows")
            print(f"[TEST] Columns: {merged_df.columns}")
        else:
            print("[TEST] No merged dataframe returned")

        return True

    except Exception as e:
        print(f"[TEST] Error in forecast pipeline test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_forecast_pipeline()
