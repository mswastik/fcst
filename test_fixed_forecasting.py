#!/usr/bin/env python3
"""
Test the fixed forecasting pipeline
"""
import sys
import os

# Add current directory to path
sys.path.append('.')

def test_fixed_forecasting_pipeline():
    """Test the fixed forecasting pipeline"""
    try:
        import polars as pl
        from datetime import datetime
        from data_service import run_enhanced_forecasting_pipeline

        print("[TEST] Testing fixed forecasting pipeline...")

        # Create sample data with long product names (similar to the error)
        sample_data = pl.DataFrame({
            'SALES_DATE': [
                datetime(2024, 1, 1), datetime(2024, 2, 1), datetime(2024, 3, 1),
                datetime(2024, 4, 1), datetime(2024, 5, 1), datetime(2024, 6, 1)
            ] * 2,  # Duplicate for 2 products
            'Act Orders Rev': [100.0, 120.0, 110.0, 130.0, 125.0, 140.0] * 2,
            'Country': ['USA'] * 6 + ['UK'] * 6,
            'CatalogNumber': [
                'GAMMA4 LONG NAILS R 9 120', 'GAMMA4 LONG NAILS R 9 120',
                'GAMMA4 LONG NAILS R 9 120', 'GAMMA4 LONG NAILS R 9 120',
                'GAMMA4 LONG NAILS R 9 120', 'GAMMA4 LONG NAILS R 9 120',
                'ULTRA LONG SCREW R 15 200', 'ULTRA LONG SCREW R 15 200',
                'ULTRA LONG SCREW R 15 200', 'ULTRA LONG SCREW R 15 200',
                'ULTRA LONG SCREW R 15 200', 'ULTRA LONG SCREW R 15 200'
            ]
        })

        print(f"[TEST] Created sample data with {len(sample_data)} rows")
        print(f"[TEST] Sample product names: {sample_data['CatalogNumber'].unique().to_list()[:2]}")

        # Test the pipeline
        print("[TEST] Running enhanced forecasting pipeline...")
        result_df, validation_results = run_enhanced_forecasting_pipeline(sample_data, "test_file")

        print(f"[TEST] Pipeline completed!")
        print(f"[TEST] Validation results: {validation_results}")

        if result_df is not None:
            print(f"[TEST] Result dataframe has {len(result_df)} rows")
            print("[TEST] SUCCESS: Pipeline handled long product names without errors")
        else:
            print("[TEST] Result is None, but no unpacking error occurred")

        return True

    except Exception as e:
        print(f"[TEST] Error in fixed forecasting pipeline test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_fixed_forecasting_pipeline()
