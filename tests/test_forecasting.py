#!/usr/bin/env python3
"""
Test script to verify forecasting functionality
"""
import sys
import os

# Add current directory to path
sys.path.append('.')

def test_forecasting_imports():
    """Test if forecasting modules can be imported"""
    try:
        from forecasting.model_factory import EnsembleForecaster
        print("[OK] EnsembleForecaster imported successfully")
        return True
    except ImportError as e:
        print(f"[ERROR] Failed to import EnsembleForecaster: {e}")
        return False

def test_data_service_imports():
    """Test if data service modules work"""
    try:
        from data_service import create_models_action, FORECASTING_AVAILABLE
        print(f"[OK] Data service imported successfully. Forecasting available: {FORECASTING_AVAILABLE}")
        return True
    except ImportError as e:
        print(f"[ERROR] Failed to import data service: {e}")
        return False

def test_db_service():
    """Test database service"""
    try:
        from db_service import get_database_service
        db_service = get_database_service()
        print("[OK] Database service initialized successfully")
        return True
    except Exception as e:
        print(f"[ERROR] Database service failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing forecasting functionality...")
    print("=" * 50)

    imports_ok = test_forecasting_imports()
    data_ok = test_data_service_imports()
    db_ok = test_db_service()

    print("=" * 50)
    if imports_ok and data_ok and db_ok:
        print("[SUCCESS] All tests passed! Forecasting should work.")
    else:
        print("[FAILURE] Some tests failed. Check the error messages above.")
