#!/usr/bin/env python3
"""
Test script to verify .env file loading and database configuration
"""
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

print("Testing Environment Variable Loading...")
print("=" * 50)

# Test Databricks environment variables
databricks_vars = [
    'DATABRICKS_HOST',
    'DATABRICKS_HTTP_PATH',
    'DATABRICKS_CLIENT_ID',
    'DATABRICKS_CLIENT_SECRET'
]

for var in databricks_vars:
    value = os.getenv(var)
    if value:
        if 'SECRET' in var:
            print(f"[OK] {var}: {'*' * len(value)} (set)")
        else:
            print(f"[OK] {var}: {value}")
    else:
        print(f"[ERROR] {var}: NOT SET")

print("\nTesting Database Configuration Import...")
try:
    from core.database_config import validate_database_config, DATABASE_CONFIG

    print("[OK] Database config module imported successfully")

    validation_result = validate_database_config()
    status = "PASSED" if validation_result['is_valid'] else "FAILED"
    print(f"[OK] Configuration validation: {status}")

    if not validation_result['is_valid']:
        print("[ERROR] Issues found:")
        for issue in validation_result['issues']:
            print(f"   - {issue}")
    else:
        print("[OK] All required environment variables are set!")

except Exception as e:
    print(f"[ERROR] Error importing database config: {e}")

print("\nTesting Enhanced Database Service Import...")
try:
    from core.enhanced_db_service import get_enhanced_database_service
    print("[OK] Enhanced database service imported successfully")
except Exception as e:
    print(f"[ERROR] Error importing enhanced database service: {e}")

print("\n" + "=" * 50)
print("Next Steps:")
print("1. Fill in your actual Databricks credentials in .env file")
print("2. Run this test again to verify configuration")
print("3. Start your application - it should work with enhanced features!")
