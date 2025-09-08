"""
Test script for Databricks database connection
"""
import os
from db_service import get_database_service

def test_databricks_connection():
    """Test the Databricks database connection and basic queries"""
    print("=== Databricks Connection Test ===")

    # Check environment variables
    print("\nEnvironment Variables:")
    host = os.getenv("DATABRICKS_HOST")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

    print(f"DATABRICKS_HOST: {'✓ Set' if host else '✗ Not set'}")
    print(f"DATABRICKS_CLIENT_ID: {'✓ Set' if client_id else '✗ Not set'}")
    print(f"DATABRICKS_CLIENT_SECRET: {'✓ Set' if client_secret else '✗ Not set'}")

    try:
        # Get database service instance
        db_service = get_database_service()

        # Show what configuration was loaded
        print(f"\nLoaded Configuration:")
        print(f"  Host: {db_service.host}")
        print(f"  HTTP Path: {db_service.http_path}")
        print(f"  Client ID: {'✓ Available' if db_service.client_id else '✗ Not available'}")
        print(f"  Client Secret: {'✓ Available' if db_service.client_secret else '✗ Not available'}")

        print("\nTesting Databricks connection...")

        # Test basic connection
        db_service._ensure_connection()
        print("✓ Connection established successfully")

        # Test basic query
        print("\nTesting basic query...")
        try:
            with db_service.conn.cursor() as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                print(f"✓ Basic query successful: {result}")
        except Exception as e:
            print(f"⚠ Basic query failed: {e}")

        # Test getting sales actuals count
        print("\nTesting sales actuals table...")
        try:
            with db_service.conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM sales_actuals")
                result = cursor.fetchone()
                count = result[0] if result else 0
                print(f"✓ Sales actuals count: {count}")
        except Exception as e:
            print(f"⚠ Sales actuals query failed: {e}")

        # Test getting product hierarchy
        print("\nTesting product hierarchy table...")
        try:
            ph_df = db_service.get_product_hierarchy()
            print(f"✓ Product hierarchy retrieved: {len(ph_df)} rows")
        except Exception as e:
            print(f"⚠ Product hierarchy query failed: {e}")

        print("\n=== Connection Test Summary ===")
        print("✓ Connection established and basic queries working")
        return True

    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Make sure environment variables are set:")
        print("   - DATABRICKS_HOST (your Databricks workspace URL)")
        print("   - DATABRICKS_CLIENT_ID (your service principal client ID)")
        print("   - DATABRICKS_CLIENT_SECRET (your service principal client secret)")
        print("2. Verify the HTTP path is correct for your SQL warehouse")
        print("3. Check that your databricks-cli is properly configured")
        return False
    finally:
        # Close the connection
        try:
            db_service.close()
        except:
            pass

if __name__ == "__main__":
    test_databricks_connection()
