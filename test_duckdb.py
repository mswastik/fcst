"""
Test script to verify DuckDB connection works properly
"""
import os
import duckdb
import tempfile

def test_duckdb_connection():
    """Test DuckDB connection with a temporary database"""
    # Create a temporary database for testing
    with tempfile.NamedTemporaryFile(suffix='.duckdb', delete=False) as tmp_file:
        temp_db_path = tmp_file.name
    
    try:
        print(f"Testing DuckDB connection with temp database: {temp_db_path}")
        
        # Connect to DuckDB
        conn = duckdb.connect(temp_db_path)
        
        # Test basic operation
        result = conn.execute("SELECT 1 as test").fetchall()
        print(f"Basic query test result: {result}")
        
        # Create a test table
        conn.execute("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
        print("Test table created successfully")
        
        # Insert a test record
        conn.execute("INSERT INTO test_table VALUES (1, 'test')")
        print("Test record inserted")
        
        # Query the test record
        result = conn.execute("SELECT * FROM test_table").fetchall()
        print(f"Retrieved test record: {result}")
        
        # Close connection
        conn.close()
        print("Connection closed successfully")
        
        # Test if we can connect again
        conn = duckdb.connect(temp_db_path)
        result = conn.execute("SELECT * FROM test_table").fetchall()
        print(f"Reconnection test result: {result}")
        conn.close()
        
        print("All tests passed! DuckDB connection works properly.")
        return True
        
    except Exception as e:
        print(f"Error in DuckDB test: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up temp file
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)

def test_with_db_service():
    """Test the database service"""
    try:
        # Import and test the database service
        from core.db_service import get_database_service
        from core.duckdb_connection_manager import get_duckdb_connection_manager
        
        print("Testing DuckDB connection manager...")
        conn_manager = get_duckdb_connection_manager()
        
        # Create a user session
        user_id = "test_user"
        session_id = conn_manager.create_user_connection(user_id)
        print(f"Created session for user: {user_id}")
        
        # Get connection
        conn = conn_manager.get_user_connection(user_id)
        
        # Test query
        result = conn.execute("SELECT 1 as test").fetchall()
        print(f"Connection manager test result: {result}")
        
        # Close session
        stats = conn_manager.close_user_connection(user_id)
        print("Closed connection successfully")
        
        print("Database service test passed!")
        return True
        
    except Exception as e:
        print(f"Error in database service test: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing DuckDB connection...")
    test1 = test_duckdb_connection()
    print("\nTesting database service...")
    test2 = test_with_db_service()
    
    if test1 and test2:
        print("\nAll tests passed! DuckDB is ready for use.")
    else:
        print("\nSome tests failed.")