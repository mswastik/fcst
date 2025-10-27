"""
Database initialization script for DuckDB
"""
import os
import duckdb
from pathlib import Path

def initialize_database(db_path: str = None):
    """Initialize the DuckDB database with required schema"""
    # Use default path if none provided
    if db_path is None:
        #db_path = os.path.join("fcst.duckdb")
        db_path = "fcst.duckdb"
    
    # Create directory if it doesn't exist
    #os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    print(f"Initializing database at: {db_path}")
    
    # Connect to DuckDB
    conn = duckdb.connect(db_path)
    
    # Read and execute the schema
    schema_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "database_schema.sql")
    
    if os.path.exists(schema_file):
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        # Execute the schema
        conn.execute(schema_sql)
        print("Database schema created successfully!")
    else:
        print(f"Schema file not found: {schema_file}")
        # Create minimal schema as fallback
        create_minimal_schema(conn)
    
    # Close the connection
    conn.close()
    print(f"Database initialized at: {db_path}")

def create_minimal_schema(conn):
    """Create minimal required schema as fallback"""
    print("Creating minimal schema...")
    
    # Basic tables for the application
    conn.execute("""
    CREATE SCHEMA IF NOT EXISTS da;
    
    CREATE TABLE IF NOT EXISTS da.product_hierarchy (
        demantra_item_skey BIGINT,
        business_sector VARCHAR(100),
        business_unit VARCHAR(100),
        franchise VARCHAR(100),
        product_line VARCHAR(100),
        ibp_level_5 VARCHAR(100),
        ibp_level_6 VARCHAR(100),
        ibp_level_7 VARCHAR(100),
        catalog_number VARCHAR(50) NOT NULL,
        uom VARCHAR(20),
        pack_content VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS da.location_hierarchy (
        location_skey BIGINT,
        selling_division VARCHAR(100),
        area VARCHAR(100),
        stryker_group_region VARCHAR(100),
        region VARCHAR(100),
        country VARCHAR(100) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS da.sales_actuals (
        id BIGINT,
        item_skey BIGINT NOT NULL,
        location_skey BIGINT NOT NULL,
        sales_date DATE NOT NULL,
        asp_final_rev DECIMAL(38,8),
        act_orders_rev DECIMAL(38,8),
        act_orders_rev_val DECIMAL(38,8),
        fcst_df_final_rev DECIMAL(38,8),
        l0_df_final_rev DECIMAL(38,8),
        l1_df_final_rev DECIMAL(38,8),
        l2_df_final_rev DECIMAL(38,8),
        fcst_df_final_rev_val DECIMAL(38,8),
        fcst_stat_prelim_rev DECIMAL(38,8),
        fcst_stat_final_rev DECIMAL(38,8),
        l0_stat_final_rev DECIMAL(38,8),
        l1_stat_final_rev DECIMAL(38,8),
        l2_stat_final_rev DECIMAL(38,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS da.forecasts (
        forecast_id BIGINT,
        item_skey BIGINT NOT NULL,
        location_skey BIGINT NOT NULL,
        forecast_date DATE NOT NULL,
        forecast_horizon INTEGER NOT NULL,
        model_type VARCHAR(50) NOT NULL,
        forecast_value DECIMAL(15,2) NOT NULL,
        confidence_lower DECIMAL(15,2),
        confidence_upper DECIMAL(15,2),
        model_version VARCHAR(50),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS da.model_validation (
        validation_id BIGINT,
        model_type VARCHAR(50) NOT NULL,
        validation_date DATE NOT NULL,
        validation_period_months INTEGER NOT NULL,
        mae DECIMAL(10,4),
        mape DECIMAL(10,4),
        rmse DECIMAL(10,4),
        accuracy_percentage DECIMAL(5,2),
        forecast_bias DECIMAL(10,4),
        silhouette_score DECIMAL(15,6),
        validation_details VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS da.product_clusters (
        cluster_id VARCHAR(255),
        item_skey BIGINT NOT NULL,
        location_skey BIGINT NOT NULL,
        cluster_number INTEGER NOT NULL,
        cluster_features VARCHAR,
        silhouette_score DECIMAL(15,6),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE TABLE IF NOT EXISTS da.final_forecasts (
        final_forecast_id BIGINT,
        forecast_id BIGINT,
        item_skey BIGINT NOT NULL,
        location_skey BIGINT NOT NULL,
        forecast_date DATE NOT NULL,
        forecast_horizon INTEGER NOT NULL,
        forecast_value DECIMAL(15,2) NOT NULL,
        forecast_cycle_month DATE NOT NULL,
        model_type VARCHAR(50) NOT NULL,
        model_version VARCHAR(50),
        approved_by VARCHAR(100) NOT NULL,
        is_current BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create essential indexes
    CREATE INDEX IF NOT EXISTS idx_sales_actuals_date ON da.sales_actuals(sales_date);
    CREATE INDEX IF NOT EXISTS idx_forecasts_date ON da.forecasts(forecast_date);
    CREATE INDEX IF NOT EXISTS idx_sales_actuals_item_location ON da.sales_actuals(item_skey, location_skey);
    CREATE INDEX IF NOT EXISTS idx_forecasts_item_location ON da.forecasts(item_skey, location_skey);
    """)

def test_connection(db_path: str = None):
    """Test the database connection"""
    if db_path is None:
        db_path = os.path.join("fcst.duckdb")
    
    try:
        conn = duckdb.connect(db_path)
        result = conn.execute("SELECT 1 as test").fetchone()
        conn.close()
        print("Database connection test successful!")
        return True
    except Exception as e:
        print(f"Database connection test failed: {e}")
        return False

if __name__ == "__main__":
    initialize_database()
    test_connection()