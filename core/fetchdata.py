from arrow_odbc import read_arrow_batches_from_odbc
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl
import os
from nicegui import run
import pyodbc
#from utils import ErrorHandler
import duckdb

drivers = sorted(pyodbc.drivers())
d = [i for i in drivers if i.find('ODBC Driver') != -1]

# Get connection manager to access the DuckDB instance
from .duckdb_connection_manager import get_duckdb_connection_manager

def fetch_and_save_sales_actuals(user_id: str = "system"):
    """
    Fetch sales actuals data using arrow_odbc and save to DuckDB da.sales_actuals table
    """
    # Get database connection
    conn_manager = get_duckdb_connection_manager()
    
    # Create connection if it doesn't exist
    try:
        conn = conn_manager.get_user_connection(user_id)
    except ValueError:
        # Connection doesn't exist, create it
        conn_manager.create_user_connection(user_id)
        conn = conn_manager.get_user_connection(user_id)
    
    query = '''
        SELECT
            s.[item_skey],[Location_skey],[SALES_DATE],
            AVG([ASP_Final_Rev]) [asp_final_rev], 
            SUM([Act_Orders_Rev]) [act_orders_rev],
            SUM([Act_Orders_Rev_Val]) [act_orders_rev_val],
            SUM(Fcst_DF_Final_Rev) as [fcst_df_final_rev], 
            SUM(s."L0_DF_Final_Rev") as [l0_df_final_rev],
            SUM(s."L1_DF_Final_Rev") as [l1_df_final_rev], 
            SUM(s.[L2_DF_Final_Rev]) as [l2_df_final_rev],
            SUM(Fcst_DF_Final_Rev_Val) as [fcst_df_final_rev_val],
            SUM(Fcst_Stat_Prelim_Rev) as [fcst_stat_prelim_rev],
            SUM(Fcst_Stat_Final_Rev) as [fcst_stat_final_rev],
            SUM(s."L0_Stat_Final_Rev") as [l0_stat_final_rev],
            SUM(s."L1_Stat_Final_Rev") as [l1_stat_final_rev], 
            SUM(s.[L2_Stat_Final_Rev]) as [l2_stat_final_rev] 
            
        FROM [Envision].[Demantra_CLD_Fact_Sales] s
        JOIN [Envision].[DIM_Demantra_CLD_products] p
        ON s.item_skey = p.demantra_item_skey AND p.[Current] = 'True'
        JOIN [Envision].[Dim_DEMANTRA_CLD_MDP_Matrix] m
        ON s.MDP_Key = m.MDP_Key

        WHERE
            [SALES_DATE] BETWEEN DATEADD(month, -37, GETDATE()) AND DATEADD(month, 24, GETDATE())
            
        GROUP BY
            s.[item_skey],s.[Location_skey],s.[SALES_DATE]
        '''
    
    # Get server from environment variable or use a default
    server = os.getenv("SERVER_NAME", "gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net")
    database = os.getenv("DATABASE_NAME", "gda_glbsyndb")
    username = os.getenv("DB_USERNAME", "Envisionread")
    password = os.getenv("DB_PASSWORD", "Env!s$@*on")
    
    # Create connection string
    connection_string = (
        f"Driver={{{d[-1] if d else 'ODBC Driver 18 for SQL Server'}}};"
        f"Server={server};database={database};"
        f"UID={username};PWD={password}"
    )
    
    print("Fetching sales actuals data...")
    try:
        reader = read_arrow_batches_from_odbc(query=query, connection_string=connection_string)
        df = pl.DataFrame()
        
        for batch in reader:
            batch_df = pl.from_arrow(batch)
            df = pl.concat([df, batch_df])
        
        print(f"Retrieved {len(df)} records from sales actuals")
        
        # Convert SALES_DATE to proper datetime format if needed
        if 'SALES_DATE' in df.columns:
            df = df.with_columns(
                pl.col('SALES_DATE').cast(pl.Datetime).dt.replace_time_zone(None)
            )
        
        # Prepare the data for insertion into DuckDB
        # Rename columns to match DuckDB schema
        rename_mapping = {
            'SALES_DATE': 'sales_date',
            'item_skey': 'item_skey',
            'Location_skey': 'location_skey',
            'asp_final_rev': 'asp_final_rev',
            'act_orders_rev': 'act_orders_rev',
            'act_orders_rev_val': 'act_orders_rev_val',
            'fcst_df_final_rev': 'fcst_df_final_rev',
            'l0_df_final_rev': 'l0_df_final_rev',
            'l1_df_final_rev': 'l1_df_final_rev',
            'l2_df_final_rev': 'l2_df_final_rev',
            'fcst_df_final_rev_val': 'fcst_df_final_rev_val',
            'fcst_stat_prelim_rev': 'fcst_stat_prelim_rev',
            'fcst_stat_final_rev': 'fcst_stat_final_rev',
            'l0_stat_final_rev': 'l0_stat_final_rev',
            'l1_stat_final_rev': 'l1_stat_final_rev',
            'l2_stat_final_rev': 'l2_stat_final_rev'
        }
        
        # Rename columns that exist in the dataframe
        for old_name, new_name in rename_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})
        
        # Write to DuckDB sales_actuals table
        # Convert to pandas first for DuckDB insert
        if not df.is_empty():
            # Ensure numeric columns are properly typed to avoid decimal casting errors
            numeric_columns = ['asp_final_rev', 'act_orders_rev', 'act_orders_rev_val', 
                              'fcst_df_final_rev', 'l0_df_final_rev', 'l1_df_final_rev', 
                              'l2_df_final_rev', 'fcst_df_final_rev_val', 'fcst_stat_prelim_rev',
                              'fcst_stat_final_rev', 'l0_stat_final_rev', 'l1_stat_final_rev', 
                              'l2_stat_final_rev']
            
            for col in numeric_columns:
                if col in df.columns:
                    # Convert to float to avoid decimal precision issues
                    df = df.with_columns([
                        pl.col(col).cast(pl.Float64, strict=False).alias(col)
                    ])
            
            df_pandas = df.to_pandas()
            
            # Insert the data into the DuckDB table (overwrite existing)
            conn.execute("DELETE FROM da.sales_actuals")
            conn.register("df_pandas", df_pandas)  # Register the DataFrame as a temporary table
            conn.execute("""
                INSERT INTO da.sales_actuals 
                (item_skey, location_skey, sales_date, asp_final_rev, act_orders_rev, act_orders_rev_val,
                 fcst_df_final_rev, l0_df_final_rev, l1_df_final_rev, l2_df_final_rev, 
                 fcst_df_final_rev_val, fcst_stat_prelim_rev, fcst_stat_final_rev, 
                 l0_stat_final_rev, l1_stat_final_rev, l2_stat_final_rev, created_at, updated_at)
                SELECT 
                    item_skey, location_skey, sales_date, asp_final_rev, act_orders_rev, act_orders_rev_val,
                    fcst_df_final_rev, l0_df_final_rev, l1_df_final_rev, l2_df_final_rev, 
                    fcst_df_final_rev_val, fcst_stat_prelim_rev, fcst_stat_final_rev, 
                    l0_stat_final_rev, l1_stat_final_rev, l2_stat_final_rev, 
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM df_pandas
            """)
            conn.unregister("df_pandas")  # Unregister the temporary table
        
        print(f"Successfully inserted {len(df)} records into da.sales_actuals table")
        return df
        
    except Exception as e:
        print(f"Error fetching sales actuals: {e}")
        import traceback
        traceback.print_exc()
        return None


def fetch_and_save_product_hierarchy(user_id: str = "system"):
    """
    Fetch product hierarchy data using arrow_odbc and save to DuckDB da.product_hierarchy table
    """
    # Get database connection
    conn_manager = get_duckdb_connection_manager()
    
    # Create connection if it doesn't exist
    try:
        conn = conn_manager.get_user_connection(user_id)
    except ValueError:
        # Connection doesn't exist, create it
        conn_manager.create_user_connection(user_id)
        conn = conn_manager.get_user_connection(user_id)
    
    query = '''
    SELECT DISTINCT
        [demantra_item_skey],[Business_Sector] [business_sector],[Franchise],[Business_Unit] [business_unit],[Product_Line] [product_line],[IBP_Level_5] [ibp_level_5],[IBP_Level_6] [ibp_level_6],[IBP_Level_7] [ibp_level_7],
        [CatalogNumber] [catalog_number],[xx_uom_conversion] as uom,[PackContent] AS [pack_content], [current]
        
    FROM [Envision].[DIM_Demantra_CLD_products]
    WHERE [current] = 'True'
    '''
    
    # Get server from environment variable or use a default
    server = os.getenv("SERVER_NAME", "gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net")
    database = os.getenv("DATABASE_NAME", "gda_glbsyndb")
    username = os.getenv("DB_USERNAME", "Envisionread")
    password = os.getenv("DB_PASSWORD", "Env!s$@*on")
    
    # Create connection string
    connection_string = (
        f"Driver={{{d[-1] if d else 'ODBC Driver 18 for SQL Server'}}};"
        f"Server={server};database={database};"
        f"UID={username};PWD={password}"
    )
    
    print("Fetching product hierarchy data...")
    try:
        reader = read_arrow_batches_from_odbc(query=query, connection_string=connection_string)
        df = pl.DataFrame()
        
        for batch in reader:
            batch_df = pl.from_arrow(batch)
            df = pl.concat([df, batch_df])
        
        print(f"Retrieved {len(df)} records from product hierarchy")
        
        # Prepare the data for insertion into DuckDB
        # Rename columns to match DuckDB schema
        rename_mapping = {
            'demantra_item_skey': 'demantra_item_skey',
            'business_sector': 'business_sector',
            'business_unit': 'business_unit',
            'franchise': 'franchise',
            'product_line': 'product_line',
            'ibp_level_5': 'ibp_level_5',
            'ibp_level_6': 'ibp_level_6',
            'ibp_level_7': 'ibp_level_7',
            'catalog_number': 'catalog_number',
            'uom': 'uom',
            'pack_content': 'pack_content',
        }
        
        # Rename columns that exist in the dataframe
        for old_name, new_name in rename_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})
        
        # Write to DuckDB product_hierarchy table
        if not df.is_empty():
            df_pandas = df.to_pandas()
            
            # Insert the data into the DuckDB table (overwrite existing)
            conn.execute("DELETE FROM da.product_hierarchy")
            conn.register("df_pandas", df_pandas)  # Register the DataFrame as a temporary table
            conn.execute("""
                INSERT INTO da.product_hierarchy 
                (demantra_item_skey, business_sector, business_unit, franchise, product_line, 
                 ibp_level_5, ibp_level_6, ibp_level_7, catalog_number, uom, pack_content, 
                 created_at, updated_at)
                SELECT 
                    demantra_item_skey, business_sector, business_unit, franchise, product_line, 
                    ibp_level_5, ibp_level_6, ibp_level_7, catalog_number, uom, pack_content,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM df_pandas
            """)
            conn.unregister("df_pandas")  # Unregister the temporary table
        
        print(f"Successfully inserted {len(df)} records into da.product_hierarchy table")
        return df
        
    except Exception as e:
        print(f"Error fetching product hierarchy: {e}")
        import traceback
        traceback.print_exc()
        return None


def fetch_and_save_location_hierarchy(user_id: str = "system"):
    """
    Fetch location hierarchy data using arrow_odbc and save to DuckDB da.location_hierarchy table
    """
    # Get database connection
    conn_manager = get_duckdb_connection_manager()
    
    # Create connection if it doesn't exist
    try:
        conn = conn_manager.get_user_connection(user_id)
    except ValueError:
        # Connection doesn't exist, create it
        conn_manager.create_user_connection(user_id)
        conn = conn_manager.get_user_connection(user_id)
    
    query = '''
        SELECT DISTINCT
            [Location_skey] as [location_skey], [SellingDivision] as [selling_division],[COUNTRY_GROUP] 'area',[StrykerGroupRegion] as [stryker_group_region],[Region] [region],[Country] as [country]
                
        FROM [Envision].[DIM_Demantra_CLD_DemantraLocation] l
     '''
    
    # Get server from environment variable or use a default
    server = os.getenv("SERVER_NAME", "gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net")
    database = os.getenv("DATABASE_NAME", "gda_glbsyndb")
    username = os.getenv("DB_USERNAME", "Envisionread")
    password = os.getenv("DB_PASSWORD", "Env!s$@*on")
    
    # Create connection string
    connection_string = (
        f"Driver={{{d[-1] if d else 'ODBC Driver 18 for SQL Server'}}};"
        f"Server={server};database={database};"
        f"UID={username};PWD={password}"
    )
    
    print("Fetching location hierarchy data...")
    try:
        reader = read_arrow_batches_from_odbc(query=query, connection_string=connection_string)
        df = pl.DataFrame()
        
        for batch in reader:
            batch_df = pl.from_arrow(batch)
            df = pl.concat([df, batch_df])
        
        print(f"Retrieved {len(df)} records from location hierarchy")
        
        # Prepare the data for insertion into DuckDB
        # Rename columns to match DuckDB schema
        rename_mapping = {
            'location_skey': 'location_skey',
            'selling_division': 'selling_division',
            'area': 'area',
            'stryker_group_region': 'stryker_group_region',
            'region': 'region',
            'country': 'country',
        }
        
        # Rename columns that exist in the dataframe
        for old_name, new_name in rename_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})
        
        # Write to DuckDB location_hierarchy table
        if not df.is_empty():
            df_pandas = df.to_pandas()
            
            # Insert the data into the DuckDB table (overwrite existing)
            conn.execute("DELETE FROM da.location_hierarchy")
            conn.register("df_pandas", df_pandas)  # Register the DataFrame as a temporary table
            conn.execute("""
                INSERT INTO da.location_hierarchy 
                (location_skey, selling_division, area, stryker_group_region, region, country, 
                 created_at, updated_at)
                SELECT 
                    location_skey, selling_division, area, stryker_group_region, region, country,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                FROM df_pandas
            """)
            conn.unregister("df_pandas")  # Unregister the temporary table
        
        print(f"Successfully inserted {len(df)} records into da.location_hierarchy table")
        return df
        
    except Exception as e:
        print(f"Error fetching location hierarchy: {e}")
        import traceback
        traceback.print_exc()
        return None


def fetch_all_data(user_id: str = "system"):
    """
    Fetch and save all data to DuckDB tables
    """
    print("Fetching and saving all data to DuckDB...")
    
    # Fetch and save sales actuals
    sales_df = fetch_and_save_sales_actuals(user_id)
    
    # Fetch and save product hierarchy
    product_df = fetch_and_save_product_hierarchy(user_id)
    
    # Fetch and save location hierarchy
    location_df = fetch_and_save_location_hierarchy(user_id)
    
    print("All data fetching and saving completed!")
    
    return {
        'sales_actuals': sales_df,
        'product_hierarchy': product_df,
        'location_hierarchy': location_df
    }


def test_connection(server: str = None, database: str = None, username: str = None, password: str = None):
    """
    Test the source database connection
    """
    if server is None:
        server = os.getenv("SERVER_NAME", "gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net")
    if database is None:
        database = os.getenv("DATABASE_NAME", "gda_glbsyndb")
    if username is None:
        username = os.getenv("DB_USERNAME", "Envisionread")
    if password is None:
        password = os.getenv("DB_PASSWORD", "Env!s$@*on")
    
    # Create connection string
    connection_string = (
        f"Driver={{{d[-1] if d else 'ODBC Driver 18 for SQL Server'}}};"
        f"Server={server};database={database};"
        f"UID={username};PWD={password}"
    )
    
    try:
        # Create a simple test query
        test_query = "SELECT 1 as test"
        reader = read_arrow_batches_from_odbc(query=test_query, connection_string=connection_string)
        
        for batch in reader:
            result = pl.from_arrow(batch)
            print(f"Connection test successful: {result}")
            return True
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False