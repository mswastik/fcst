"""
Databricks Service Layer for Forecasting Application
Provides database operations using Databricks SQL Connector
"""
import os
from databricks import sql
from databricks.sdk.core import Config
import polars as pl
import pandas as pd
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta
from pathlib import Path
import json
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class DatabaseService:
    """Main database service for Databricks SQL operations"""

    _instance = None
    _initialized = False

    def __new__(cls, http_path: str = None, host: str = None, client_id: str = None, client_secret: str = None):
        if cls._instance is None:
            cls._instance = super(DatabaseService, cls).__new__(cls)
        return cls._instance

    def __init__(self, http_path: str = None, host: str = None, client_id: str = None, client_secret: str = None):
        if not self._initialized:
            # Use environment variables directly (same as working dashboard.py pattern)
            self.http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/62d47c983bb6df91")
            self.host = host or os.getenv("DATABRICKS_HOST")
            self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID")
            self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET")

            self.conn = None
            self._connection_initialized = False
            DatabaseService._initialized = True
    
    def _ensure_connection(self):
        """Ensure database connection is initialized (lazy initialization)"""
        if not self._connection_initialized:
            try:
                # Use the same authentication pattern as working dashboard.py
                config = Config(
                    host=self.host,
                    client_id=self.client_id,
                    client_secret=self.client_secret
                )

                # Connect using the same pattern as dashboard.py
                self.conn = sql.connect(
                    server_hostname=config.host,
                    http_path=self.http_path,
                    credentials_provider=lambda: config.authenticate
                )

                logger.info("Databricks connection established successfully")
                self._connection_initialized = True

            except Exception as e:
                logger.error(f"Failed to initialize Databricks connection: {e}")
                logger.error(f"Connection details - Host: {self.host}, HTTP Path: {self.http_path}")
                logger.error(f"Make sure environment variables are set: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET")
                raise
    
    def _tables_exist(self) -> bool:
        """Check if main tables exist"""
        try:
            # For Databricks, we'll assume tables exist since they're already created
            # You can implement this check if needed using SHOW TABLES or similar
            return True
        except:
            return False

    def _create_schema(self):
        """Create database schema from SQL file (not needed for Databricks as tables already exist)"""
        logger.info("Using existing Databricks tables - no schema creation needed")

    def _create_basic_schema(self):
        """Create basic schema if SQL file not found (not needed for Databricks)"""
        logger.info("Using existing Databricks tables - no schema creation needed")
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions (Databricks handles transactions automatically)"""
        # Databricks SQL connector handles transactions differently
        # We'll just yield the connection and let Databricks handle commits
        self._ensure_connection()
        try:
            yield self.conn
        except Exception as e:
            logger.error(f"Transaction error: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self._connection_initialized = False
    
    # Product Hierarchy Operations
    def upsert_product_hierarchy(self, df: pl.DataFrame) -> int:
        """Insert or update product hierarchy data (for Databricks, we'll use INSERT OR REPLACE)"""
        self._ensure_connection()
        try:
            # Convert to pandas for Databricks compatibility
            pdf = df.to_pandas()

            with self.transaction():
                with self.conn.cursor() as cursor:
                    # Clear existing data
                    cursor.execute("DELETE FROM da.product_hierarchy")

                    # Insert new data using parameterized query
                    insert_query = """
                    INSERT INTO da.product_hierarchy
                    (demantra_item_skey, business_sector, business_unit, franchise, product_line,
                     ibp_level_5, ibp_level_6, ibp_level_7, catalog_number, uom, pack_content)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """

                    # Convert DataFrame to list of tuples for batch insert
                    data_tuples = [tuple(row) for row in pdf.values]
                    cursor.executemany(insert_query, data_tuples)

                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to upsert product hierarchy: {e}")
            raise
    
    def get_product_hierarchy(self, filters: Dict[str, Any] = None) -> pl.DataFrame:
        """Get product hierarchy with optional filters"""
        self._ensure_connection()
        query = "SELECT * FROM da.product_hierarchy"
        params = []

        if filters:
            conditions = []
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f"{key} = ?")
                    params.append(value)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            df = pl.from_arrow(cursor.fetchall_arrow())
        return df
    
    # Location Hierarchy Operations
    def upsert_location_hierarchy(self, df: pl.DataFrame) -> int:
        """Insert or update location hierarchy data"""
        self._ensure_connection()
        try:
            pdf = df.to_pandas()

            with self.transaction():
                with self.conn.cursor() as cursor:
                    cursor.execute("DELETE FROM da.location_hierarchy")

                    insert_query = """
                    INSERT INTO da.location_hierarchy
                    (location_skey, selling_division, area, stryker_group_region, region, country)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """

                    data_tuples = [tuple(row) for row in pdf.values]
                    cursor.executemany(insert_query, data_tuples)

                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to upsert location hierarchy: {e}")
            raise

    def get_location_hierarchy(self, filters: Dict[str, Any] = None) -> pl.DataFrame:
        """Get location hierarchy with optional filters"""
        self._ensure_connection()
        query = "SELECT * FROM da.location_hierarchy"
        params = []

        if filters:
            conditions = []
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f"{key} = ?")
                    params.append(value)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            df = pl.from_arrow(cursor.fetchall_arrow())
        return df
    
    # Sales Actuals Operations
    def upsert_sales_actuals(self, df: pl.DataFrame) -> int:
        """Insert or update sales actuals data"""
        self._ensure_connection()
        try:
            # Prepare data with proper IDs
            df_prepared = self._prepare_sales_data(df)

            # Filter out rows with missing foreign keys to avoid constraint violations
            df_prepared = df_prepared.filter(
                (pl.col("item_skey").is_not_null()) &
                (pl.col("location_skey").is_not_null())
            )

            if len(df_prepared) == 0:
                logger.warning("No valid sales data to insert after filtering missing keys")
                return 0

            # Convert to pandas for Databricks
            pdf = df_prepared.to_pandas()

            with self.transaction():
                with self.conn.cursor() as cursor:
                    # Use UPSERT logic based on item_skey, location_skey, sales_date
                    delete_query = """
                    DELETE FROM da.sales_actuals
                    WHERE (item_skey, location_skey, sales_date) IN (
                        SELECT ?, ?, ?
                        FROM VALUES (?, ?, ?)
                    )
                    """

                    # First delete existing records
                    for _, row in pdf.iterrows():
                        cursor.execute(delete_query, (
                            row['item_skey'], row['location_skey'], row['sales_date'],
                            row['item_skey'], row['location_skey'], row['sales_date']
                        ))

                    # Insert new records
                    insert_query = """
                    INSERT INTO da.sales_actuals
                    (id, item_skey, location_skey, sales_date, asp_final_rev, act_orders_rev, act_orders_rev_val,
                     fcst_df_final_rev, l0_df_final_rev, l1_df_final_rev, l2_df_final_rev, fcst_df_final_rev_val,
                     fcst_stat_prelim_rev, fcst_stat_final_rev, l0_stat_final_rev, l1_stat_final_rev, l2_stat_final_rev)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """

                    data_tuples = [tuple(row) for row in pdf.values]
                    cursor.executemany(insert_query, data_tuples)

                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to upsert sales actuals: {e}")
            raise
    
    def _prepare_sales_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Prepare sales data with proper IDs and structure"""
        # Generate unique row IDs
        df = df.with_row_index("id")
        
        # Select and rename columns to match schema (based on sql.py sales_actuals function)
        columns_map = {
            'item_skey': 'item_skey',
            'Location_skey': 'location_skey',
            'SALES_DATE': 'sales_date',
            'ASP Final Rev': 'asp_final_rev',
            'Act Orders Rev': 'act_orders_rev',
            'Act Orders Rev Val': 'act_orders_rev_val',
            'Fcst DF Final Rev': 'fcst_df_final_rev',
            'L0 DF Final Rev': 'l0_df_final_rev',
            'L1 DF Final Rev': 'l1_df_final_rev',
            'L2 DF Final Rev': 'l2_df_final_rev',
            'Fcst DF Final Rev Val': 'fcst_df_final_rev_val',
            'Fcst Stat Prelim Rev': 'fcst_stat_prelim_rev',
            'Fcst Stat Final Rev': 'fcst_stat_final_rev',
            'L0 Stat Final Rev': 'l0_stat_final_rev',
            'L1 Stat Final Rev': 'l1_stat_final_rev',
            'L2 Stat Final Rev': 'l2_stat_final_rev'
        }
        
        # Select available columns
        select_cols = ['id']
        for old_col, new_col in columns_map.items():
            if old_col in df.columns:
                select_cols.append(old_col)
        
        df_selected = df.select(select_cols)
        
        # Rename columns
        for old_col, new_col in columns_map.items():
            if old_col in df_selected.columns:
                df_selected = df_selected.rename({old_col: new_col})
        
        return df_selected
    
    def get_cluster_count(self) -> int:
        """Get total number of cluster records in database"""
        self._ensure_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM product_clusters
                """)
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Could not get cluster count: {e}")
            return 0

    def get_forecast_count(self) -> int:
        """Get total number of forecast records in database"""
        self._ensure_connection()
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM forecasts
                """)
                result = cursor.fetchone()
                return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Could not get forecast count: {e}")
            return 0
    
    def get_sales_actuals(self,
                         item_skeys: List[int] = None,
                         location_skeys: List[int] = None,
                         date_range: Tuple[datetime, datetime] = None,
                         limit: int = None) -> pl.DataFrame:
        """Get sales actuals with flexible filtering"""
        self._ensure_connection()

        query = """
        SELECT sa.*, ph.catalog_number, ph.franchise, ph.ibp_level_5, ph.ibp_level_6,
               ph.business_sector, ph.business_unit, ph.product_line, ph.ibp_level_7,
               lh.country, lh.region, lh.area, lh.stryker_group_region, lh.selling_division
        FROM da.sales_actuals sa
        LEFT JOIN da.product_hierarchy ph ON sa.item_skey = ph.demantra_item_skey
        LEFT JOIN da.location_hierarchy lh ON sa.location_skey = lh.location_skey
        """

        conditions = []
        params = []

        if item_skeys:
            placeholders = ','.join(['?' for _ in item_skeys])
            conditions.append(f"sa.item_skey IN ({placeholders})")
            params.extend(item_skeys)

        if location_skeys:
            placeholders = ','.join(['?' for _ in location_skeys])
            conditions.append(f"sa.location_skey IN ({placeholders})")
            params.extend(location_skeys)

        if date_range:
            conditions.append("sa.sales_date BETWEEN ? AND ?")
            params.extend(date_range)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY sa.sales_date, sa.item_skey, sa.location_skey"

        if limit:
            query += f" LIMIT {limit}"

        with self.conn.cursor() as cursor:
            cursor.execute(query, params)
            df = pl.from_arrow(cursor.fetchall_arrow())

        # Rename columns to match expected format
        df = df.rename({
            'act_orders_rev': 'Act Orders Rev',
            'fcst_stat_prelim_rev': 'Fcst Stat Prelim Rev',
            'fcst_stat_final_rev': 'Fcst Stat Final Rev',
            'l2_stat_final_rev': 'L2 Stat Final Rev',
            'fcst_df_final_rev': 'Fcst DF Final Rev',
            'l2_df_final_rev': 'L2 DF Final Rev',
            'sales_date': 'SALES_DATE',
            'catalog_number': 'CatalogNumber',
            'region': 'Region',
            'country': 'Country',
            'area': 'Area',
            'business_unit': 'Business Unit',
            'franchise': 'Franchise',
            'ibp_level_5': 'IBP Level 5',
            'ibp_level_6': 'IBP Level 6'
        }, strict=False)

        return df
    
    # Cluster Operations
    def upsert_clusters(self, df: pl.DataFrame) -> int:
        """Insert or update cluster assignments"""
        self._ensure_connection()
        try:
            # Prepare cluster data
            cluster_data = self._prepare_cluster_data(df)
            pdf = cluster_data.to_pandas()

            with self.transaction():
                with self.conn.cursor() as cursor:
                    # Delete existing clusters for these product-location combinations
                    delete_query = """
                        DELETE FROM da.product_clusters
                        WHERE (item_skey, location_skey) IN (
                            SELECT ?, ?
                            FROM VALUES (?, ?)
                        )
                    """

                    for _, row in pdf.iterrows():
                        cursor.execute(delete_query, (
                            row['item_skey'], row['location_skey'],
                            row['item_skey'], row['location_skey']
                        ))

                    # Insert new clusters
                    insert_query = """
                        INSERT INTO da.product_clusters
                        (cluster_id, item_skey, location_skey, cluster_number, cluster_features)
                        VALUES (?, ?, ?, ?, ?)
                    """

                    data_tuples = [tuple(row) for row in pdf.values]
                    cursor.executemany(insert_query, data_tuples)

                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to upsert clusters: {e}")
            raise
        """Prepare cluster data for database insertion"""
        # Ensure we have item_skey and location_skey
        if 'item_skey' not in df.columns:
            # If we have the old format, create mapping
            if 'Country' in df.columns and 'CatalogNumber' in df.columns:
                # This would need proper mapping - for now use placeholder
                df = df.with_columns(
                    item_skey=pl.lit(0).cast(pl.Int64)  # Placeholder - needs proper mapping
                )
        
        if 'location_skey' not in df.columns:
            if 'Country' in df.columns:
                # This would need proper mapping - for now use placeholder
                df = df.with_columns(
                    location_skey=pl.lit(0).cast(pl.Int64)  # Placeholder - needs proper mapping
                )
        
        # Create cluster_id
        df = df.with_columns(
            cluster_id=pl.col('item_skey').cast(pl.Utf8) + '_' + pl.col('location_skey').cast(pl.Utf8)
        )
        
        # Select required columns
        cluster_cols = ['cluster_id', 'item_skey', 'location_skey']
        
        if 'cluster' in df.columns:
            cluster_cols.append('cluster')
            df = df.rename({'cluster': 'cluster_number'})
        else:
            df = df.with_columns(cluster_number=pl.lit(0))
            cluster_cols.append('cluster_number')
        
        # Add empty cluster_features for now
        df = df.with_columns(cluster_features=pl.lit(None))
        cluster_cols.append('cluster_features')
        
        return df.select(cluster_cols).unique()
    
    def get_clusters(self) -> pl.DataFrame:
        """Get all cluster assignments"""
        self._ensure_connection()
        query = """
        SELECT pc.*, ph.catalog_number, ph.franchise, lh.country, lh.region
        FROM da.product_clusters pc
        LEFT JOIN da.product_hierarchy ph ON pc.item_skey = ph.demantra_item_skey
        LEFT JOIN da.location_hierarchy lh ON pc.location_skey = lh.location_skey
        """

        with self.conn.cursor() as cursor:
            cursor.execute(query)
            df = pl.from_arrow(cursor.fetchall_arrow())
        return df
    
    # Forecast Operations
    def insert_forecasts(self, df: pl.DataFrame, model_type: str = "NHITS") -> int:
        """Insert forecast results"""
        self._ensure_connection()
        try:
            forecast_data = self._prepare_forecast_data(df, model_type)
            pdf = forecast_data.to_pandas()

            with self.transaction():
                with self.conn.cursor() as cursor:
                    insert_query = """
                    INSERT INTO da.forecasts
                    (forecast_id, item_skey, location_skey, forecast_date,
                     forecast_horizon, model_type, forecast_value)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """

                    data_tuples = [tuple(row) for row in pdf.values]
                    cursor.executemany(insert_query, data_tuples)

                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to insert forecasts: {e}")
            raise
    
    def _prepare_forecast_data(self, df: pl.DataFrame, model_type: str) -> pl.DataFrame:
        """Prepare forecast data for database insertion"""
        try:
            # Handle EnsembleForecaster output format
            # Expected input columns from EnsembleForecaster: ['unique_id', 'ds', 'ensemble', 'cluster']
            # Expected database columns: ['forecast_id', 'item_skey', 'location_skey', 'forecast_date', 'forecast_horizon', 'model_type', 'forecast_value']
            
            # Add required database columns with proper mapping
            df = df.with_columns(
                model_type=pl.lit(model_type),
                forecast_horizon=pl.lit(60)  # Default 60 months horizon
            ).with_row_index("forecast_id")
            
            # Map forecast columns to database schema
            if 'ds' in df.columns:
                df = df.with_columns(forecast_date=pl.col('ds').cast(pl.Date))
            else:
                df = df.with_columns(forecast_date=pl.lit(None).cast(pl.Date))
            
            if 'ensemble' in df.columns:
                df = df.with_columns(forecast_value=pl.col('ensemble').cast(pl.Float64))
            elif 'NHITS' in df.columns:
                df = df.with_columns(forecast_value=pl.col('NHITS').cast(pl.Float64))
            elif 'AutoARIMA' in df.columns:
                df = df.with_columns(forecast_value=pl.col('AutoARIMA').cast(pl.Float64))
            else:
                df = df.with_columns(forecast_value=pl.lit(0.0).cast(pl.Float64))
            
            # Extract item_skey and location_skey from unique_id if present
            if 'unique_id' in df.columns:
                # Split unique_id format: "Country,CatalogNumber"
                df = df.with_columns(
                    country_code=pl.col('unique_id').str.split(',').list.get(0),
                    catalog_number=pl.col('unique_id').str.split(',').list.get(1)
                )
                
                # Set placeholder values for foreign keys (these would need proper mapping in production)
                df = df.with_columns(
                    item_skey=pl.lit(1).cast(pl.Int64),  # Placeholder - needs proper mapping
                    location_skey=pl.lit(1).cast(pl.Int64)  # Placeholder - needs proper mapping
                )
            else:
                # Default placeholder values
                df = df.with_columns(
                    item_skey=pl.lit(1).cast(pl.Int64),
                    location_skey=pl.lit(1).cast(pl.Int64)
                )
            
            # Select only the required columns for database insertion
            required_cols = ['forecast_id', 'item_skey', 'location_skey', 'forecast_date', 
                           'forecast_horizon', 'model_type', 'forecast_value']
            
            # Filter to only include existing columns
            available_cols = [col for col in required_cols if col in df.columns]
            df = df.select(available_cols)
            
            # Add missing required columns with default values
            for col in required_cols:
                if col not in df.columns:
                    if col in ['forecast_id', 'item_skey', 'location_skey', 'forecast_horizon']:
                        df = df.with_columns(pl.lit(1).cast(pl.Int64).alias(col))
                    elif col == 'forecast_value':
                        df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                    elif col == 'model_type':
                        df = df.with_columns(pl.lit(model_type).alias(col))
                    elif col == 'forecast_date':
                        df = df.with_columns(pl.lit(None).cast(pl.Date).alias(col))
            
            return df
            
        except Exception as e:
            print(f"Error preparing forecast data: {e}")
            # Return a minimal dataframe to prevent complete failure
            return pl.DataFrame({
                'forecast_id': [1],
                'item_skey': [1],
                'location_skey': [1], 
                'forecast_date': [None],
                'forecast_horizon': [60],
                'model_type': [model_type],
                'forecast_value': [0.0]
            })
    
    # Utility Methods
    def get_filter_options(self) -> Dict[str, List[str]]:
        """Get available filter options for UI"""
        self._ensure_connection()
        try:
            # Get unique products
            products_query = """
            SELECT DISTINCT catalog_number, franchise, ibp_level_5, ibp_level_6
            FROM da.product_hierarchy
            WHERE catalog_number IS NOT NULL
            ORDER BY catalog_number
            """
            with self.conn.cursor() as cursor:
                cursor.execute(products_query)
                products_df = pl.from_arrow(cursor.fetchall_arrow())

            # Get unique locations
            locations_query = """
            SELECT DISTINCT country, region, area
            FROM da.location_hierarchy
            WHERE country IS NOT NULL
            ORDER BY country
            """
            with self.conn.cursor() as cursor:
                cursor.execute(locations_query)
                locations_df = pl.from_arrow(cursor.fetchall_arrow())

            # Extract unique values, filtering out nulls
            catalog_numbers = [x for x in products_df['catalog_number'].unique().to_list() if x is not None]
            franchises = [x for x in products_df['franchise'].unique().to_list() if x is not None]
            ibp_level_5s = [x for x in products_df['ibp_level_5'].unique().to_list() if x is not None]
            ibp_level_6s = [x for x in products_df['ibp_level_6'].unique().to_list() if x is not None]

            countries = [x for x in locations_df['country'].unique().to_list() if x is not None]
            regions = [x for x in locations_df['region'].unique().to_list() if x is not None]
            areas = [x for x in locations_df['area'].unique().to_list() if x is not None]

            return {
                'products': products_df.to_dicts(),
                'locations': locations_df.to_dicts(),
                'catalog_numbers': catalog_numbers,
                'countries': countries,
                'franchises': franchises,
                'regions': regions,
                'areas': areas,
                'ibp_level_5s': ibp_level_5s,
                'ibp_level_6s': ibp_level_6s
            }
        except Exception as e:
            logger.error(f"Failed to get filter options: {e}")
            return {}
    
    def estimate_filtered_data_size(self, location_col: str = None, location_val: str = None,
                                   product_col: str = None, product_val: str = None) -> int:
        """Estimate the number of rows that would be returned with given filters"""
        self._ensure_connection()
        try:
            # Build WHERE conditions
            where_conditions = []
            params = []

            # Map display names to database column names
            column_mapping = {
                'Region': 'region',
                'Country': 'country',
                'Area': 'area',
                'Franchise': 'franchise',
                'IBP Level 5': 'ibp_level_5',
                'IBP Level 6': 'ibp_level_6',
                'CatalogNumber': 'catalog_number'
            }

            if location_col and location_val:
                db_location_col = column_mapping.get(location_col, location_col.lower().replace(' ', '_'))
                where_conditions.append(f"{db_location_col} = ?")
                params.append(location_val)

            if product_col and product_val:
                db_product_col = column_mapping.get(product_col, product_col.lower().replace(' ', '_'))
                where_conditions.append(f"{db_product_col} = ?")
                params.append(product_val)

            where_clause = " AND ".join(where_conditions) if where_conditions else ""

            # Count query with filters
            count_query = f"""
            SELECT COUNT(*) as count
            FROM da.sales_actuals sa
            JOIN da.product_hierarchy ph ON sa.item_skey = ph.demantra_item_skey
            JOIN da.location_hierarchy lh ON sa.location_skey = lh.location_skey
            {"WHERE " + where_clause if where_clause else ""}
            """

            with self.conn.cursor() as cursor:
                if where_clause:
                    cursor.execute(count_query, params)
                else:
                    cursor.execute(count_query)
                result = cursor.fetchone()

            return result[0] if result else 0

        except Exception as e:
            logger.error(f"Failed to estimate data size: {e}")
            # Return a conservative estimate if estimation fails
            return 1000000

    def get_filtered_sales_actuals(self, location_col: str = None, location_val: str = None,
                                  product_col: str = None, product_val: str = None) -> pl.DataFrame:
        """Get sales actuals data with filters applied at database level"""
        self._ensure_connection()
        try:
            # Build WHERE conditions
            where_conditions = []
            params = []

            # Map display names to database column names
            column_mapping = {
                'Region': 'region',
                'Country': 'country',
                'Area': 'area',
                'Franchise': 'franchise',
                'IBP Level 5': 'ibp_level_5',
                'IBP Level 6': 'ibp_level_6',
                'CatalogNumber': 'catalog_number'
            }

            if location_col and location_val:
                db_location_col = column_mapping.get(location_col, location_col.lower().replace(' ', '_'))
                where_conditions.append(f"lh.{db_location_col} = ?")
                params.append(location_val)

            if product_col and product_val:
                db_product_col = column_mapping.get(product_col, product_col.lower().replace(' ', '_'))
                where_conditions.append(f"ph.{db_product_col} = ?")
                params.append(product_val)

            where_clause = " AND ".join(where_conditions) if where_conditions else ""

            # Query with filters applied at database level
            query = f"""
            SELECT
                sa.sales_date,
                sa.act_orders_rev,
                sa.fcst_stat_prelim_rev,
                sa.fcst_stat_final_rev,
                sa.l2_stat_final_rev,
                sa.fcst_df_final_rev,
                sa.l2_df_final_rev,
                sa.act_orders_rev_val,
                sa.l1_df_final_rev,
                sa.l0_df_final_rev,
                sa.fcst_df_final_rev_val,

                -- Location fields
                lh.country,
                lh.region,
                lh.area,
                lh.selling_division,
                lh.stryker_group_region,

                -- Product fields
                ph.catalog_number,
                ph.business_sector,
                ph.business_unit,
                ph.franchise,
                ph.product_line,
                ph.ibp_level_5,
                ph.ibp_level_6,
                ph.ibp_level_7,
                ph.uom,
                ph.pack_content
            FROM da.sales_actuals sa
            JOIN da.product_hierarchy ph ON sa.item_skey = ph.demantra_item_skey
            JOIN da.location_hierarchy lh ON sa.location_skey = lh.location_skey
            {"WHERE " + where_clause if where_clause else ""}
            ORDER BY sa.sales_date
            """

            with self.conn.cursor() as cursor:
                if where_clause:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                df = pl.from_arrow(cursor.fetchall_arrow())

            return df

        except Exception as e:
            logger.error(f"Failed to get filtered sales actuals: {e}")
            return pl.DataFrame()


# Migration utilities
class DataMigrator:
    """Utility class for migrating from parquet files to DuckDB"""
    
    def __init__(self, db_service: DatabaseService):
        self.db_service = db_service
    
    def migrate_from_parquet(self, data_dir: str = "data") -> Dict[str, int]:
        """Migrate all data from parquet files to DuckDB"""
        results = {}
        data_path = Path(data_dir)
        
        try:
            # Migrate product hierarchy
            phierarchy_path = data_path / "phierarchy.parquet"
            if phierarchy_path.exists():
                ph_df = pl.read_parquet(str(phierarchy_path))
                ph_df = self._prepare_product_hierarchy(ph_df)
                results['product_hierarchy'] = self.db_service.upsert_product_hierarchy(ph_df)
            
            # Migrate location hierarchy
            lhierarchy_path = data_path / "lhierarchy.parquet"
            if lhierarchy_path.exists():
                lh_df = pl.read_parquet(str(lhierarchy_path))
                lh_df = self._prepare_location_hierarchy(lh_df)
                results['location_hierarchy'] = self.db_service.upsert_location_hierarchy(lh_df)
            
            # Migrate main data files
            for parquet_file in data_path.glob("*.parquet"):
                if parquet_file.name not in ["phierarchy.parquet", "lhierarchy.parquet"]:
                    try:
                        df = pl.read_parquet(str(parquet_file))
                        
                        # Migrate sales actuals
                        sales_count = self.db_service.upsert_sales_actuals(df)
                        results[f'sales_actuals_{parquet_file.stem}'] = sales_count
                        
                        # Migrate clusters if present
                        if 'cluster' in df.columns:
                            cluster_count = self.db_service.upsert_clusters(df)
                            results[f'clusters_{parquet_file.stem}'] = cluster_count
                            
                    except Exception as e:
                        logger.error(f"Failed to migrate {parquet_file}: {e}")
                        results[f'error_{parquet_file.stem}'] = str(e)
            
            return results
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            raise
    
    def _prepare_product_hierarchy(self, df: pl.DataFrame) -> pl.DataFrame:
        """Prepare product hierarchy data for database (based on phierarchy function in sql.py)"""
        # Column mapping from source to target
        columns_map = {
            'demantra_item_skey': 'demantra_item_skey',
            'Business Sector': 'business_sector',
            'Business Unit': 'business_unit',
            'Franchise': 'franchise',
            'Product Line': 'product_line',
            'IBP Level 5': 'ibp_level_5',
            'IBP Level 6': 'ibp_level_6',
            'IBP Level 7': 'ibp_level_7',
            'CatalogNumber': 'catalog_number',
            'UOM': 'uom',
            'Pack Content': 'pack_content'
        }
        
        # Start with empty dataframe with all required columns
        result_df = pl.DataFrame()
        
        # Process each column, adding NULL if missing
        for old_col, new_col in columns_map.items():
            if old_col in df.columns:
                if len(result_df) == 0:
                    result_df = df.select(old_col).rename({old_col: new_col})
                else:
                    temp_df = df.select(old_col).rename({old_col: new_col})
                    result_df = result_df.with_columns(temp_df[new_col])
            else:
                # Add NULL column if missing
                if len(result_df) == 0:
                    result_df = pl.DataFrame({new_col: [None] * len(df)})
                else:
                    result_df = result_df.with_columns(pl.lit(None).alias(new_col))
        
        # If we still have empty result, create from original df
        if len(result_df) == 0 and len(df) > 0:
            result_df = pl.DataFrame({
                'demantra_item_skey': [None] * len(df),
                'business_sector': [None] * len(df),
                'business_unit': [None] * len(df),
                'franchise': [None] * len(df),
                'product_line': [None] * len(df),
                'ibp_level_5': [None] * len(df),
                'ibp_level_6': [None] * len(df),
                'ibp_level_7': [None] * len(df),
                'catalog_number': [None] * len(df),
                'uom': [None] * len(df),
                'pack_content': [None] * len(df)
            })
        
        return result_df.unique()
    
    def _prepare_location_hierarchy(self, df: pl.DataFrame) -> pl.DataFrame:
        """Prepare location hierarchy data for database (based on lhierarchy function in sql.py)"""
        # Column mapping from source to target
        columns_map = {
            'Location_skey': 'location_skey',
            'Selling Division': 'selling_division',
            'Area': 'area',
            'Stryker Group Region': 'stryker_group_region',
            'Region': 'region',
            'Country': 'country'
        }
        
        # Start with empty dataframe with all required columns
        result_df = pl.DataFrame()
        
        # Process each column, adding NULL if missing
        for old_col, new_col in columns_map.items():
            if old_col in df.columns:
                if len(result_df) == 0:
                    result_df = df.select(old_col).rename({old_col: new_col})
                else:
                    temp_df = df.select(old_col).rename({old_col: new_col})
                    result_df = result_df.with_columns(temp_df[new_col])
            else:
                # Add NULL column if missing
                if len(result_df) == 0:
                    result_df = pl.DataFrame({new_col: [None] * len(df)})
                else:
                    result_df = result_df.with_columns(pl.lit(None).alias(new_col))
        
        # If we still have empty result, create from original df
        if len(result_df) == 0 and len(df) > 0:
            result_df = pl.DataFrame({
                'location_skey': [None] * len(df),
                'selling_division': [None] * len(df),
                'area': [None] * len(df),
                'stryker_group_region': [None] * len(df),
                'region': [None] * len(df),
                'country': [None] * len(df)
            })
        
        return result_df.unique()


# Global database service instance
_db_service = None

def get_database_service() -> DatabaseService:
    """Get global database service instance"""
    global _db_service
    if _db_service is None:
        _db_service = DatabaseService()
    return _db_service

def close_database_service():
    """Close global database service"""
    global _db_service
    if _db_service:
        _db_service.close()
        _db_service = None
