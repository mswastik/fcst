"""
DuckDB Service Layer for Forecasting Application
Provides database operations and replaces parquet file operations
"""
import duckdb
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
    """Main database service for DuckDB operations"""
    
    _instance = None
    _initialized = False
    
    def __new__(cls, db_path: str = "forecasting.duckdb"):
        if cls._instance is None:
            cls._instance = super(DatabaseService, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, db_path: str = "forecasting.duckdb"):
        if not self._initialized:
            self.db_path = db_path
            self.conn = None
            self._connection_initialized = False
            DatabaseService._initialized = True
    
    def _ensure_connection(self):
        """Ensure database connection is initialized (lazy initialization)"""
        if not self._connection_initialized:
            try:
                self.conn = duckdb.connect(self.db_path)
                
                # Check if tables exist, if not create them
                if not self._tables_exist():
                    self._create_schema()
                    logger.info("Database schema created successfully")
                else:
                    logger.info("Database schema already exists")
                
                self._connection_initialized = True
            except Exception as e:
                logger.error(f"Failed to initialize database: {e}")
                raise
    
    def _tables_exist(self) -> bool:
        """Check if main tables exist"""
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) as table_count 
                FROM information_schema.tables 
                WHERE table_name IN ('product_hierarchy', 'location_hierarchy', 'sales_actuals')
            """).fetchone()
            return result[0] == 3
        except:
            return False
    
    def _create_schema(self):
        """Create database schema from SQL file"""
        schema_path = Path(__file__).parent / "database_schema.sql"
        if schema_path.exists():
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            self.conn.execute(schema_sql)
        else:
            # Fallback: create basic schema
            self._create_basic_schema()
    
    def _create_basic_schema(self):
        """Create basic schema if SQL file not found"""
        schema_sql = """
        CREATE TABLE IF NOT EXISTS product_hierarchy (
            demantra_item_skey BIGINT PRIMARY KEY,
            business_sector VARCHAR,
            business_unit VARCHAR,
            franchise VARCHAR,
            product_line VARCHAR,
            ibp_level_5 VARCHAR,
            ibp_level_6 VARCHAR,
            ibp_level_7 VARCHAR,
            catalog_number VARCHAR NOT NULL,
            uom VARCHAR,
            pack_content VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS location_hierarchy (
            location_skey BIGINT PRIMARY KEY,
            selling_division VARCHAR,
            area VARCHAR,
            stryker_group_region VARCHAR,
            region VARCHAR,
            country VARCHAR NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS sales_actuals (
            id BIGINT PRIMARY KEY,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS product_clusters (
            cluster_id VARCHAR PRIMARY KEY,
            item_skey BIGINT NOT NULL,
            location_skey BIGINT NOT NULL,
            cluster_number INTEGER NOT NULL,
            cluster_features JSON,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(item_skey, location_skey)
        );
        
        CREATE TABLE IF NOT EXISTS forecasts (
            forecast_id BIGINT PRIMARY KEY,
            item_skey BIGINT NOT NULL,
            location_skey BIGINT NOT NULL,
            forecast_date DATE NOT NULL,
            forecast_horizon INTEGER NOT NULL,
            model_type VARCHAR NOT NULL,
            forecast_value DECIMAL(15,2) NOT NULL,
            confidence_lower DECIMAL(15,2),
            confidence_upper DECIMAL(15,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        self.conn.execute(schema_sql)
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions"""
        try:
            self.conn.execute("BEGIN TRANSACTION")
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Transaction rolled back: {e}")
            raise
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    # Product Hierarchy Operations
    def upsert_product_hierarchy(self, df: pl.DataFrame) -> int:
        """Insert or update product hierarchy data"""
        self._ensure_connection()
        try:
            # Convert to pandas for DuckDB compatibility
            pdf = df.to_pandas()
            
            with self.transaction():
                # Clear existing data
                self.conn.execute("DELETE FROM product_hierarchy")
                
                # Insert new data
                self.conn.execute("""
                    INSERT INTO product_hierarchy 
                    (demantra_item_skey, business_sector, business_unit, franchise, product_line,
                     ibp_level_5, ibp_level_6, ibp_level_7, catalog_number, uom, pack_content)
                    SELECT * FROM pdf
                """)
                
                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to upsert product hierarchy: {e}")
            raise
    
    def get_product_hierarchy(self, filters: Dict[str, Any] = None) -> pl.DataFrame:
        """Get product hierarchy with optional filters"""
        self._ensure_connection()
        query = "SELECT * FROM product_hierarchy"
        params = []
        
        if filters:
            conditions = []
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f"{key} = ?")
                    params.append(value)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        result = self.conn.execute(query, params).fetchdf()
        return pl.from_pandas(result)
    
    # Location Hierarchy Operations
    def upsert_location_hierarchy(self, df: pl.DataFrame) -> int:
        """Insert or update location hierarchy data"""
        self._ensure_connection()
        try:
            pdf = df.to_pandas()
            
            with self.transaction():
                self.conn.execute("DELETE FROM location_hierarchy")
                self.conn.execute("""
                    INSERT INTO location_hierarchy 
                    (location_skey, selling_division, area, stryker_group_region, region, country)
                    SELECT * FROM pdf
                """)
                
                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to upsert location hierarchy: {e}")
            raise
    
    def get_location_hierarchy(self, filters: Dict[str, Any] = None) -> pl.DataFrame:
        """Get location hierarchy with optional filters"""
        self._ensure_connection()
        query = "SELECT * FROM location_hierarchy"
        params = []
        
        if filters:
            conditions = []
            for key, value in filters.items():
                if value is not None:
                    conditions.append(f"{key} = ?")
                    params.append(value)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        result = self.conn.execute(query, params).fetchdf()
        return pl.from_pandas(result)
    
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
            
            #pdf = df_prepared.to_pandas()
            
            with self.transaction():
                # Use UPSERT logic based on item_skey, location_skey, sales_date
                self.conn.execute("""
                    DELETE FROM sales_actuals 
                    WHERE (item_skey, location_skey, sales_date) IN (
                        SELECT item_skey, location_skey, sales_date FROM df_prepared
                    )
                """)
                
                self.conn.execute("""
                    INSERT INTO sales_actuals 
                    (id, item_skey, location_skey, sales_date, asp_final_rev, act_orders_rev, act_orders_rev_val,
                     fcst_df_final_rev, l0_df_final_rev, l1_df_final_rev, l2_df_final_rev, fcst_df_final_rev_val,
                     fcst_stat_prelim_rev, fcst_stat_final_rev, l0_stat_final_rev, l1_stat_final_rev, l2_stat_final_rev)
                    SELECT * FROM df_prepared
                """)
                
                return len(df_prepared)
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
            result = self.conn.execute("""
                SELECT COUNT(*) as count 
                FROM product_clusters
            """).fetchone()
            return result[0] if result else 0
        except Exception as e:
            logger.warning(f"Could not get cluster count: {e}")
            return 0
    
    def get_forecast_count(self) -> int:
        """Get total number of forecast records in database"""
        self._ensure_connection()
        try:
            result = self.conn.execute("""
                SELECT COUNT(*) as count 
                FROM forecasts
            """).fetchone()
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
        FROM sales_actuals sa
        LEFT JOIN product_hierarchy ph ON sa.item_skey = ph.demantra_item_skey
        LEFT JOIN location_hierarchy lh ON sa.location_skey = lh.location_skey
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
        
        result = self.conn.execute(query, params).pl() #.fetchdf()
        result = result.rename({
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
                },strict=False)
        return result
    
    # Cluster Operations
    def upsert_clusters(self, df: pl.DataFrame) -> int:
        """Insert or update cluster assignments"""
        self._ensure_connection()
        try:
            # Prepare cluster data
            cluster_data = self._prepare_cluster_data(df)
            pdf = cluster_data.to_pandas()
            
            with self.transaction():
                # Delete existing clusters for these product-location combinations
                self.conn.execute("""
                    DELETE FROM product_clusters 
                    WHERE (item_skey, location_skey) IN (
                        SELECT item_skey, location_skey FROM pdf
                    )
                """)
                
                # Insert new clusters
                self.conn.execute("""
                    INSERT INTO product_clusters 
                    (cluster_id, item_skey, location_skey, cluster_number, cluster_features)
                    SELECT * FROM pdf
                """)
                
                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to upsert clusters: {e}")
            raise
    
    def _prepare_cluster_data(self, df: pl.DataFrame) -> pl.DataFrame:
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
        FROM product_clusters pc
        LEFT JOIN product_hierarchy ph ON pc.item_skey = ph.demantra_item_skey
        LEFT JOIN location_hierarchy lh ON pc.location_skey = lh.location_skey
        """
        
        result = self.conn.execute(query).pl() #.fetchdf()
        return result
    
    # Forecast Operations
    def insert_forecasts(self, df: pl.DataFrame, model_type: str = "NHITS") -> int:
        """Insert forecast results"""
        self._ensure_connection()
        try:
            forecast_data = self._prepare_forecast_data(df, model_type)
            pdf = forecast_data.to_pandas()
            
            with self.transaction():
                self.conn.execute("""
                    INSERT INTO forecasts 
                    (forecast_id, item_skey, location_skey, forecast_date, 
                     forecast_horizon, model_type, forecast_value)
                    SELECT * FROM pdf
                """)
                
                return len(pdf)
        except Exception as e:
            logger.error(f"Failed to insert forecasts: {e}")
            raise
    
    def _prepare_forecast_data(self, df: pl.DataFrame, model_type: str) -> pl.DataFrame:
        """Prepare forecast data for database insertion"""
        # Implementation depends on forecast data structure
        # This is a placeholder - adjust based on actual forecast format
        df = df.with_columns(
            model_type=pl.lit(model_type),
            forecast_horizon=pl.lit(1)
        ).with_row_index("forecast_id")
        
        # Ensure we have the required keys
        if 'item_skey' not in df.columns:
            df = df.with_columns(item_skey=pl.lit(0).cast(pl.Int64))
        if 'location_skey' not in df.columns:
            df = df.with_columns(location_skey=pl.lit(0).cast(pl.Int64))
        if 'forecast_date' not in df.columns:
            df = df.with_columns(forecast_date=pl.lit(None).cast(pl.Date))
        if 'forecast_value' not in df.columns:
            df = df.with_columns(forecast_value=pl.lit(0.0).cast(pl.Float64))
            
        return df
    
    # Utility Methods
    def get_filter_options(self) -> Dict[str, List[str]]:
        """Get available filter options for UI"""
        self._ensure_connection()
        try:
            # Get unique products
            products_query = """
            SELECT DISTINCT catalog_number, franchise, ibp_level_5, ibp_level_6
            FROM product_hierarchy 
            WHERE catalog_number IS NOT NULL
            ORDER BY catalog_number
            """
            products_df = pl.from_pandas(self.conn.execute(products_query).fetchdf())
            
            # Get unique locations
            locations_query = """
            SELECT DISTINCT country, region, area
            FROM location_hierarchy 
            WHERE country IS NOT NULL
            ORDER BY country
            """
            locations_df = pl.from_pandas(self.conn.execute(locations_query).fetchdf())
            
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
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics for the database"""
        self._ensure_connection()
        try:
            stats = {}
            
            # Count records in each table
            tables = ['product_hierarchy', 'location_hierarchy', 'sales_actuals', 
                     'product_clusters', 'forecasts']
            
            for table in tables:
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                result = self.conn.execute(count_query).fetchone()
                stats[f"{table}_count"] = result[0] if result else 0
            
            # Date range of sales data
            date_query = """
            SELECT MIN(sales_date) as min_date, MAX(sales_date) as max_date
            FROM sales_actuals
            """
            date_result = self.conn.execute(date_query).fetchone()
            if date_result:
                stats['data_start_date'] = date_result[0]
                stats['data_end_date'] = date_result[1]
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get summary stats: {e}")
            return {}


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
            'Business_Sector': 'business_sector',
            'Business_Unit': 'business_unit',
            'Franchise': 'franchise',
            'Product Line': 'product_line',
            'IBP_Level_5': 'ibp_level_5',
            'IBP_Level_6': 'ibp_level_6',
            'IBP_Level_7': 'ibp_level_7',
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
