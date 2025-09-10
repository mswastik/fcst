"""
Enhanced Database Service with Multi-User Support
Supports multiple concurrent users with separate database connections
"""
import os
import threading
import time
import logging
import uuid
from typing import Optional, List, Dict, Any, Tuple, Callable
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import polars as pl

# Import the updated multi-user connection manager
from .databricks_connection_manager import get_databricks_connection_manager

logger = logging.getLogger(__name__)

class EnhancedDatabaseService:
    """Enhanced database service with multi-user support"""

    def __init__(self):
        self.connection_manager = get_databricks_connection_manager()
        logger.info("Enhanced Database Service initialized with multi-user support")

        # Define schema mapping for consistent DataFrame creation
        self.schema_mapping = {
            # String types
            'VARCHAR': pl.Utf8,
            'NVARCHAR': pl.Utf8,
            'TEXT': pl.Utf8,
            'STRING': pl.Utf8,

            # Numeric types
            'INT': pl.Int64,
            'BIGINT': pl.Int64,
            'SMALLINT': pl.Int32,
            'TINYINT': pl.Int16,

            # Decimal types (handle as Float64 for consistency)
            'DECIMAL': pl.Float64,
            'NUMERIC': pl.Float64,
            'MONEY': pl.Float64,

            # Float types
            'FLOAT': pl.Float64,
            'REAL': pl.Float32,
            'DOUBLE': pl.Float64,

            # Date/Time types
            'DATE': pl.Date,
            'TIME': pl.Time,
            'DATETIME': pl.Datetime,
            'TIMESTAMP': pl.Datetime,

            # Boolean
            'BIT': pl.Boolean,
            'BOOL': pl.Boolean,
            'BOOLEAN': pl.Boolean,
        }

    def create_user_session(self, user_id: str) -> str:
        """Create a new database session for a user"""
        try:
            session_id = self.connection_manager.create_user_connection(user_id)
            logger.info(f"Created database session for user: {user_id}")
            return session_id
        except Exception as e:
            logger.error(f"Failed to create session for user {user_id}: {e}")
            raise

    def execute_query(self, query: str, params: tuple = None, user_id: str = None) -> pl.DataFrame:
        """Execute a query for a specific user and return Polars DataFrame using Arrow format"""
        if not user_id:
            raise ValueError("user_id is required for multi-user operation")

        # Auto-create connection if it doesn't exist
        try:
            conn = self.connection_manager.get_user_connection(user_id)
        except ValueError:
            # Connection doesn't exist, create it
            self.connection_manager.create_user_connection(user_id)
            conn = self.connection_manager.get_user_connection(user_id)

        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Use Arrow format for direct conversion to Polars
            arrow_table = cursor.fetchall_arrow()
            df = pl.from_arrow(arrow_table)

            # Handle any remaining datetime timezone issues
            for col in df.columns:
                if df[col].dtype == pl.Datetime:
                    try:
                        # Convert timezone-aware datetimes to UTC and remove timezone
                        df = df.with_columns(
                            pl.col(col).dt.convert_time_zone("UTC").dt.replace_time_zone(None)
                        )
                    except Exception:
                        # If timezone conversion fails, convert to string
                        df = df.with_columns(pl.col(col).cast(pl.Utf8))

            cursor.close()
            return df

        except Exception as e:
            logger.error(f"Query execution failed for user {user_id}: {e}")
            raise

    def execute_query_raw(self, query: str, params: tuple = None, user_id: str = None):
        """Execute a query and return raw cursor results"""
        if not user_id:
            raise ValueError("user_id is required for multi-user operation")

        conn = self.connection_manager.get_user_connection(user_id)
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def get_sales_actuals(self, filters: Dict = None, limit: int = None, user_id: str = None) -> pl.DataFrame:
        """Get sales actuals data for a specific user"""
        if not user_id:
            user_id = "system"

        query = """
        SELECT sa.*, ph.*, lh.*
        FROM da.sales_actuals sa
        LEFT JOIN da.product_hierarchy ph ON sa.product_id = ph.product_id
        LEFT JOIN da.location_hierarchy lh ON sa.location_id = lh.location_id
        """

        conditions = []
        params = []

        if filters:
            if 'product_id' in filters:
                conditions.append("sa.product_id = ?")
                params.append(filters['product_id'])
            if 'location_id' in filters:
                conditions.append("sa.location_id = ?")
                params.append(filters['location_id'])
            if 'start_date' in filters:
                conditions.append("sa.sales_date >= ?")
                params.append(filters['start_date'])
            if 'end_date' in filters:
                conditions.append("sa.sales_date <= ?")
                params.append(filters['end_date'])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        if limit:
            query += f" LIMIT {limit}"

        return self.execute_query(query, tuple(params) if params else None, user_id)

    def estimate_filtered_data_size(self, location_col: str = None, location_val: str = None, product_col: str = None, product_val: str = None, user_id: str = None) -> int:
        """Estimate the number of rows that would be returned with given filters"""
        if not user_id:
            user_id = "system"

        # Auto-create connection if it doesn't exist
        try:
            self.connection_manager.get_user_connection(user_id)
        except ValueError:
            self.connection_manager.create_user_connection(user_id)

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

        # Count query with filters applied at database level
        count_query = f"""
        SELECT COUNT(*) as count
        FROM da.sales_actuals sa
        JOIN da.product_hierarchy ph ON sa.item_skey = ph.demantra_item_skey
        JOIN da.location_hierarchy lh ON sa.location_skey = lh.location_skey
        {"WHERE " + where_clause if where_clause else ""}
        """

        result = self.execute_query(count_query, tuple(params) if params else None, user_id)
        return result['count'][0] if len(result) > 0 and 'count' in result.columns else 0

    def get_filtered_sales_actuals(self, location_col: str = None, location_val: str = None, product_col: str = None, product_val: str = None, user_id: str = None) -> pl.DataFrame:
        """Get filtered sales actuals data for a specific user"""
        if not user_id:
            user_id = "system"

        # Auto-create connection if it doesn't exist
        try:
            self.connection_manager.get_user_connection(user_id)
        except ValueError:
            self.connection_manager.create_user_connection(user_id)

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

        # Query with filters applied at database level - optimized for performance
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
        INNER JOIN da.product_hierarchy ph ON sa.item_skey = ph.demantra_item_skey
        INNER JOIN da.location_hierarchy lh ON sa.location_skey = lh.location_skey
        {"WHERE " + where_clause if where_clause else ""}
        ORDER BY sa.sales_date DESC, sa.item_skey, sa.location_skey
        LIMIT 1000000
        """

        return self.execute_query(query, tuple(params) if params else None, user_id)

    # Cache for filter options with a 5-minute TTL
    _filter_options_cache = {}
    _last_refresh_time = 0
    _cache_ttl = 300  # 5 minutes in seconds
    _cache_lock = threading.Lock()

    def get_filter_options(self, user_id: str = None, force_refresh: bool = False) -> Dict[str, List[str]]:
        """Get available filter options from hierarchy tables with caching.
        
        Args:
            user_id: Optional user ID (defaults to 'system')
            force_refresh: If True, bypass cache and refresh data
            
        Returns:
            Dictionary of filter options
        """
        current_time = time.time()
        cache_expired = (current_time - self._last_refresh_time) > self._cache_ttl
        
        # Return cached data if available and not forcing refresh
        if not force_refresh and not cache_expired and self._filter_options_cache:
            logger.debug("Returning cached filter options")
            return self._filter_options_cache
            
        with self._cache_lock:
            # Check again in case another thread already refreshed the cache
            if not force_refresh and not cache_expired and self._filter_options_cache:
                logger.debug("Returning cached filter options (double-checked)")
                return self._filter_options_cache
                
            # Use default user_id if not provided
            user_id = user_id or "system"
            logger.info(f"Refreshing filter options for user {user_id}")

            try:
                # Get or create connection
                try:
                    logger.debug(f"Getting connection for user {user_id}")
                    self.connection_manager.get_user_connection(user_id)
                except ValueError as ve:
                    logger.warning(f"Creating new connection for user {user_id}: {ve}")
                    self.connection_manager.create_user_connection(user_id)

                options = {}
                
                # Single query to get all product hierarchy data
                product_hierarchy_query = """
                SELECT 
                    ARRAY_AGG(DISTINCT franchise) FILTER (WHERE franchise IS NOT NULL) as franchises,
                    ARRAY_AGG(DISTINCT ibp_level_5) FILTER (WHERE ibp_level_5 IS NOT NULL) as ibp_level_5s,
                    ARRAY_AGG(DISTINCT ibp_level_6) FILTER (WHERE ibp_level_6 IS NOT NULL) as ibp_level_6s,
                    ARRAY_AGG(DISTINCT catalog_number) FILTER (WHERE catalog_number IS NOT NULL) as catalog_numbers
                FROM da.product_hierarchy
                """
                
                # Single query to get all location hierarchy data
                location_hierarchy_query = """
                SELECT 
                    ARRAY_AGG(DISTINCT region) FILTER (WHERE region IS NOT NULL) as regions,
                    ARRAY_AGG(DISTINCT country) FILTER (WHERE country IS NOT NULL) as countries,
                    ARRAY_AGG(DISTINCT area) FILTER (WHERE area IS NOT NULL) as areas
                FROM da.location_hierarchy
                """
                
                logger.debug("Executing filter options queries")
                
                # Execute queries in parallel
                with ThreadPoolExecutor(max_workers=2) as executor:
                    logger.debug("Submitting product hierarchy query")
                    product_future = executor.submit(
                        self.execute_query, 
                        product_hierarchy_query,
                        None,  # No params
                        user_id
                    )
                    
                    logger.debug("Submitting location hierarchy query")
                    location_future = executor.submit(
                        self.execute_query,
                        location_hierarchy_query,
                        None,  # No params
                        user_id
                    )
                    
                    # Process product hierarchy results
                    try:
                        logger.debug("Waiting for product hierarchy results")
                        product_result = product_future.result()
                        if product_result is not None and not product_result.is_empty():
                            row = product_result.row(0, named=True)
                            options.update({
                                'franchises': row.get('franchises', []) or [],
                                'ibp_level_5s': row.get('ibp_level_5s', []) or [],
                                'ibp_level_6s': row.get('ibp_level_6s', []) or [],
                                'catalog_numbers': row.get('catalog_numbers', []) or []
                            })
                            logger.debug(f"Got product options: {', '.join(k for k, v in options.items() if v)}")
                        else:
                            logger.warning("No product hierarchy data found")
                    except Exception as e:
                        logger.error(f"Error processing product hierarchy: {e}", exc_info=True)
                    
                    # Process location hierarchy results
                    try:
                        logger.debug("Waiting for location hierarchy results")
                        location_result = location_future.result()
                        if location_result is not None and not location_result.is_empty():
                            row = location_result.row(0, named=True)
                            options.update({
                                'regions': row.get('regions', []) or [],
                                'countries': row.get('countries', []) or [],
                                'areas': row.get('areas', []) or []
                            })
                            logger.debug(f"Got location options: {', '.join(k for k, v in options.items() if v and k not in options.get('franchises', []))}")
                        else:
                            logger.warning("No location hierarchy data found")
                    except Exception as e:
                        logger.error(f"Error processing location hierarchy: {e}", exc_info=True)
                
                # Ensure all expected keys exist with at least empty lists
                for key in ['franchises', 'ibp_level_5s', 'ibp_level_6s', 'catalog_numbers', 
                           'regions', 'countries', 'states', 'cities', 'locations']:
                    if key not in options:
                        options[key] = []
                
                # Update cache
                self._filter_options_cache = options
                self._last_refresh_time = current_time
                
                logger.info(f"Successfully refreshed filter options. Found {sum(len(v) for v in options.values())} total options")
                return options
                
            except Exception as e:
                error_msg = f"Error getting filter options: {str(e)}"
                logger.error(error_msg, exc_info=True)
                
                # Return cached data if available, even if stale
                if self._filter_options_cache:
                    logger.warning("Using cached filter options due to error")
                    return self._filter_options_cache
                
                # If no cached data, return empty options
                logger.warning("No cached filter options available, returning empty options")
                return {
                    'franchises': [],
                    'ibp_level_5s': [],
                    'ibp_level_6s': [],
                    'catalog_numbers': [],
                    'regions': [],
                    'countries': [],
                    'states': [],
                    'cities': [],
                    'locations': []
                }

    def close_user_session(self, user_id: str):
        """Close the database session for a specific user"""
        self.connection_manager.close_user_connection(user_id)
        logger.info(f"Closed database session for user: {user_id}")
        """Get statistics about current connections"""
        return self.connection_manager.get_connection_stats()

    def cleanup_old_sessions(self, max_age_seconds: int = 3600):
        """Clean up old user sessions"""
        self.connection_manager.cleanup_old_connections(max_age_seconds)
        logger.info("Cleaned up old database sessions")

# Global instance
_enhanced_db_service = None

def get_enhanced_database_service() -> EnhancedDatabaseService:
    """Get the global enhanced database service instance"""
    global _enhanced_db_service
    if _enhanced_db_service is None:
        _enhanced_db_service = EnhancedDatabaseService()
    return _enhanced_db_service
