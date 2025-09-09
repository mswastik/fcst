"""
Enhanced Database Service with Multi-User Support
Supports multiple concurrent users with separate database connections
"""
import os
import logging
import uuid
from typing import Optional, List, Dict, Any, Tuple, Callable
from datetime import datetime, timedelta
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

    def get_filter_options(self, user_id: str = None) -> Dict[str, List[str]]:
        """Get available filter options from hierarchy tables for a specific user"""
        # Use default user_id if not provided (for backward compatibility)
        if not user_id:
            user_id = "system"

        # Auto-create connection for default system user if it doesn't exist
        try:
            self.connection_manager.get_user_connection(user_id)
        except ValueError:
            self.connection_manager.create_user_connection(user_id)

        options = {}

        try:
            # Product filters - get as DataFrame and extract unique values safely
            product_query = "SELECT DISTINCT franchise FROM da.product_hierarchy WHERE franchise IS NOT NULL ORDER BY franchise"
            product_df = self.execute_query(product_query, user_id=user_id)
            if product_df is not None and len(product_df) > 0 and 'franchise' in product_df.columns:
                options['franchises'] = product_df['franchise'].unique().to_list()
            else:
                options['franchises'] = []

            ibp5_query = "SELECT DISTINCT ibp_level_5 FROM da.product_hierarchy WHERE ibp_level_5 IS NOT NULL ORDER BY ibp_level_5"
            ibp5_df = self.execute_query(ibp5_query, user_id=user_id)
            if ibp5_df is not None and len(ibp5_df) > 0 and 'ibp_level_5' in ibp5_df.columns:
                options['ibp_level_5s'] = ibp5_df['ibp_level_5'].unique().to_list()
            else:
                options['ibp_level_5s'] = []

            ibp6_query = "SELECT DISTINCT ibp_level_6 FROM da.product_hierarchy WHERE ibp_level_6 IS NOT NULL ORDER BY ibp_level_6"
            ibp6_df = self.execute_query(ibp6_query, user_id=user_id)
            if ibp6_df is not None and len(ibp6_df) > 0 and 'ibp_level_6' in ibp6_df.columns:
                options['ibp_level_6s'] = ibp6_df['ibp_level_6'].unique().to_list()
            else:
                options['ibp_level_6s'] = []

            catalog_query = "SELECT DISTINCT catalog_number FROM da.product_hierarchy WHERE catalog_number IS NOT NULL ORDER BY catalog_number"
            catalog_df = self.execute_query(catalog_query, user_id=user_id)
            if catalog_df is not None and len(catalog_df) > 0 and 'catalog_number' in catalog_df.columns:
                options['catalog_numbers'] = catalog_df['catalog_number'].unique().to_list()
            else:
                options['catalog_numbers'] = []

            # Location filters
            region_query = "SELECT DISTINCT region FROM da.location_hierarchy WHERE region IS NOT NULL ORDER BY region"
            region_df = self.execute_query(region_query, user_id=user_id)
            if region_df is not None and len(region_df) > 0 and 'region' in region_df.columns:
                options['regions'] = region_df['region'].unique().to_list()
            else:
                options['regions'] = []

            country_query = "SELECT DISTINCT country FROM da.location_hierarchy WHERE country IS NOT NULL ORDER BY country"
            country_df = self.execute_query(country_query, user_id=user_id)
            if country_df is not None and len(country_df) > 0 and 'country' in country_df.columns:
                options['countries'] = country_df['country'].unique().to_list()
            else:
                options['countries'] = []

            area_query = "SELECT DISTINCT area FROM da.location_hierarchy WHERE area IS NOT NULL ORDER BY area"
            area_df = self.execute_query(area_query, user_id=user_id)
            if area_df is not None and len(area_df) > 0 and 'area' in area_df.columns:
                options['areas'] = area_df['area'].unique().to_list()
            else:
                options['areas'] = []

        except Exception as e:
            logger.error(f"Error getting filter options for user {user_id}: {e}")
            # Return empty options on error
            options = {
                'franchises': [],
                'ibp_level_5s': [],
                'ibp_level_6s': [],
                'catalog_numbers': [],
                'regions': [],
                'countries': [],
                'areas': []
            }

        return options

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
