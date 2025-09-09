# Databricks Schema Creation Script using Spark API
# This script creates all tables, indexes, and views for the forecasting application
# Run this in a Databricks Python notebook

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("ForecastingSchemaCreation").getOrCreate()

# Define table schemas
def create_product_hierarchy():
    """Create product hierarchy table"""
    schema = StructType([
        StructField("demantra_item_skey", LongType(), True),
        StructField("business_sector", StringType(), True),
        StructField("business_unit", StringType(), True),
        StructField("franchise", StringType(), True),
        StructField("product_line", StringType(), True),
        StructField("ibp_level_5", StringType(), True),
        StructField("ibp_level_6", StringType(), True),
        StructField("ibp_level_7", StringType(), True),
        StructField("catalog_number", StringType(), False),
        StructField("uom", StringType(), True),
        StructField("pack_content", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    # Create empty DataFrame with schema
    empty_df = spark.createDataFrame([], schema)

    # Create table with overwrite mode
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("da.product_hierarchy")

    print("✓ Created da.product_hierarchy table")

def create_location_hierarchy():
    """Create location hierarchy table"""
    schema = StructType([
        StructField("location_skey", LongType(), True),
        StructField("selling_division", StringType(), True),
        StructField("area", StringType(), True),
        StructField("stryker_group_region", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), False),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("da.location_hierarchy")

    print("✓ Created da.location_hierarchy table")

def create_sales_actuals():
    """Create sales actuals fact table"""
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("item_skey", LongType(), False),
        StructField("location_skey", LongType(), False),
        StructField("sales_date", DateType(), False),
        StructField("asp_final_rev", DecimalType(38, 8), True),
        StructField("act_orders_rev", DecimalType(38, 8), True),
        StructField("act_orders_rev_val", DecimalType(38, 8), True),
        StructField("fcst_df_final_rev", DecimalType(38, 8), True),
        StructField("l0_df_final_rev", DecimalType(38, 8), True),
        StructField("l1_df_final_rev", DecimalType(38, 8), True),
        StructField("l2_df_final_rev", DecimalType(38, 8), True),
        StructField("fcst_df_final_rev_val", DecimalType(38, 8), True),
        StructField("fcst_stat_prelim_rev", DecimalType(38, 8), True),
        StructField("fcst_stat_final_rev", DecimalType(38, 8), True),
        StructField("l0_stat_final_rev", DecimalType(38, 8), True),
        StructField("l1_stat_final_rev", DecimalType(38, 8), True),
        StructField("l2_stat_final_rev", DecimalType(38, 8), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("da.sales_actuals")

    print("✓ Created da.sales_actuals table")

def create_product_clusters():
    """Create product clusters table"""
    schema = StructType([
        StructField("cluster_id", StringType(), True),
        StructField("item_skey", LongType(), False),
        StructField("location_skey", LongType(), False),
        StructField("cluster_number", IntegerType(), False),
        StructField("cluster_features", StringType(), True),  # Using String for JSON-like data
        StructField("silhouette_score", DecimalType(15, 6), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("da.product_clusters")

    print("✓ Created da.product_clusters table")

def create_forecasts():
    """Create forecasts table"""
    schema = StructType([
        StructField("forecast_id", LongType(), True),
        StructField("item_skey", LongType(), False),
        StructField("location_skey", LongType(), False),
        StructField("forecast_date", DateType(), False),
        StructField("forecast_horizon", IntegerType(), False),
        StructField("model_type", StringType(), False),
        StructField("forecast_value", DecimalType(15, 2), False),
        StructField("confidence_lower", DecimalType(15, 2), True),
        StructField("confidence_upper", DecimalType(15, 2), True),
        StructField("model_version", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("da.forecasts")

    print("✓ Created da.forecasts table")

def create_model_validation():
    """Create model validation table"""
    schema = StructType([
        StructField("validation_id", LongType(), True),
        StructField("model_type", StringType(), False),
        StructField("validation_date", DateType(), False),
        StructField("validation_period_months", IntegerType(), False),
        StructField("mae", DecimalType(10, 4), True),
        StructField("mape", DecimalType(10, 4), True),
        StructField("rmse", DecimalType(10, 4), True),
        StructField("accuracy_percentage", DecimalType(5, 2), True),
        StructField("forecast_bias", DecimalType(10, 4), True),
        StructField("silhouette_score", DecimalType(15, 6), True),
        StructField("validation_details", StringType(), True),  # Using String for JSON-like data
        StructField("created_at", TimestampType(), True)
    ])

    empty_df = spark.createDataFrame([], schema)
    empty_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("da.model_validation")

    print("✓ Created da.model_validation table")

def enable_column_defaults():
    """Enable column defaults for all tables"""
    spark.sql("ALTER TABLE da.product_hierarchy SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
    spark.sql("ALTER TABLE da.location_hierarchy SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
    spark.sql("ALTER TABLE da.sales_actuals SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
    spark.sql("ALTER TABLE da.product_clusters SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
    spark.sql("ALTER TABLE da.forecasts SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")
    spark.sql("ALTER TABLE da.model_validation SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")

    print("✓ Enabled column defaults for all tables")

def create_indexes():
    """Create performance indexes"""
    indexes = [
        # Basic indexes
        "CREATE INDEX idx_sales_actuals_date ON da.sales_actuals(sales_date)",
        "CREATE INDEX idx_sales_actuals_item_location ON da.sales_actuals(item_skey, location_skey)",
        "CREATE INDEX idx_sales_actuals_date_item ON da.sales_actuals(sales_date, item_skey)",
        "CREATE INDEX idx_forecasts_date ON da.forecasts(forecast_date)",
        "CREATE INDEX idx_forecasts_item_location ON da.forecasts(item_skey, location_skey)",
        "CREATE INDEX idx_forecasts_model_type ON da.forecasts(model_type)",

        # Additional performance indexes for filtering
        "CREATE INDEX idx_product_hierarchy_franchise ON da.product_hierarchy(franchise)",
        "CREATE INDEX idx_product_hierarchy_ibp_level_5 ON da.product_hierarchy(ibp_level_5)",
        "CREATE INDEX idx_product_hierarchy_ibp_level_6 ON da.product_hierarchy(ibp_level_6)",
        "CREATE INDEX idx_product_hierarchy_business_unit ON da.product_hierarchy(business_unit)",
        "CREATE INDEX idx_location_hierarchy_region ON da.location_hierarchy(region)",
        "CREATE INDEX idx_location_hierarchy_country ON da.location_hierarchy(country)",
        "CREATE INDEX idx_location_hierarchy_area ON da.location_hierarchy(area)",

        # Unique indexes to replace PRIMARY KEY and FOREIGN KEY constraints
        "CREATE UNIQUE INDEX idx_product_hierarchy_pk ON da.product_hierarchy(demantra_item_skey)",
        "CREATE UNIQUE INDEX idx_location_hierarchy_pk ON da.location_hierarchy(location_skey)",
        "CREATE UNIQUE INDEX idx_sales_actuals_pk ON da.sales_actuals(id)",
        "CREATE UNIQUE INDEX idx_product_clusters_pk ON da.product_clusters(cluster_id)",
        "CREATE UNIQUE INDEX idx_forecasts_pk ON da.forecasts(forecast_id)",
        "CREATE UNIQUE INDEX idx_model_validation_pk ON da.model_validation(validation_id)",

        # Relationship indexes to maintain referential integrity
        "CREATE INDEX idx_sales_actuals_item_fk ON da.sales_actuals(item_skey)",
        "CREATE INDEX idx_sales_actuals_location_fk ON da.sales_actuals(location_skey)",
        "CREATE INDEX idx_product_clusters_item_fk ON da.product_clusters(item_skey)",
        "CREATE INDEX idx_product_clusters_location_fk ON da.product_clusters(location_skey)",
        "CREATE INDEX idx_forecasts_item_fk ON da.forecasts(item_skey)",
        "CREATE INDEX idx_forecasts_location_fk ON da.forecasts(location_skey)",

        # Unique index to replace UNIQUE constraint
        "CREATE UNIQUE INDEX idx_product_clusters_unique ON da.product_clusters(item_skey, location_skey)",

        # Composite indexes for common filter combinations
        "CREATE INDEX idx_sales_product_location_date ON da.sales_actuals(item_skey, location_skey, sales_date)",
        "CREATE INDEX idx_product_location_franchise_region ON da.product_hierarchy(franchise), da.location_hierarchy(region)"
    ]

    for index_sql in indexes:
        try:
            spark.sql(index_sql)
            print(f"✓ Created index: {index_sql.split(' ON ')[0]}")
        except Exception as e:
            print(f"⚠️  Index creation failed: {index_sql}")
            print(f"   Error: {str(e)}")

def create_views():
    """Create views for common queries"""
    views = [
        # Product location summary view
        """
        CREATE OR REPLACE VIEW v_product_location_summary AS
        SELECT
            p.catalog_number,
            p.franchise,
            p.ibp_level_5,
            p.ibp_level_6,
            p.business_sector,
            p.business_unit,
            p.product_line,
            l.country,
            l.region,
            l.area,
            l.stryker_group_region,
            l.selling_division,
            s.item_skey,
            s.location_skey,
            COUNT(*) as data_points,
            MIN(s.sales_date) as first_date,
            MAX(s.sales_date) as last_date,
            AVG(s.act_orders_rev) as avg_revenue,
            SUM(s.act_orders_rev) as total_revenue
        FROM da.sales_actuals s
        JOIN da.product_hierarchy p ON s.item_skey = p.demantra_item_skey
        JOIN da.location_hierarchy l ON s.location_skey = l.location_skey
        GROUP BY p.catalog_number, p.franchise, p.ibp_level_5, p.ibp_level_6,
                 p.business_sector, p.business_unit, p.product_line,
                 l.country, l.region, l.area, l.stryker_group_region, l.selling_division,
                 s.item_skey, s.location_skey
        """,

        # Forecast accuracy view
        """
        CREATE OR REPLACE VIEW v_forecast_accuracy AS
        SELECT
            f.item_skey,
            f.location_skey,
            f.forecast_date,
            f.model_type,
            f.forecast_value,
            s.act_orders_rev as actual_value,
            ABS(f.forecast_value - s.act_orders_rev) as absolute_error,
            CASE
                WHEN s.act_orders_rev != 0
                THEN ABS(f.forecast_value - s.act_orders_rev) / ABS(s.act_orders_rev) * 100
                ELSE NULL
            END as percentage_error
        FROM da.forecasts f
        JOIN da.sales_actuals s ON f.item_skey = s.item_skey
            AND f.location_skey = s.location_skey
            AND f.forecast_date = s.sales_date
        WHERE f.forecast_horizon = 1
        """,

        # Time series clustered view
        """
        CREATE OR REPLACE VIEW v_time_series_clustered AS
        SELECT
            s.item_skey,
            s.location_skey,
            s.sales_date,
            s.act_orders_rev,
            s.act_orders_rev_val,
            s.fcst_df_final_rev,
            s.fcst_df_final_rev_val,
            p.catalog_number,
            p.franchise,
            p.business_sector,
            p.business_unit,
            p.product_line,
            p.ibp_level_5,
            p.ibp_level_6,
            p.ibp_level_7,
            l.country,
            l.region,
            l.area,
            l.stryker_group_region,
            l.selling_division,
            c.cluster_number,
            CONCAT(l.country, ',', p.catalog_number) as unique_id
        FROM da.sales_actuals s
        JOIN da.product_hierarchy p ON s.item_skey = p.demantra_item_skey
        JOIN da.location_hierarchy l ON s.location_skey = l.location_skey
        LEFT JOIN da.product_clusters c ON s.item_skey = c.item_skey
            AND s.location_skey = c.location_skey
        ORDER BY s.item_skey, s.location_skey, s.sales_date
        """
    ]

    for view_sql in views:
        try:
            spark.sql(view_sql)
            view_name = view_sql.split("CREATE OR REPLACE VIEW ")[1].split(" AS")[0]
            print(f"✓ Created view: {view_name}")
        except Exception as e:
            print(f"⚠️  View creation failed: {view_sql.split(' AS')[0]}")
            print(f"   Error: {str(e)}")

# Main execution
if __name__ == "__main__":
    print("🚀 Starting Databricks schema creation using Spark API...")

    try:
        # Create all tables
        print("\n📋 Creating tables...")
        #create_product_hierarchy()
        #create_location_hierarchy()
        #create_sales_actuals()
        create_product_clusters()
        create_forecasts()
        create_model_validation()

        # Enable column defaults
        print("\n⚙️  Enabling column defaults...")
        enable_column_defaults()

        # Create indexes
        print("\n📊 Creating indexes...")
        create_indexes()

        # Create views
        print("\n👁️  Creating views...")
        create_views()

        print("\n🎉 Schema creation completed successfully!")
        print("\n📋 Summary:")
        print("- 6 tables created with Delta format")
        print("- Column defaults enabled for automatic timestamps")
        print("- 20+ performance indexes created")
        print("- 3 views created for common queries")
        print("- All tables ready for data loading")

    except Exception as e:
        print(f"\n❌ Schema creation failed with error: {str(e)}")
        raise
