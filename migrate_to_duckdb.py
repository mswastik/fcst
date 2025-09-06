#!/usr/bin/env python3
"""
Migration script to move from parquet files to DuckDB
Run this script to migrate your existing data to the new database structure
"""
import sys
import logging
from pathlib import Path
from db_service import DatabaseService, DataMigrator
import polars as pl

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main migration function"""
    print("🚀 Starting migration from Parquet files to DuckDB...")
    print("=" * 60)
    
    try:
        # Initialize database service
        print("📊 Initializing DuckDB database...")
        db_service = DatabaseService("forecasting.duckdb")
        
        # Initialize migrator
        migrator = DataMigrator(db_service)
        
        # Check if data directory exists
        data_dir = Path("data")
        if not data_dir.exists():
            print("❌ Data directory not found. Please ensure 'data' folder exists with parquet files.")
            return False
        
        # List available parquet files
        parquet_files = list(data_dir.glob("*.parquet"))
        print(f"📁 Found {len(parquet_files)} parquet files to migrate:")
        for file in parquet_files:
            print(f"   - {file.name}")
        
        if not parquet_files:
            print("❌ No parquet files found in data directory.")
            return False
        
        print("\n🔄 Starting migration process...")
        
        # Perform migration
        results = migrator.migrate_from_parquet("data")
        
        # Display results
        print("\n✅ Migration completed!")
        print("=" * 60)
        print("📈 Migration Summary:")
        
        total_records = 0
        for key, value in results.items():
            if isinstance(value, int):
                print(f"   {key}: {value:,} records")
                total_records += value
            else:
                print(f"   {key}: {value}")
        
        print(f"\n📊 Total records migrated: {total_records:,}")
        
        # Get database summary
        print("\n📋 Database Summary:")
        stats = db_service.get_summary_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        # Test basic queries
        print("\n🧪 Testing database queries...")
        
        # Test product hierarchy
        products = db_service.get_product_hierarchy()
        print(f"   Product hierarchy: {len(products)} records")
        
        # Test location hierarchy
        locations = db_service.get_location_hierarchy()
        print(f"   Location hierarchy: {len(locations)} records")
        
        # Test sales actuals (limit to 1000 for testing)
        sales = db_service.get_sales_actuals(limit=1000)
        print(f"   Sales actuals (sample): {len(sales)} records")
        
        # Verify new schema columns are present
        if len(sales) > 0:
            key_cols = ['item_skey', 'location_skey', 'act_orders_rev_val', 'fcst_df_final_rev_val']
            present_cols = [col for col in key_cols if col in sales.columns]
            print(f"   Key columns present: {present_cols}")
        
        # Test clusters
        clusters = db_service.get_clusters()
        print(f"   Product clusters: {len(clusters)} records")
        
        print("\n🎉 Migration successful! Your data is now in DuckDB.")
        print("💡 Next steps:")
        print("   1. Use sql.py functions to populate the database with live data")
        print("   2. Update your application to use db_service instead of direct parquet access")
        print("   3. Test the application with the new database structure")
        print("   4. Keep parquet files as backup until you're confident in the migration")
        print("\n📋 Note: The new schema uses item_skey and location_skey foreign keys")
        print("   - Run sql.py functions to get data with proper keys from your data source")
        print("   - Use the new schema structure for all database operations")
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        print(f"\n❌ Migration failed: {e}")
        print("💡 Please check the logs and try again.")
        return False
    
    finally:
        # Close database connection
        if 'db_service' in locals():
            db_service.close()

def validate_migration():
    """Validate the migrated data"""
    print("\n🔍 Validating migrated data...")
    
    try:
        db_service = DatabaseService("forecasting.duckdb")
        
        # Compare with original parquet files
        data_dir = Path("data")
        validation_results = {}
        
        # Validate hierarchy tables
        try:
            if (data_dir / "phierarchy.parquet").exists():
                ph_original = pl.read_parquet(str(data_dir / "phierarchy.parquet"))
                ph_db = db_service.get_product_hierarchy()
                validation_results['phierarchy.parquet'] = {
                    'original_count': len(ph_original),
                    'db_count': len(ph_db),
                    'match': len(ph_original) == len(ph_db)
                }
            
            if (data_dir / "lhierarchy.parquet").exists():
                lh_original = pl.read_parquet(str(data_dir / "lhierarchy.parquet"))
                lh_db = db_service.get_location_hierarchy()
                validation_results['lhierarchy.parquet'] = {
                    'original_count': len(lh_original),
                    'db_count': len(lh_db),
                    'match': len(lh_original) == len(lh_db)
                }
        except Exception as e:
            validation_results['hierarchy_validation'] = {'error': str(e)}
        
        # Validate fact sales data
        try:
            if (data_dir / "fact_sales.parquet").exists():
                fs_original = pl.read_parquet(str(data_dir / "fact_sales.parquet"))
                fs_db = db_service.get_sales_actuals()
                validation_results['fact_sales.parquet'] = {
                    'original_count': len(fs_original),
                    'db_count': len(fs_db),
                    'match': len(fs_original) == len(fs_db)
                }
        except Exception as e:
            validation_results['fact_sales_validation'] = {'error': str(e)}
        
        # Display validation results
        print("📊 Validation Results:")
        all_valid = True
        for filename, result in validation_results.items():
            if 'error' in result:
                print(f"   ❌ {filename}: Error - {result['error']}")
                all_valid = False
            elif result['match']:
                print(f"   ✅ {filename}: {result['original_count']} records - MATCH")
            else:
                print(f"   ⚠️  {filename}: Original={result['original_count']}, DB={result['db_count']} - MISMATCH")
                all_valid = False
        
        if all_valid:
            print("\n✅ All validation checks passed!")
        else:
            print("\n⚠️  Some validation checks failed. Please review the results.")
        
        db_service.close()
        return all_valid
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--validate":
        validate_migration()
    else:
        success = main()
        
        # Optionally run validation
        if success:
            print("\n" + "=" * 60)
            validate_migration()
