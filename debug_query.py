#!/usr/bin/env python3
"""
Debug script to test database queries
"""
from core.db_service import DatabaseService
import polars as pl

def test_queries():
    # Get database service instance (create new instance)
    db_service = DatabaseService()

    # Test the query that should work
    print('Testing query with Region=Asia and Franchise=Endoscopy...')

    # First, let's check what data exists for these filters
    print('\nChecking location hierarchy for region=Asia...')
    query1 = '''
    SELECT DISTINCT region, country, area
    FROM location_hierarchy
    WHERE region = 'Asia'
    LIMIT 5
    '''
    with db_service.conn.cursor() as cursor:
        cursor.execute(query1)
        result = cursor.fetchall()
        print('Location results:', result)

    print('\nChecking product hierarchy for franchise=Endoscopy...')
    query2 = '''
    SELECT DISTINCT franchise, business_unit, business_sector
    FROM product_hierarchy
    WHERE franchise = 'Endoscopy'
    LIMIT 5
    '''
    with db_service.conn.cursor() as cursor:
        cursor.execute(query2)
        result = cursor.fetchall()
        print('Product results:', result)

    print('\nChecking sales_actuals count...')
    query3 = '''
    SELECT COUNT(*) as total_rows
    FROM sales_actuals sa
    JOIN product_hierarchy ph ON sa.item_skey = ph.demantra_item_skey
    JOIN location_hierarchy lh ON sa.location_skey = lh.location_skey
    WHERE lh.region = 'Asia' AND ph.franchise = 'Endoscopy'
    '''
    with db_service.conn.cursor() as cursor:
        cursor.execute(query3)
        result = cursor.fetchone()
        print('Sales actuals count with filters:', result)

    # Test the actual method being called
    print('\nTesting get_filtered_sales_actuals method...')
    result_df = db_service.get_filtered_sales_actuals(
        location_col='Region',
        location_val='Asia',
        product_col='Franchise',
        product_val='Endoscopy'
    )
    print(f'Method result: {len(result_df)} rows')
    if len(result_df) > 0:
        print('Sample data:')
        print(result_df.head())

if __name__ == '__main__':
    test_queries()
