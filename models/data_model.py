import pandas as pd
import polars as pl
import random
from datetime import datetime, timedelta

# Global data
df = None
filtered_df = None
filtered_products = []
filtered_models = []
by_month = False

# Constants

try:
    products_filt=pl.read_parquet(f'data//phierarchy.parquet')
    locations_filt=pl.read_parquet(f'data//lhierarchy.parquet')
except:
    products_filt=[]
    locations_filt=[]
products = ["CatalogNumber", "IBP Level 5", "IBP Level 6"]
locations = ['Area','Region','Country']
levels = ["CatalogNumber", "IBP Level 5", "IBP Level 6"]

def initialize_data():
    """Initialize the application data"""
    global df, filtered_df
    #df = generate_sample_data(f'data/APAC.parquet')
    #filtered_df = df.clone()

def generate_sample_data(path):
    global df, filtered_df
    """Generate sample data for the application"""
    '''data = []
    now = datetime.now()
    for _ in range(100):
        date = (now - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        data.append({
            'date': date,
            'product': random.choice(products),
            'location': random.choice(locations),
            'level': random.choice(levels),
            'value': random.randint(100, 1000),
            'month': date.split('-')[1]
        })'''
    #df= pl.read_parquet('data//APAC.parquet')
    df= pl.read_parquet(path)
    filtered_df = df.clone()
    return df
df= pl.read_parquet('data//APAC.parquet')
def get_filter_options(prod=products[0],loc=locations[0]):
    """Return filter options for UI dropdowns"""
    return {
        #'products_filt':products_filt[prod].unique().to_list(),
        #'locations_filt':locations_filt[loc].unique().to_list(),
        'products_filt':df[prod].unique().to_list(),
        'locations_filt':df[loc].unique().to_list(),
        'products': products,
        'locations': locations,
        'levels': levels
    }

def get_chart_data(type,filtered_df):
    """Get data formatted for charts"""
    if len(filtered_df) == 0:
        return None
    
    chart_data = filtered_df.clone()
    
    # Group by month if toggle is active
    if by_month:
        #chart_data['group'] = chart_data['month']
        chart_data= chart_data.with_columns(group = pl.col('month'))
    else:
        # Group by product for simplicity
        chart_data= chart_data.with_columns(group = pl.col('SALES_DATE'))
    
    # Prepare data based on chart type
    if type == 'column':
        agg_data = chart_data.group_by('group').sum()['group','`Act Orders Rev']
        return {
            'categories': agg_data['group'].to_list(),
            'values': agg_data['`Act Orders Rev'].to_list()
        }
    elif type == 'line':
        # Sort by date and aggregate
        #chart_data['date_obj'] = pd.to_datetime(chart_data['SALES_DATE'])
        chart_data = chart_data.sort('group')
        
        if by_month:
            agg_data = chart_data.group_by('group').sum()['`Act Orders Rev']
            x_values = agg_data['group'].to_list()
        else:
            # Aggregate by date for line chart
            agg_data = chart_data.group_by('group').sum()['group','`Act Orders Rev']
            x_values = agg_data['group'].to_list()
            
        return {
            'categories': x_values,
            'values': agg_data['`Act Orders Rev'].to_list()
        }
    
    return None