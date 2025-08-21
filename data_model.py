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
products = ["Franchise", "IBP Level 5", "IBP Level 6","CatalogNumber"]
locations = ['Area','Region','Country']
levels = ["Franchise", "IBP Level 5", "IBP Level 6","CatalogNumber"]

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
    df=df.with_columns(pl.col('`Act Orders Rev','`Fcst Stat Prelim Rev','`Fcst Stat Final Rev','L2 Stat Final Rev','`Fcst DF Final Rev','L2 DF Final Rev').cast(pl.Float32))
    filtered_df = df.clone()
    return df
#df= pl.read_parquet('data//APAC.parquet')
def get_filter_options(prod=products[0],loc=locations[0]):
    """Return filter options for UI dropdowns"""
    try:
        return {
            #'products_filt':products_filt[prod].unique().to_list(),
            #'locations_filt':locations_filt[loc].unique().to_list(),
            'products_filt':df[prod].unique().to_list(),
            'locations_filt':df[loc].unique().to_list(),
            'products': products,
            'locations': locations,
            'levels': levels}
    except:
        return {
            #'products_filt':products_filt[prod].unique().to_list(),
            #'locations_filt':locations_filt[loc].unique().to_list(),
            'products_filt':[],
            'locations_filt':[],
            'products': products,
            'locations': locations,
            'levels': levels}

def get_chart_data(type,filtered_df):
    """Get data formatted for charts"""
    #print(filtered_df)
    if len(filtered_df) == 0:
        return None
    filtered_df=pl.DataFrame(filtered_df)
    chart_data = filtered_df.clone()
    # Group by month if toggle is active
    if by_month:
        chart_data= chart_data.with_columns(group = pl.col('SALES_DATE').dt.strftime('%b')) # Use abbreviated month name for consistency
    else:
        # Group by product for simplicity
        chart_data= chart_data.with_columns(group = pl.col('SALES_DATE'))
    
    # Prepare data based on chart type
    if type == 'column':
        chart_data=chart_data.with_columns(Month=pl.col('SALES_DATE').dt.strftime('%b')) # Abbreviated month name
        chart_data=chart_data.with_columns(Year=pl.col('SALES_DATE').dt.year())
        
        # Group by Year and Month for actuals
        agg_actuals = chart_data.group_by(['Year', 'Month']).sum()['Year', 'Month', '`Act Orders Rev']
        
        # Group by Year and Month for forecasts (only if NHITS exists)
        agg_forecasts = None
        if 'NHITS' in chart_data.columns:
            agg_forecasts = chart_data.group_by(['Year', 'Month']).sum()['Year', 'Month', 'NHITS']
            
        # Merge actuals and forecasts
        if agg_forecasts is not None:
            agg_data = agg_actuals.join(agg_forecasts, on=['Year', 'Month'], how='outer')
        else:
            agg_data = agg_actuals.with_columns(pl.lit(None).alias('NHITS')) # Add NHITS column with None if not present

        # Sort by Year and then by Month (to ensure correct order on x-axis)
        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        agg_data = agg_data.with_columns(pl.col('Month').map_elements(lambda x: month_order.index(x), return_dtype=pl.Int32).alias('MonthOrder'))
        agg_data = agg_data.sort(['Year', 'MonthOrder'])
        
        unique_months = month_order # Ensure all months are present on x-axis
        unique_years = sorted(agg_data['Year'].unique().to_list())

        series_data = []
        colors = ['#5470C6', '#91CC75', '#EE6666', '#73C0DE', '#3BA272', '#FC8452', '#9A60B4', '#EA7CCC']
        
        for i, year in enumerate(unique_years):
            year_data = agg_data.filter(pl.col('Year') == year)
            
            actual_values = []
            forecast_values = []
            for month in month_order:
                month_row = year_data.filter(pl.col('Month') == month)
                if len(month_row) > 0:
                    actual_values.append(month_row['`Act Orders Rev'][0])
                    forecast_values.append(month_row['NHITS'][0] if 'NHITS' in month_row.columns else None)
                else:
                    actual_values.append(None)
                    forecast_values.append(None)

            current_color = colors[i % len(colors)]
            
            series_data.append({
                'name': f'{year} - Actual',
                'type': 'bar',
                'data': actual_values,
                'color': current_color
            })
            if any(fv is not None for fv in forecast_values):
                series_data.append({
                    'name': f'{year} - Forecast',
                    'type': 'line',
                    'data': forecast_values,
                    'color': current_color,
                    'lineStyle': {
                        'type': 'dashed'
                    }
                })

        return {
            'months': unique_months,
            'series': series_data
        }
    elif type == 'line':
        # Sort by date and aggregate
        chart_data = chart_data.sort('group')
        
        if by_month:
            agg_data = chart_data.group_by('group').sum()['`Act Orders Rev']
            forecast_values = []
            if 'NHITS' in chart_data.columns:
                agg_data = agg_data.with_columns(chart_data.group_by('group').sum()['NHITS'])
                forecast_values = agg_data['NHITS'].to_list()
            x_values = agg_data['group'].to_list()
        else:
            # Aggregate by date for line chart
            agg_data = chart_data.group_by('group').sum()['group','`Act Orders Rev']
            forecast_values = []
            if 'NHITS' in chart_data.columns:
                agg_data = agg_data.with_columns(chart_data.group_by('group').sum()['NHITS'])
                forecast_values = agg_data['NHITS'].to_list()
            x_values = agg_data['group'].to_list()
            
        return {
            'categories': x_values,
            'values': agg_data['`Act Orders Rev'].to_list(),
            'forecast_values': forecast_values
        }
    
    return None
