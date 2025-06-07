from nicegui import ui
from models.data_model import get_chart_data
#import polars as pl

def render_column_chart(container,filtered_df):
    """Render column chart in the provided container"""
    container.clear()
    with container:
        data = get_chart_data('column',filtered_df)
        if data:
            ui.echart({
                #'legend': {'data': [s['name'] for s in data['series']]}, # Add legend
                'xAxis': {'data': data['months']}, # Use the 'months' list for x-axis
                'yAxis': {'type': 'value'},
                'series': data['series'], # Use the pre-formatted series data
                'tooltip':{
                    'trigger': 'axis',
                },
            }).classes('w-full h-full')
        else:
            ui.label('No data to display').classes('text-center text-gray-500')

def render_line_chart(container,filtered_df):
    """Render line chart in the provided container"""
    container.clear()
    with container:
        data = get_chart_data('line',filtered_df)
        if data:
            ui.echart({
                'xAxis': {'type':'time','axisLabel': {'formatter': '{MMM} {yy}'}},
                'yAxis': {'type': 'value'},
                'series': [
                    {
                        'type': 'line',
                        'name': 'Actual',
                        'data': [[data['categories'][i],data['values'][i]] for i in range(len(data['categories']))],
                        'marker': {'enabled': True}
                    }
                ] + ([{
                    'type': 'line',
                    'name': 'Forecast',
                    'data': [[data['categories'][i],data['forecast_values'][i]] for i in range(len(data['forecast_values']))],
                    'marker': {'enabled': True},
                    'color': '#FF0000' # Red color for forecast
                }] if data['forecast_values'] else []),
                'tooltip':{ 'trigger': 'axis',},
            }).classes('w-full h-full')
        else:
            ui.label('No data to display').classes('text-center text-gray-500')

async def update_charts(column_container, line_container,filtered_df):
    """Update both charts"""
    render_column_chart(column_container,filtered_df)
    render_line_chart(line_container,filtered_df)
