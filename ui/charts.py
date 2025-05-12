from nicegui import ui
from models.data_model import get_chart_data

def render_column_chart(container,filtered_df):
    """Render column chart in the provided container"""
    container.clear()
    with container:
        data = get_chart_data('column',filtered_df)
        #data= filtered_df.clone()
        if data:
            ui.echart({
                'title': {'text': 'Column'},
                'xAxis': {'type':'time','axisLabel': {'formatter': '{MMM} {yy}'}},
                'yAxis': {'type': 'value'},
                'series': [{
                    'type': 'bar',
                    'name': 'values',
                    'data': [[data['categories'][i],data['values'][i]] for i in range(len(data['categories']))],
                    'colorByPoint': True
                }],
                'tooltip':{},
            }).classes('w-full h-full')
        else:
            ui.label('No data to display').classes('text-center text-gray-500')

def render_line_chart(container,filtered_df):
    """Render line chart in the provided container"""
    container.clear()
    with container:
        data = get_chart_data('line',filtered_df)
        #data = filtered_df
        if data:
            ui.echart({
                'title': {'text': 'Line'},
                'xAxis': {'type':'time','axisLabel': {'formatter': '{MMM} {yy}'}},
                'yAxis': {'type': 'value'},
                'series': [{
                    'type': 'line',
                    'name': 'Trend',
                    'data': [[data['categories'][i],data['values'][i]] for i in range(len(data['categories']))],
                    'marker': {'enabled': True}
                }],
                'tooltip':{},
            }).classes('w-full h-full')
        else:
            ui.label('No data to display').classes('text-center text-gray-500')

async def update_charts(column_container, line_container,filtered_df):
    """Update both charts"""
    render_column_chart(column_container,filtered_df)
    render_line_chart(line_container,filtered_df)