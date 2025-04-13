from nicegui import ui
from models.data_model import get_chart_data

def render_column_chart(container):
    """Render column chart in the provided container"""
    container.clear()
    with container:
        data = get_chart_data('column')
        if data:
            ui.echart({
                'title': {'text': 'Column'},
                'xAxis': {'categories': data['categories']},
                'series': [{
                    'type': 'column',
                    'name': 'Value',
                    'data': data['values'],
                    'colorByPoint': True
                }]
            }).classes('w-full h-full')
        else:
            ui.label('No data to display').classes('text-center text-gray-500')

def render_line_chart(container):
    """Render line chart in the provided container"""
    container.clear()
    with container:
        data = get_chart_data('line')
        if data:
            ui.echart({
                'title': {'text': 'Line'},
                'xAxis': {'categories': data['categories']},
                'series': [{
                    'type': 'line',
                    'name': 'Trend',
                    'data': data['values'],
                    'marker': {'enabled': True}
                }]
            }).classes('w-full h-full')
        else:
            ui.label('No data to display').classes('text-center text-gray-500')

def update_charts(column_container, line_container):
    """Update both charts"""
    render_column_chart(column_container)
    render_line_chart(line_container)