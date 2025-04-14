from nicegui import ui
from models.data_model import get_filter_options, filtered_products, filtered_models
from services.data_service import (
    apply_filters, toggle_month_view, create_models_action, 
    generate_fc_action, change_fc_action, download_data_action, export_data_action
)
from ui.charts import update_charts
import os

def create_dashboard():
    """Create the main dashboard UI"""
    # Get filter options
    options = get_filter_options()
    
    # Create references for UI components that need to be updated
    products_list = None
    models_list = None
    column_chart_container = None
    line_chart_container = None
    details_container = None
    
    # Filter state
    filter_state = {
        'data_files': None,
        'location1': None,
        'location2': None,
        'product1': None,
        'product2': None,
        'level': None
    }
    
    async def update_ui(filtered_df):
        """Update all UI components after filter changes"""
        # Update lists
        products_list.clear()
        models_list.clear()
        
        for product in filtered_products:
            products_list.append(product)
        
        for model in filtered_models:
            models_list.append(model)
        
        # Update charts
        await update_charts(column_chart_container, line_chart_container,filtered_df)
        
        # Update details section
        details_container.clear()
        with details_container:
            ui.label('Selected Product and Model data').classes('text-lg font-bold')
            if len(filtered_products) > 0:
                ui.label(f"{len(filtered_products)} products and {len(filtered_models)} models selected")
    
    async def on_filter_change(filter_name, value):
        """Handle filter change events"""
        filter_state[filter_name] = value
        print(filter_state.get(filter_name))
        if ((filter_state.get('location2')) and (filter_state.get('location1'))) or ((filter_state.get('product2')) and (filter_state.get('product1'))):
            filtered_df = apply_filters(filter_state)
            await update_ui(filtered_df['filtered_df'])
    
    def on_month_toggle(e):
        """Handle month view toggle"""
        toggle_month_view(e.value)
        update_charts(column_chart_container, line_chart_container)
    
    # Create UI layout
    with ui.card().classes('w-full p-4'):
        # Filters row
        with ui.row().classes('w-full gap-2'):
            data_files_select = ui.select(
                label='DataFiles',
                options=os.listdir("data/"),
                with_input=True,
                on_change=lambda e: on_filter_change('data_files', e.value)
            ).classes('w-40')
            
            location_select1 = ui.select(
                label='Location',
                options=[''] + options['locations'],
                with_input=True,
                on_change=lambda e: on_filter_change('location1', e.value)
            ).classes('w-40').bind_value(filter_state,'location1')
            
            location_select2 = ui.select(
                label='Location',
                options=[''] + options['locations_filt'],
                with_input=True,
                on_change=lambda e: on_filter_change('location2', e.value)
            ).classes('w-40').bind_value(filter_state,'location2')
            
            product_select1 = ui.select(
                label='Product',
                options=[''] + options['products'],
                with_input=True,
                on_change=lambda e: on_filter_change('product1', e.value)
            ).classes('w-40').bind_value(filter_state,'product1')
            
            product_select2 = ui.select(
                label='Product',
                options=[''] + options['products_filt'],
                with_input=True,
                on_change=lambda e: on_filter_change('product2', e.value)
            ).classes('w-40').bind_value(filter_state,'product2')
            
            level_select = ui.select(
                label='Level',
                options=[''] + options['levels'],
                with_input=True,
                on_change=lambda e: on_filter_change('level', e.value)
            ).classes('w-40')
            ui.button('Download Data', on_click=lambda: ui.notify(download_data_action(), type='info')).classes('ml-auto')
        
        # Main content area
        with ui.row().classes('w-full mt-2 ml-0 gap-2'):
            # Left sidebar with lists
            with ui.column().classes('w-1/6 gap-2'):
                # Filtered Products list
                with ui.card().classes('w-full h-64'):
                    ui.label('Filtered Products').classes('font-bold')
                    products_list = ui.list()
                
            # Models list
            with ui.column().classes('w-1/6 gap-2'):
                with ui.card().classes('w-full h-64'):
                    ui.label('Models for filtered Products').classes('font-bold')
                    models_list = ui.list()
            
            # Charts and controls section
           # with ui.column().classes('w-full gap-0'):
            # Charts row
            with ui.row().classes('w-[1490px] gap-2 mr-0'):
                # Column chart
                with ui.column().classes('w-[720px] h-96 gap-0'):
                    column_chart_container = ui.card().classes('w-full h-full')
                
                # Line chart
                with ui.column().classes('w-[720px] h-96 gap-0'):
                    line_chart_container = ui.card().classes('w-full h-full')
                
            # Toggle and export row
            with ui.row().classes('w-full justify-between items-center mt-2'):
                with ui.row().classes('items-center gap-2'):
                    ui.switch(on_change=on_month_toggle)
                    ui.label('By Month')
                
                with ui.row().classes('gap-2'):
                    ui.button('Create Models', on_click=lambda: ui.notify(create_models_action(), type='info')).classes('bg-green-100')
                    ui.button('Generate FC', on_click=lambda: ui.notify(generate_fc_action(), type='info')).classes('bg-green-100')
                    ui.button('Change FC', on_click=lambda: ui.notify(change_fc_action(), type='info')).classes('bg-green-100')
                    ui.button('Export', on_click=lambda: ui.notify(export_data_action(), type='info'))
        
        # Bottom details panel
        with ui.card().classes('w-full mt-4 p-4 h-48'):
            details_container = ui.column().classes('w-full')
            with details_container:
                ui.label('Selected Product and Model data').classes('text-lg font-bold')
    
    # Initialize UI with current data
    on_filter_change('', None)  # Trigger initial filter application