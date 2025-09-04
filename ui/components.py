"""
UI Components for the dashboard.
Extracted from the monolithic dashboard function for better separation of concerns.
"""
from nicegui import ui, app, run
import polars as pl
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Callable, Optional
from state_manager import get_global_state
from data_model import get_filter_options, generate_sample_data
from data_service import apply_filters, create_models_action, change_fc_action, create_clusters, run_enhanced_forecasting_pipeline


class FilterComponents:
    """Handles filter-related UI components and logic."""
    
    def __init__(self, filter_state: Dict[str, Any], on_filter_change: Callable):
        self.filter_state = filter_state
        self.on_filter_change = on_filter_change
        self.options = get_filter_options()
    
    def create_filter_row(self):
        """Create the main filter row with all filter components."""
        with ui.row().classes('w-full gap-2'):
            self._create_data_files_select()
            self._create_location_selects()
            self._create_product_selects()
            self._create_level_select()
            self._create_get_data_button()
    
    def _create_data_files_select(self):
        """Create data files selection dropdown."""
        return ui.select(
            label='DataFiles',
            options=os.listdir("data/"),
            with_input=True,
            on_change=lambda e: self.on_filter_change('data_files', e.value)
        ).classes('w-40')
    
    def _create_location_selects(self):
        """Create location filter dropdowns."""
        location_select1 = ui.select(
            label='Location',
            options=self.options['locations'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('location1', e.value)
        ).classes('w-40').bind_value(self.filter_state, 'location1')
        
        self.location_select2 = ui.select(
            label='Location',
            options=self.options['locations_filt'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('location2', e.value)
        ).classes('w-40').bind_value(self.filter_state, 'location2')
        
        return location_select1, self.location_select2
    
    def _create_product_selects(self):
        """Create product filter dropdowns."""
        product_select1 = ui.select(
            label='Product',
            options=self.options['products'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('product1', e.value),
            clearable=True
        ).classes('w-40').bind_value(self.filter_state, 'product1')
        
        self.product_select2 = ui.select(
            label='Product',
            options=self.options['products_filt'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('product2', e.value),
            clearable=True
        ).classes('w-40').bind_value(self.filter_state, 'product2')
        
        return product_select1, self.product_select2
    
    def _create_level_select(self):
        """Create level selection dropdown."""
        return ui.select(
            label='Level',
            options=[''] + self.options['levels'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('level', e.value)
        ).classes('w-40')
    
    def _create_get_data_button(self):
        """Create the Get Data button."""
        return ui.button('Get Data', on_click=self._show_download_dialog).classes('ml-auto')
    
    def _show_download_dialog(self):
        """Show the data download dialog."""
        DownloadDialog().show()
    
    def update_location_options(self, options: list):
        """Update location select options."""
        if hasattr(self, 'location_select2'):
            self.location_select2.options = options
            self.location_select2.update()
    
    def update_product_options(self, options: list):
        """Update product select options."""
        if hasattr(self, 'product_select2'):
            self.product_select2.options = options
            self.product_select2.update()


class DownloadDialog:
    """Handles the data download dialog functionality."""
    
    def __init__(self):
        self.dwn_data = self._create_download_data()
    
    def _create_download_data(self):
        """Create download data object."""
        class DownloadData:
            def __init__(self):
                self.lhv, self.lvv, self.phv, self.pvv, self.pmv, self.fmv = '', '', '', '', 36, 24
                self.sp = False
                self.row_lab = ''
        return DownloadData()
    
    def show(self):
        """Show the download dialog."""
        with ui.dialog() as dialog, ui.card():
            ui.label("Select filter parameters to download")
            self._create_dialog_content()
            self._create_dialog_buttons(dialog)
            self._create_progress_indicators()
        dialog.open()
    
    def _create_dialog_content(self):
        """Create the main content of the dialog."""
        with ui.row():
            ui.select(
                options=['StrykerGroupRegion', 'Region', 'Country'],
                label='Location'
            ).bind_value(self.dwn_data, 'lhv').classes('w-48')
            ui.input(
                label='Location Values',
                placeholder='Enter comma separated values'
            ).bind_value(self.dwn_data, 'lvv').classes('w-80')
        
        with ui.row():
            ui.select(
                options=['Franchise', 'Business_Unit', 'IBP_Level_5', 'CatalogNumber'],
                label='Product'
            ).bind_value(self.dwn_data, 'phv').classes('w-48')
            ui.input(
                label='Product Values',
                placeholder='Enter comma separated values'
            ).bind_value(self.dwn_data, 'pvv').classes('w-80')
        
        with ui.row():
            ui.number(label='Past Months').bind_value(self.dwn_data, 'pmv')
            ui.number(label='Future Months').bind_value(self.dwn_data, 'fmv')
    
    def _create_dialog_buttons(self, dialog):
        """Create dialog action buttons."""
        with ui.row():
            ui.button('Download', on_click=self._handle_download)
            ui.button('Cancel', on_click=dialog.close).classes('ml-auto')
    
    def _create_progress_indicators(self):
        """Create progress indicators."""
        with ui.row():
            ui.spinner(size='lg').bind_visibility(self.dwn_data, 'sp')
            ui.label('').bind_text(self.dwn_data, 'row_lab')
            self.time_lab = ui.label()
            self.timer = ui.timer(1.0, self._update_timer, active=False)
    
    def _handle_download(self):
        """Handle the download process."""
        # Placeholder for actual download logic
        ui.notify('Download functionality would be implemented here', type='info')
    
    def _update_timer(self):
        """Update the timer display."""
        # Timer update logic would go here
        pass


class ChartComponents:
    """Handles chart-related UI components."""
    
    def __init__(self):
        self.column_chart_container = None
        self.line_chart_container = None
    
    def create_charts_row(self):
        """Create the charts row with both column and line charts."""
        with ui.row().classes('w-[1190px] gap-2 mr-0'):
            self.column_chart_container = self._create_column_chart()
            self.line_chart_container = self._create_line_chart()
        
        return self.column_chart_container, self.line_chart_container
    
    def _create_column_chart(self):
        """Create column chart container."""
        with ui.column().classes('w-[590px] h-96 gap-0'):
            return ui.card().classes('w-full h-full')
    
    def _create_line_chart(self):
        """Create line chart container."""
        with ui.column().classes('w-[590px] h-96 gap-0'):
            return ui.card().classes('w-full h-full')


class ActionButtons:
    """Handles action button components and their functionality."""
    
    def __init__(self, dwn_data, filter_state: Dict[str, Any]):
        self.dwn_data = dwn_data
        self.filter_state = filter_state
    
    def create_action_buttons(self):
        """Create all action buttons."""
        with ui.row().classes('gap-2'):
            ui.button('Segmentation', on_click=self._run_cluster).classes('bg-green-100')
            ui.button('Generate Forecast', on_click=self._run_create_models).classes('bg-green-100')
            ui.button('Change FC', on_click=self._change_forecast).classes('bg-green-100')
            ui.button('View', on_click=self._show_view_dialog).classes('bg-green-100')
    
    async def _run_cluster(self):
        """Handle clustering action."""
        ui.notify('Creating clusters...', type='info')
        self.dwn_data.df = await run.cpu_bound(
            create_clusters, 
            self.dwn_data.df, 
            self.filter_state['data_files']
        )
        ui.notify('Clusters created!', type='success')
    
    async def _run_create_models(self):
        """Handle model creation action."""
        n = ui.notification(timeout=None)
        n.message = "Running!! "
        n.spinner = True
        
        self.dwn_data.df, _ = await run.cpu_bound(
            run_enhanced_forecasting_pipeline, 
            self.dwn_data.df, 
            self.filter_state['data_files']
        )
        
        n.message = 'Done!'
        n.spinner = False
        n.dismiss()
        ui.notify('Models created and forecasts saved!', type='success')
    
    def _change_forecast(self):
        """Handle forecast change action."""
        ui.notify(change_fc_action(), type='info')
    
    def _show_view_dialog(self):
        """Show the view data dialog."""
        ViewDataDialog().show()


class ViewDataDialog:
    """Handles the view data dialog functionality."""
    
    def show(self):
        """Show the view data dialog."""
        if 'dwn_df_json' not in app.storage.user:
            ui.notify('No data available to view', type='warning')
            return
        
        # Create temporary file for data processing
        import tempfile
        temp_dir = tempfile.gettempdir()
        temp_file = os.path.join(temp_dir, 'temp_data.json')
        
        with open(temp_file, 'w') as f:
            f.write(app.storage.user['dwn_df_json'])
        
        # Define schema to avoid deserialization errors
        schema = {
            'SALES_DATE': pl.Utf8,
            '`Act Orders Rev': pl.Float64,
            'unique_id': pl.Utf8,
            'CatalogNumber': pl.Utf8,
            'Country': pl.Utf8,
            'Area': pl.Utf8,
            'Stryker Group Region': pl.Utf8,
            'Region': pl.Utf8,
            'Business Sector': pl.Utf8,
            'Business Unit': pl.Utf8,
            'Franchise': pl.Utf8,
            'Product Line': pl.Utf8,
            'IBP Level 5': pl.Utf8,
            'IBP Level 6': pl.Utf8,
            'IBP Level 7': pl.Utf8,
            'UOM': pl.Float64,
            'Pack Content': pl.Float64,
            '`L0 ASP Final Rev': pl.Float64,
            'Act Orders Rev Val': pl.Float64,
            'L2 DF Final Rev': pl.Float64,
            'L1 DF Final Rev': pl.Float64,
            'L0 DF Final Rev': pl.Float64,
            'L2 Stat Final Rev': pl.Float64,
            '`Fcst DF Final Rev': pl.Float64,
            '`Fcst Stat Final Rev': pl.Float64,
            '`Fcst Stat Prelim Rev': pl.Float64,
            'Fcst DF Final Rev Val': pl.Float64,
            'birch': pl.Float64,
            'cluster': pl.Utf8,
            'AutoARIMA': pl.Float64,
            'AutoETS': pl.Float64,
            'SeasonalNaive': pl.Float64,
            'ensemble': pl.Float64,
            'NHITS': pl.Float64,
            'LSTM': pl.Float64,
            'Selling Division': pl.Utf8
        }
        
        try:
            full_df = pl.read_json(temp_file, schema=schema)
        except Exception:
            # Fallback without schema if specific schema fails
            full_df = pl.read_json(temp_file, infer_schema_length=None)
        
        # Convert date and numeric columns
        if 'SALES_DATE' in full_df.columns:
            full_df = full_df.with_columns(pl.col('SALES_DATE').str.to_datetime())
        if '`Act Orders Rev' in full_df.columns:
            full_df = full_df.with_columns(pl.col('`Act Orders Rev').cast(pl.Float32))
        
        # Handle cluster column if present
        if 'cluster' in full_df.columns:
            full_df = full_df.with_columns(
                cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id")
            )
            full_df = full_df.with_columns(pl.col('cluster').cast(pl.Utf8))
        
        self._show_date_filter_dialog(full_df)
    
    def _show_date_filter_dialog(self, full_df: pl.DataFrame):
        """Show date filtering dialog."""
        if 'SALES_DATE' in full_df.columns:
            min_date = full_df['SALES_DATE'].min().date()
            max_date = full_df['SALES_DATE'].max().date()
        else:
            min_date = (datetime.now() - timedelta(days=365)).date()
            max_date = datetime.now().date()
        
        with ui.dialog() as date_dialog, ui.card().style('min-width: 700px'):
            ui.label('Filter Data by Date Range').classes('text-h6 mb-4')
            ui.label(f'Data available: {min_date} to {max_date}').classes('text-caption mb-4')
            
            with ui.row().classes('w-full gap-4'):
                start_date_input = ui.date(value=min_date)
                end_date_input = ui.date(value=max_date)
            
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button('Cancel', on_click=lambda: ui.navigate.back()).props('flat')
                ui.button(
                    'Load Data',
                    on_click=lambda: self._filter_and_load_data(
                        full_df, start_date_input.value, end_date_input.value, date_dialog
                    )
                ).props('color=primary')
        
        date_dialog.open()
    
    async def _filter_and_load_data(self, full_df: pl.DataFrame, start_date, end_date, dialog):
        """Filter and load the selected data."""
        try:
            ui.notify("Filtering data...", type='info')
            
            start_datetime = datetime.strptime(str(start_date), '%Y-%m-%d')
            end_datetime = datetime.strptime(str(end_date), '%Y-%m-%d') + timedelta(days=1)
            
            filtered_df = full_df.filter(
                (pl.col('SALES_DATE') >= start_datetime) & 
                (pl.col('SALES_DATE') < end_datetime)
            )
            
            if filtered_df.height == 0:
                ui.notify(f"No data found between {start_date} and {end_date}", type='warning')
                return
            
            filtered_json = filtered_df.write_json()
            dialog.close()
            app.storage.user['dwn_df_json'] = filtered_json
            
            ui.navigate.to('/raw_data', new_tab=True)
            ui.notify(f"Successfully loaded {filtered_df.height} records", type='positive')
        
        except Exception as e:
            ui.notify(f"Error: {str(e)}", type='negative')


class DetailsTable:
    """Handles the details table component."""
    
    def __init__(self, filter_state: Dict[str, Any], update_ui_callback: Callable):
        self.filter_state = filter_state
        self.update_ui_callback = update_ui_callback
    
    def create_details_container(self):
        """Create the details container with table."""
        with ui.card().classes('w-full m-0 p-0 h-full'):
            details_container = ui.column().classes('w-full h-full')
            with details_container:
                ui.label('Select Product and Model data').classes('p-2 text-lg font-bold')
        
        return details_container
    
    async def create_table(self, filtered_df: pl.DataFrame, container):
        """Create and populate the data table."""
        if len(filtered_df) == 0:
            return
        
        container.clear()
        with container:
            ui.label('Select Product and Model data').classes('p-2 text-lg font-bold')
            f1 = filtered_df.with_columns(pl.col('SALES_DATE').dt.date())
            
            # Create the appropriate table based on filter state
            if self.filter_state['level']:
                table_df = f1.pivot(
                    'SALES_DATE',
                    index=[self.filter_state['location1'], self.filter_state['level']],
                    values='`Act Orders Rev',
                    aggregate_function='sum',
                    sort_columns=True
                )
            elif self.filter_state['location1']:
                table_df = f1.pivot(
                    'SALES_DATE',
                    index=self.filter_state['location1'],
                    values='`Act Orders Rev',
                    aggregate_function='sum',
                    sort_columns=True
                )
            else:
                table_df = f1.pivot(
                    'SALES_DATE',
                    index='Region',
                    values='`Act Orders Rev',
                    aggregate_function='sum',
                    sort_columns=True
                )
            
            ui.table.from_polars(table_df, pagination=10).classes('w-full').props('virtual-scroll').on('rowClick', self._on_row_click)
    
    async def _on_row_click(self, e):
        """Handle row click events."""
        catalog_number = e.args[1]['CatalogNumber']
        self.filter_state['product1'] = 'CatalogNumber'
        self.filter_state['product2'] = catalog_number
        
        apply_filters(self.filter_state)
        state = get_global_state()
        global_filtered_df = state.filtered_df
        await self.update_ui_callback(global_filtered_df)
        ui.notify(f"Filtered by CatalogNumber: {catalog_number}", type='info')
