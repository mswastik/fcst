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
from simple_pipeline import standalone_forecasting_pipeline
from forecasting.model_validator import ModelValidator, ValidationReportGenerator


class FilterComponents:
    """Handles filter-related UI components and logic."""
    
    def __init__(self, filter_state: Dict[str, Any], on_filter_change: Callable):
        self.filter_state = filter_state
        self.on_filter_change = on_filter_change
        # Get filter options from the global state
        from state_manager import get_global_state
        state = get_global_state()
        self.options = state.get_filter_options()
    
    def create_filter_row(self):
        """Create the main filter row with all filter components."""
        with ui.row().classes('w-full gap-2'):
            self._create_location_selects()
            self._create_product_selects()
            self._create_level_select()
            self._create_data_files_select()
            self._create_get_data_button()
    
    def _create_data_files_select(self):
        """Create data files selection dropdown - now loads from DuckDB."""
        '''return ui.select(
            label='Load Data',
            options=['Load from Database'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('data_files', e.value)
            ).classes('w-40')'''
        return ui.button(
            'Load Data',
            on_click=lambda e: self.on_filter_change('data_files', e)
        ).classes('ml-5 mt-3')
    
    def _create_location_selects(self):
        """Create location filter dropdowns."""
        location_select1 = ui.select(
            label='Location',
            options=self.options['locations'],
            with_input=False,
            value='Region',
            on_change=lambda e: self.on_filter_change('location1', e.value)
        ).classes('w-40') #.bind_value(self.filter_state, 'location1')
        
        self.location_select2 = ui.select(
            label='Region',
            options=self.options['locations_filt'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('location2', e.value)
        ).classes('w-40').bind_value(self.filter_state, 'location2')
        
        return location_select1, self.location_select2
    
    def _create_product_selects(self):
        """Create product filter dropdowns."""
        self.product_select1 = ui.select(
            label='Product',
            options=self.options['products'],
            with_input=False,
            value='Franchise',
            on_change=lambda e: self._on_product_hierarchy_change(e.value),
        ).classes('w-40')
        
        self.product_select2 = ui.select(
            label='Franchise',
            options=self.options['products_filt'],
            with_input=True,
            on_change=lambda e: self.on_filter_change('product2', e.value),
            clearable=True
        ).classes('w-40').bind_value(self.filter_state, 'product2')
        
        return self.product_select1, self.product_select2
    
    def _on_product_hierarchy_change(self, value):
        """Handle product hierarchy selection change."""
        # Update the filter state
        self.on_filter_change('product1', value)
        
        # Update the second dropdown label and get new options
        self.product_select2._props.update({'label': value})
        
        # Get updated options based on the selected hierarchy level
        from state_manager import get_global_state
        from data_model import get_filter_options
        
        # Use the data_model function which properly handles the database lookup
        options = get_filter_options(prod=value, loc=self.filter_state.get('location1'))
        
        # Update the options in the second dropdown
        self.product_select2.options = options['products_filt']
        self.product_select2.update()
    
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
        with ui.row().classes('w-full gap-2 mr-0'):
            self.column_chart_container = self._create_column_chart()
            self.line_chart_container = self._create_line_chart()
        return self.column_chart_container, self.line_chart_container
    
    def _create_column_chart(self):
        """Create column chart container."""
        #with ui.column().classes('w-1/2 h-96 gap-0'):
        return ui.card().classes('flex-1 w-1/2 h-96')   #.classes('w-full h-full')
    
    def _create_line_chart(self):
        """Create line chart container."""
        #with ui.column().classes('w-1/2 h-96 gap-0'):
        return ui.card().classes('flex-1 w-1/2 h-96')  #.classes('w-full h-full')


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
            ui.button('Validate Models', on_click=self._run_validation).classes('bg-blue-100')
            ui.button('Change FC', on_click=self._change_forecast).classes('bg-green-100')
            ui.button('View', on_click=self._show_view_dialog).classes('bg-green-100')
    
    async def _run_cluster(self):
        """Handle clustering action."""
        # Get filtered data from global state for clustering
        from state_manager import get_global_state
        state = get_global_state()
        filtered_df = state.filtered_df if state.filtered_df is not None else self.dwn_data.df
        
        if filtered_df is None or len(filtered_df) == 0:
            ui.notify('No data available for clustering. Please load and filter data first.', type='warning')
            return
        
        # Show progress notification
        n = ui.notification(timeout=None)
        n.message = "Creating clusters... This may take a few minutes."
        n.spinner = True
        
        try:
            # Create a simple wrapper for clustering that uses filtered data
            async def cluster_wrapper():
                from data_service import create_clusters
                result = create_clusters(filtered_df, "", state)
                
                # Verify data was saved to database
                from db_service import get_database_service
                db_service = get_database_service()
                cluster_count = db_service.get_cluster_count()
                print(f"Clusters saved to database. Total cluster records: {cluster_count}")
                
                return result
            
            self.dwn_data.df = await cluster_wrapper()
            
            n.message = 'Clustering completed successfully!'
            n.spinner = False
            n.dismiss()
            ui.notify('Clusters created and saved to database!', type='success')
            
        except Exception as e:
            n.dismiss()
            ui.notify(f'Clustering failed: {str(e)}', type='negative')
            print(f"Clustering error: {e}")
    
    async def _run_create_models(self):
        """Handle model creation action."""
        # Get filtered data from global state
        from state_manager import get_global_state
        state = get_global_state()
        filtered_df = state.filtered_df if state.filtered_df is not None else self.dwn_data.df
        
        if filtered_df is None or len(filtered_df) == 0:
            ui.notify('No data available for forecasting. Please load and filter data first.', type='warning')
            return
        
        # Show detailed progress notification
        n = ui.notification(timeout=None)
        n.message = "Initializing forecasting pipeline..."
        n.spinner = True
        
        try:
            # Update progress
            n.message = "Processing data and running forecasting models... This may take several minutes."
            
            result_dict, validation_results = await run.cpu_bound(
                standalone_forecasting_pipeline, 
                filtered_df.to_dict(as_series=False)
            )
            
            # Update progress
            n.message = "Saving results to database..."
            
            # Reconstruct dataframe from dictionary result
            if result_dict is not None:
                self.dwn_data.df = pl.DataFrame(result_dict)
                # Update global state with the processed data
                state.df = self.dwn_data.df
                
                # Verify data was saved to database
                from db_service import get_database_service
                db_service = get_database_service()
                
                # Check if forecasts were saved (if forecast table exists)
                try:
                    forecast_count = db_service.get_forecast_count()
                    print(f"Forecasts saved to database. Total forecast records: {forecast_count}")
                except:
                    print("Forecast table not yet implemented, but clustering data was processed")
                
                n.message = 'Forecasting completed successfully!'
                n.spinner = False
                n.dismiss()
                ui.notify('Models created and results saved to database!', type='success')
            else:
                n.dismiss()
                ui.notify('Forecasting completed but no results returned', type='warning')
                
        except Exception as e:
            n.dismiss()
            ui.notify(f'Forecasting failed: {str(e)}', type='negative')
            print(f"Forecasting error: {e}")
    
    def _change_forecast(self):
        """Handle forecast change action."""
        ui.notify(change_fc_action(), type='info')
    
    async def _run_validation(self):
        """Handle model validation action."""
        if not hasattr(self.dwn_data, 'df') or self.dwn_data.df is None or len(self.dwn_data.df) == 0:
            ui.notify('No data available for validation. Please load data first.', type='warning')
            return
        
        n = ui.notification(timeout=None)
        n.message = "Running model validation for last 3 months..."
        n.spinner = True
        
        try:
            validator = ModelValidator()
            # Use filtered data for validation
            from state_manager import get_global_state
            state = get_global_state()
            filtered_df = state.filtered_df if state.filtered_df is not None else self.dwn_data.df
            
            validation_results = await run.cpu_bound(
                validator.validate_last_3_months,
                filtered_df.to_dict(as_series=False)
            )
            
            n.dismiss()
            ValidationResultsDialog(validation_results).show()
            
        except Exception as e:
            n.dismiss()
            ui.notify(f'Validation failed: {str(e)}', type='negative')
    
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
            'Act Orders Rev': pl.Float64,
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
            'ASP Final Rev': pl.Float64,
            'Act Orders Rev Val': pl.Float64,
            'L2 DF Final Rev': pl.Float64,
            'L1 DF Final Rev': pl.Float64,
            'L0 DF Final Rev': pl.Float64,
            'L2 Stat Final Rev': pl.Float64,
            'Fcst DF Final Rev': pl.Float64,
            'Fcst Stat Final Rev': pl.Float64,
            'Fcst Stat Prelim Rev': pl.Float64,
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
        if 'Act Orders Rev' in full_df.columns:
            full_df = full_df.with_columns(pl.col('Act Orders Rev').cast(pl.Float32))
        
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
                    values='Act Orders Rev',
                    aggregate_function='sum',
                    sort_columns=True
                )
            elif self.filter_state['location1']:
                table_df = f1.pivot(
                    'SALES_DATE',
                    index=self.filter_state['location1'],
                    values='Act Orders Rev',
                    aggregate_function='sum',
                    sort_columns=True
                )
            else:
                # Use the first available location column as fallback
                available_location_cols = ['Region', 'Country', 'Area']
                index_col = None
                for col in available_location_cols:
                    if col in f1.columns:
                        index_col = col
                        break
                
                if index_col:
                    table_df = f1.pivot(
                        'SALES_DATE',
                        index=index_col,
                        values='Act Orders Rev',
                        aggregate_function='sum',
                        sort_columns=True
                    )
                else:
                    # If no location columns available, create a simple aggregated table
                    table_df = f1.group_by('SALES_DATE').agg(
                        pl.col('Act Orders Rev').sum()
                    ).sort('SALES_DATE')
            
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


class ValidationResultsDialog:
    """Handles the validation results display dialog."""
    
    def __init__(self, validation_results: Dict):
        self.validation_results = validation_results
    
    def show(self):
        """Show the validation results dialog."""
        with ui.dialog().props('maximized') as dialog, ui.card().classes('w-full h-full'):
            self._create_dialog_header()
            self._create_dialog_content()
            self._create_dialog_footer(dialog)
        dialog.open()
    
    def _create_dialog_header(self):
        """Create the dialog header."""
        with ui.row().classes('w-full justify-between items-center p-4 bg-blue-50'):
            ui.label('Model Validation Results - Last 3 Months').classes('text-h5 font-bold')
            ui.label(f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}').classes('text-caption')
    
    def _create_dialog_content(self):
        """Create the main content of the dialog."""
        with ui.column().classes('w-full h-full p-4 gap-4'):
            # Summary statistics
            self._create_summary_section()
            
            # Detailed results tabs
            with ui.tabs().classes('w-full') as tabs:
                comparison_tab = ui.tab('Comparison')
                ensemble_tab = ui.tab('Ensemble')
                nhits_tab = ui.tab('NHITS')
                lstm_tab = ui.tab('LSTM')
                arima_tab = ui.tab('AutoARIMA')
                ets_tab = ui.tab('AutoETS')
                naive_tab = ui.tab('SeasonalNaive')
                report_tab = ui.tab('Full Report')
            
            with ui.tab_panels(tabs, value=comparison_tab).classes('w-full h-96'):
                with ui.tab_panel(comparison_tab):
                    self._create_comparison_panel()
                
                with ui.tab_panel(ensemble_tab):
                    self._create_model_panel('ensemble', 'Ensemble')
                
                with ui.tab_panel(nhits_tab):
                    self._create_model_panel('nhits', 'NHITS')
                
                with ui.tab_panel(lstm_tab):
                    self._create_model_panel('lstm', 'LSTM')
                
                with ui.tab_panel(arima_tab):
                    self._create_model_panel('autoarima', 'AutoARIMA')
                
                with ui.tab_panel(ets_tab):
                    self._create_model_panel('autoets', 'AutoETS')
                
                with ui.tab_panel(naive_tab):
                    self._create_model_panel('seasonalnaive', 'SeasonalNaive')
                
                with ui.tab_panel(report_tab):
                    self._create_report_panel()
    
    def _create_summary_section(self):
        """Create the summary statistics section."""
        validator = ModelValidator()
        validator.validation_results = self.validation_results
        summary = validator.get_summary_statistics()
        
        with ui.card().classes('w-full'):
            ui.label('Summary Statistics').classes('text-h6 font-bold mb-2')
            
            # Create summary cards for all models
            model_colors = {
                'ensemble': 'bg-green-50',
                'nhits': 'bg-blue-50', 
                'lstm': 'bg-purple-50',
                'autoarima': 'bg-orange-50',
                'autoets': 'bg-red-50',
                'seasonalnaive': 'bg-yellow-50'
            }
            
            model_display_names = {
                'ensemble': 'Ensemble',
                'nhits': 'NHITS',
                'lstm': 'LSTM',
                'autoarima': 'AutoARIMA',
                'autoets': 'AutoETS',
                'seasonalnaive': 'SeasonalNaive'
            }
            
            # Create rows of model summary cards
            with ui.row().classes('w-full gap-2 flex-wrap'):
                for model_key, model_name in model_display_names.items():
                    if model_key in summary and summary[model_key].get('num_validations', 0) > 0:
                        with ui.card().classes(f'flex-1 min-w-48 {model_colors.get(model_key, "bg-gray-50")}'):
                            ui.label(model_name).classes('font-bold text-center')
                            model_stats = summary[model_key]
                            ui.label(f"Avg Accuracy: {model_stats.get('avg_accuracy', 0):.1f}%")
                            ui.label(f"Avg MAE: {model_stats.get('avg_mae', 0):.2f}")
                            ui.label(f"Validations: {model_stats.get('num_validations', 0)}")
            
            # Overall comparison summary
            if 'overall_comparison' in summary and summary['overall_comparison']:
                with ui.card().classes('w-full bg-gray-100 mt-4'):
                    ui.label('Overall Performance').classes('font-bold text-center mb-2')
                    overall = summary['overall_comparison']
                    
                    with ui.row().classes('w-full gap-4 justify-center'):
                        for key, value in overall.items():
                            if key.endswith('_wins') and value > 0:
                                model_name = key.replace('_wins', '').title()
                                ui.label(f"{model_name}: {value} wins").classes('text-sm')
    
    def _create_comparison_panel(self):
        """Create the model comparison panel."""
        comparison_results = self.validation_results.get('comparison', [])
        
        if not comparison_results:
            ui.label('No comparison data available').classes('text-center text-gray-500 mt-8')
            return
        
        # Create summary table
        summary_df = ValidationReportGenerator.generate_summary_table(self.validation_results)
        
        if summary_df.height > 0:
            with ui.column().classes('w-full'):
                ui.label('Month-by-Month Comparison').classes('text-h6 font-bold mb-4')
                ui.table.from_polars(summary_df).classes('w-full')
        
        # Create comparison chart
        self._create_comparison_chart(comparison_results)
    
    def _create_comparison_chart(self, comparison_results):
        """Create a comparison chart."""
        if not comparison_results:
            return
        
        # Prepare data for chart - include all models
        months = [comp['month'] for comp in comparison_results]
        
        # Define model colors and names
        model_info = {
            'ensemble': {'name': 'Ensemble', 'color': '#10B981'},
            'nhits': {'name': 'NHITS', 'color': '#3B82F6'},
            'lstm': {'name': 'LSTM', 'color': '#8B5CF6'},
            'autoarima': {'name': 'AutoARIMA', 'color': '#F59E0B'},
            'autoets': {'name': 'AutoETS', 'color': '#EF4444'},
            'seasonalnaive': {'name': 'SeasonalNaive', 'color': '#84CC16'}
        }
        
        # Collect data for all models
        chart_series = []
        for model_key, info in model_info.items():
            model_data = []
            for comp in comparison_results:
                accuracy = comp.get(f'{model_key}_accuracy')
                model_data.append(accuracy if accuracy is not None else None)
            
            # Only add series if there's at least one non-null value
            if any(val is not None for val in model_data):
                chart_series.append({
                    'name': info['name'],
                    'data': model_data,
                    'color': info['color'],
                    'connectNulls': False
                })
        
        if chart_series:
            with ui.card().classes('w-full mt-4'):
                ui.label('Accuracy Comparison Chart - All Models').classes('text-h6 font-bold mb-2')
                
                chart_config = {
                    'chart': {'type': 'line', 'height': 400},
                    'title': {'text': 'Model Accuracy Comparison'},
                    'xAxis': {'categories': months},
                    'yAxis': {'title': {'text': 'Accuracy (%)'}, 'min': 0, 'max': 100},
                    'series': chart_series,
                    'legend': {'enabled': True},
                    'tooltip': {
                        'shared': True,
                        'valueSuffix': '%'
                    }
                }
                
                ui.highchart(chart_config).classes('w-full')
    
    def _create_model_panel(self, model_key: str, model_display_name: str):
        """Create a results panel for a specific model."""
        model_results = self.validation_results.get(model_key, [])
        
        if not model_results:
            ui.label(f'No {model_display_name} validation results available').classes('text-center text-gray-500 mt-8')
            return
        
        # Create detailed results table
        model_data = []
        for result in model_results:
            model_data.append({
                'Month': result.month,
                'Accuracy': f"{result.accuracy_percentage:.2f}%",
                'MAE': f"{result.mae:.2f}",
                'MAPE': f"{result.mape:.2f}%",
                'RMSE': f"{result.rmse:.2f}",
                'Bias': f"{result.forecast_bias:.2f}"
            })
        
        if model_data:
            model_df = pl.DataFrame(model_data)
            ui.label(f'{model_display_name} Model Detailed Results').classes('text-h6 font-bold mb-4')
            ui.table.from_polars(model_df).classes('w-full')
    
    def _create_report_panel(self):
        """Create the full report panel."""
        report_text = ValidationReportGenerator.generate_text_report(self.validation_results)
        
        with ui.column().classes('w-full'):
            ui.label('Full Validation Report').classes('text-h6 font-bold mb-4')
            
            with ui.card().classes('w-full bg-gray-50'):
                ui.code(report_text).classes('w-full whitespace-pre-wrap text-sm')
    
    def _create_dialog_footer(self, dialog):
        """Create the dialog footer with action buttons."""
        with ui.row().classes('w-full justify-end gap-2 p-4 bg-gray-50'):
            ui.button('Export Report', on_click=self._export_report).props('color=primary outline')
            ui.button('Close', on_click=dialog.close).props('color=primary')
    
    def _export_report(self):
        """Export the validation report."""
        try:
            report_text = ValidationReportGenerator.generate_text_report(self.validation_results)
            
            # Save to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"validation_report_{timestamp}.txt"
            filepath = os.path.join("data", filename)
            
            with open(filepath, 'w') as f:
                f.write(report_text)
            
            ui.notify(f'Report exported to {filepath}', type='positive')
            
        except Exception as e:
            ui.notify(f'Export failed: {str(e)}', type='negative')
