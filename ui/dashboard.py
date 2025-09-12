from nicegui import ui,run,app
from core.data_model import get_filter_options, generate_sample_data
from core.data_service import apply_filters, create_models_action, change_fc_action, create_clusters, run_enhanced_forecasting_pipeline
from core.state_manager import get_global_state
from core.utils import DataUtils, DatabaseUtils, UIUtils, ErrorHandler
from ui.charts import update_charts
import os
import polars as pl
from datetime import datetime, timedelta
import json
import re

def clean_markdown_content(content: str) -> str:
    """
    Clean and normalize markdown content to fix formatting issues.

    Args:
        content: Raw markdown content string

    Returns:
        Cleaned markdown content with proper spacing and heading levels
    """
    if not content:
        return content

    # Split into lines for processing
    lines = content.split('\n')
    cleaned_lines = []
    in_code_block = False
    code_block_marker = ''

    for i, line in enumerate(lines):
        # Handle headings - reduce level but preserve structure
        if line.strip().startswith('#'):
            # Reduce heading levels: # -> ##, ## -> ###, ### -> ####, etc.
            heading_match = re.match(r'^(#{1,6})\s+(.+)$', line.strip())
            if heading_match:
                hashes, text = heading_match.groups()
                # Ensure minimum level is ##
                new_level = min(len(hashes) + 1, 6)
                new_hashes = '##' * new_level
                line = f"{new_hashes} {text}"

        # Handle lists - ensure proper spacing
        elif line.strip().startswith(('- ', '* ', '+ ', '1. ', '2. ', '3. ', '4. ', '5. ')):
            # Ensure list items have proper spacing before them
            if i > 0 and cleaned_lines and cleaned_lines[-1].strip():
                # Add blank line before list item if previous line is not blank
                if not cleaned_lines[-1].strip().startswith(('#', '-', '*', '+')) and not cleaned_lines[-1].strip().startswith(tuple(f'{n}.' for n in range(1, 10))):
                    cleaned_lines.append('')

        # Handle paragraphs - ensure proper spacing
        elif line.strip():
            # If this is a regular paragraph line
            if i > 0 and cleaned_lines and cleaned_lines[-1].strip():
                # Check if previous line is also a paragraph (not heading, list, or blank)
                prev_line = cleaned_lines[-1].strip()
                if (prev_line and
                    not prev_line.startswith(('#', '-', '*', '+')) and
                    not any(prev_line.startswith(f'{n}.') for n in range(1, 10)) and
                    not re.match(r'^\s*$', prev_line)):
                    # Add blank line between paragraphs if they're consecutive
                    if len(line.strip()) > 50:  # Likely a paragraph, not a short line
                        cleaned_lines.append('')

        cleaned_lines.append(line)

    # Join back and clean up excessive blank lines
    result = '\n'.join(cleaned_lines)

    # Remove excessive consecutive blank lines (more than 2)
    result = re.sub(r'\n{3,}', '\n\n', result)

    # Ensure content ends with proper spacing
    if result and not result.endswith('\n'):
        result += '\n'

    return result

if not os.path.exists('data/'):
    os.makedirs('data/')

class dwn_data():
    def __init__(self):
        self.lhv,self.lvv,self.phv,self.pvv,self.pmv,self.fmv='','','','',36,24
        self.df=pl.DataFrame()
        self.sp=False
        self.row_lab=''

@ui.page("/")
def create_dashboard():
    """Create the main dashboard UI using modular components."""
    from ui.components import FilterComponents, ChartComponents, ActionButtons, DetailsTable, AuthHeader
    
    ui.colors(primary='#555')
    dwn = dwn_data()
    
    # Add authentication header
    auth_header = AuthHeader()
    auth_header.create_header('/')
    
    # Initialize filter state
    filter_state = {
        'location1': 'Region',
        'location2': '',
        'product1': 'Franchise',  # Default value
        'product2': '',
        'level': ''
    }
    
    async def update_ui(filtered_df):
        """Update all UI components after filter changes"""
        print(f"DEBUG: update_ui called with filtered_df: {filtered_df is not None}, rows: {len(filtered_df) if filtered_df is not None else 0}")
        try:
            # Note: Loading states should already be cleared by the calling function
            # We don't set them to True here since data is already loaded
            state = get_global_state()
            print(f"DEBUG: Loading states at start of update_ui - charts: {state.loading_charts}, table: {state.loading_table}, data: {state.loading_data}")

            # Force UI update to show current state (non-blocking)
            try:
                await ui.run_javascript('void 0', timeout=1.0)  # Increased timeout for large datasets
            except Exception as js_error:
                print(f"DEBUG: JavaScript update timeout (expected with large datasets): {js_error}")
                # Continue anyway - components will re-render when updated

            # Update charts (loading state should already be False)
            print(f"DEBUG: Calling update_charts with containers - column: {chart_components.column_chart_content}, line: {chart_components.line_chart_content}")
            try:
                await update_charts(chart_components.column_chart_content,
                                 chart_components.line_chart_content, filtered_df)
                print("DEBUG: update_charts completed successfully")
            except Exception as e:
                print(f"DEBUG: Error in update_charts: {e}")
                raise

            # Update details table
            print("DEBUG: Calling create_table")
            await details_table.create_table(filtered_df, details_container)

            # Ensure all loading states are cleared after UI update
            state = get_global_state()
            state.set_loading_state('charts', False)
            state.set_loading_state('table', False)
            state.set_loading_state('data', False)
            print(f"DEBUG: Final loading states - charts: {state.loading_charts}, table: {state.loading_table}, data: {state.loading_data}")

            print("DEBUG: UI update completed successfully")
        except Exception as e:
            # Clear loading states on error
            state = get_global_state()
            print(f"DEBUG: Error in update_ui, clearing loading states: {e}")
            state.set_loading_state('charts', False)
            state.set_loading_state('table', False)
            ErrorHandler.handle_ui_update_error(e, "UI update")
    
    async def on_filter_change(filter_name, value):
        """Handle filter change events"""
        print(f"DEBUG: Filter change detected - {filter_name}: {value}")
        
        # Update the filter state first
        if filter_name:  # Only update if filter_name is not empty
            filter_state[filter_name] = value
        
        # Get the current state
        state = get_global_state()
        
        # Set loading states
        state.set_loading_state('charts', True, 'Loading filtered data...')
        state.set_loading_state('table', True, 'Loading filtered data...')
        state.set_loading_state('data', True, 'Processing filters...')
        
        print(f"DEBUG: Loading states set - charts: {state.loading_charts}, table: {state.loading_table}")
        
        # Handle specific filter changes that require UI updates
        if filter_name == 'location1':
            # Update location options when location1 changes
            try:
                options = state.get_filter_options(
                    filter_state.get('product1', 'Franchise'),
                    value  # The new location1 value
                )
                if 'locations_filt' in options:
                    filter_components.update_location_options(options['locations_filt'])
                    print(f"DEBUG: Updated location options with {len(options['locations_filt'])} items")
                
                # Update level options based on the new filters
                if 'levels' in options:
                    filter_components.update_level_options(options['levels'])
                    
            except Exception as e:
                print(f"Error updating location options: {e}")
        
        elif filter_name == 'product1':
            # Update product options when product1 changes
            try:
                options = state.get_filter_options(
                    value,  # The new product1 value
                    filter_state.get('location1', 'Region')
                )
                if 'products_filt' in options:
                    filter_components.update_product_options(options['products_filt'])
                    print(f"DEBUG: Updated product options with {len(options['products_filt'])} items")
                
                # Update level options based on the new filters
                if 'levels' in options:
                    filter_components.update_level_options(options['levels'])
                    
            except Exception as e:
                print(f"Error updating product options: {e}")
        
        # Force UI update to show loading indicators immediately
        try:
            await ui.run_javascript('void 0', timeout=0.5)
        except Exception as js_error:
            print(f"DEBUG: JavaScript update timeout on filter change (expected): {js_error}")
        
        # Process the filter change if we have all required filters
        if filter_name in ['location1', 'location2', 'product1', 'product2', 'level']:
            print("DEBUG: Processing location/product/level filter change")
            
            # Check if we have complete filter conditions before loading data
            load_data_condition = (
                (filter_state.get('location1') and filter_state.get('location2')) and
                (filter_state.get('product1') and filter_state.get('product2'))
            )
        
        # Force UI update to show loading indicators immediately
        try:
            await ui.run_javascript('void 0', timeout=0.5)
        except Exception as js_error:
            print(f"DEBUG: JavaScript update timeout on filter change (expected): {js_error}")
        
        # Process the filter change
        if filter_name in ['location1', 'location2', 'product1', 'product2', 'level']:
            print("DEBUG: Processing location/product/level filter change")
            
            # Check if we have complete filter conditions before loading data
            load_data_condition = (
                (filter_state.get('location2') and filter_state.get('location1')) and
                (filter_state.get('product2') and filter_state.get('product1'))
            )
            print(f"DEBUG: Load data condition met: {load_data_condition}")
            print(f"DEBUG: Current filter state: location1={filter_state.get('location1')}, location2={filter_state.get('location2')}, product1={filter_state.get('product1')}, product2={filter_state.get('product2')}")
            
            if load_data_condition:
                print("DEBUG: All filters set, proceeding with data loading")
                await process_filter_change(filter_state)
            else:
                print("DEBUG: Not all filters set yet, waiting for complete filter selection")
                # Clear loading states since we're not processing yet
                state.set_loading_state('charts', False)
                state.set_loading_state('table', False)
                state.set_loading_state('data', False)
        elif filter_name == 'data_files':
            print("DEBUG: Processing data file change")
            await process_data_file_change(value)
        
        if filter_name == 'location1':
            filter_components.location_select2._props.update({'label': value})
            options = get_filter_options(
                filter_state.get('product1'), filter_state.get('location1')
            )['locations_filt']
            filter_components.update_location_options(options)
        
        if filter_name == 'product1':
            filter_components.product_select2._props.update({'label': value})
            options = get_filter_options(
                filter_state.get('product1'), filter_state.get('location1')
            )['products_filt']
            filter_components.update_product_options(options)
        
    async def process_filter_change(filter_state):
        """Process location/product/level filter changes"""
        print(f"DEBUG: process_filter_change called with filter_state: {filter_state}")
        try:
            # Check data size before loading
            print("DEBUG: Checking data size before loading...")
            size_check_result = await check_data_size_before_loading(filter_state)
            print(f"DEBUG: Data size check result: {size_check_result}")
            
            if size_check_result:
                print("DEBUG: Data size check passed, loading filtered data...")
                await load_filtered_data(filter_state)
                print("DEBUG: Filtered data loading completed")
            else:
                print("DEBUG: Data size check failed, clearing loading states")
                state = get_global_state()
                state.set_loading_state('charts', False)
                state.set_loading_state('table', False)
                state.set_loading_state('data', False)
        except Exception as e:
            print(f"DEBUG: Error in process_filter_change: {e}")
            import traceback
            print(f"DEBUG: Full traceback: {traceback.format_exc()}")
            state = get_global_state()
            state.set_loading_state('charts', False)
            state.set_loading_state('table', False)
            state.set_loading_state('data', False)
    
    async def process_data_file_change(value):
        """Process data file changes"""
        try:
            # Load sample data from database
            state = get_global_state()
            df = state.load_sample_data()
            if df is not None:
                app.storage.user['dwn_df_json'] = df.write_json()
                # Store current filter state (empty for sample data)
                app.storage.user['current_filters'] = {
                    'location1': None,
                    'location2': None,
                    'product1': None,
                    'product2': None
                }
                await update_ui(df)
        except Exception as e:
            print(f"DEBUG: Error processing data file change: {e}")
            state = get_global_state()
            state.set_loading_state('charts', False)
            state.set_loading_state('table', False)
            state.set_loading_state('data', False)
        
        # Apply filters when both location and product filters are set
        load_data_condition = (
            (filter_state.get('location2') and filter_state.get('location1')) and
            (filter_state.get('product2') and filter_state.get('product1'))
        )

        if load_data_condition:
            try:
                # Set loading state for data fetching
                state = get_global_state()
                state.set_loading_state('data', True, 'Loading data from database...')
                
                # Check data size before loading to prevent memory issues
                if await check_data_size_before_loading(filter_state):
                    # Apply filters by querying database directly
                    filtered_result = apply_filters(filter_state)
                    await update_ui(filtered_result['filtered_df'])
                    app.storage.user['dwn_df_json'] = filtered_result['fdf']
                else:
                    # Data too large, don't load
                    state.set_loading_state('data', False)
                    return
            finally:
                # Clear data loading state
                state.set_loading_state('data', False)
    
    async def check_data_size_before_loading(filter_state):
        """Check estimated data size before loading and warn user if too large"""
        try:
            db_service = DatabaseUtils.get_database_service()
            if db_service is None:
                return True

            # Estimate data size based on filters - handle partial filters
            estimated_rows = db_service.estimate_filtered_data_size(
                location_col=filter_state.get('location1'),
                location_val=filter_state.get('location2'),
                product_col=filter_state.get('product1'),
                product_val=filter_state.get('product2')
            )

            # Rough estimation: each row ~1KB, so 16GB = ~16M rows
            max_rows = 16_000_000

            if estimated_rows > max_rows:
                UIUtils.show_error_message(
                    f"Warning: Estimated data size ({estimated_rows:,} rows) may exceed 16GB RAM limit. "
                    f"Please apply more specific filters or contact admin.",
                    type='warning'
                )
                return False
            elif estimated_rows > max_rows * 0.8:  # 80% of limit
                UIUtils.show_info_message(
                    f"Caution: Large dataset ({estimated_rows:,} rows) detected. "
                    f"Loading may take time and use significant memory."
                )

            return True  # Proceed with loading

        except Exception as e:
            UIUtils.show_error_message(f"Error estimating data size: {str(e)}", type='warning')
            return True  # Proceed anyway if estimation fails

    async def load_filtered_data(filter_state):
        """Load and process filtered data from database"""
        print(f"DEBUG: load_filtered_data called with filter_state: {filter_state}")
        try:
            print("DEBUG: Loading filtered data from database...")
            db_service = DatabaseUtils.get_database_service()
            if db_service is None:
                print("DEBUG: Database service is None, clearing loading states")
                state = get_global_state()
                state.set_loading_state('charts', False)
                state.set_loading_state('table', False)
                state.set_loading_state('data', False)
                return

            state = get_global_state()

            # Load data with filters applied at database level
            print(f"DEBUG: Querying database with filters: location_col={filter_state.get('location1')}, location_val={filter_state.get('location2')}, product_col={filter_state.get('product1')}, product_val={filter_state.get('product2')}")
            dwn.df = db_service.get_filtered_sales_actuals(
                location_col=filter_state.get('location1'),
                location_val=filter_state.get('location2'),
                product_col=filter_state.get('product1'),
                product_val=filter_state.get('product2')
            )
            
            print(f"DEBUG: Database query completed, rows returned: {len(dwn.df) if dwn.df is not None else 0}")
            
            if dwn.df is None or len(dwn.df) == 0:
                print("DEBUG: No data returned from database, clearing loading states")
                state.set_loading_state('charts', False)
                state.set_loading_state('table', False)
                state.set_loading_state('data', False)
                return

            # Apply standard data preparation
            print("DEBUG: Applying data preparation...")
            dwn.df = DataUtils.prepare_data_for_ui(dwn.df)
            print(f"DEBUG: Data preparation completed, final rows: {len(dwn.df)}")

            # Update state and storage
            state.df = dwn.df.clone()
            state.full_df = dwn.df.clone()
            state.filtered_df = dwn.df.clone()
            app.storage.user['dwn_df_json'] = dwn.df.write_json()
            
            # Store current filter state for raw_data_page
            app.storage.user['current_filters'] = {
                'location1': filter_state.get('location1'),
                'location2': filter_state.get('location2'),
                'product1': filter_state.get('product1'),
                'product2': filter_state.get('product2')
            }
            print(f"DEBUG: Stored current filters in app storage: {app.storage.user['current_filters']}")

            print("DEBUG: State updated, calling update_ui...")
            # Update chart titles with current filter values
            chart_components.update_chart_titles({
                'location': filter_state.get('location2', 'All Locations'),
                'product': filter_state.get('product2', 'All Products')
            })
            await update_ui(dwn.df)
            print("DEBUG: update_ui completed successfully")

        except Exception as e:
            print(f"DEBUG: Error in load_filtered_data: {e}")
            import traceback
            print(f"DEBUG: Full traceback: {traceback.format_exc()}")
            state = get_global_state()
            state.set_loading_state('charts', False)
            state.set_loading_state('table', False)
            state.set_loading_state('data', False)
            ErrorHandler.handle_data_loading_error(e, "Filtered data loading")
        finally:
            # Clear loading state
            state.set_loading_state('data', False)
    with ui.card().classes('w-full h-full p-2'):
        # Create filter components
        filter_components = FilterComponents(filter_state, on_filter_change)
        filter_components.create_filter_row()
        
        # Initialize with default filter options
        try:
            state = get_global_state()
            # Get initial filter options
            options = state.get_filter_options()
            
            # Update the filter components with initial values
            if 'products' in options and hasattr(filter_components, 'product_select1'):
                filter_components.product_select1.options = options['products']
                filter_components.product_select1.update()
                
            if 'products_filt' in options and hasattr(filter_components, 'product_select2'):
                filter_components.update_product_options(options['products_filt'])
                
            if 'locations' in options and hasattr(filter_components, 'location_select1'):
                filter_components.location_select1.options = options['locations']
                filter_components.location_select1.update()
                
            if 'locations_filt' in options and hasattr(filter_components, 'location_select2'):
                filter_components.update_location_options(options['locations_filt'])
                
            if 'levels' in options and hasattr(filter_components, 'level_select'):
                filter_components.update_level_options(options['levels'])
                
            print("DEBUG: Initialized filter components with options")
            
        except Exception as e:
            print(f"Error initializing filter components: {e}")
        
        # Main content area
        with ui.row().classes('w-full mt-2 ml-0 gap-2'):
            # Create chart components
            chart_components = ChartComponents()
            chart_components.create_charts_row()
            
            # Create action buttons
            action_buttons = ActionButtons(dwn, filter_state)
            action_buttons.create_action_buttons()

        # Bottom details panel
        details_table = DetailsTable(filter_state, update_ui)
        details_container = details_table.create_details_container()
    
    # Initialize UI with default filter values
    try:
        # Set default values if not already set
        if not filter_state.get('location1'):
            filter_state['location1'] = 'Region'
        if not filter_state.get('product1'):
            filter_state['product1'] = 'Franchise'
            
        # Trigger initial filter change to load data
        ui.timer(0.1, lambda: on_filter_change('', None), once=True)
        print("DEBUG: Initialized UI with default filter values")
        
    except Exception as e:
        print(f"Error during UI initialization: {e}")

@ui.page("/raw_data")
def raw_data_page():
    """Enhanced raw data page with forecast data integration for pivot functionality."""
    from pathlib import Path
    from core.data_service import DatabaseUtils
    import polars as pl

    # Get existing data
    #df_json = '[]'
    #if 'dwn_df_json' in app.storage.user:
    #    df_json = app.storage.user['dwn_df_json']

    # Add authentication header
    from ui.components import AuthHeader
    auth_header = AuthHeader()
    auth_header.create_header('/raw_data')

    # Query forecast data from database
    #forecast_data = []
    db_service = DatabaseUtils.get_database_service()
    column_mapping = {
            'Region': 'region',
            'Country': 'country',
            'Area': 'area',
            'Franchise': 'franchise',
            'IBP Level 5': 'ibp_level_5',
            'IBP Level 6': 'ibp_level_6',
            'CatalogNumber': 'catalog_number'
        }
    current_filters = app.storage.user['current_filters']
    forecast_query = """
        SELECT
            ff.item_skey, ff.location_skey, ff.forecast_date, ff.forecast_horizon, ff.forecast_value,
            ff.model_type, ff.forecast_cycle_month, ff.is_current
        FROM da.final_forecasts ff
        WHERE ff.is_current = TRUE
        ORDER BY ff.forecast_date DESC, ff.item_skey, ff.location_skey
    """
    forecast_query1 = """
            SELECT 
                item_skey, 
                location_skey, 
                forecast_date, 
                forecast_horizon, 
                forecast_value, 
                model_type
            FROM da.forecasts 
            WHERE 1=1
            """ + (
                f" AND location_skey IN (SELECT location_skey FROM da.location_hierarchy WHERE {column_mapping.get(current_filters.get('location1', 'country'))} = '{current_filters.get('location2', '')}')"
                if current_filters.get('location1') and current_filters.get('location2') else ""
            ) + (
                f" AND item_skey IN (SELECT demantra_item_skey FROM da.product_hierarchy WHERE {column_mapping.get(current_filters.get('product1', 'franchise'))} = '{current_filters.get('product2', '')}')"
                if current_filters.get('product1') and current_filters.get('product2') else ""
            ) + """
            ORDER BY forecast_date DESC
            """
    user_id = app.storage.user.get('user_id', 'system')
    forecast_result = db_service.execute_query(forecast_query1, user_id=user_id)
    sales_df = db_service.get_filtered_sales_actuals(
        location_col=current_filters.get('location1'),
        location_val=current_filters.get('location2'), 
        product_col=current_filters.get('product1'),
        product_val=current_filters.get('product2')
    )
    sales_df = sales_df.with_columns(pl.col('sales_date').alias('join_date'))

    forecast_df = forecast_result.clone()
    forecast_df = forecast_df.with_columns(pl.col('forecast_date').cast(pl.Datetime("us")).alias('join_date'))
    forecast_df = forecast_df.with_columns(pl.lit('forecast').alias('data_type'))

    # Perform join if both datasets exist
    if sales_df is not None and forecast_df is not None:
        # For sales data: use item_skey and country (or region/area)
        sales_df = sales_df.with_columns(
            (pl.col('item_skey').cast(pl.Utf8) + '_' + 
                pl.col('location_skey').cast(pl.Utf8)).alias('unique_id')
        )
        print(f"DEBUG: Created unique_id for sales using item_skey + location_skey")
        
        # For forecast data: use item_skey and location_skey
        forecast_df = forecast_df.with_columns(
            (pl.col('item_skey').cast(pl.Utf8) + '_' + 
                pl.col('location_skey').cast(pl.Utf8)).alias('unique_id')
        )
        
        print(f"DEBUG: Sales unique_ids sample: {sales_df['unique_id'].head(5).to_list()}")
        print(f"DEBUG: Forecast unique_ids sample: {forecast_df['unique_id'].head(5).to_list()}")
        
        # Debug: Check if there are any matching unique_ids
        sales_unique_ids = set(sales_df['unique_id'].unique().to_list())
        forecast_unique_ids = set(forecast_df['unique_id'].unique().to_list())
        matching_ids = sales_unique_ids.intersection(forecast_unique_ids)
        print(f"DEBUG: Number of matching unique_ids: {len(matching_ids)}")
        if len(matching_ids) > 0:
            print(f"DEBUG: Sample matching IDs: {list(matching_ids)[:3]}")
        else:
            print("DEBUG: No matching unique_ids found!")
            
        # Debug: Check raw values before unique_id creation
        print(f"DEBUG: Sales item_skey sample: {sales_df['item_skey'].head(3).to_list()}")
        print(f"DEBUG: Sales location_skey sample: {sales_df['location_skey'].head(3).to_list()}")
        print(f"DEBUG: Forecast item_skey sample: {forecast_df['item_skey'].head(3).to_list()}")
        print(f"DEBUG: Forecast location_skey sample: {forecast_df['location_skey'].head(3).to_list()}")
        
        # Left join to keep all sales data and add forecast where available
        combined_df = sales_df.join(
            forecast_df.select(['unique_id', 'join_date', 'forecast_value', 'model_type', 'forecast_horizon', 'data_type']),
            on=['unique_id', 'join_date'],
            how='left'
        )
        # Rename columns to match expected format in data.html
        column_mapping = {
            'act_orders_rev': 'Act Orders Rev',
            'fcst_stat_final_rev': 'Fcst Stat Final Rev',
            'fcst_stat_prelim_rev': 'Fcst Stat Prelim Rev',
            'l2_stat_final_rev': 'L2 Stat Final Rev',
            'fcst_df_final_rev': 'Fcst DF Final Rev',
            'l2_df_final_rev': 'L2 DF Final Rev',
            'act_orders_rev_val': 'Act Orders Rev Val',
            'l1_df_final_rev': 'L1 DF Final Rev',
            'l0_df_final_rev': 'L0 DF Final Rev',
            'fcst_df_final_rev_val': 'Fcst DF Final Rev Val',
            'sales_date': 'SALES_DATE',
            'country': 'Country',
            'region': 'Region',
            'area': 'Area',
            'selling_division': 'SellingDivision',
            'stryker_group_region': 'StrykerGroupRegion',
            'catalog_number': 'CatalogNumber',
            'business_sector': 'Business Sector',
            'business_unit': 'Business Unit',
            'franchise': 'Franchise',
            'product_line': 'Product Line',
            'ibp_level_5': 'IBP Level 5',
            'ibp_level_6': 'IBP Level 6',
            'ibp_level_7': 'IBP Level 7',
            'uom': 'UOM',
            'pack_content': 'PackContent',
            'model_type': 'model_type',
            'forecast_horizon': 'forecast_horizon',
            'forecast_value': 'forecast_value',
            'data_type': 'data_type'
        }
        
        print(f"Combined data shape after join: {combined_df.shape}")
        print(f"Combined data columns: {combined_df.columns}")
        combined_df=combined_df.rename(column_mapping)
        combined_df=combined_df.with_columns(data_type=pl.when(pl.col('data_type')!='forecast').then(pl.lit('sales_actuals')).otherwise(pl.col('data_type')))
        combined_data = combined_df.to_dicts()
        
        print(f"DEBUG: Sample of combined data: {combined_df}")

    # Convert various types to JSON-serializable formats
    def json_serial(obj):
        from datetime import date, datetime
        from decimal import Decimal
        
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Type {type(obj)} not serializable")
    
    # Convert to JSON with custom serialization
    combined_json = json.dumps(combined_data, default=json_serial) if combined_data else '[]'

    # Load HTML template with combined data
    ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.min.js"></script>')
    ui.add_head_html(f"<style>{(Path(__file__).parent / 'style.css').read_text()}</style>")
    with open(Path(__file__).parent / 'data.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    ui.add_body_html(html_content.replace('{{df_json}}', combined_json))


@ui.page("/llms")
async def llm():
    # Add authentication header to llms page
    from ui.components import AuthHeader
    auth_header = AuthHeader()
    auth_header.create_header('/llms')
    
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from databricks import sql
    from databricks.sdk.core import Config
    #from databricks import sdk
    from datetime import datetime, timedelta
    
    num_periods = 36
    end_date = '2025-08-01'
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    start_date = end_date - timedelta(days=(num_periods - 1) *30)
    start_date = start_date.replace(day=1)
    start_date = start_date.strftime('%Y-%m-%d')
    
    query = f"""
        SELECT top 10
        FROM
        (
            SELECT
                SellingDivision AS SellingDivision,
                COUNTRY_GROUP AS Area,
                StrykerGroupRegion AS StrykerGroupRegion,
                Region,
                Country,
                p.CatalogNumber,
                p.Business_Sector AS BusinessSector,
                p.Business_Unit AS BusinessUnit,
                p.Franchise,
                p.Product_Line AS ProductLine,
                p.IBP_Level_5 AS IBP_Level_5,
                p.IBP_Level_6 AS IBP_Level_6,
                p.IBP_Level_7 AS IBP_Level_7,
                SALES_DATE,
                p.xx_uom_conversion AS UOM,
                s.NPI_Flag AS NPI_Flag,
                p.PackContent AS PackContent,

                SUM(L0_ASP_Final_Rev) AS L0_ASP_Final_Rev,
                SUM(Act_Orders_Rev) AS Act_Orders_Rev,
                SUM(Act_Orders_Rev_Val) AS Act_Orders_Rev_Val,
                SUM(s.L2_DF_Final_Rev) AS L2_DF_Final_Rev,
                SUM(s.L1_DF_Final_Rev) AS L1_DF_Final_Rev,
                SUM(s.L0_DF_Final_Rev) AS L0_DF_Final_Rev,
                SUM(s.L2_Stat_Final_Rev) AS L2_Stat_Final_Rev,
                SUM(Fcst_DF_Final_Rev) AS Fcst_DF_Final_Rev,
                SUM(Fcst_Stat_Final_Rev) AS Fcst_Stat_Final_Rev,
                SUM(Fcst_Stat_Prelim_Rev) AS Fcst_Stat_Prelim_Rev,
                SUM(Fcst_DF_Final_Rev_Val) AS Fcst_DF_Final_Rev_Val,
                SUM(Act_Orders_Final_Rev) AS Act_Orders_Final_Rev

            FROM Envision.Demantra_CLD_Fact_Sales s

            JOIN Envision.DIM_Demantra_CLD_DemantraLocation l
                ON s.Location_sKey = l.Location_skey

            JOIN Envision.Dim_DEMANTRA_CLD_MDP_Matrix m
                ON s.MDP_Key = m.MDP_Key

            JOIN Envision.DIM_Demantra_CLD_products p
                ON s.item_skey = p.demantra_item_skey
                AND p.[Current] = 'True'

            WHERE s.SALES_DATE BETWEEN '2025-08-01' AND '2025-09-01'
            AND [Country] in ('INDIA') 

            GROUP BY
                SellingDivision,
                COUNTRY_GROUP,
                StrykerGroupRegion,
                Region,
                Country,
                p.Business_Sector,
                p.Business_Unit,
                p.Franchise,
                p.IBP_Level_5,
                p.IBP_Level_6,
                p.IBP_Level_7,
                p.Product_Line,
                SALES_DATE,
                p.CatalogNumber,
                p.Itemid,
                p.xx_uom_conversion,
                s.NPI_Flag,
                p.PackContent
        ) final
        """
    '''
    async def query(e):
        connection_string=f"Driver={{ODBC Driver 18 for SQL Server}};Server={ss};database=gda_glbsyndb;Encrypt=Yes;Authentication=ActiveDirectoryInteractive;"
        reader = read_arrow_batches_from_odbc(query=query,connection_string=connection_string,parameters=fran)
        df1=pl.DataFrame()
        df=df.filter(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=3))
        for batch in reader:
            df1=pl.concat([df1,pl.from_arrow(batch)])
        df1=df1.with_columns(pl.col('SALES_DATE').cast(pl.Datetime).dt.cast_time_unit('us'))
    return query_fact_sales
    '''
    #print(spark.table("hive_metastore.da.Fact_Sales1").limit(100))
    http_path = f"/sql/1.0/warehouses/62d47c983bb6df91"
    
    config = Config(
        host=os.getenv("DATABRICKS_HOST"),
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET")
        )


    w = WorkspaceClient(config=config)
    #cfg = Config()
    #print(cfg)
    clusters = w.clusters.list()
    for cluster in clusters:
        print(f"Cluster: {cluster.cluster_name}, ID: {cluster.cluster_id}")
    print("Available warehouses:")
    for wh in w.warehouses.list():
        print(f"- {wh.name} ({wh.id})")
    
    conn = sql.connect(server_hostname=config.host,
        #http_path="http://adb-677543366313482.2.azuredatabricks.net",
        http_path=http_path,
        credentials_provider=lambda: config.authenticate,)

    query = "SELECT * FROM hive_metastore.da.Fact_Sales1 LIMIT 100"
    print(query)
    with conn.cursor() as cursor:
        cursor.execute(query)
        df = pl.from_arrow(cursor.fetchall_arrow())
    conn.close()
    print(df)
    #spark.table("hive_metastore.da.Fact_Sales").limit(100)
    spark = DatabricksSession.builder.clusterId('0805-063508-emq3q7q8').getOrCreate()
    #print(spark.table("hive_metastore.da.Fact_Sales1").limit(100))
    #df = spark.read.table("samples.nyctaxi.trips")
      # cfg with auth for Service Principal

@ui.page("/agent")
async def agent():
    # Add authentication header to agent page
    from ui.components import AuthHeader
    auth_header = AuthHeader()
    auth_header.create_header('/agent')
    
    from ddgs import DDGS
    from bs4 import BeautifulSoup
    import requests
    from openai import OpenAI

    client = OpenAI(base_url="http://localhost:8080/v1",api_key="sk" )

    def search_web(query, max_results=5):
        with DDGS() as ddgs:
            return [r['href'] for r in ddgs.text(query, max_results=max_results,safesearch="on", backend="google, brave, yahoo")]

    def scrape_page(url):
        try:
            html = requests.get(url, timeout=5).text
            soup = BeautifulSoup(html, "html.parser")
            print(" ".join([p.get_text() for p in soup.find_all("p")])[:3000])
            return " ".join([p.get_text() for p in soup.find_all("p")])[:3000]  # limit size
        except:
            return ""
        
    class SearchQuery:
        def __init__(self):
            self._product = ""
            self._region = ""
            # Default query templates with placeholders
            self._query1_template = "{product} medical device category market growth potential for next 5 years in {region}"
            self._query2_template = "{product} medical device category competitors of Stryker in {region}"
            self._search_query1 = self._query1_template
            self._search_query2 = self._query2_template
            self._objective_template = "help demand planners generate long term forecasts by providing brief in 100 words and 3 bullet points that contains insights " \
                    "on market dynamics that can impact demand of Stryker {product} medical device in {region} in coming years both positively and negatively." \
                    "Also mention the expected CAGR over next 5 years of the category"
            self._objective = self._objective_template
            
        def _format_query(self, template):
            """Format a query template with current product and region."""
            return template.format(
                product=self._product if self._product else "{product}",
                region=self._region if self._region else "{region}"
            )
            
        @property
        def product(self):
            return self._product
            
        @product.setter
        def product(self, value):
            self._product = value
            self._update_search_queries()
            
        @property
        def region(self):
            return self._region
            
        @region.setter
        def region(self, value):
            self._region = value
            self._update_search_queries()
            
        def _update_search_queries(self):
            """Update search queries and objective with current product and region."""
            self.search_query1 = self._query1_template
            self.search_query2 = self._query2_template
            # Update the objective with current product and region
            if hasattr(self, '_objective_template'):
                self.objective = self._objective_template
            
        @property
        def search_query1(self):
            return self._format_query(self._search_query1)
            
        @search_query1.setter
        def search_query1(self, value):
            self._search_query1 = value
            self._query1_template = value
            
        @property
        def search_query2(self):
            return self._format_query(self._search_query2)
            
        @search_query2.setter
        def search_query2(self, value):
            self._search_query2 = value
            self._query2_template = value
            
        @property
        def queries(self):
            return [self.search_query1, self.search_query2]
        
        def _format_objective(self, template):
            """Format the objective template with current product and region."""
            return template.format(
                product=self._product if self._product else "{product}",
                region=self._region if self._region else "{region}"
            )
            
        @property
        def objective(self):
            return self._format_objective(self._objective)
            
        @objective.setter
        def objective(self, value):
            self._objective = value
            self._objective_template = value

    def summarize(text, prompt="Summarize:"):
        stream = client.chat.completions.create(
        model="gemma3n",  # use whatever name your server registered
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": text}
        ],
        max_tokens=500,
        stream=True
        )
        collected = ""
        for chunk in stream:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta.content
            if delta:
                yield delta
                collected += delta
        return collected

    async def run_agent(search_manager):
        output_area.clear()
        with output_area:
            for q in search_manager.queries:
                # Skip if either product or region is not specified
                if not search_manager.product or not search_manager.region:
                    ui.notify("Please enter both product and region", type='warning')
                    return
                    
                ui.notify(f"Running search: {q}")
                urls = search_web(q)
                if not urls:
                    ui.notify("No results found", type='warning')
                    continue
                
                with ui.column().classes('w-1/2 flex-shrink-0 no-wrap overflow-x-hidden'):
                    # Display the search query
                    ui.label(q).classes("font-semibold mt-4 sticky top-0 bg-white z-10")
                    text_area = ui.markdown().classes("min-h-0 overflow-visible overflow-x-hidden word-wrap")
                    
                    # Initialize content with loading message
                    text_area.set_content("## Gathering information...\n\nPlease wait while we collect and analyze the sources.")
                    
                    # Process all URLs first
                    sources = []
                    all_content = []
                    
                    for i, url in enumerate(urls, 1):
                        try:
                            # Add source to our references
                            sources.append(f'{i}. <a href="{url}" target="_blank">{url}</a>')
                            
                            # Scrape the page
                            scraped = scrape_page(url)
                            if scraped:
                                all_content.append(scraped)
                            
                        except Exception as e:
                            print(f"Error processing {url}: {str(e)}")
                    
                    if not all_content:
                        text_area.set_content("### No content found\n\nCould not retrieve any content from the sources.")
                        return
                    
                    # Combine all content
                    combined_text = "\n---\n".join(all_content)
                    
                    # Generate a single summary from all sources
                    text_area.set_content("### Analyzing information...\nCreating a comprehensive summary from all sources...")
                    
                    summary = ""
                    final_content = ""
                    text_area.set_content(final_content)

                    print(goal_input.value)
                    # Stream the combined summary
                    for token in summarize(
                        combined_text,
                        prompt=(
                            f"You are a {role_input.value}. Your task is to {goal_input.value} "
                            f"Use this as context: {q}. "
                            "Be concise but thorough in your analysis and do not output disclaimer."
                        )
                    ):
                        summary += token
                        text_area.set_content(final_content + summary)
                        await ui.run_javascript('void 0', timeout=90)
                    
                    # Add sources section
                    if sources:
                        cleaned_summary = clean_markdown_content(summary)
                        final_content = f"{cleaned_summary}\n#### Sources\n" + "\n".join(sources)
                        text_area.set_content(final_content)
    
    with ui.row(wrap=False).classes('w-full'):
        # Toggle button for sidebar
        toggle_sidebar = ui.button('⚙️', on_click=lambda: drawer.toggle()).classes('absolute top-4 right-1 z-10')
        with ui.column().classes('w-48'):
            # Cache for database results to prevent multiple queries
            cached_region_options = []
            cached_table_data = []
            
            product_input = ui.input("Enter Product",on_change=lambda e: setattr(search_manager, 'product', e.value)).classes('w-full')
            
            # Initialize region select options with caching
            if not cached_region_options:
                try:
                    db_service = DatabaseUtils.get_database_service()
                    if db_service is not None:
                        user_id = app.storage.user.get('user_id', 'system')
                        country_query = """
                            SELECT DISTINCT country 
                            FROM da.location_hierarchy 
                            WHERE country IS NOT NULL 
                            ORDER BY country
                        """
                        country_result = db_service.execute_query(country_query, user_id=user_id)
                        if country_result is not None and len(country_result) > 0:
                            cached_region_options[:] = ['All Regions'] + [row['country'] for row in country_result[['country']].unique().to_dicts()]
                except Exception as e:
                    print(f"Error loading region options: {e}")
                    cached_region_options[:] = ['All Regions']
            
            region_input = ui.select(
                label="Select Region",
                options=cached_region_options,
                value='All Regions',
                with_input=True,
                on_change=lambda e: filter_table_by_region(e.value)).classes('w-full')
            
            def filter_table_by_region(selected_region):
                """Filter the merged table based on selected region"""
                try:
                    if selected_region == 'All Regions':
                        merged_table.rows = cached_table_data
                    else:
                        # Use more efficient filtering with list comprehension
                        filtered_data = [row for row in cached_table_data if row['country'] == selected_region]
                        merged_table.rows = filtered_data
                    merged_table.update()
                    
                    # Update search manager region
                    setattr(search_manager, 'region', selected_region if selected_region != 'All Regions' else '')
                except Exception as e:
                    print(f"Error filtering table: {e}")
                    # Fallback to show all data
                    merged_table.rows = cached_table_data
                    merged_table.update()
            
            search_button = ui.button("Search",on_click=lambda: run_agent(search_manager)).classes("mt-4")

        with ui.row().classes('w-full'):
            # Use cached data from the first column
            all_table_data = cached_table_data
            
            # Create the table first
            merged_table=ui.table(columns=[
                {'label':'Business Unit','name':'Business Unit','field':'business_unit', 'align': 'left'}, 
                {'label':'Country','name':'Country','field':'country', 'align': 'left'},
                {'label':'Last Year YoY','name':'Last Year YoY','field':'last_year_yoy', 'align': 'right', ':format': 'value => value ? value + "%" : "N/A"'},
                {'label':'YTD Growth','name':'YTD Growth','field':'ytd_growth', 'align': 'right', ':format': 'value => value ? value + "%" : "N/A"'}
            ], rows=[],row_key="business_unit",selection='single').style("height:700px;width:500px;overflow-y: auto;")
            
            # Initialize tables with database data only if not already cached
            def initialize_tables():
                """Initialize Product Line and Country tables with data from database"""
                if cached_table_data:  # Already loaded
                    print(f"Using cached data: {len(cached_table_data)} combinations")
                    merged_table.rows = cached_table_data
                    merged_table.update()
                    return
                    
                db_service = DatabaseUtils.get_database_service()
                if db_service is None:
                    print("Database service not available for table initialization")
                    return
                
                user_id = app.storage.user.get('user_id', 'system')
                
                # Initialize merged Product Line and Country table
                try:
                    # Optimized query with YoY growth and YTD growth calculation
                    combined_query = """
                        WITH yearly_sales AS (
                            SELECT 
                                p.business_unit,
                                l.country,
                                YEAR(s.sales_date) as sales_year,
                                SUM(s.act_orders_rev) as total_sales
                            FROM da.sales_actuals s
                            JOIN da.product_hierarchy p ON s.item_skey = p.demantra_item_skey
                            JOIN da.location_hierarchy l ON s.location_skey = l.location_skey
                            WHERE p.business_unit IS NOT NULL 
                            AND l.country IS NOT NULL
                            AND s.act_orders_rev > 0
                            GROUP BY p.business_unit, l.country, YEAR(s.sales_date)
                        ),
                        ytd_sales AS (
                            SELECT 
                                p.business_unit,
                                l.country,
                                YEAR(s.sales_date) as sales_year,
                                SUM(s.act_orders_rev) as ytd_sales
                            FROM da.sales_actuals s
                            JOIN da.product_hierarchy p ON s.item_skey = p.demantra_item_skey
                            JOIN da.location_hierarchy l ON s.location_skey = l.location_skey
                            WHERE p.business_unit IS NOT NULL 
                            AND l.country IS NOT NULL
                            AND s.act_orders_rev > 0
                            AND s.sales_date <= CURRENT_DATE
                            AND YEAR(s.sales_date) >= YEAR(CURRENT_DATE) - 1
                            -- Ensure same months comparison: for previous year, only include up to current month
                            AND (
                                YEAR(s.sales_date) = YEAR(CURRENT_DATE)
                                OR
                                (YEAR(s.sales_date) = YEAR(CURRENT_DATE) - 1 AND MONTH(s.sales_date) <= MONTH(CURRENT_DATE))
                            )
                            GROUP BY p.business_unit, l.country, YEAR(s.sales_date)
                        ),
                        growth_metrics AS (
                            -- Last year's YoY growth (completed year)
                            SELECT 
                                curr.business_unit,
                                curr.country,
                                'last_year_yoy' as metric_type,
                                CASE 
                                    WHEN prev.total_sales > 0 THEN 
                                        ROUND(((curr.total_sales - prev.total_sales) / prev.total_sales) * 100, 2)
                                    ELSE NULL
                                END as growth_value
                            FROM yearly_sales curr
                            LEFT JOIN yearly_sales prev ON 
                                curr.business_unit = prev.business_unit 
                                AND curr.country = prev.country 
                                AND curr.sales_year = prev.sales_year + 1
                            WHERE curr.sales_year = YEAR(CURRENT_DATE) - 1
                            
                            UNION ALL
                            
                            -- Current year YTD growth
                            SELECT 
                                curr.business_unit,
                                curr.country,
                                'ytd_growth' as metric_type,
                                CASE 
                                    WHEN prev.ytd_sales > 0 THEN 
                                        ROUND(((curr.ytd_sales - prev.ytd_sales) / prev.ytd_sales) * 100, 2)
                                    ELSE NULL
                                END as growth_value
                            FROM ytd_sales curr
                            LEFT JOIN ytd_sales prev ON 
                                curr.business_unit = prev.business_unit 
                                AND curr.country = prev.country 
                                AND curr.sales_year = prev.sales_year + 1
                            WHERE curr.sales_year = YEAR(CURRENT_DATE)
                        )
                        SELECT DISTINCT 
                            g.business_unit, 
                            g.country,
                            MAX(CASE WHEN g.metric_type = 'last_year_yoy' THEN g.growth_value END) as last_year_yoy,
                            MAX(CASE WHEN g.metric_type = 'ytd_growth' THEN g.growth_value END) as ytd_growth
                        FROM growth_metrics g
                        GROUP BY g.business_unit, g.country
                        ORDER BY g.business_unit, g.country
                    """
                    combined_result = db_service.execute_query(combined_query, user_id=user_id)
                    if combined_result is not None and len(combined_result) > 0:
                        # Handle NULL values properly
                        combined_data = []
                        for row in combined_result[['business_unit', 'country', 'last_year_yoy', 'ytd_growth']].to_dicts():
                            combined_data.append({
                                'business_unit': row['business_unit'],
                                'country': row['country'],
                                'last_year_yoy': round(float(row['last_year_yoy']), 2) if row['last_year_yoy'] is not None else None,
                                'ytd_growth': round(float(row['ytd_growth']), 2) if row['ytd_growth'] is not None else None
                            })
                        merged_table.rows = combined_data
                        # Store in both caches
                        cached_table_data[:] = combined_data
                        all_table_data[:] = combined_data
                        merged_table.update()
                        print(f"Loaded {len(combined_data)} Business Unit and Country combinations from database")
                    else:
                        print("No data found in sales_actuals")
                except Exception as e:
                    print(f"Error initializing merged table: {e}")
            
            # Initialize tables only once
            initialize_tables()
            
            # Set up selection handler after table is created
            merged_table.on_select(lambda e: (
                product_input.set_value(e.selection[0]['business_unit']),
                region_input.set_value(e.selection[0]['country'])
            ))
            
            # Initialize table with stored data
            if all_table_data:
                merged_table.rows = all_table_data
                merged_table.update()
            
            with ui.column().classes('w-3/4 p-2'):
                # Main content column - increased width for better output visibility
                ui.label("Medical Device Market Research Agent").classes("text-xl font-bold")
                output_area = ui.row().classes("p-2 bg-gray-100 rounded gap-2 flex-wrap")
    
    # Create right-side drawer for agent configuration
    with ui.drawer('right',value=False).classes('bg-gray-50') as drawer:
        drawer.props('width=300')
        with ui.column().classes('w-full gap-2'):
            ui.label('Agent Configuration').classes('text-xl font-bold mb-4')
            
            # Initialize search manager
            search_manager = SearchQuery()
            
            search_input1 = ui.textarea(
                label="Search Query 1",
                value=search_manager._query1_template,
                on_change=lambda e: setattr(search_manager, 'search_query1', e.value)
            ).props('input-style="height:65px"').classes('w-full')
            
            search_input2 = ui.textarea(
                label="Search Query 2",
                value=search_manager._query2_template,
                on_change=lambda e: setattr(search_manager, 'search_query2', e.value)
            ).props('input-style="height:65px"').classes('w-full')
            
            # Button to reset to default queries
            ui.button(
                "Reset to Default",
                on_click=lambda: [
                    search_input1.set_text(search_manager._query1_template),
                    search_input2.set_text(search_manager._query2_template)
                ]
            ).classes('mt-4')
            
            role_input = ui.select(
                label='Role',
                options=[
                    'Demand Planner',
                    'Market Analyst',
                    'Business Development Manager',
                ],value='Demand Planner'
            ).classes('w-full')
            
            goal_input = ui.textarea(
                label='Objective',
                value=search_manager.objective,
                on_change=lambda e: setattr(search_manager, 'objective', e.value)
            ).props('input-style="height:180px"').classes('w-full')
            
            # Update the goal input when search_manager's objective changes
            #def update_goal_input():
            #    goal_input.set_value(search_manager.objective)
                
            # Add a callback to update the goal input when product or region changes
            #def on_product_region_change(e):
            #    update_goal_input()
                
            # Set up change handlers for product and region inputs
            #product_input.on_change(on_product_region_change)
            #region_input.on_change(on_product_region_change)