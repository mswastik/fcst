from nicegui import ui,run,app
from models.data_model import get_filter_options, filtered_products, filtered_models, generate_sample_data, filtered_df
from services.data_service import (
    apply_filters, toggle_month_view, create_models_action, 
    generate_fc_action, change_fc_action, download_data_action, export_data_action
)
from sql import sqlpd,query_st
from ui.charts import update_charts
import os
import asyncio
import polars as pl
import json
import io

if not os.path.exists('data/'):
    os.makedirs('data/')

class dwn_data():
    def __init__(self):
        self.lhv,self.lvv,self.phv,self.pvv,self.pmv,self.fmv='','','','',6,6
        self.df=pl.DataFrame()
        self.sp=False
        self.row_lab=''

@ui.page("/")
def create_dashboard():
    ui.colors(primary='#555')
    dwn = dwn_data()
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
        #products_list.clear()
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
            #ui.label('Selected Product and Model data').classes('text-lg font-bold')
            if len(filtered_df) > 0:
                #ui.label(f"{len(filtered_products)} products and {len(filtered_models)} models selected")
                f1=filtered_df.with_columns(pl.col('SALES_DATE').dt.date())

                async def on_row_click_handler(e):
                    print(e)
                    catalog_number = e.args[1]['CatalogNumber'] # Assuming 'CatalogNumber' is in the row data
                    filter_state['product1'] = 'CatalogNumber'
                    filter_state['product2'] = catalog_number
                    
                    # Apply filters and update UI
                    # apply_filters updates the global filtered_df in models.data_model
                    apply_filters(filter_state) 
                    from models.data_model import filtered_df as global_filtered_df
                    await update_ui(global_filtered_df)
                    ui.notify(f"Filtered by CatalogNumber: {catalog_number}", type='info')

                if filter_state['level']:
                    ui.table.from_polars(f1.pivot('SALES_DATE',index=[filter_state['location1'],filter_state['level']],values='`Act Orders Rev',aggregate_function='sum',sort_columns=True),pagination=10).classes('w-full').props('virtual-scroll').on('rowClick', on_row_click_handler)
                else:
                    ui.table.from_polars(f1.pivot('SALES_DATE',index=filter_state['location1'],values='`Act Orders Rev',aggregate_function='sum',sort_columns=True),pagination=10).classes('w-full').props('virtual-scroll').on('rowClick', on_row_click_handler)
                #ui.table.from_polars(filtered_df,pagination=10)
    
    async def on_filter_change(filter_name, value):
        """Handle filter change events"""
        global filtered_df
        filter_state[filter_name] = value
        if filter_name=='data_files':
            #dwn.df=pl.read_parquet(f'data/{value}')
            dwn.df=generate_sample_data(f'data/{value}')
            app.storage.user['dwn_df_json'] = dwn.df.write_json()
        if filter_name=='location1':
            location_select2._props.update({'label':value})
            if filter_state.get('product1')!=None:
                location_select2.options=get_filter_options(filter_state.get('product1'),filter_state.get('location1'))['locations_filt']
            else:
                location_select2.options=get_filter_options(loc=filter_state.get('location1'))['locations_filt']
            location_select2.update()
        if filter_name=='product1':
            product_select2._props.update({'label':value})
            if filter_state.get('location1')!=None:
                product_select2.options=get_filter_options(filter_state.get('product1'),filter_state.get('location1'))['products_filt']
            else:
                product_select2.options=get_filter_options(prod=filter_state.get('product1'))['products_filt']
            product_select2.update()
        if ((filter_state.get('location2')) and (filter_state.get('location1'))) or ((filter_state.get('product2')) and (filter_state.get('product1'))):
            filtered_df = apply_filters(filter_state)
            await update_ui(filtered_df['filtered_df'])
        filtered_df=filtered_df['filtered_df']

    
    def on_month_toggle(e):
        """Handle month view toggle"""
        toggle_month_view(e.value)
        update_charts(column_chart_container, line_chart_container) 
        
    def on_download(e):
        #global lhv,lvv,phv,pvv,pmv,fmv
        elapsed_time = 0
        async def update_timer():
            nonlocal elapsed_time
            if dwn.sp:
                elapsed_time += 1  # Update every 100ms
                time_lab.text = f'Time elapsed: {elapsed_time:.0f} seconds'
        async def dwn_diag(e):
            #asyncio.run(sqlpd(lh,lv,ph,pv,fm))
            dwn.sp = True
            timer.activate()
            try:
                row_count = await run.io_bound(query_st, dwn.lhv, dwn.lvv, dwn.phv, dwn.pvv, int(dwn.pmv), int(dwn.fmv))
                dwn.row_lab = f"Downloading {row_count} rows..."
                await run.io_bound(sqlpd, dwn.lhv, dwn.lvv, dwn.phv, dwn.pvv,int(dwn.pmv),int(dwn.fmv))
                ui.notify('Download complete!', type='success')
            except Exception as e:
                ui.notify(f'Download failed: {e}', type='error')
            finally:
                dwn.sp = False
                timer.deactivate()
                dwn.row_lab = '' # Clear the label

        with ui.dialog() as dialog, ui.card():
            ui.label("Select filter parameters to download")
            with ui.row():
                lh=ui.select(options=['StrykerGroupRegion','Region','Country'],label='Location').bind_value(dwn,'lhv').classes('w-48')
                lv=ui.input(label='Location Values',placeholder='Enter comma seperated values').bind_value(dwn,'lvv').classes('w-80')
            with ui.row():
                ph=ui.select(options=['Franchise','Business_Unit','IBP_Level_5','CatalogNumer'],label='Product').bind_value(dwn,'phv').classes('w-48')
                pv=ui.input(label='Product Values',placeholder='Enter comma seperated values').bind_value(dwn,'pvv').classes('w-80')
            with ui.row():
                pm=ui.number(label='Past Months').bind_value(dwn,'pmv')
                fm=ui.number(label='Future Months').bind_value(dwn,'fmv')
            with ui.row():
                ui.button('Download',on_click=dwn_diag)
                ui.button('Cancel',on_click=dialog.close).classes('ml-auto')
            with ui.row():
                sp=ui.spinner(size='lg').bind_visibility(dwn,'sp')
                row_lab=ui.label('').bind_text(dwn,'row_lab')
                time_lab = ui.label()
                timer =ui.timer(1.0, update_timer, active=False)
        dialog.open()
    
    # Create UI layout
    with ui.card().classes('w-full h-full p-2'):
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
                options=options['locations'],
                with_input=True,
                on_change=lambda e: on_filter_change('location1', e.value)
            ).classes('w-40').bind_value(filter_state,'location1')
            
            location_select2 = ui.select(
                label='Location',
                options= options['locations_filt'],
                with_input=True,
                on_change=lambda e: on_filter_change('location2', e.value)
            ).classes('w-40').bind_value(filter_state,'location2')
            
            product_select1 = ui.select(
                label='Product',
                options=options['products'],
                with_input=True,
                on_change=lambda e: on_filter_change('product1', e.value),
                clearable=True
            ).classes('w-40').bind_value(filter_state,'product1')
            
            product_select2 = ui.select(
                label='Product',
                options= options['products_filt'],
                with_input=True,
                on_change=lambda e: on_filter_change('product2', e.value),
                clearable=True
            ).classes('w-40').bind_value(filter_state,'product2')
            
            level_select = ui.select(
                label='Level',
                options=[''] + options['levels'],
                with_input=True,
                on_change=lambda e: on_filter_change('level', e.value)
            ).classes('w-40')
            #ui.button('Download Data', on_click=lambda: ui.notify(download_data_action(), type='info')).classes('ml-auto')
            ui.button('Download Data', on_click=on_download).classes('ml-auto')
        
        # Main content area
        with ui.row().classes('w-full mt-2 ml-0 gap-2'):
            # Left sidebar with lists
            '''with ui.column().classes('w-1/6 gap-2'):
                # Filtered Products list
                with ui.card().classes('w-full h-64'):
                    ui.label('Filtered Products').classes('font-bold')
                    products_list = ui.list()'''
                
            # Models list
            with ui.column().classes('w-1/6 gap-2'):
                with ui.card().classes('w-full h-96'):
                    ui.label('Models for filtered Products').classes('font-bold')
                    models_list = ui.list()
            
            # Charts and controls section
           # with ui.column().classes('w-full gap-0'):
            # Charts row
            with ui.row().classes('w-[1190px] gap-2 mr-0'):
                # Column chart
                with ui.column().classes('w-[590px] h-96 gap-0'):
                    column_chart_container = ui.card().classes('w-full h-full')
                
                # Line chart
                with ui.column().classes('w-[590px] h-96 gap-0'):
                    line_chart_container = ui.card().classes('w-full h-full')
                
            # Toggle and export row
            with ui.row().classes('w-full justify-between items-center mt-2'):
                with ui.row().classes('items-center gap-2'):
                    ui.switch(on_change=on_month_toggle)
                    ui.label('By Month')
                
                with ui.row().classes('gap-2'):
                    async def run_create_models():
                        ui.notify('Creating models...', type='info')
                        dwn.df = await run.cpu_bound(create_models_action, dwn.df, filter_state['data_files'])
                        await update_ui(dwn.df)
                        ui.notify('Models created and forecasts saved!', type='success')

                    ui.button('Create Models', on_click=run_create_models).classes('bg-green-100')
                    ui.button('Generate FC', on_click=lambda: ui.notify(generate_fc_action(), type='info')).classes('bg-green-100')
                    ui.button('Change FC', on_click=lambda: ui.notify(change_fc_action(), type='info')).classes('bg-green-100')
                    ui.button('View', on_click=lambda: ui.navigate.to('/raw_data', new_tab=True))
        
        # Bottom details panel
        with ui.card().classes('w-full m-0 p-0 h-full'):
            details_container = ui.column().classes('w-full h-full')
            with details_container:
                ui.label('Select Product and Model data').classes('p-2 text-lg font-bold')
                
    try:
    # Initialize UI with current data
        on_filter_change('', None)  # Trigger initial filter application
    except:
        pass
'''
@ui.page("/raw_data")
def raw_data_page():
    from pathlib import Path
    ui.add_head_html('<script src="https://cdnjs.cloudflare.com/ajax/libs/jquery/3.7.1/jquery.min.js" integrity="sha512-v2CJ7UaYy4JwqLDIrZUI/4hqeoQieOmAZNXBeQyjo21dadnwR+8ZaIJVT8EE2iyI61OV8e6M8PP2/4hpQINQ/g==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>')
    ui.add_head_html('<script src="https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.14.1/jquery-ui.min.js" integrity="sha512-MSOo1aY+3pXCOCdGAYoBZ6YGI0aragoQsg1mKKBHXCYPIWxamwOE7Drh+N5CPgGI5SA9IEKJiPjdfqWFWmZtRA==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>')
    ui.add_head_html('<script src="https://cdnjs.cloudflare.com/ajax/libs/plotly.js/3.0.1/plotly.min.js" integrity="sha512-GvBV4yZL+5zT68skQaXRW80H+S82WupIppDunAVt6pCLVdFmTv9tstetOqXv76L/z9WFl+0zY28QFKu0LAVFSg==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>')
    ui.add_head_html(f"<script>{(Path(__file__).parent / 'pivot.js').read_text()}</script>") 
    #ui.add_head_html('<script src="https://cdnjs.cloudflare.com/ajax/libs/pivottable/2.23.0/pivot.min.js" integrity="sha512-XgJh9jgd6gAHu9PcRBBAp0Hda8Tg87zi09Q2639t0tQpFFQhGpeCgaiEFji36Ozijjx9agZxB0w53edOFGCQ0g==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>')
    #ui.add_head_html('<script src="https://cdnjs.cloudflare.com/ajax/libs/pivottable/2.23.0/d3_renderers.min.js" integrity="sha512-qxm3as02fhBV1Z8J8VjE5jQDm/xqF4kuQZRYgK2XeolnGiZFLAXX3XCUp+VdiPv7cX6sv83p6Mht0vXrHMEX+w==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>')
    ui.add_head_html('<script src="https://cdnjs.cloudflare.com/ajax/libs/pivottable/2.23.0/plotly_renderers.min.js" integrity="sha512-nGY6wbyP3gWxAjsZwsjWahe9nKLCTTyTLn1dpwuNHb9CKLjogHAMVIbbr4wNYL0dKOsWTCrlpx9RDY+bB1MFrQ==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>')
    ui.add_head_html('<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.14.1/themes/base/jquery-ui.min.css" integrity="sha512-TFee0335YRJoyiqz8hA8KV3P0tXa5CpRBSoM0Wnkn7JoJx1kaq1yXL/rb8YFpWXkMOjRcv5txv+C6UluttluCQ==" crossorigin="anonymous" referrerpolicy="no-referrer" />')
    ui.add_head_html('<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jqueryui/1.14.1/themes/base/theme.min.css" integrity="sha512-lfR3NT1DltR5o7HyoeYWngQbo6Ec4ITaZuIw6oAxIiCNYu22U5kpwHy9wAaN0vvBj3U6Uy2NNtAfiaTiaKcDxfhTg==" crossorigin="anonymous" referrerpolicy="no-referrer" />')
    ui.add_head_html(f"<style>{(Path(__file__).parent / 'style.css').read_text()}</style>") 
    if 'dwn_df_json' in app.storage.user:
        df_json = app.storage.user['dwn_df_json']
        df = pl.read_json(io.StringIO(df_json),infer_schema_length=None)
        if not df.is_empty():
            ui.label('Raw Data View').classes('text-2xl font-bold')
            #ui.table.from_polars(df).classes('w-full h-full')
            ui.add_body_html('<div id="output"></div>')
            ui.run_javascript(f\\\'''var renderers = $.extend($.pivotUtilities.renderers,$.pivotUtilities.plotly_renderers);
                              $("#output").pivotUI({df_json},
                            {{renderers:renderers}});\\\''')
        else:
            ui.label('No data available. Please generate or load data from the dashboard.').classes('text-lg text-red-500')
    else:
        ui.label('No data available. Please generate or load data from the dashboard.').classes('text-lg text-red-500')
    ui.button('Back to Dashboard', on_click=lambda: ui.open('/')).classes('mt-4')
'''
@ui.page("/raw_data")
def raw_data_page():
     from pathlib import Path
     if 'dwn_df_json' in app.storage.user:
        df_json = app.storage.user['dwn_df_json']
     ui.add_head_html('<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/3.9.1/chart.min.js"></script>')
     ui.add_head_html(f"<style>{(Path(__file__).parent / 'style1.css').read_text()}</style>") 
     ui.add_body_html('''<body>
    <div class="container">
        <div class="main-content">
            <div class="sidebar">
                <div class="field-group">
                    <h4>Available Fields</h4>
                    <div class="field-list" data-zone="fields" id="available-fields"></div>
                </div>
            </div>
            <div class="sidebar">
                <div class="field-group">
                    <h4>Rows</h4>
                    <div class="field-list" data-zone="rows" id="row-fields"></div>
                </div>
                <div class="field-group">
                    <h4>Columns</h4>
                    <div class="field-list" data-zone="columns" id="column-fields"></div>
                </div>

                <div class="field-group">
                    <h4>Values</h4>
                    <div class="field-list" data-zone="values" id="value-fields"></div>
                </div>
                <div class="field-group">
                    <h4>Filters</h4>
                    <div class="filter-controls">
                        <select id="filter-field" class="filter-select"><div class="seldrop"></div></select>
                      <div style="display:grid;grid-template-columns: 1fr 1fr;gap: 4px;">
                        <button class="btn small" onclick="applyFilter()">Apply Filter</button>
                        <button class="btn small danger" onclick="clearFilters()">Clear Filters</button>
                      </div>
                    </div>
                    <div id="active-filters" class="active-filters"></div>
                </div>
            </div>
            
            <div class="content-area">
                <!-- <div class="controls">
                    <button class="btn" onclick="generatePivot()">Generate Analysis</button>
                    <button class="btn danger" onclick="clearAll()">Clear All</button>
                    <button class="btn secondary" onclick="exportData()">Export Results</button>
                </div> -->

                <div class="view-tabs">
                    <button class="tab" onclick="switchTab('table')">Table View</button>
                    <button class="tab active" onclick="switchTab('chart')">Charts</button>
                    <button class="tab" onclick="switchTab('metrics')">Key Metrics</button>
                    <button class="tab" onclick="switchTab('raw')">Raw Data</button>
                </div>

                <div id="table-view" class="tab-content">
                    <div id="pivot-output">
                        <div class="empty-state">
                            <h3>No Pivot Table Generated</h3>
                            <p>Drag fields to the Rows, Columns, or Values sections and click "Generate Analysis" to start analyzing your data.</p>
                        </div>
                    </div>
                </div>

                <div id="chart-view" class="tab-content active">
                    <div class="chart-controls">
                        <select class="chart-select" id="chart-type" onchange="updateChart()">
                            <option value="line">Line Chart</option>      
                            <option value="bar">Bar Chart</option>
                            <option value="pie">Pie Chart</option>
                            <option value="doughnut">Doughnut Chart</option>
                            <option value="radar">Radar Chart</option>
                            <option value="polarArea">Polar Area</option>
                        </select>
                        <!-- <button class="btn" onclick="updateChart()">Update Chart</button> -->
                    </div>
                    <div class="chart-container">
                        <canvas id="pivotChart" width="400" height="200"></canvas>
                    </div>
                </div>

                <div id="metrics-view" class="tab-content" style="width:100%">
                    <div id="metrics-grid" class="flex-view">
                        <div class="empty-state">
                            <h3>No Metrics Available</h3>
                            <p>Generate a pivot table first to see key metrics.</p>
                        </div>
                    </div>
                </div>

                <div id="raw-view" class="tab-content">
                    <div class="pivot-table">
                        <div id="raw-data-output">
                            <div class="empty-state">
                                <h3>No Data Loaded</h3>
                                <p>Load data to view raw data table.</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
    function convertMultiSelectToAutocomplete() {
            // Find all select elements with multiple attribute
            const multiSelects = document.querySelectorAll('select[multiple]');
            
            multiSelects.forEach(select => {
                // Skip if already converted
                if (select.classList.contains('converted')) return;
                
                // Mark as converted
                select.classList.add('converted');
                
                // Get original options
                const options = Array.from(select.options).map(opt => ({
                    value: opt.value,
                    text: opt.textContent,
                    selected: opt.selected
                }));
                
                // Create custom multiselect container
                const container = document.createElement('div');
                container.className = 'custom-multiselect';
                
                const multiselectContainer = document.createElement('div');
                multiselectContainer.className = 'multiselect-container';
                multiselectContainer.tabIndex = 0;
                
                const selectedItems = document.createElement('div');
                selectedItems.className = 'selected-items';
                
                const searchInput = document.createElement('input');
                searchInput.type = 'text';
                searchInput.className = 'search-input';
                searchInput.placeholder = 'Type to search...';
                
                const arrow = document.createElement('div');
                arrow.className = 'dropdown-arrow';
                
                const dropdown = document.createElement('div');
                dropdown.className = 'options-dropdown';
                
                // Assemble structure
                selectedItems.appendChild(searchInput);
                multiselectContainer.appendChild(selectedItems);
                multiselectContainer.appendChild(arrow);
                container.appendChild(multiselectContainer);
                container.appendChild(dropdown);
                
                // Insert after original select and hide original
                select.parentNode.insertBefore(container, select.nextSibling);
                select.style.display = 'none';
                
                let selectedValues = new Set(options.filter(opt => opt.selected).map(opt => opt.value));
                let isOpen = false;
                
                function updateSelectedDisplay() {
                    // Clear current display except search input
                    const tags = selectedItems.querySelectorAll('.selected-tag');
                    tags.forEach(tag => tag.remove());
                    
                    // Add selected items as tags
                    selectedValues.forEach(value => {
                        const option = options.find(opt => opt.value === value);
                        if (option) {
                            const tag = document.createElement('div');
                            tag.className = 'selected-tag';
                            tag.innerHTML = `
                                ${option.text}
                                <span class="tag-remove" data-value="${value}">×</span>
                            `;
                            selectedItems.insertBefore(tag, searchInput);
                        }
                    });
                    
                    // Update original select
                    Array.from(select.options).forEach(opt => {
                        opt.selected = selectedValues.has(opt.value);
                    });
                    
                    // Show placeholder if no selections
                    if (selectedValues.size === 0) {
                        searchInput.placeholder = 'Type to search...';
                    } else {
                        searchInput.placeholder = '';
                    }
                } // Closing brace for updateSelectedDisplay
                
                function filterOptions(searchTerm = '') {
                    dropdown.innerHTML = '';
                    
                    const filteredOptions = options.filter(opt =>
                        opt.text.toLowerCase().includes(searchTerm.toLowerCase())
                    );
                    
                    if (filteredOptions.length === 0) {
                        const noOptions = document.createElement('div');
                        noOptions.className = 'no-options';
                        noOptions.textContent = 'No options found';
                        dropdown.appendChild(noOptions);
                        return;
                    }
                    
                    filteredOptions.forEach(option => {
                        const item = document.createElement('div');
                        item.className = 'option-item';
                        if (selectedValues.has(option.value)) {
                            item.classList.add('selected');
                        }
                        
                        item.innerHTML = `
                            <input type="checkbox" class="option-checkbox" ${selectedValues.has(option.value) ? 'checked' : ''}>
                            <span>${option.text}</span>
                        `;
                        
                        item.addEventListener('click', (e) => {
                            e.preventDefault();
                            const checkbox = item.querySelector('.option-checkbox');
                            
                            if (selectedValues.has(option.value)) {
                                selectedValues.delete(option.value);
                                checkbox.checked = false;
                                item.classList.remove('selected');
                            } else {
                                selectedValues.add(option.value);
                                checkbox.checked = true;
                                item.classList.add('selected');
                            }
                            
                            updateSelectedDisplay();
                        });
                        
                        dropdown.appendChild(item);
                    });
                }
                
                function openDropdown() {
                    if (!isOpen) {
                        isOpen = true;
                        dropdown.classList.add('open');
                        arrow.classList.add('open');
                        filterOptions(searchInput.value);
                        searchInput.focus();
                    }
                }
                
                function closeDropdown() {
                    if (isOpen) {
                        isOpen = false;
                        dropdown.classList.remove('open');
                        arrow.classList.remove('open');
                        searchInput.value = '';
                    }
                }
                
                // Event listeners
                multiselectContainer.addEventListener('click', (e) => {
                    if (e.target === searchInput) return;
                    openDropdown();
                });
                multiselectContainer.addEventListener('change', (e) => {
                        applyFilter();
                    });
                
                searchInput.addEventListener('input', (e) => {
                    if (!isOpen) openDropdown();
                    filterOptions(e.target.value);
                });
                
                searchInput.addEventListener('focus', openDropdown);
                
                // Remove tag functionality
                selectedItems.addEventListener('click', (e) => {
                    if (e.target.classList.contains('tag-remove')) {
                        const value = e.target.dataset.value;
                        selectedValues.delete(value);
                        updateSelectedDisplay();
                        
                        // Update dropdown if open
                        if (isOpen) {
                            filterOptions(searchInput.value);
                        }
                    }
                });
                
                // Close dropdown when clicking outside
                document.addEventListener('click', (e) => {
                    if (!container.contains(e.target)) {
                        closeDropdown();
                    }
                });
                
                // Keyboard navigation
                multiselectContainer.addEventListener('keydown', (e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        if (!isOpen) {
                            openDropdown();
                        }
                    } else if (e.key === 'Escape') {
                        closeDropdown();
                    }
                });
                
                // Initialize display
                updateSelectedDisplay();
                filterOptions();
                // The selectElement.setAttribute('onchange', 'applyFilter()'); was moved inside updateSelectedDisplay
            }); // Closing brace for multiSelects.forEach
        }; // Closing brace for convertMultiSelectToAutocomplete
        let originalData = null;
        let currentData = null;
        let currentPivotData = null;
        let currentChart = null;
        let draggedElement = null;
        let activeFilters = [];

        // Data input functions
        function toggleDataInput() {
            const panel = document.getElementById('data-input-panel');
            panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
        }

        function updateAvailableFields() {
            if (currentData.length === 0) return;
            
            const fields = Object.keys(currentData[0]);
            const availableFields = document.getElementById('available-fields');
            
            // Clear existing fields
            availableFields.innerHTML = '';
            
            // Add new fields
            fields.forEach(field => {
                const fieldItem = document.createElement('div');
                fieldItem.className = 'field-item';
                fieldItem.draggable = true;
                fieldItem.setAttribute('data-field', field);
                fieldItem.textContent = field;
                fieldItem.addEventListener('dragstart', handleDragStart);
                fieldItem.addEventListener('dragend', handleDragEnd);
                availableFields.appendChild(fieldItem);
            });
            populateFilterFields();
        }

        function populateFilterFields() {
            const filterFieldSelect = document.getElementById('filter-field');
            filterFieldSelect.innerHTML = '<option value="">Select Field</option>';
            const allowedFields = ['StrykerGroupRegion', 'Area', 'Region', 'Country', 'Business Sector', 'Business Unit', 'Franchise', 'Product Line', 'IBP Level 5', 'IBP Level 6', 'IBP Level 7','CatalogNumber'];
            
            allowedFields.forEach(field => {
                const option = document.createElement('option');
                option.value = field;
                option.textContent = field;
                filterFieldSelect.appendChild(option);
            });
            filterFieldSelect.onchange = updateFilterValueInput;
        }

        function updateFilterValueInput() {
            const filterField = document.getElementById('filter-field').value;
            const filterValueContainer = document.querySelector('.filter-controls'); // Get the filter controls div
            let filterValueElement = document.getElementById('filter-value');

            // Remove existing filter value input/select
            if (filterValueElement) {
                filterValueElement.remove();
            }

            if (!filterField) {
                const input = document.createElement('input');
                input.type = 'text';
                input.id = 'filter-value';
                input.placeholder = 'Filter value';
                input.className = 'filter-input';
                filterValueContainer.insertBefore(input, filterValueContainer.children[2]); // Insert before apply button
                return;
            }

            const selectElement = document.createElement('select');
            selectElement.id = 'filter-value';
            selectElement.setAttribute('multiple', 'true');
            selectElement.className = 'filter-select';
            selectElement.innerHTML = '<option value="">Select Value(s)</option>';

            const uniqueValues = getUniqueValues(filterField);
            uniqueValues.forEach(value => {
                const option = document.createElement('option');
                option.value = value;
                option.textContent = value;
                selectElement.appendChild(option);
            });
            filterValueContainer.insertBefore(selectElement, filterValueContainer.children[1]);
            convertMultiSelectToAutocomplete();
        }

        function getUniqueValues(field) {
            const values = new Set();
            originalData.forEach(row => {
                if (row[field] !== undefined && row[field] !== null) {
                    values.add(String(row[field]));
                }
            });
            return Array.from(values).sort();
        }
        function clearFilters() {
            activeFilters = [];
            filterData();
            document.querySelectorAll('.custom-multiselect').forEach(element => {
                element.remove();
            });
            renderActiveFilters();
        }

        function applyFilter() {
            clearFilters()
            const field = document.getElementById('filter-field').value;
            const filterValueElement = document.getElementById('filter-value');
            let values = [];

            if (filterValueElement.tagName === 'SELECT' && filterValueElement.multiple) {
                values = Array.from(filterValueElement.selectedOptions).map(option => option.value);
            } else {
                values = [filterValueElement.value];
            }

            if (!field || values.length === 0 || (values.length === 1 && values[0] === '')) {
                alert('Please select a field and enter/select a value(s) to filter.');
                return;
            }

            activeFilters.push({ field, values });
            filterData();
            renderActiveFilters();
            // Reset filter value input/select
            if (filterValueElement.tagName === 'INPUT') {
                filterValueElement.value = '';
            } else {
                filterValueElement.value = ''; // Reset select to default option
            }
        }

        function filterData() {
            let filtered = [...originalData];
            activeFilters.forEach(filter => {
                filtered = filtered.filter(row => {
                    const rowValue = String(row[filter.field]).toLowerCase();
                    // If filter.values is an array, check if rowValue is included in the array
                    // Otherwise, use the old includes logic for single string values
                    if (Array.isArray(filter.values)) {
                        return filter.values.some(val => rowValue.includes(String(val).toLowerCase()));
                    } else {
                        return rowValue.includes(String(filter.values).toLowerCase());
                    }
                });
            });
            currentData = filtered;
            updateRawDataView();
            generatePivot(); // Re-generate pivot with filtered data
        }

        function renderActiveFilters() {
            const activeFiltersDiv = document.getElementById('active-filters');
            activeFiltersDiv.innerHTML = '';
            if (activeFilters.length === 0) {
                activeFiltersDiv.innerHTML = '<p>No active filters.</p>';
                return;
            }
            activeFilters.forEach((filter, index) => {
                const filterTag = document.createElement('span');
                filterTag.className = 'filter-tag';
                const displayValue = Array.isArray(filter.values) ? filter.values.join(', ') : filter.values;
                filterTag.innerHTML = `${filter.field}: "${displayValue}" <span class="remove-filter" data-index="${index}">&times;</span>`;
                filterTag.querySelector('.remove-filter').addEventListener('click', removeFilter);
                activeFiltersDiv.appendChild(filterTag);
            });
        }

        function removeFilter(event) {
            const index = parseInt(event.target.getAttribute('data-index'));
            activeFilters.splice(index, 1);
            filterData();
            renderActiveFilters();
        }

        function updateRawDataView() {
            const rawOutput = document.getElementById('raw-data-output');
            
            if (currentData.length === 0) {
                rawOutput.innerHTML = `
                    <div class="empty-state">
                        <h3>No Data Loaded</h3>
                        <p>Load data to view raw data table.</p>
                    </div>
                `;
                return;
            }

            const fields = Object.keys(currentData[0]);
            let html = '<table>';
            
            // Header
            html += '<tr>';
            fields.forEach(field => {
                html += `<th>${field}</th>`;
            });
            html += '</tr>';
            
            // Data rows (limit to first 100 for performance)
            const displayData = currentData.slice(0, 100);
            displayData.forEach(row => {
                html += '<tr>';
                fields.forEach(field => {
                    const value = row[field];
                    const cellClass = typeof value === 'number' ? 'numeric' : '';
                    html += `<td class="${cellClass}">${formatValue(value)}</td>`;
                });
                html += '</tr>';
            });
            
            if (currentData.length > 100) {
                html += `<tr><td colspan="${fields.length}" style="text-align: center; font-style: italic; color: #666;">Showing first 100 of ${currentData.length} rows</td></tr>`;
            }
            
            html += '</table>';
            rawOutput.innerHTML = html;
        }

        // Tab switching
        function switchTab(tabName) {
            // Update tab buttons
            document.querySelectorAll('.tab').forEach(tab => tab.classList.remove('active'));
            event.target.classList.add('active');
            
            // Update tab content
            document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
            document.getElementById(tabName + '-view').classList.add('active');
            
            // If switching to chart view, update chart
            if (tabName === 'chart' && currentPivotData) {
                setTimeout(() => updateChart(), 100);
            }
        }

        // Initialize drag and drop
        function initializeDragAndDrop() {
            const dropZones = document.querySelectorAll('.field-list');

            dropZones.forEach(zone => {
                zone.addEventListener('dragover', handleDragOver);
                zone.addEventListener('drop', handleDrop);
                zone.addEventListener('dragenter', handleDragEnter);
                zone.addEventListener('dragleave', handleDragLeave);
            });
        }

        function handleDragStart(e) {
            draggedElement = e.target;
            e.target.classList.add('dragging');
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/html', e.target.outerHTML);
        }

        function handleDragEnd(e) {
            e.target.classList.remove('dragging');
        }

        function handleDragOver(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';
        }

        function handleDragEnter(e) {
            e.preventDefault();
            e.target.classList.add('drag-over');
        }

        function handleDragLeave(e) {
            e.target.classList.remove('drag-over');
        }

        function handleDrop(e) {
            e.preventDefault();
            e.target.classList.remove('drag-over');

            if (draggedElement) {
                const fieldName = draggedElement.getAttribute('data-field');
                const targetZone = e.target.getAttribute('data-zone');
                
                // Remove from current location
                draggedElement.remove();
                
                // Add to new location
                if (targetZone === 'values') {
                    addFieldToValues(fieldName, e.target);
                } else {
                    addFieldToZone(fieldName, e.target);
                }
                
                draggedElement = null;
                generatePivot(); // Trigger pivot table generation after drop
            }
        }

        function addFieldToZone(fieldName, targetZone) {
            const newField = document.createElement('div');
            newField.className = 'field-item';
            newField.draggable = true;
            newField.setAttribute('data-field', fieldName);
            newField.textContent = fieldName;
            
            // Add event listeners
            newField.addEventListener('dragstart', handleDragStart);
            newField.addEventListener('dragend', handleDragEnd);
            
            targetZone.appendChild(newField);
            generatePivot(); // Trigger pivot table generation after adding field
        }

        function addFieldToValues(fieldName, targetZone) {
            const newField = document.createElement('div');
            newField.className = 'field-item';
            newField.draggable = true;
            newField.setAttribute('data-field', fieldName);
            
            // Add aggregation selector for value fields
            const select = document.createElement('select');
            select.className = 'aggregation-select';
            select.innerHTML = `
                <option value="sum">Sum</option>
                <option value="avg">Mean</option>
                <option value="count">Count</option>
                <option value="min">Min</option>
                <option value="max">Max</option>
            `;
            
            newField.innerHTML = `${fieldName} `;
            newField.appendChild(select);
            
            // Add event listeners
            newField.addEventListener('dragstart', handleDragStart);
            newField.addEventListener('dragend', handleDragEnd);
            select.addEventListener('change', generatePivot); // Add this line
            
            targetZone.appendChild(newField);
            generatePivot(); // Trigger pivot table generation after adding value field
        }

        function generatePivot() {
            const rowFields = getFieldsFromZone('rows');
            const columnFields = getFieldsFromZone('columns');
            const valueFields = getFieldsFromZone('values');

            currentPivotData = createPivotTable(currentData, rowFields, columnFields, valueFields);
            renderPivotTable(currentPivotData, rowFields, columnFields, valueFields);
            generateMetrics(currentPivotData, rowFields, columnFields, valueFields);
            
            // Update chart if chart tab is active
            if (document.getElementById('chart-view').classList.contains('active')) {
                updateChart();
            }
        }

        function getFieldsFromZone(zone) {
            const zoneElement = document.getElementById(`${zone === 'rows' ? 'row' : zone === 'columns' ? 'column' : 'value'}-fields`);
            const fields = [];
            console.log(zoneElement)
            
            zoneElement.querySelectorAll('.field-item').forEach(item => {
                const field = item.getAttribute('data-field');
                if (zone === 'values') {
                    const aggregation = item.querySelector('.aggregation-select')?.value || 'sum';
                    fields.push({field, aggregation});
                } else {
                    fields.push(field);
                }
            });
            
            return fields;
        }

        function createPivotTable(data, rowFields, columnFields, valueFields) {
            const pivot = {};
            const columnValues = new Set();
            
            // Collect all unique column values
            if (columnFields.length > 0) {
                data.forEach(row => {
                    const colKey = columnFields.map(field => row[field]).join(' | ');
                    columnValues.add(colKey);
                });
            }

            // Group data by row fields
            data.forEach(row => {
                const rowKey = rowFields.length > 0 ? rowFields.map(field => row[field]).join(' | ') : 'Total';
                const colKey = columnFields.length > 0 ? columnFields.map(field => row[field]).join(' | ') : 'Total';
                
                if (!pivot[rowKey]) {
                    pivot[rowKey] = {};
                }
                
                if (!pivot[rowKey][colKey]) {
                    pivot[rowKey][colKey] = {};
                }
                
                valueFields.forEach(valueField => {
                    const {field, aggregation} = valueField;
                    const key = `${field}_${aggregation}`;
                    
                    if (!pivot[rowKey][colKey][key]) {
                        pivot[rowKey][colKey][key] = [];
                    }
                    
                    pivot[rowKey][colKey][key].push(row[field]);
                });
            });

            // Calculate aggregations
            Object.keys(pivot).forEach(rowKey => {
                Object.keys(pivot[rowKey]).forEach(colKey => {
                    Object.keys(pivot[rowKey][colKey]).forEach(valueKey => {
                        const values = pivot[rowKey][colKey][valueKey];
                        const aggregation = valueKey.split('_')[1];
                        
                        pivot[rowKey][colKey][valueKey] = calculateAggregation(values, aggregation);
                    });
                });
            });

            return {
                data: pivot,
                columnValues: Array.from(columnValues).sort(),
                rowKeys: Object.keys(pivot).sort()
            };
        }

        function calculateAggregation(values, aggregation) {
            switch (aggregation) {
                case 'sum':
                    return values.reduce((sum, val) => sum + (val || 0), 0);
                case 'avg':
                    return values.reduce((sum, val) => sum + (val || 0), 0) / values.length;
                case 'count':
                    return values.length;
                case 'min':
                    return Math.min(...values.filter(v => v != null));
                case 'max':
                    return Math.max(...values.filter(v => v != null));
                default:
                    return values.reduce((sum, val) => sum + (val || 0), 0);
            }
        }

        function renderPivotTable(pivotData, rowFields, columnFields, valueFields) {
            const outputDiv = document.getElementById('pivot-output');
            
            let html = '<div class="pivot-table"><table>';
            
            // Header row
            html += '<tr>';
            if (rowFields.length > 0) {
                html += `<th>${rowFields.join(' / ')}</th>`;
            }
            
            if (columnFields.length === 0) {
                valueFields.forEach(vf => {
                    html += `<th>${vf.field} (${vf.aggregation})</th>`;
                });
            } else {
                pivotData.columnValues.forEach(colValue => {
                    valueFields.forEach(vf => {
                        html += `<th>${colValue}<br><small>${vf.field} (${vf.aggregation})</small></th>`;
                    });
                });
            }
            html += '</tr>';
            
            // Data rows
            pivotData.rowKeys.forEach(rowKey => {
                html += '<tr>';
                if (rowFields.length > 0) {
                    html += `<td><strong>${rowKey}</strong></td>`;
                }
                
                if (columnFields.length === 0) {
                    valueFields.forEach(vf => {
                        const key = `${vf.field}_${vf.aggregation}`;
                        const value = pivotData.data[rowKey]['Total'] ? pivotData.data[rowKey]['Total'][key] : 0;
                        html += `<td class="numeric">${formatNumber(value)}</td>`;
                    });
                } else {
                    pivotData.columnValues.forEach(colValue => {
                        valueFields.forEach(vf => {
                            const key = `${vf.field}_${vf.aggregation}`;
                            const value = pivotData.data[rowKey][colValue] ? pivotData.data[rowKey][colValue][key] : 0;
                            html += `<td class="numeric">${formatNumber(value)}</td>`;
                        });
                    });
                }
                html += '</tr>';
            });
            
            html += '</table></div>';
            outputDiv.innerHTML = html;
        }

        function generateMetrics(pivotData, rowFields, columnFields, valueFields) {
            const metricsGrid = document.getElementById('metrics-grid');
            
            if (!pivotData || valueFields.length === 0) {
                metricsGrid.innerHTML = `
                    <div class="empty-state">
                        <h3>No Metrics Available</h3>
                        <p>Generate a pivot table first to see key metrics.</p>
                    </div>
                `;
                return;
            }

            let html = '';
            
            valueFields.forEach(vf => {
                const key = `${vf.field}_${vf.aggregation}`;
                let totalValue = 0;
                let count = 0;
                let min = Infinity;
                let max = -Infinity;
                
                // Calculate overall metrics
                Object.keys(pivotData.data).forEach(rowKey => {
                    Object.keys(pivotData.data[rowKey]).forEach(colKey => {
                        const value = pivotData.data[rowKey][colKey][key];
                        if (value !== undefined && value !== null && !isNaN(value)) {
                            totalValue += value;
                            count++;
                            min = Math.min(min, value);
                            max = Math.max(max, value);
                        }
                    });
                });
                
                const average = count > 0 ? totalValue / count : 0;
                
                html += `
                    <div class="metric-card">
                        <div class="metric-value">${formatNumber(totalValue)}</div>
                        <div class="metric-label">Total ${vf.field}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${formatNumber(average)}</div>
                        <div class="metric-label">Average ${vf.field}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${formatNumber(max)}</div>
                        <div class="metric-label">Max ${vf.field}</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">${formatNumber(min)}</div>
                        <div class="metric-label">Min ${vf.field}</div>
                    </div>
                `;
            });
            
            // Add count metrics
            html += `
                <div class="metric-card">
                    <div class="metric-value">${pivotData.rowKeys.length}</div>
                    <div class="metric-label">Total Rows</div>
                </div>
                <div class="metric-card">
                    <div class="metric-value">${currentData.length}</div>
                    <div class="metric-label">Source Records</div>
                </div>
            `;
            
            metricsGrid.innerHTML = html;
        }

        function updateChart() {
            if (!currentPivotData) {
                return;
            }
            
            const chartType = document.getElementById('chart-type').value;
            const ctx = document.getElementById('pivotChart').getContext('2d');
            
            if (currentChart) {
                currentChart.destroy();
            }
            
            const chartData = prepareChartData();
            
            const config = {
                type: chartType,
                data: chartData,
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        mode: 'index',
                        intersect: false,
                    },
                    plugins: {
                        title: {
                            display: true,
                            text: 'Pivot Data Visualization'
                        },
                        legend: {
                            display: chartType !== 'pie' && chartType !== 'doughnut'
                        }
                    },
                    scales: chartType === 'pie' || chartType === 'doughnut' || chartType === 'polarArea' ? {} : {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            };
            
            currentChart = new Chart(ctx, config);
        }

        function prepareChartData() {
            if (!currentPivotData) return { labels: [], datasets: [] };
            
            const columnFields = getFieldsFromZone('columns');
            const valueFields = getFieldsFromZone('values');
            
            // Use column values for x-axis labels
            // const labels = columnFields.length > 0 ? currentPivotData.columnValues : currentPivotData.rowKeys;
            const labels = currentPivotData.rowKeys;
            const datasets = [];
            
            if (columnFields.length === 0) {
                // Simple case: no columns, rows as categories
                valueFields.forEach((vf, index) => {
                    const key = `${vf.field}_${vf.aggregation}`;
                    const data = currentPivotData.rowKeys.map(rowKey => {
                        return currentPivotData.data[rowKey]['Total']?.[key] || 0;
                    });
                    
                    datasets.push({
                        label: `${vf.field} (${vf.aggregation})`,
                        data: data,
                        backgroundColor: generateColors(currentPivotData.rowKeys.length, index),
                        borderColor: generateColors(currentPivotData.rowKeys.length, index, 0.8),
                        borderWidth: 2
                    });
                });
              } else {
                // If column fields are present, each combination of column value and value field becomes a dataset
                currentPivotData.columnValues.forEach((colValue, colIndex) => {
                    valueFields.forEach((vf, valIndex) => {
                        const key = `${vf.field}_${vf.aggregation}`;
                        const data = currentPivotData.rowKeys.map(rowKey => {
                            return currentPivotData.data[rowKey]?.[colValue]?.[key] || 0;
                        });
                        
                        datasets.push({
                            label: `${colValue} - ${vf.field} (${vf.aggregation})`,
                            data: data,
                            backgroundColor: generateColors(currentPivotData.columnValues.length * valueFields.length, colIndex * valueFields.length + valIndex, 0.7),
                            borderColor: generateColors(currentPivotData.columnValues.length * valueFields.length, colIndex * valueFields.length + valIndex, 1),
                            borderWidth: 2
                        });
                    });
                });
            }
            
            return { labels, datasets };
        }

        function generateColors(count, index, alpha = 0.7) {
            const colors = [
                `rgba(102, 126, 234, ${alpha})`,
                `rgba(118, 75, 162, ${alpha})`,
                `rgba(255, 99, 132, ${alpha})`,
                `rgba(54, 162, 235, ${alpha})`,
                `rgba(255, 205, 86, ${alpha})`,
                `rgba(75, 192, 192, ${alpha})`,
                `rgba(153, 102, 255, ${alpha})`,
                `rgba(255, 159, 64, ${alpha})`,
                `rgba(199, 199, 199, ${alpha})`,
                `rgba(83, 102, 255, ${alpha})`
            ];
            
            //if (count === 1) {
            //    return colors.slice(0, 10);
            //}
            
            return colors[index % colors.length];
        }

        function formatNumber(num) {
            if (num === null || num === undefined) return '0';
            if (typeof num === 'number') {
                return num.toLocaleString(undefined, { maximumFractionDigits: 2 });
            }
            return num;
        }

        function formatValue(value) {
            if (typeof value === 'number') {
                return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
            }
            return value || '';
        }

        function clearAll() {
            const availableFields = document.getElementById('available-fields');
            const zones = ['row-fields', 'column-fields', 'value-fields'];
            
            zones.forEach(zoneId => {
                const zone = document.getElementById(zoneId);
                while (zone.firstChild) {
                    const field = zone.firstChild;
                    const fieldName = field.getAttribute('data-field');
                    
                    // Create new field in available fields
                    const newField = document.createElement('div');
                    newField.className = 'field-item';
                    newField.draggable = true;
                    newField.setAttribute('data-field', fieldName);
                    newField.textContent = fieldName;
                    
                    newField.addEventListener('dragstart', handleDragStart);
                    newField.addEventListener('dragend', handleDragEnd);
                    
                    availableFields.appendChild(newField);
                    zone.removeChild(field);
                }
            });
            
            // Clear outputs
            document.getElementById('pivot-output').innerHTML = `
                <div class="empty-state">
                    <h3>No Pivot Table Generated</h3>
                    <p>Drag fields to the Rows, Columns, or Values sections and click "Generate Analysis" to start analyzing your data.</p>
                </div>
            `;
            
            document.getElementById('metrics-grid').innerHTML = `
                <div class="empty-state">
                    <h3>No Metrics Available</h3>
                    <p>Generate a pivot table first to see key metrics.</p>
                </div>
            `;
            
            if (currentChart) {
                currentChart.destroy();
                currentChart = null;
            }
            currentPivotData = null;
            generatePivot(); // Trigger pivot table generation after clearing all
        }

        function exportData() {
            if (!currentPivotData) {
                alert('Generate a pivot table first to export data.');
                return;
            }
            
            const rowFields = getFieldsFromZone('rows');
            const columnFields = getFieldsFromZone('columns');
            const valueFields = getFieldsFromZone('values');
            
            let csvContent = '';
            
            // Header row
            const headers = [];
            if (rowFields.length > 0) {
                headers.push(rowFields.join(' / '));
            }
            
            if (columnFields.length === 0) {
                valueFields.forEach(vf => {
                    headers.push(`${vf.field} (${vf.aggregation})`);
                });
            } else {
                currentPivotData.columnValues.forEach(colValue => {
                    valueFields.forEach(vf => {
                        headers.push(`${colValue} - ${vf.field} (${vf.aggregation})`);
                    });
                });
            }
            
            csvContent += headers.join(',') + '\\n';
            
            // Data rows
            currentPivotData.rowKeys.forEach(rowKey => {
                const row = [];
                if (rowFields.length > 0) {
                    row.push(`"${rowKey}"`);
                }
                
                if (columnFields.length === 0) {
                    valueFields.forEach(vf => {
                        const key = `${vf.field}_${vf.aggregation}`;
                        const value = currentPivotData.data[rowKey]['Total'] ? currentPivotData.data[rowKey]['Total'][key] : 0;
                        row.push(value);
                    });
                } else {
                    currentPivotData.columnValues.forEach(colValue => {
                        valueFields.forEach(vf => {
                            const key = `${vf.field}_${vf.aggregation}`;
                            const value = currentPivotData.data[rowKey][colValue] ? currentPivotData.data[rowKey][colValue][key] : 0;
                            row.push(value);
                        });
                    });
                }
                
                csvContent += row.join(',') + '\\n';
            });
            
            // Download CSV
            const blob = new Blob([csvContent], { type: 'text/csv' });
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'pivot_analysis.csv';
            a.click();
            window.URL.revokeObjectURL(url);
        }
        // Function to programmatically load data
        function loadDataProgrammatically(data) {
            if (!Array.isArray(data) || data.length === 0) {
                alert('Invalid data provided. Please provide an array of objects.');
                return;
            }
            originalData = data; // Store original data
            currentData = [...originalData]; // Initialize currentData with original
            updateAvailableFields();
            updateRawDataView();
            clearAll(); // Clear existing pivot/charts
            clearFilters(); // Clear any active filters
            //alert('Data loaded programmatically!');
            // Optionally, generate pivot table immediately after loading data
            
            // Set default columns
            const rowFieldsContainer = document.getElementById('row-fields');
            addFieldToZone('SALES_DATE', rowFieldsContainer);

            // Set default values
            const valueFieldsContainer = document.getElementById('value-fields');
            addFieldToValues('`Act Orders Rev', valueFieldsContainer);
            addFieldToValues('NHITS', valueFieldsContainer);
            addFieldToValues('`Fcst Stat Final Rev', valueFieldsContainer);

            generatePivot(); // Generate pivot table with default values
        }

        // Expose the function globally for external calls (e.g., from parent window or other scripts)
        window.loadPivotData = loadDataProgrammatically;
        loadDataProgrammatically('''+df_json+''')

        // Initialize the app
        document.addEventListener('DOMContentLoaded', function() {
            initializeDragAndDrop();
            updateRawDataView();
        });

        convertMultiSelectToAutocomplete();
    </script></body>                  
    ''')