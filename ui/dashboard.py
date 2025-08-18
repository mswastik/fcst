from nicegui import ui,run,app
from data_model import get_filter_options, filtered_products, filtered_models, generate_sample_data, filtered_df
from data_service import apply_filters, create_models_action, change_fc_action, create_clusters, run_enhanced_forecasting_pipeline
from sql import sqlpd,query_st
from ui.charts import update_charts
import os
#import asyncio
import polars as pl
#import json
#import io

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
    filter_state = {'data_files': None,'location1': None,'location2': None,'product1': None,'product2': None,'level': None }
    
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
                    from data_model import filtered_df as global_filtered_df
                    await update_ui(global_filtered_df)
                    ui.notify(f"Filtered by CatalogNumber: {catalog_number}", type='info')

                if filter_state['level']:
                    ui.table.from_polars(f1.pivot('SALES_DATE',index=[filter_state['location1'],filter_state['level']],values='`Act Orders Rev',aggregate_function='sum',sort_columns=True),pagination=10).classes('w-full').props('virtual-scroll').on('rowClick', on_row_click_handler)
                elif filter_state['location1']:
                    ui.table.from_polars(f1.pivot('SALES_DATE',index=filter_state['location1'],values='`Act Orders Rev',aggregate_function='sum',sort_columns=True),pagination=10).classes('w-full').props('virtual-scroll').on('rowClick', on_row_click_handler)
                else:
                    ui.table.from_polars(f1.pivot('SALES_DATE',index='Region',values='`Act Orders Rev',aggregate_function='sum',sort_columns=True),pagination=10).classes('w-full').props('virtual-scroll').on('rowClick', on_row_click_handler)
                #ui.table.from_polars(filtered_df,pagination=10)
    
    async def on_filter_change(filter_name, value):
        """Handle filter change events"""
        global filtered_df
        filter_state[filter_name] = value
        if filter_name=='data_files':
            #dwn.df=pl.read_parquet(f'data/{value}')
            dwn.df=generate_sample_data(f'data/{value}')
            app.storage.user['dwn_df_json'] = dwn.df.write_json()
            await update_ui(dwn.df.group_by(['SALES_DATE','Business Unit','Region']).sum())
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
        try:
            filtered_df=filtered_df['filtered_df']
        except: pass
        
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
                ph=ui.select(options=['Franchise','Business_Unit','IBP_Level_5','CatalogNumber'],label='Product').bind_value(dwn,'phv').classes('w-48')
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
            data_files_select = ui.select(label='DataFiles',options=os.listdir("data/"),with_input=True,on_change=lambda e: on_filter_change('data_files', e.value)
            ).classes('w-40')
            
            location_select1 = ui.select(label='Location',options=options['locations'], with_input=True, on_change=lambda e: on_filter_change('location1', e.value)
            ).classes('w-40').bind_value(filter_state,'location1')
            
            location_select2 = ui.select( label='Location', options= options['locations_filt'],with_input=True,on_change=lambda e: on_filter_change('location2', e.value)
            ).classes('w-40').bind_value(filter_state,'location2')
            
            product_select1 = ui.select(label='Product', options=options['products'],with_input=True, on_change=lambda e: on_filter_change('product1', e.value),
                clearable=True
            ).classes('w-40').bind_value(filter_state,'product1')
            
            product_select2 = ui.select(label='Product', options= options['products_filt'],with_input=True, on_change=lambda e: on_filter_change('product2', e.value),
                clearable=True
            ).classes('w-40').bind_value(filter_state,'product2')
            
            level_select = ui.select(label='Level', options=[''] + options['levels'], with_input=True, on_change=lambda e: on_filter_change('level', e.value)
            ).classes('w-40')
            #ui.button('Download Data', on_click=lambda: ui.notify(download_data_action(), type='info')).classes('ml-auto')
            ui.button('Get Data', on_click=on_download).classes('ml-auto')
        
        # Main content area
        with ui.row().classes('w-full mt-2 ml-0 gap-2'):
            # Models list
            with ui.column().classes('w-1/6 gap-2'):
                with ui.card().classes('w-full h-96'):
                    ui.label('Models for filtered Products').classes('font-bold')
                    models_list = ui.list()
            
            # Charts row
            with ui.row().classes('w-[1190px] gap-2 mr-0'):
                # Column chart
                with ui.column().classes('w-[590px] h-96 gap-0'):
                    column_chart_container = ui.card().classes('w-full h-full')
                
                # Line chart
                with ui.column().classes('w-[590px] h-96 gap-0'):
                    line_chart_container = ui.card().classes('w-full h-full')

            async def run_create_models():
                n=ui.notification(timeout=None)
                '''if 'birch' not in dwn.df.columns:
                    n.message="Running Clustering!"
                    n.spinner = True
                    await run_cluster()
                    n.message="Clustering Done!"
                for c in dwn.df['birch'].unique():
                    cd = dwn.df.filter(pl.col('birch')==c)
                    n.message="Running for cluster: "+str(c+1)+"/"+str(max(dwn.df['birch'].unique().to_list())+1)
                    n.spinner = True
                    dwn.df = await run.cpu_bound(create_models_action, cd, filter_state['data_files'])'''
                n.message="Running!! "
                n.spinner =True
                dwn.df = await run.cpu_bound(run_enhanced_forecasting_pipeline, dwn.df, filter_state['data_files'])
                n.message = 'Done!'
                n.spinner = False
                n.dismiss()
                await update_ui(dwn.df)
                ui.notify('Models created and forecasts saved!', type='success')
            
            async def run_cluster():
                ui.notify('Creating clusters...', type='info')
                dwn.df = await run.cpu_bound(create_clusters, dwn.df, filter_state['data_files'])
                await update_ui(dwn.df)
                ui.notify('Clusters created!', type='success')

            with ui.row().classes('gap-2'):
                ui.button('Segmentation', on_click=run_cluster).classes('bg-green-100')
                ui.button('Generate Forecast', on_click=run_create_models).classes('bg-green-100')
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

@ui.page("/raw_data")
def raw_data_page():
     from pathlib import Path
     if 'dwn_df_json' in app.storage.user:
        df_json = app.storage.user['dwn_df_json']
     ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.min.js"></script>')
     ui.add_head_html(f"<style>{(Path(__file__).parent / 'style.css').read_text()}</style>") 
     ui.add_body_html(f"{(Path(__file__).parent / 'data.html').read_text()}".replace('{{df_json}}', df_json))