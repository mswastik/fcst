from nicegui import ui,run,app
from core.data_model import get_filter_options, generate_sample_data
from core.data_service import apply_filters, create_models_action, change_fc_action, create_clusters, run_enhanced_forecasting_pipeline
from core.state_manager import get_global_state
from core.utils import DataUtils, DatabaseUtils, UIUtils, ErrorHandler
from ui.charts import update_charts
import os
import polars as pl
from datetime import datetime, timedelta

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
    auth_header.create_header()
    
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
            # Set loading states for both charts and table
            state = get_global_state()
            print(f"DEBUG: Setting loading states - before: charts={state.loading_charts}, table={state.loading_table}, data={state.loading_data}")
            state.set_loading_state('charts', True, 'Updating charts...')
            state.set_loading_state('table', True, 'Updating table...')
            print(f"DEBUG: Loading states set - after: charts={state.loading_charts}, table={state.loading_table}, data={state.loading_data}")
            
            # Force UI update to show loading indicators (non-blocking)
            try:
                await ui.run_javascript('void 0', timeout=1.0)  # Increased timeout for large datasets
            except Exception as js_error:
                print(f"DEBUG: JavaScript update timeout (expected with large datasets): {js_error}")
                # Continue anyway - loading indicators will show when components re-render
            
            # Update charts
            print("DEBUG: Calling update_charts")
            await update_charts(chart_components.column_chart_container,
                               chart_components.line_chart_container, filtered_df)
            
            # Clear chart loading states after successful rendering
            state.set_loading_state('charts', False)
            print(f"DEBUG: Chart loading states cleared - charts: {state.loading_charts}")

            # Update details table
            print("DEBUG: Calling create_table")
            await details_table.create_table(filtered_df, details_container)
            
            # Clear table loading state after successful rendering
            state.set_loading_state('table', False)
            print(f"DEBUG: Table loading state cleared - table: {state.loading_table}")
            
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
        
        # Immediately set loading states when filter changes
        state = get_global_state()
        state.set_loading_state('charts', True, 'Loading filtered data...')
        state.set_loading_state('table', True, 'Loading filtered data...')
        state.set_loading_state('data', True, 'Processing filters...')
        print(f"DEBUG: Loading states set immediately on filter change - charts: {state.loading_charts}, table: {state.loading_table}")
        
        # Update the filter state
        filter_state[filter_name] = value
        
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

            print("DEBUG: State updated, calling update_ui...")
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
        # This functionality is now handled by the ViewDataDialog component
        
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
    
    # Initialize UI
    try:
        # Call the sync version for initial setup
        on_filter_change('', None)
    except Exception:
        pass

@ui.page("/raw_data")
def raw_data_page():
     from pathlib import Path
     df_json = '[]'  # Empty JSON array as default
     if 'dwn_df_json' in app.storage.user:
        df_json = app.storage.user['dwn_df_json']
     
     ui.add_head_html('<script src="https://cdn.jsdelivr.net/npm/chart.js@4.5.0/dist/chart.umd.min.js"></script>')
     ui.add_head_html(f"<style>{(Path(__file__).parent / 'style.css').read_text()}</style>") 
     ui.add_body_html(f"{(Path(__file__).parent / 'data.html').read_text()}".replace('{{df_json}}', df_json))

@ui.page("/llms")
async def llm():
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from databricks import sql
    from databricks.sdk.core import Config
    from databricks import sdk
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
        #print(df.head())
    conn.close()
    print(df)
    #spark.table("hive_metastore.da.Fact_Sales").limit(100)
    spark = DatabricksSession.builder.clusterId('0805-063508-emq3q7q8').getOrCreate()
    #spark = DatabricksSession.builder.clusterId(os.environ['DB_CLUSTER_ID']).getOrCreate()
    #print(spark.table("hive_metastore.da.Fact_Sales1").limit(100))
    #df = spark.read.table("samples.nyctaxi.trips")
      # cfg with auth for Service Principal
    '''
    sp_cfg = sdk.config.Config()
    # request handler
    async def query(user, request: gr.Request):
        # user's email
        email = request.headers.get("X-Forwarded-Email")
        # queries the database (or cache) to fetch user session using the SP
        user_session = get_user_session(sp_cfg, email)
        # user's access token
        user_token = request.headers.get("X-Forwarded-Access-Token")
        # queries the SQL Warehouse on behalf of the end-user
        result = query_warehouse(user_token)
        # save stats in user session
        save_user_session(sp_cfg, email)
    return result
    '''

@ui.page("/agent")
async def agent():
    from ddgs import DDGS
    from bs4 import BeautifulSoup
    import requests
    from openai import OpenAI

    client = OpenAI(base_url="http://localhost:8080/v1",api_key="sk" )
    
    '''
    llm = LLM(
        model="openai/qwen3",  # Changed to local model identifier
        temperature=0.7,
        base_url="http://localhost:8080/v1",  # Assuming this is your llama.cpp server
        api_key="empty"  # Empty string to prevent OPENAI_API_KEY lookup
    )
    
    # Create agent without external tools
    agent = Agent(
        role='Market Research Expert',
        goal='Provide top brands and latest information based on user query',
        backstory="An AI assistant with custom LLM settings.",
        tools=[ScrapeWebsiteTool()],  # No external tools needed
        llm=llm
    )
    '''
    def search_web(query, max_results=5):
        with DDGS() as ddgs:
            return [r['href'] for r in ddgs.text(query, max_results=max_results)]

    def scrape_page(url):
        try:
            html = requests.get(url, timeout=5).text
            soup = BeautifulSoup(html, "html.parser")
            print(" ".join([p.get_text() for p in soup.find_all("p")])[:2000])
            return " ".join([p.get_text() for p in soup.find_all("p")])[:2000]  # limit size
        except:
            return ""
        
    def summarize(text, prompt="Summarize:"):
        stream = client.chat.completions.create(
        model="gemma3n",  # use whatever name your server registered
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": text}
        ],
        max_tokens=400,
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
        #return response.choices[0].message.content
    def loadfile(e):
        ldf =pl.read_parquet("data/"+e)
        pt.update_from_polars(ldf[['Product Line']].unique())
        ct.update_from_polars(ldf[['Country']].unique())

    async def run_agent(product, region, output_area):
        queries = [
            f"{product} mdeical device category market growth potential for next 5 years in {region}",
            f"{product} mdeical device category competitors of Stryker in {region}"
        ]
        output_area.clear()
        with output_area:
            for q in queries:
                urls = search_web(q)
                scraped = [scrape_page(u) for u in urls]
                combined_text = " ".join(scraped)
                with ui.column().classes('w-1/2'):
                    ui.label(q).classes("font-semibold mt-4")
                    text_area = ui.markdown().classes("whitespace-pre-wrap")
                    content = ''
                    for token in summarize(combined_text, prompt=f"You are a {role_input.value}, your goal is to {goal_input.value} from these paragraphs: {q}"):
                        content += token
                        text_area.set_content(content)
                        await ui.run_javascript('void 0',timeout=5) # Force UI update
    
    with ui.row(wrap=False).classes('w-full'):
        with ui.column().classes('w-1/6'):
            role_input = ui.textarea(label='Role',value="Business Development Manager")
            goal_input=ui.textarea(label='Goal',value="help demand planners generate long term forecasts by providing brief 2 bullet points containing insights " \
                "on market dynamics that can impact Stryker market share and growth and 1 bullet point containing CAGR over next 5 year of the category")
            search_input=ui.textarea(label="Search Text")
        with ui.column().classes('w-1/6'):
            data_files_select = ui.select(label='DataFiles',options=os.listdir("data/"),with_input=True,on_change=lambda e: loadfile(e.value)
                ).classes('w-40')
            pt=ui.table(columns=[{'name':'Product Line','field':'Product Line'}]
                        ,rows=[],row_key="name",on_select=lambda e:region_input.set_value(e.selection[0]['Product Line']),selection='single')
            ct=ui.table(columns=[{'name':'Country','field':'Country'}],rows=[],row_key="name",
                        on_select=lambda e:product_input.set_value(e.selection[0]['Country']),selection='single')
        with ui.column().classes('w-4/6 p-2 items-center'):
            ui.label("Medical Device Market Research Agent").classes("text-xl font-bold")
            with ui.row():
                product_input = ui.input("Enter Product") #.classes("w-96") #.bind_value_from(pt, 'name')
                region_input = ui.input("Enter Region") #.classes("w-96")
            ui.button("Search", on_click=lambda: run_agent(product_input.value, region_input.value, output_area)).classes("mt-4")
            output_area = ui.row(wrap=False).classes("p-2 bg-gray-100 rounded")