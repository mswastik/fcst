from nicegui import ui,app
from sql import sqlpd, phierarchy, lhierarchy
import polars as pl
import os
from script import forecast
from datetime import datetime
from dateutil.relativedelta import relativedelta
from forecast import gen_fc, create_models

ui.colors(brand='#ffb500')
today=datetime.today()

navcl='w-24 drop-shadow-none shadow-none text-[#000] hover:bg-black hover:font-brand hover:text-[#ffb500] rounded-none transition-all duration-200 ease-linear'
def nav():
    with ui.row(wrap=False).style(f'width:99.9dvw;max-height: 35px; margin-top:-15px; margin-left:-16px').classes('bg-brand'):
        with ui.element('div'):
            ui.button('Home',color=None,on_click=lambda: ui.navigate.to('/')).classes(replace=navcl).props('flat')
        with ui.element('div'):
            ui.button('Detail',color=None,on_click=lambda: ui.navigate.to('/detail')).classes(replace=navcl).props('flat')
        with ui.element('div'):
            ui.button('SQL',color=None,on_click=lambda: ui.navigate.to('/sql')).classes(add=navcl,remove='q-hoverable').props('flat')
        with ui.element('div'):
            ui.button('AI Chat',color=None,on_click=lambda: ui.navigate.to('/chat',new_tab=True)).classes(add=navcl,remove='q-hoverable').props('flat')
#path = os.path.expanduser("~")+'/fcst'

@ui.page('/')
async def home():
    ui.colors(brand='#ffb500',primary='black')
    nav()
    await ui.context.client.connected()
    r1=ui.row().style(f'width:99dvw')

    phier=pl.DataFrame(schema=['Area','Franchise'])
    lhier=pl.DataFrame(schema=['Area','Franchise'])

    async def loaddata(e):
        global phier, lhier
        try:
            phier=pl.read_parquet(f'data//phierarchy.parquet')
            lhier=pl.read_parquet(f'data//lhierarchy.parquet')
            df=pl.read_parquet(f'data//{e.value}')
            df=df.with_columns(pl.col('`Fcst Stat Prelim Rev').round(0))
            df=df.with_columns(pl.col('`Act Orders Rev').round(0))
            df1=df.clone()
            app.storage.tab['df']=df
            ref.refresh()
        except:
            pass

    async def filt_data(e):
        global df1
        df1=app.storage.tab.get('df').filter(pl.col(app.storage.tab.get('lh')).is_in(app.storage.tab.get('loc'))).filter(pl.col(app.storage.tab.get('ph')).is_in(app.storage.tab.get('prod')))
        df1=df1.with_columns(pl.col('SALES_DATE').dt.month().alias('Month'))
        df1=df1.with_columns(pl.col('SALES_DATE').dt.year().alias('Year'))
        df1=df1.with_columns(ActWFC=pl.when(pl.col('SALES_DATE')<today-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(pl.col('`Fcst Stat Prelim Rev')))
        app.storage.tab['ddf']=df1.pivot(on='Month',index=[app.storage.tab['ph'],'Year'],values='ActWFC',aggregate_function='sum',sort_columns=True)
        app.storage.tab['ddf']=app.storage.tab['ddf'][[app.storage.tab['ph'],'Year']+[str(i) for i in range(1,13)]]
        reft.refresh()
    
    async def cr_mod(e):
        create_models(df1,cc,app.storage.tab.get('lh'),app.storage.tab.get('ph'))

    @ui.refreshable
    async def ref():
        global phier, lhier
        app.storage.tab['lh']=loc_lvl.value
        app.storage.tab['ph']=prod_lvl.value
        loc_val = ui.select(label=loc_lvl.value,options=lhier[loc_lvl.value].unique().to_list(),with_input=True,multiple=True).bind_value_to(app.storage.tab,'loc').style('min-width: 90px; width:150px; margin-top:-14px').props('use-chips')
        prod_val = ui.select(label=prod_lvl.value,with_input=True,options=phier[prod_lvl.value].unique().to_list(),multiple=True).bind_value_to(app.storage.tab,'prod').style('min-width: 90px; width:150px; margin-top:-14px').props('use-chips')
        fh = ui.number(label='Months',max=36,step=6).style('max-width: 80px; margin-top:-13px')
        but = ui.button('Get Data',color='brand',on_click=filt_data).classes('hover:bg-black')
        genm = ui.button('Create Models',color='brand',on_click=filt_data).classes('hover:bg-black')
        genfc = ui.button('Get Forecast',color='brand',on_click=filt_data).classes('hover:bg-black')
    with r1:
        file_in = ui.select(label='Select File',options=os.listdir('data/'),on_change=loaddata).style('min-width: 120px; margin-top:-14px')
        loc_lvl = ui.select(label='Location Level',options=['Area','Region','Country'],with_input=True,value='Area',on_change=ref.refresh).style('min-width: 70px; width:100px; margin-top:-14px')
        prod_lvl = ui.select(label='Product Level',with_input=True,options=['Franchise','Business Unit','IBP Level 5','CatalogNumber'],value='Franchise',on_change=ref.refresh).style('min-width: 80px; width: 120px; margin-top:-14px')
        ref()
    with ui.tabs().classes('w-full') as tabs:
        one = ui.tab('Forecast')
        two = ui.tab('Model')
    with ui.tab_panels(tabs, value=one).classes('w-full'):
        with ui.tab_panel(one):
            @ui.refreshable
            def reft():
                #ui.table.from_polars(df1.group_by([loc_lvl.value,prod_lvl.value]).sum()[loc_lvl.value,prod_lvl.value,'`Act Orders Rev','`Fcst Stat Prelim Rev'])
                ui.table.from_polars(app.storage.tab.get('ddf'))
            reft()
            fb=ui.button('Forecast',color='brand',on_click=forecast(df1.group_by([loc_lvl.value,prod_lvl.value]).sum()['`Act Orders Rev']))
        with ui.tab_panel(two):
            ui.label('Second tab')


@ui.page('/sql')
async def sql():
    ui.colors(brand='#ffb500',primary='black')
    nav()
    r1=ui.row().style(f'width:99dvw')
    @ui.refreshable
    def ref():
        loc_val = ui.select(label=loc_lvl.value,options=lhier[loc_lvl.value].unique().to_list(),with_input=True,multiple=True).style('min-width: 90px; margin-top:-14px')
        prod_val = ui.select(label=prod_lvl.value,with_input=True,options=phier[prod_lvl.value].unique().to_list(),multiple=True).style('min-width: 90px;margin-top:-14px')
        fh = ui.number(label='Months',max=36,step=6).style('max-width: 80px; margin-top:-14px')
        but = ui.button('Get Data',color='brand',on_click=sqlpd(loc_lvl.value,loc_val.value,prod_lvl.value,prod_val.value,fh)).classes('hover:bg-black')
        hbut = ui.button('Update Hierarchy',color='brand',on_click=phierarchy).classes('hover:bg-black')
    with r1:
        loc_lvl = ui.select(label='Location Level',options=['Area','Region','Country'],with_input=True,value='Area',on_change=ref.refresh).style('min-width: 120px; margin-top:-14px')
        prod_lvl = ui.select(label='Product Level',with_input=True,options=['Franchise','Business Unit','IBP Level 5','CatalogNumber'],value='Franchise',on_change=ref.refresh).style('min-width: 90px; margin-top:-14px')
        ref()

@ui.page('/chat')
async def chat():
    nav()
    def loaddata(e):
        df=pl.read_parquet(f'data//{e.value}')
        return df[:30].to_pandas().to_csv()
    file_in = ui.select(label='Select File',options=os.listdir('data/'),on_change=loaddata).style('min-width: 120px; margin-top:-14px')
    messages=[]
    @ui.refreshable
    def chat(own_id='user'):
        if messages:
            for ind in messages:
                ui.chat_message(name=ind['name'],text=ind['content'], sent=ind['role']==own_id, stamp=ind['stamp'], avatar=ind['avatar'],text_html=True).classes('font-sans text-base leading-relaxed')
        else:
            ui.label('No messages yet').classes('mx-auto my-36')
        ui.run_javascript('window.scrollTo(0, document.body.scrollHeight)')
    ui.run_javascript('window.scrollTo(0, document.body.scrollHeight)')
    from llama_cpp import Llama
    from nicegui import run
    import asyncio
    llm = Llama(
        model_path="gguf\\qwen2.5-3b-instruct-q8_0.gguf",
        # n_gpu_layers=-1, # Uncomment to use GPU acceleration
        # seed=1337, # Uncomment to set a specific seed
        n_ctx=4648, # Uncomment to increase the context window
    )

    async def comp(PROMPT):
        sta.set_visibility(True)
        sp.visible=True
        sta.update()
        sp.update()
        response = await run.io_bound(llm.create_chat_completion, messages=[{"role": "user","content": PROMPT}])
        #res=''
        #for i in response:
        #    if 'content' in i['choices'][-1]['delta']:
        #        res+=i['choices'][-1]['delta']['content']
        messages.append({'role':'system','sent':False,'name':llm.model_path[5:-5],'avatar':f'https://robohash.org/system?bgset=bg2','content':response['choices'][-1]['message']['content'],'stamp': datetime.now().strftime('%X')})
        chat.refresh()

    async def send() -> None:
        messages.append({'role':'user','name':'You','avatar':f'https://robohash.org/user?bgset=bg2','content': text.value + loaddata(file_in),'stamp' : datetime.now().strftime('%X')})
        prompt=text.value
        text.value = ''
        chat.refresh()
        #await comp(prompt)
        sta.set_visibility(True)
        sp.visible=True
        sta.update()
        sp.update()
        response = await run.io_bound(llm.create_chat_completion, messages=[{"role": "user","content": prompt + loaddata(file_in)}])
        #res=''
        #for i in response:
        #    if 'content' in i['choices'][-1]['delta']:
        #        res+=i['choices'][-1]['delta']['content']
        messages.append({'role':'system','name':llm.model_path[5:-5],'sent':False,'avatar':f'https://robohash.org/system?bgset=bg2','content':response['choices'][-1]['message']['content'],'stamp': datetime.now().strftime('%X')})
        chat.refresh()
        sta.set_visibility(False)
        sp.visible=False
        sta.update()
        sp.update()


    with ui.footer().classes('bg-white').style('margin-bottom:6px;padding-top:5px'), ui.column().classes('w-4/5 mx-auto').style('margin-bottom:6px;padding-top:5px'):
        with ui.row().classes('w-4/5 no-wrap items-center').style('max-height:15px'):
            sta = ui.label(f'{llm.model_path[5:-5]} is responding').classes('text-slate-500')
            sp = ui.spinner('dots', size='lg', color='red')
            sta.visible, sp.visible=False,False
        with ui.row().classes('w-4/5 no-wrap items-center'):
            with ui.avatar().on('click', lambda: ui.navigate.to(home)):
                ui.image(f'https://robohash.org/user?bgset=bg2')
            text = ui.input(placeholder='Message').on('keydown.enter', send).props('rounded outlined input-class=mx-3').classes('flex-grow')

    await ui.context.client.connected()  # chat_messages(...) uses run_javascript which is only possible after connecting
    with ui.column().classes('w-4/5 mx-auto items-stretch'):
        chat('user')

    
ui.run(title="Forecast Review",reconnect_timeout=170)