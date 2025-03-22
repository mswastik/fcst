from nicegui import ui,app
from sql import sqlpd, phierarchy, lhierarchy
import polars as pl
import os
from script import forecast
from datetime import datetime
from dateutil.relativedelta import relativedelta

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
            ui.button('Chat',color=None,on_click=lambda: ui.navigate.to('/chat')).classes(add=navcl,remove='q-hoverable').props('flat')
#path = os.path.expanduser("~")+'/fcst'
try:
    phier=pl.read_parquet(f'data//phierarchy.parquet')
    lhier=pl.read_parquet(f'data//lhierarchy.parquet')
    df=pl.read_parquet(f'data//APAC.parquet')
    df=df.with_columns(pl.col('`Fcst Stat Prelim Rev').round(0))
    df=df.with_columns(pl.col('`Act Orders Rev').round(0))
    df1=df.clone()
except:
    phier=pl.DataFrame(schema=['Area','Franchise'])
    lhier=pl.DataFrame(schema=['Area','Franchise'])

@ui.page('/')
async def home():
    ui.colors(brand='#ffb500',primary='black')
    nav()
    await ui.context.client.connected()
    r1=ui.row().style(f'width:99dvw')

    def filt_data(e):
        global df1
        print(app.storage.tab.get('lh'))
        print(app.storage.tab.get('loc'))
        df1=df.filter(pl.col(app.storage.tab.get('lh')).is_in(app.storage.tab.get('loc'))).filter(pl.col(app.storage.tab.get('ph')).is_in(app.storage.tab.get('prod')))
        df1=df1.with_columns(pl.col('SALES_DATE').dt.month().alias('Month'))
        #df1=df1.with_columns(pl.col('Month').str.to_integer())
        print(df1)
        df1=df1.with_columns(pl.col('SALES_DATE').dt.year().alias('Year'))
        df1=df1.with_columns(ActWFC=pl.when(pl.col('SALES_DATE')<today-relativedelta(months=1)).then(pl.col('`Act Orders Rev')).otherwise(pl.col('`Fcst Stat Prelim Rev')))
        app.storage.tab['ddf']=df1.pivot(on='Month',index=[app.storage.tab['ph'],'Year'],values='ActWFC',aggregate_function='sum',sort_columns=True)
        #app.storage.tab['ddf']=app.storage.tab['ddf'][[app.storage.tab['ph'],'Year']+df1['Month'].unique().cast(pl.Utf8).to_list()]
        app.storage.tab['ddf']=app.storage.tab['ddf'][[app.storage.tab['ph'],'Year']+[str(i) for i in range(1,13)]]
        reft.refresh()

    @ui.refreshable
    def ref():
        app.storage.tab['lh']=loc_lvl.value
        app.storage.tab['ph']=prod_lvl.value
        #loc_val = ui.select(label=loc_lvl.value,options=lhier[loc_lvl.value].unique().to_list(),with_input=True,multiple=True,label='with chips',on_change=lambda e: app.storage.tab['loc']=e.value).style('min-width: 90px; margin-top:-14px')
        loc_val = ui.select(label=loc_lvl.value,options=lhier[loc_lvl.value].unique().to_list(),with_input=True,multiple=True).bind_value_to(app.storage.tab,'loc').style('min-width: 90px; margin-top:-14px').props('use-chips')
        prod_val = ui.select(label=prod_lvl.value,with_input=True,options=phier[prod_lvl.value].unique().to_list(),multiple=True).bind_value_to(app.storage.tab,'prod').style('min-width: 90px;margin-top:-14px').props('use-chips')
        fh = ui.number(label='Months',max=36,step=6).style('max-width: 80px; margin-top:-14px')
        but = ui.button('Get Data',color='brand',on_click=filt_data).classes('hover:bg-black')
    with r1:
        loc_lvl = ui.select(label='Location Level',options=['Area','Region','Country'],with_input=True,value='Area',on_change=ref.refresh).style('min-width: 120px; margin-top:-14px')
        prod_lvl = ui.select(label='Product Level',with_input=True,options=['Franchise','Business Unit','IBP Level 5','CatalogNumber'],value='Franchise',on_change=ref.refresh).style('min-width: 90px; margin-top:-14px')
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
def sql():
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
    
ui.run(title="Forecast Review",reconnect_timeout=170)