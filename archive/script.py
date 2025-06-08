import polars as pl
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA
from datetime import datetime
from dateutil.relativedelta import relativedelta
from nicegui import ui

df= pl.read_parquet('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet')
today = datetime.today()

def clean(df):
    df=df[['CatalogNumber','`Act Orders Rev', 'Fcst Stat Prelim Rev', 'L2 Stat Final Rev']]
    return df

def forecast(e,df):
    sf = StatsForecast(models=[AutoARIMA(season_length=12)],freq='ME')
    sf.fit(df['`Act Orders Rev'])
    print(sf.predict(h=12, level=[95]))
    return sf.predict(h=12, level=[95])
    
def rdf1(df,r2):
    #lab.text = str(a.cat) + " - "+ a.gdf['IBP Level 5'].unique()[0]+ " - "+ a.gdf['Franchise'].unique()[0]
    #dfw=ui.aggrid({'columnDefs': [{"field":cn,"editable": True} for cn in pdf.columns], 'rowData': pdf.to_dicts()}).on('cellValueChanged', lambda e: scf(e)
    #                                                                                                        ).style('min-width:950px;max-height:200px;margin-top:-15px')   
    ll=[{'type': 'line', 'data': []},{'type': 'line', 'data': []},{'type': 'line', 'data': []},{'type': 'line', 'data': []},{'type': 'line', 'data': []},{'type': 'line', 'data': []}]
    ll.extend([{'type':'bar','name':i,'data': []} for i in df['year'].unique().to_list()])
    k=6
    with r2:
        ch1=ui.echart({'xAxis':{'data':df['month'].unique().to_list()},'yAxis': {},'series': ll}).style('height:360px;width:780px; margin-top:13px; margin-left:0px; margin-right:6px')
        ch1.options['series'][0]['data']= df.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).filter(pl.col('SALES_DATE')<datetime(today.year,today.month,1)
                                                    +relativedelta(months=12)).sort(['month'],descending=False)['`Fcst DF Final Rev'].to_list()
        ch1.options['series'][0]['name']='DF'
        ch1.options['series'][1]['data']= df.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).filter(pl.col('SALES_DATE')<datetime(today.year,today.month,1)
                                                    +relativedelta(months=12)).sort(['month'],descending=False)['`Fcst Stat Final Rev'].to_list()
        ch1.options['series'][1]['name']='Stat'
        ch1.options['tooltip']={}
        ch1.options['legend']={'left':180}
        ch1.options['grid']={'right':5,'left':51,'bottom':34,'top':39}
        ch1.options['title']={'text':a.cat}
        ch1.options['toolbox']={'show': 'true','feature': { 'saveAsImage': {}}}
        for y in df['year'].unique():
            ch1.options['series'][k]['data']= df.filter(pl.col('year')==y).group_by('month').sum().sort('month',descending=False)['`Act Orders Rev'].to_list()
            ch1.options['series'][k]['name']=y
            k+=1
        ch2=ui.echart({'xAxis':{'type':'time','axisLabel': {'formatter': '{MMM} {yy}'}},'yAxis': {},'series':[{'type': 'line', 'data': []},{'type': 'line', 'data': []},{'type': 'line', 'data': [],'markLine':{'itemStyle':{'color':'grey'},'data':[]}}] }).style(
            'height:360px;width:650px; margin-top:13px;margin-right:0px;')
        ch2.options['series'][0]['data']= [[i['SALES_DATE'],i['`Fcst DF Final Rev']] for i in df.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('SALES_DATE',descending=False).iter_rows(named=True)]
        ch2.options['series'][0]['name']='DF'
        ch2.options['series'][1]['data']= [[i['SALES_DATE'],i['`Fcst Stat Final Rev']] for i in df.filter(pl.col('SALES_DATE')>=datetime(today.year,today.month,1)).sort('SALES_DATE',descending=False).iter_rows(named=True)]
        ch2.options['series'][1]['name']='Stat'
        ch2.options['series'][2]['data']= [[i['SALES_DATE'],i['`Act Orders Rev']] for i in df.sort('SALES_DATE',descending=False).iter_rows(named=True)]
        ch2.options['series'][2]['markLine']['data']= [{'xAxis':(datetime(today.year,today.month,1)+relativedelta(months=2)).isoformat(timespec="hours"),'name':'Lag2 month'}]
        ch2.options['series'][2]['name']='Orders'
        ch2.options['tooltip']={'trigger':'axis'}
        ch2.options['toolbox']={'show': 'true','feature': { 'saveAsImage': {}}}
        ch2.options['legend']={}
        ch2.options['grid']={'right':0,'left':51,'bottom':34,'top':39}
        #return ch1, ch2