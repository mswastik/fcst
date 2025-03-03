import polars as pl
from statsforecast import StatsForecast
from statsforecast.models import AutoARIMA

df= pl.read_parquet('C:\\Users\\smishra14\\OneDrive - Stryker\\data\\APAC.parquet')

def clean(df):
    df=df[['CatalogNumber','`Act Orders Rev', 'Fcst Stat Prelim Rev', 'L2 Stat Final Rev']]
    return df

def forecast(e,df):
    sf = StatsForecast(models=[AutoARIMA(season_length=12)],freq='ME')
    sf.fit(df['`Act Orders Rev'])
    print(sf.predict(h=12, level=[95]))
    return sf.predict(h=12, level=[95])
    