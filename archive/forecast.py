import pandas as pd
import polars as pl
from xgboost import XGBRegressor, XGBRFRegressor
import lightgbm as lgb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from mlforecast import MLForecast
from mlforecast.lag_transforms import ExpandingMean, RollingMean
from mlforecast.target_transforms import Differences
from sklearn.ensemble import VotingRegressor
import pickle, os

today= datetime.now()

'''
df=pd.read_parquet("C:\\Users\\smishra14\\OneDrive - Stryker\\data\\Endoscopy.parquet")
df=df[['CatalogNumber','SALES_DATE','`Act Orders Rev','`Fcst Stat Prelim Rev']]
df1=df[df['SALES_DATE']<today-relativedelta(months=1)]

cc=df.groupby('CatalogNumber').sum(numeric_only=True)['`Act Orders Rev'].sort_values(ascending=False).reset_index()

fdf=df1[df1['CatalogNumber'].isin(cc['CatalogNumber'][:100])]
fdf=fdf.rename(columns={'SALES_DATE':'ds','CatalogNumber':'unique_id','`Act Orders Rev':'y'})
fdf=fdf[['ds','unique_id','y']]
fdf=fdf.groupby(['unique_id','ds']).sum().reset_index()
pdf=fdf[fdf['ds']<today-relativedelta(months=6)]
fcst = MLForecast(
    models=[XGBRFRegressor(),XGBRegressor()],
    freq='MS',
    lags=[3,4,5,6,12,24],)
fcst.fit(pdf.fillna(0))
preds=fcst.predict(60)
fcst.save('models/')
'''

def preprocess(df):
    df=df.with_columns(month=pl.col('SALES_DATE').dt.month())
    df=df.with_columns(days=(pl.col('SALES_DATE')-datetime(year=2021,month=1,day=1)).dt.total_days())
    df=df.with_columns(lag12=pl.col('`Act Orders Rev').shift(12).over('CatalogNumber'))
    df=df.with_columns(pl.when(pl.col('lag12').is_null()).then(pl.col('`Act Orders Rev')).otherwise(pl.col('lag12')).alias('lag12'))
    return df

def fcpreprocess1(df,mon,cat):
    fdf=pl.DataFrame({'CatalogNumber':[cat]*mon,'SALES_DATE':pl.date_range(df['SALES_DATE'].max()+relativedelta(months=1),df['SALES_DATE'].max()+relativedelta(months=mon), "1mo",eager=True)})
    df=df.with_columns(actwfc=pl.col('`Act Orders Rev'))
    fdf=fdf.with_columns(month=pl.col('SALES_DATE').dt.month())
    fdf=fdf.with_columns(days=(pl.col('SALES_DATE')-datetime(year=2021,month=1,day=1)).dt.total_days())
    fdf=pl.concat([df,fdf],how='diagonal_relaxed')
    fdf=fdf.with_columns(lag12=pl.col('actwfc').shift(12).over('CatalogNumber'))
    fdf=fdf.with_columns(pl.when(pl.col('lag12').is_null()).then(pl.col('`Act Orders Rev')).otherwise(pl.col('lag12')).alias('lag12'))
    return fdf

def fcpreprocess(df,mon,cat,pred):
    df3=df.filter(pl.col('SALES_DATE')>df['SALES_DATE'].max()-relativedelta(months=mon)).with_columns(actwfc=pred,pred=pred)
    df=pl.concat([df.filter(pl.col('SALES_DATE')<=df['SALES_DATE'].max()-relativedelta(months=mon)),df3],how='diagonal_relaxed')
    fdf=pl.DataFrame({'CatalogNumber':[cat]*mon,'SALES_DATE':pl.date_range(df['SALES_DATE'].max()+relativedelta(months=1),df['SALES_DATE'].max()+relativedelta(months=mon), "1mo",eager=True)})
    fdf=fdf.with_columns(month=pl.col('SALES_DATE').dt.month())
    fdf=fdf.with_columns(days=(pl.col('SALES_DATE')-datetime(year=2021,month=1,day=1)).dt.total_days())
    fdf=pl.concat([df,fdf],how='diagonal_relaxed')
    fdf=fdf.with_columns(lag12=pl.col('actwfc').shift(12).over('CatalogNumber'))
    fdf=fdf.with_columns(pl.when(pl.col('lag12').is_null()).then(pl.col('`Act Orders Rev')).otherwise(pl.col('lag12')).alias('lag12'))
    return fdf

def create_models(df,cc,lh,ph,overwrite=False):
    pp=preprocess(df)
    for i in cc['CatalogNumber'][:10]:
        if overwrite==True | (not os.path.isfile(f'models/{i}.pkl','wb')):
            p1=pp.filter(pl.col('CatalogNumber')==i).to_pandas()
            mod1=XGBRegressor()
            mod2=XGBRegressor(booster='gblinear')
            #mod2=LGBRegressor(linear_tree= True)
            pipe=VotingRegressor([('xgbm',mod1),('lgbm',mod2)])
            pipe.fit(X=p1[['month','days','lag12']],y=p1['`Act Orders Rev'])
            with open(f'models/{lh}/{ph}/{i}.pkl','wb') as f:
                pickle.dump(pipe,f)

def gen_fc(df1,cc):
    fin_df=pl.DataFrame()
    for i in cc['CatalogNumber'][:10]:
        df2=df1.filter(pl.col('CatalogNumber')==i)
        df2=fcpreprocess1(df2,12,i)
        with open(f'models/{i}.pkl','rb') as f:
            pr=pickle.load(f)
            md=df1['SALES_DATE'].max()
            for t in range(5):
                pred=pr.predict(df2.filter(pl.col('SALES_DATE')>md)[['month','days','lag12']].to_pandas())
                md=df2['SALES_DATE'].max()
                df2=fcpreprocess(df2,12,i,pred)
        df2=df2.filter(pl.col('SALES_DATE')<=df2['SALES_DATE'].max()-relativedelta(months=12))
        fin_df=pl.concat([fin_df,df2])
    return fin_df