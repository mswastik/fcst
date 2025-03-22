import pandas as pd
from xgboost import XGBRegressor, XGBRFRegressor
import lightgbm as lgb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from mlforecast import MLForecast
from mlforecast.lag_transforms import ExpandingMean, RollingMean
from mlforecast.target_transforms import Differences

today= datetime.now()

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