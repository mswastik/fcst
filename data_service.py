import pandas as pd
import polars as pl
from data_model import df, filtered_df, filtered_products, filtered_models, by_month
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS
from neuralforecast.losses.pytorch import MSE
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from dateutil.relativedelta import relativedelta

def clean_data(df):
    print(df)
    df=df.to_pandas()
    # Columns to drop
    columns_to_drop = [
        'UOM', 'NPI Flag', 'Pack Content', '`L0 ASP Final Rev',
        'Act Orders Rev Val', 'L2 DF Final Rev', 'L1 DF Final Rev',
        'L0 DF Final Rev', 'L2 Stat Final Rev', '`Fcst DF Final Rev',
        '`Fcst Stat Final Rev', '`Fcst Stat Prelim Rev', 'Fcst DF Final Rev Val'
    ]
    # Drop columns if they exist in the dataframe
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    # Fill NaN values with 0
    df = df.fillna(0)
    return df


def add_unique_id(df):
    # Create unique_id by concatenating multiple columns
    df["unique_id"] = df['Country'].astype(str) + "," + df['CatalogNumber'].astype(str)
    return df


def filter_last_36_months(df):
    # Ensure SALES_DATE is datetime
    df['SALES_DATE'] = pd.to_datetime(df['SALES_DATE'])
    # Get today's date and compute last full month
    today = datetime.today()
    last_full_month = datetime(today.year, today.month, 1) - relativedelta(months=1)
    # Calculate 36 months back
    start_date = last_full_month - relativedelta(months=35)  # 35 + 1 = 36 months
    # Filter
    filtered_df = df[(df['SALES_DATE'] >= start_date) & (df['SALES_DATE'] <= last_full_month)]
    return filtered_df

def prepare_data(df):
    clean_df = clean_data(df)
    udf = add_unique_id(clean_df)
    dft = filter_last_36_months(udf)
    return dft

def create_static_df(df):
    # Select relevant static features
    static_df = df[[
        'unique_id', 'Selling Division', 'Area', 'Stryker Group Region', 'Region', 'Country',
        'CatalogNumber', 'Business Sector', 'Business Unit', 'Franchise',
        'Product Line', 'IBP Level 5', 'IBP Level 6', 'IBP Level 7'
    ]].drop_duplicates()

    # Copy for encoding
    static_df_encoded = static_df.copy()

    # Apply Label Encoding to all categorical/object columns except unique_id
    for col in static_df_encoded.columns:
        if static_df_encoded[col].dtype == 'object' and col != 'unique_id':
            static_df_encoded[col] = LabelEncoder().fit_transform(static_df_encoded[col].astype(str))
    return static_df_encoded


def apply_filters(filters):
    """Apply filters to the dataset"""
    global filtered_df, filtered_products, filtered_models
    from data_model import df, filtered_df, filtered_products, filtered_models
    #mask = pd.Series(True, index=df.index)
    #mask=pl.Series([True])
    
    if filters.get('data_files'):
        # Apply data files filter (placeholder)
        pass
        
    if filters.get('location2') and filters.get('location1'):
       df = df.filter(pl.col(filters['location1']) == filters['location2'])
        
    if filters.get('product2') and filters.get('product1'):
        df = df.filter(pl.col(filters['product1']) == filters['product2'])
    
    # Update global filtered dataframe
    if filters.get('level'):
        df=df.group_by(['SALES_DATE',filters['level'],filters['location1']]).sum()
    else:
        df=df.group_by(['SALES_DATE',filters['product1'],filters['location1']]).sum()
    filtered_df = df
    
    # Update filtered products list
    try:
        filtered_products = filtered_df[filters['product2']].unique().tolist()
    except:
        pass
    # Update models list (placeholder for real model data)
    filtered_models = [f"Model for {product}" for product in filtered_products]
    
    return {
        'filtered_df': filtered_df,
        'filtered_products': filtered_products,
        'filtered_models': filtered_models
    }

def toggle_month_view(state):
    """Toggle between normal and month view"""
    global by_month
    #from models.data_model import by_month
    by_month = state
    return by_month

def create_clusters(df, file_path):
    from sklearn.cluster import Birch
    if 'unique_id' not in df.columns:
        df = df.with_columns(unique_id = pl.col('Country') + "," + pl.col('CatalogNumber'))
    df1 = df[['unique_id', 'SALES_DATE', '`Act Orders Rev']]
    df1 = df1.with_columns(ynorm=((pl.col('`Act Orders Rev')-pl.col('`Act Orders Rev').mean()) / pl.col('`Act Orders Rev').std()).over('unique_id'))
    df1 = df1.fill_nan(0)
    df1=df1.pivot(index='unique_id',on='SALES_DATE',values='ynorm',aggregate_function='sum')
    bi=Birch(n_clusters=6).fit(df1[:,1:])
    df1=df1.drop('birch',strict=False)
    df1=df1.with_columns(birch=bi.labels_)
    df1=df1['unique_id','birch']
    df=df.join(df1,on='unique_id')
    df.write_parquet(f"data/{file_path}")

def create_models_action(df, file_path):
    """Business logic for creating models"""
    forecast_list = []
    # dft = prepare_data(df) # df is already prepared
    dft = prepare_data(df)

    franchises = dft['Franchise'].unique()

    for franchise in tqdm(franchises, desc="Processing each Franchise"):
        df_filt = dft[dft['Franchise'] == franchise].copy()

        # Prepare columns
        df_fr = df_filt.rename(columns={'SALES_DATE': 'ds', '`Act Orders Rev': 'y'})
        df_fr['ds'] = pd.to_datetime(df_fr['ds'])
        df_fr['y'] = df_fr['y'].astype(float)
        df_fr = df_fr[['unique_id', 'ds', 'y']] 

        # Select static df
        static_df = create_static_df(df_filt)

        # NHITS model setup
        horizon = 56
        input_size = 56
        stacks = 3

        models = [NHITS(
            h=horizon,
            input_size=input_size,
            max_steps=800,
            stack_types=['identity'] * stacks,
            n_blocks=[3]*stacks,
            mlp_units=[[256, 256, 128]] * stacks,
            n_pool_kernel_size=[2, 4, 6],
            n_freq_downsample=[2, 4, 6],
            interpolation_mode='nearest',
            activation='ReLU',
            dropout_prob_theta=0.3,
            scaler_type='robust',
            loss=MSE(),
            valid_loss=MSE(),
            batch_size=64,
            windows_batch_size=64,
            random_seed=1,
            start_padding_enabled=True,
            learning_rate=1e-3,
            val_check_steps=100,
        )]

        nf = NeuralForecast(models=models, freq='MS')
        nf.fit(df=df_fr)
        
        # Forecast
        forecasts = nf.predict()
        forecasts['Franchise'] = franchise
        forecast_list.append(forecasts)

    # Combine all forecasts
    final_forecasts = pd.concat(forecast_list).reset_index(drop=True)
    
    # Define the column order used to construct 'unique_id'
    unique_id_columns = ["Country","CatalogNumber"]

    # Split 'unique_id' back into individual columns
    final_forecasts[unique_id_columns] = final_forecasts['unique_id'].str.split(",", expand=True)
    final_forecasts=final_forecasts.rename(columns={'ds':'SALES_DATE'})

    # Convert SALES_DATE to datetime
    final_forecasts['SALES_DATE'] = pd.to_datetime(final_forecasts['SALES_DATE']).astype("datetime64[us]")
    final_forecasts['NHITS'] = final_forecasts['NHITS'].astype('float16')
    ph=pl.read_parquet('data/phierarchy.parquet')
    try:
        lh=pl.read_parquet('data/lhierarchy.parquet').drop('Selling Division').unique()
    except:
        lh=pl.read_parquet('data/lhierarchy.parquet')
    final_forecasts=pl.from_pandas(final_forecasts).drop(['Franchise','unique_id'])
    final_forecasts=final_forecasts.join(ph,on='CatalogNumber',how='left')
    final_forecasts=final_forecasts.join(lh,on='Country',how='left')
    # Read the original parquet file
    original_df_polars = pl.read_parquet(f"data/{file_path}").drop('Selling Division').unique()
    final_forecasts=final_forecasts.filter(pl.col('Stryker Group Region')==original_df_polars['Stryker Group Region'].unique()[0])

    # Perform the join
    merged_df_polars = original_df_polars.join(final_forecasts,
                        on=['SALES_DATE', 'CatalogNumber', 'Country','Area','Stryker Group Region','Region',
                        'Business Sector','Business Unit','Franchise','Product Line','IBP Level 5','IBP Level 6','IBP Level 7'],
                                       how='outer',coalesce=True)
    # Save the merged dataframe back to the parquet file
    merged_df_polars.write_parquet(f"data/{file_path}")
    # Update the global filtered_df in models/data_model.py
    from data_model import filtered_df as global_filtered_df
    global_filtered_df = merged_df_polars

    #print(final_forecasts)
    return merged_df_polars # Return the updated dataframe

def generate_fc_action():
    """Business logic for generating forecast"""
    # Actual forecast generation logic would go here
    return f"Generating forecasts for {len(filtered_products)} products"

def change_fc_action():
    """Business logic for changing forecast settings"""
    # Actual forecast settings logic would go here
    return "Changing forecast settings"

def download_data_action():
    """Business logic for downloading data"""
    # Actual download implementation would go here
    return f"Downloading data for {len(filtered_df)} records"

def export_data_action(df):
    """Business logic for exporting data"""
    # Actual export implementation would go here
    return f"Exporting data for {len(filtered_df)} records"
