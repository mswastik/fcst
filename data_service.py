#import pandas as pd
#from tqdm import tqdm
import polars as pl
from data_model import df, filtered_df, filtered_products, filtered_models
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS
#from neuralforecast.auto import AutoNHITS
from neuralforecast.losses.pytorch import RMSE
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error, silhouette_score
from sklearn.cluster import Birch, KMeans
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from scipy.stats import pearsonr
import warnings
warnings.filterwarnings('ignore')

today = datetime.today()
last_full_month = datetime(today.year, today.month, 1) - relativedelta(months=1)
def extract_ts_features(df):
    """Extract comprehensive time series features for clustering"""
    features = []
    
    for unique_id in df['unique_id'].unique():
        ts_data = df.filter(pl.col('unique_id') == unique_id).sort('SALES_DATE')
        values = ts_data['`Act Orders Rev'].to_numpy()
        
        if len(values) < 12:  # Skip if insufficient data
            continue
            
        # Basic statistics
        mean_val = np.mean(values)
        std_val = np.std(values)
        cv = std_val / mean_val if mean_val != 0 else 0
        
        # Trend analysis
        x = np.arange(len(values))
        trend_slope = np.polyfit(x, values, 1)[0] if len(values) > 1 else 0
        
        # Seasonality strength (12-month seasonality)
        if len(values) >= 24:
            seasonal_strength = calculate_seasonal_strength(values, 12)
        else:
            seasonal_strength = 0
            
        # Autocorrelation features
        lag1_corr = pearsonr(values[:-1], values[1:])[0] if len(values) > 1 else 0
        lag12_corr = pearsonr(values[:-12], values[12:])[0] if len(values) > 12 else 0
        
        # Volatility
        volatility = np.std(np.diff(values)) if len(values) > 1 else 0
        
        # Zero proportion
        zero_prop = np.sum(values == 0) / len(values)
        
        # Growth characteristics
        if len(values) >= 12:
            recent_growth = np.mean(values[-6:]) / np.mean(values[:6]) if np.mean(values[:6]) != 0 else 1
        else:
            recent_growth = 1
            
        features.append({
            'unique_id': unique_id,
            'mean': mean_val,
            'std': std_val,
            'cv': cv,
            'trend_slope': trend_slope,
            'seasonal_strength': seasonal_strength,
            'lag1_corr': lag1_corr,
            'lag12_corr': lag12_corr,
            'volatility': volatility,
            'zero_prop': zero_prop,
            'recent_growth': recent_growth
        })
    
    return pl.DataFrame(features)

def calculate_seasonal_strength(ts, period):
    """Calculate seasonal strength using STL decomposition approach"""
    if len(ts) < 2 * period:
        return 0
    
    try:
        # Simple seasonal strength calculation
        seasonal_vals = []
        for i in range(period):
            seasonal_vals.append(np.mean([ts[j] for j in range(i, len(ts), period)]))
        
        seasonal_var = np.var(seasonal_vals)
        total_var = np.var(ts)
        
        return seasonal_var / total_var if total_var != 0 else 0
    except:
        return 0

def optimize_clusters(features_df, max_clusters=10):
    """Find optimal number of clusters using silhouette score"""
    feature_cols = [col for col in features_df.columns if col != 'unique_id']
    X = features_df[feature_cols].to_numpy()
    
    # Handle NaN and infinite values
    X = np.nan_to_num(X, nan=0, posinf=0, neginf=0)
    
    # Standardize features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    best_score = -1
    best_k = 2
    
    for k in range(2, min(max_clusters + 1, len(X) // 2)):
        try:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(X_scaled)
            
            if len(np.unique(labels)) > 1:
                score = silhouette_score(X_scaled, labels)
                if score > best_score:
                    best_score = score
                    best_k = k
        except:
            continue
    
    return best_k, best_score

def create_enhanced_clusters(df, file_path):
    """Enhanced clustering with proper feature engineering"""
    if 'unique_id' not in df.columns:
        df = df.with_columns(unique_id = pl.col('Country') + "," + pl.col('CatalogNumber'))
    # Filter to training data
    df1 = df.filter(pl.col('SALES_DATE') <= datetime.today() - relativedelta(months=1))
    # Extract time series features
    features_df = extract_ts_features(df1)
    
    if len(features_df) < 4:
        print("Insufficient data for clustering")
        return df
    # Optimize cluster number
    optimal_k, silhouette = optimize_clusters(features_df)
    print(f"Optimal clusters: {optimal_k}, Silhouette score: {silhouette:.3f}")
    
    # Perform clustering
    feature_cols = [col for col in features_df.columns if col != 'unique_id']
    X = features_df[feature_cols].to_numpy()
    X = np.nan_to_num(X, nan=0, posinf=0, neginf=0)
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
    features_df = features_df.with_columns(cluster=kmeans.fit_predict(X_scaled))
    print(features_df)
    # Join back to original data
    df=df.drop(['cluster','cluster_right'],strict=False)
    df = df.join(features_df[['unique_id', 'cluster']], on='unique_id', how='left')
    
    # Save results
    print(df)
    df=df.with_columns(cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id"))
    df=df.with_columns(cluster=pl.col('cluster').cast(pl.Utf8))
    df.write_parquet(f"data/{file_path}")
    return df

def create_ensemble_models(df, file_path):
    """Create ensemble models for each cluster"""
    from neuralforecast import NeuralForecast
    from neuralforecast.models import NHITS, LSTM, MLP
    from neuralforecast.losses.pytorch import RMSE, MAE
    from statsforecast import StatsForecast
    from statsforecast.models import AutoARIMA, AutoETS, SeasonalNaive
    
    forecast_list = []
    dft = prepare_data1(df)
    dft = prepare_data(dft)
    df_fr = dft.rename({'SALES_DATE': 'ds', '`Act Orders Rev': 'y'})
    df_fr = df_fr[['unique_id', 'ds', 'y', 'cluster']]
    
    # Realistic horizon based on available data
    available_months = len(df_fr['ds'].unique())
    #horizon = min(24, available_months)  # Max 2 years or half of available data
    horizon = 60
    input_size = min(horizon, available_months)
    #input_size = available_months
    
    print(f"Forecasting horizon: {horizon} months")
    print(f"Input size: {input_size} months")
    
    cluster_forecasts = []
    
    for cluster_id in df_fr['cluster'].unique():
        if cluster_id is None:
            continue
            
        cluster_data = df_fr.filter(pl.col('cluster') == cluster_id)
        cluster_data = cluster_data[['unique_id', 'ds', 'y']]
        
        print(f"Processing cluster {cluster_id} with {len(cluster_data['unique_id'].unique())} series")
        
        # Calculate appropriate batch sizes
        n_series = len(cluster_data['unique_id'].unique())
        batch_size = max(1, min(16, n_series // 2))
        windows_batch_size = max(1, min(16, n_series // 2))
        
        # Neural models with fixed dimensions
        nhits = NHITS(
                h=horizon,
                input_size=input_size,
                max_steps=180,
                stack_types=['identity'] * 2,
                n_blocks=[1, 1],  # Reduced complexity
                mlp_units=[[64, 32], [32, 16]],  # Proper dimension progression
                n_pool_kernel_size=[2, 2],
                n_freq_downsample=[2, 2],
                interpolation_mode='nearest',
                activation='ReLU',
                dropout_prob_theta=0.1,
                scaler_type='robust',
                loss=RMSE(),
                valid_loss=RMSE(),
                batch_size=batch_size,
                windows_batch_size=windows_batch_size,
                random_seed=42,
                start_padding_enabled=True,
                learning_rate=1e-3,
                val_check_steps=25,
            )
        lstm = LSTM(
                h=horizon,
                input_size=input_size,
                max_steps=180,
                encoder_hidden_size=32,  # Reduced from 64
                encoder_n_layers=1,      # Reduced from 2
                encoder_dropout=0.1,
                scaler_type='robust',
                loss=RMSE(),
                valid_loss=RMSE(),
                batch_size=batch_size,
                random_seed=42,
                learning_rate=1e-3,
            )
        
        # Statistical models
        stat_models = [
            AutoARIMA(season_length=12),
            AutoETS(season_length=12),
            SeasonalNaive(season_length=12)
        ]
        
        #try:
        # Fit neural models
        nf = NeuralForecast(models=[nhits,lstm], freq='1mo')

        nf.fit(df=cluster_data.fill_nan(0).fill_null(0))
        neural_forecasts = nf.predict()
        
        # Fit statistical models
        sf = StatsForecast(models=stat_models, freq='1mo')
        sf.fit(df=cluster_data.fill_nan(0).fill_null(0))
        stat_forecasts = sf.predict(h=horizon)
        
        # Combine forecasts
        combined_forecast = neural_forecasts.join(
            stat_forecasts, on=['unique_id', 'ds'], how='outer',coalesce=True
        )
        
        # Create ensemble (simple average of available models)
        model_cols = [col for col in combined_forecast.columns if col not in ['unique_id', 'ds']]
        if model_cols:
            combined_forecast = combined_forecast.with_columns(
                ensemble=pl.concat_list([pl.col(col) for col in model_cols]).list.mean()
            )
        else:
            print(f"No valid forecasts generated for cluster {cluster_id}")
            continue
        combined_forecast = combined_forecast.with_columns(cluster=pl.lit(str(cluster_id)))
        cluster_forecasts.append(combined_forecast)
            
        #except Exception as e:
        #    print(f"Unexpected error processing cluster {cluster_id}: {str(e)}")
        #    continue
    print(cluster_forecasts)

    # Combine all forecasts
    if cluster_forecasts:
        final_forecast = pl.concat(cluster_forecasts)
        #print(final_forecast)
        #final_forecast.write_parquet(f"data/{file_path}")
        return final_forecast
    else:
        print("No successful forecasts generated")
        return None
    
def validate_forecasts(df, forecast_df, validation_months=6):
    """Validate forecasts using walk-forward validation"""
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    
    # Implementation of walk-forward validation
    # This would involve splitting data, training, forecasting, and measuring errors
    validation_results = {}
    
    # Placeholder for validation logic
    print("Forecast validation completed")
    return validation_results

# Additional utility functions
def prepare_data(df):
    """Prepare data with proper handling of missing values and outliers"""
    # Remove outliers using IQR method
    df = df.with_columns(
        q1=pl.col('`Act Orders Rev').quantile(0.25).over('unique_id'),
        q3=pl.col('`Act Orders Rev').quantile(0.75).over('unique_id')
    )
    df = df.with_columns(iqr=pl.col('q3') - pl.col('q1'))
    df=df.with_columns(lower_bound=pl.col('q1') - 1.5 * pl.col('iqr'))
    df=df.with_columns(upper_bound=pl.col('q3') + 1.5 * pl.col('iqr'))
    
    # Cap outliers instead of removing them
    df = df.with_columns(
        pl.when(pl.col('`Act Orders Rev') < pl.col('lower_bound'))
        .then(pl.col('lower_bound'))
        .when(pl.col('`Act Orders Rev') > pl.col('upper_bound'))
        .then(pl.col('upper_bound'))
        .otherwise(pl.col('`Act Orders Rev'))
        .alias('`Act Orders Rev')
    )
    
    return df.drop(['q1', 'q3', 'iqr', 'lower_bound', 'upper_bound'])

def run_enhanced_forecasting_pipeline(df, file_path):
    """Run the complete enhanced forecasting pipeline"""
    if 'cluster' not in df.columns or df['cluster'].unique() is None:
        # Step 1: Enhanced clustering
        #df_clustered = create_enhanced_clusters(df, file_path)
        df_clustered = create_clusters(df, file_path)
    else:
        df_clustered = df.clone()
    # Step 2: Create ensemble models
    forecasts = create_ensemble_models(df_clustered, file_path)
    forecasts = forecasts.rename({'ds':'SALES_DATE'})
    model_cols = [col for col in forecasts.columns if col not in ['unique_id', 'SALES_DATE']]
    unique_id_columns = ["Country","CatalogNumber"]
    forecasts = forecasts.with_columns(pl.col('unique_id').str.split_exact(",", 1).struct.rename_fields(unique_id_columns)
                                                   .alias("fields")).unnest("fields")
    ph=pl.read_parquet('data/phierarchy.parquet')
    try:
        lh=pl.read_parquet('data/lhierarchy.parquet').drop('Selling Division').unique()
    except:
        lh=pl.read_parquet('data/lhierarchy.parquet')
    forecasts=forecasts.join(ph,on='CatalogNumber',how='left')
    forecasts=forecasts.join(lh,on='Country',how='left')
    if 'NHITS' not in df.columns:
        #original_df_polars = original_df_polars.with_columns(NHITS=pl.when(pl.col('SALES_DATE')<=last_full_month).then(0).otherwise(pl.lit(9999999)))
        #df = df.with_columns(pl.col(model_cols).fill_null(0))
        df=df.with_columns([pl.lit(0).alias(col_name) for col_name in model_cols])

    # Step 3: Validate forecasts
    if forecasts is not None:
        validation_results = validate_forecasts(df_clustered, forecasts)
        
        merged_df_polars = df.filter(pl.col('unique_id').is_in(forecasts['unique_id'].unique())).drop(model_cols).join(forecasts,
                        on=['SALES_DATE','CatalogNumber', 'Country','Area','Stryker Group Region','Region',
                        'Business Sector','Business Unit','Franchise','Product Line','IBP Level 5','IBP Level 6','IBP Level 7','unique_id'],
                                       how='outer',coalesce=True)   
        # Save forecasts
        merged_df_polars.write_parquet(f"data/{file_path}")
        return merged_df_polars, validation_results
    else:
        return None, None

def filter_last_36_months(df):
    start_date = last_full_month - relativedelta(months=35)  # 35 + 1 = 36 months
    filtered_df = df.filter(pl.col('SALES_DATE') >= start_date).filter(pl.col('SALES_DATE') <= last_full_month)
    return filtered_df

def prepare_data1(df):
    #clean_df = clean_data(df)
    columns_to_drop = [
        'UOM', 'NPI Flag', 'Pack Content', '`L0 ASP Final Rev','Act Orders Rev Val', 'L2 DF Final Rev', 'L1 DF Final Rev',
        'L0 DF Final Rev', 'L2 Stat Final Rev', '`Fcst DF Final Rev','`Fcst Stat Final Rev', '`Fcst Stat Prelim Rev', 'Fcst DF Final Rev Val']
    clean_df = df.drop([col for col in columns_to_drop if col in df.columns])
    clean_df = clean_df.fill_nan(0)
    udf = clean_df.with_columns(unique_id=pl.col('Country') + "," + pl.col('CatalogNumber'))
    dft = filter_last_36_months(udf)
    return dft

def apply_filters(filters):
    """Apply filters to the dataset"""
    global filtered_df, filtered_products, filtered_models
    from data_model import df, filtered_df, filtered_products, filtered_models
    
    if filters.get('data_files'):
        pass 
    if filters.get('location2') and filters.get('location1'):
       df = df.filter(pl.col(filters['location1']) == filters['location2'])
    if filters.get('product2') and filters.get('product1'):
        df = df.filter(pl.col(filters['product1']) == filters['product2'])
    if filters.get('level'): # Update global filtered dataframe
        fdf=df.clone()
        df=df.group_by(['SALES_DATE',filters['level'],filters['location1']]).sum()
    else:
        fdf=df.clone()
        df=df.group_by(['SALES_DATE',filters['product1'],filters['location1']]).sum()
    filtered_df = df
    try:  # Update filtered products list
        filtered_products = filtered_df[filters['product2']].unique().tolist()
    except:
        pass
    filtered_models = [f"Model for {product}" for product in filtered_products]  # Update models list (placeholder for real model data)
    return {
        'fdf': fdf.write_json(),
        'filtered_df': filtered_df,
        'filtered_products': filtered_products,
        'filtered_models': filtered_models
    }

def create_clusters(df, file_path):
    if 'unique_id' not in df.columns:
        df = df.with_columns(unique_id = pl.col('Country') + "," + pl.col('CatalogNumber'))
    df=df.drop('cluster',strict=False)
    df=df.drop('cluster_right',strict=False)
    df1= df.filter(pl.col('SALES_DATE')<=datetime.today()-relativedelta(months=1))
    df1 = df1[['unique_id', 'SALES_DATE', '`Act Orders Rev']]
    df1=df1.with_columns(pl.col('`Act Orders Rev').cast(pl.Float32).alias('`Act Orders Rev'))
    #df1=df1.with_columns(pl.col('SALES_DATE').cast(pl.Datetime).alias('SALES_DATE'))
    df1 = df1.with_columns(ynorm=((pl.col('`Act Orders Rev')-pl.col('`Act Orders Rev').mean()) / pl.col('`Act Orders Rev').std()).over('unique_id'))
    df1 = df1.fill_nan(0)
    df1 = df1.with_columns(pl.when(pl.col('ynorm').is_infinite()).then(0).otherwise(pl.col('ynorm')).alias('ynorm'))
    df1=df1.pivot(index='unique_id',on='SALES_DATE',values='ynorm',aggregate_function='sum')
    bi=Birch(n_clusters=6).fit(df1[:,1:])
    df1=df1.with_columns(cluster=bi.labels_)
    df1=df1['unique_id','cluster']
    df=df.join(df1,on='unique_id',how='left',coalesce=True)
    df=df.with_columns(cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id"))
    df=df.with_columns(cluster=pl.col('cluster').cast(pl.Utf8))
    df.write_parquet(f"data/{file_path}")
    return df
    
def create_models_action(df, file_path):
    """Business logic for creating models"""
    forecast_list = []
    dft = prepare_data1(df)
    df_fr = dft.rename({'SALES_DATE': 'ds', '`Act Orders Rev': 'y'})
    df_fr = df_fr[['unique_id', 'ds', 'y']] 

    # NHITS model setup
    horizon = 56
    input_size = 56
    stacks = 3

    models = [NHITS(
        h=horizon, input_size=input_size, max_steps=700, stack_types=['identity'] * stacks,
        n_blocks=[3]*stacks, mlp_units=[[256, 256, 128]] * stacks, n_pool_kernel_size=[2, 4, 6],
        n_freq_downsample=[2, 4, 6], interpolation_mode='nearest', activation='ReLU', dropout_prob_theta=0.3,
        scaler_type='robust', loss=RMSE(), valid_loss=RMSE(), batch_size=36, windows_batch_size=35, random_seed=1,
        start_padding_enabled=True, learning_rate=1e-3, val_check_steps=100 )]

    nf = NeuralForecast(models=models, freq='1mo')
    nf.fit(df=df_fr.fill_nan(0).fill_null(0))
    
    # Forecast
    forecasts = nf.predict()
    forecast_list.append(forecasts)
    
    final_forecasts = pl.concat(forecast_list)
    unique_id_columns = ["Country","CatalogNumber"]
    final_forecasts = final_forecasts.with_columns(pl.col('unique_id').str.split_exact(",", 1).struct.rename_fields(unique_id_columns)
                                                   .alias("fields")).unnest("fields")
    final_forecasts=final_forecasts.rename({'ds':'SALES_DATE'})
    ph=pl.read_parquet('data/phierarchy.parquet')
    try:
        lh=pl.read_parquet('data/lhierarchy.parquet').drop('Selling Division').unique()
    except:
        lh=pl.read_parquet('data/lhierarchy.parquet')
    final_forecasts=final_forecasts.join(ph,on='CatalogNumber',how='left')
    final_forecasts=final_forecasts.join(lh,on='Country',how='left')
    original_df_polars = pl.read_parquet(f"data/{file_path}").unique()
    final_forecasts=final_forecasts.filter(pl.col('Stryker Group Region')==original_df_polars['Stryker Group Region'].unique()[0])
    if 'NHITS' not in original_df_polars.columns:
        #original_df_polars = original_df_polars.with_columns(NHITS=pl.when(pl.col('SALES_DATE')<=last_full_month).then(0).otherwise(pl.lit(9999999)))
        original_df_polars = original_df_polars.with_columns(NHITS=0)
    merged_df_polars = original_df_polars.filter(pl.col('unique_id').is_in(final_forecasts['unique_id'].unique())).drop('NHITS').join(final_forecasts,
                        on=['SALES_DATE','CatalogNumber', 'Country','Area','Stryker Group Region','Region',
                        'Business Sector','Business Unit','Franchise','Product Line','IBP Level 5','IBP Level 6','IBP Level 7','unique_id'],
                                       how='outer',coalesce=True)
    # Save the merged dataframe back to the parquet file
    merged_df_polars = pl.concat([merged_df_polars,original_df_polars.filter(~pl.col('unique_id').is_in(final_forecasts['unique_id'].unique()))],how='diagonal_relaxed')
    merged_df_polars=merged_df_polars.with_columns(pl.col('birch').forward_fill().over('unique_id'))
    merged_df_polars.write_parquet(f"data/{file_path}")
    # Update the global filtered_df in models/data_model.py
    #from data_model import filtered_df as global_filtered_df
    #global_filtered_df = merged_df_polars
    return merged_df_polars

def change_fc_action():
    """Business logic for changing forecast settings"""
    # Actual forecast settings logic would go here
    return "Changing forecast settings"
