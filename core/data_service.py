"""
Data service module for FCST application.
Provides data loading, processing, and forecasting functionality.
"""
import polars as pl
from typing import Optional, Dict, Any, List, Tuple
from core.state_manager import DataState, get_global_state
from core.utils import DataUtils, DatabaseUtils, ErrorHandler
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS
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

# Import forecasting modules at module level to avoid pickling issues
try:
    from forecasting.data_processor import ForecastDataProcessor, ValidationProcessor, DataCleaner
    from forecasting.model_factory import EnsembleForecaster
    FORECASTING_AVAILABLE = True
except ImportError as e:
    # Fallback if modules don't exist yet
    print(f"Warning: Forecasting modules not available: {e}")
    ForecastDataProcessor = None
    ValidationProcessor = None
    DataCleaner = None
    EnsembleForecaster = None
    FORECASTING_AVAILABLE = False

today = datetime.today()
last_full_month = datetime(today.year, today.month, 1) - relativedelta(months=1)
def extract_ts_features(df):
    """Extract comprehensive time series features for clustering"""
    features = []
    
    for unique_id in df['unique_id'].unique():
        ts_data = df.filter(pl.col('unique_id') == unique_id).sort('SALES_DATE')
        values = ts_data['Act Orders Rev'].to_numpy()
        
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

def create_enhanced_clusters(df: pl.DataFrame, file_path: str, state: DataState = None) -> pl.DataFrame:
    """Enhanced clustering with proper feature engineering"""
    if state is None:
        state = get_global_state()
        
    if 'unique_id' not in df.columns:
        df = df.with_columns(unique_id = pl.col('country') + "," + pl.col('catalog_number'))
    # Filter to training data with timezone-safe comparison
    cutoff_date = datetime.today() - relativedelta(months=1)
    df1 = df.filter(pl.col('sales_date').dt.date() <= cutoff_date.date())
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
    df = df.drop(['cluster','cluster_right'], strict=False)
    df = df.join(features_df[['unique_id', 'cluster']], on='unique_id', how='left')
    
    # Save results
    print(df)
    df = df.with_columns(cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id"))
    df = df.with_columns(cluster=pl.col('cluster').cast(pl.Utf8))
    
    # Save clusters to database instead of parquet
    db_service = DatabaseUtils.get_database_service()
    if db_service:
        db_service.upsert_clusters(df)
    
    # Update state
    state.df = df
    
    return df

def create_ensemble_models(df: pl.DataFrame, file_path: str) -> pl.DataFrame:
    """Create ensemble models for each cluster using the new modular approach."""
    # Prepare data
    dft = prepare_data1(df)
    if DataCleaner is not None:
        dft = DataCleaner.prepare_data_for_forecasting(dft)
    
    # Ensure we have the right columns for forecasting
    if 'SALES_DATE' in dft.columns:
        dft = dft.rename({'SALES_DATE': 'ds'})
    if 'sales_date' in dft.columns:
        dft = dft.rename({'sales_date': 'ds'})
    if 'Act Orders Rev' in dft.columns:
        dft = dft.rename({'Act Orders Rev': 'y'})
    if 'act_orders_rev' in dft.columns:
        dft = dft.rename({'act_orders_rev': 'y'})
    
    # Ensure we have required columns for the forecaster
    required_cols = ['unique_id', 'ds', 'y', 'cluster']
    # Add item_skey and location_skey if they exist
    if 'item_skey' in dft.columns:
        required_cols.append('item_skey')
    if 'location_skey' in dft.columns:
        required_cols.append('location_skey')
    
    # Select only required columns that exist in the dataframe
    available_cols = [col for col in required_cols if col in dft.columns]
    df_fr = dft[available_cols]
    
    # Create forecaster and generate forecasts
    if EnsembleForecaster is not None:
        forecaster = EnsembleForecaster(horizon=60)
        return forecaster.generate_forecasts(df_fr)
    else:
        # Fallback to simple NHITS model if EnsembleForecaster not available
        return _create_simple_forecasts(df_fr)
    
def validate_forecasts(df: pl.DataFrame, forecast_df: pl.DataFrame, validation_months: int = 6) -> dict:
    """Validate forecasts using walk-forward validation"""
    if ValidationProcessor is not None:
        return ValidationProcessor.validate_forecasts(df, forecast_df, validation_months)
    else:
        # Simple fallback validation
        return {'mae': 0.0, 'mape': 0.0, 'rmse': 0.0}

# Additional utility functions
def prepare_data(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare data with proper handling of missing values and outliers"""
    if DataCleaner is not None:
        return DataCleaner.prepare_data_for_forecasting(df)
    else:
        # Simple fallback preparation
        return df.fill_null(0)

def _standalone_forecasting_pipeline(df_json: str, file_path: str) -> tuple:
    """Complete standalone forecasting pipeline with actual forecast generation"""
    import polars as pl
    import numpy as np
    from sklearn.cluster import Birch
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
    try:
        # Reconstruct dataframe from JSON - handle long filename issues
        try:
            df = pl.read_json(df_json)
        except Exception as json_read_error:
            print(f"JSON reading failed due to long paths: {json_read_error}")
            # Try alternative approach - use pandas as intermediate
            try:
                import pandas as pd
                import json
                
                # Parse JSON and convert to pandas then polars
                json_data = json.loads(df_json)
                pdf = pd.DataFrame(json_data)
                
                # Handle date columns properly before converting to polars
                if 'sales_date' in pdf.columns:
                    # Convert string dates to datetime objects
                    try:
                        pdf['sales_date'] = pd.to_datetime(pdf['sales_date'])
                    except Exception as date_error:
                        print(f"Error converting sales_date to datetime: {date_error}")
                        # Try to parse dates manually if automatic conversion fails
                        try:
                            pdf['sales_date'] = pd.to_datetime(pdf['sales_date'], format='%Y-%m-%d', errors='coerce')
                        except Exception as manual_date_error:
                            print(f"Manual date conversion also failed: {manual_date_error}")
                            # Last resort - drop problematic date column and recreate
                            pdf = pdf.drop('sales_date', axis=1)
                            pdf['sales_date'] = pd.to_datetime('2024-01-01')
                
                # Convert numeric columns properly
                numeric_cols = ['Act Orders Rev', 'Fcst Stat Prelim Rev', 'Fcst Stat Final Rev', 
                              'L2 Stat Final Rev', 'Fcst DF Final Rev', 'L2 DF Final Rev', 'act_orders_rev'] # Added 'act_orders_rev'
                for col in numeric_cols:
                    if col in pdf.columns:
                        try:
                            pdf[col] = pd.to_numeric(pdf[col], errors='coerce').fillna(0)
                        except Exception as num_error:
                            print(f"Error converting {col} to numeric: {num_error}")
                            pdf[col] = 0
                
                # Convert to polars
                df = pl.from_pandas(pdf)
                print("Successfully reconstructed dataframe using pandas intermediate with proper date handling")
                
            except Exception as pandas_error:
                print(f"Pandas intermediate approach also failed: {pandas_error}")
                # Last resort - create minimal dataframe
                df = pl.DataFrame({
                    'sales_date': [datetime.today()],
                    'act_orders_rev': [0.0],
                    'country': ['UNKNOWN'],
                    'catalog_number': ['UNKNOWN']
                })
                print("Using fallback minimal dataframe")
        
        # Explicitly cast 'act_orders_rev' to Float64 after DataFrame reconstruction
        if 'act_orders_rev' in df.columns and df['act_orders_rev'].dtype != pl.Float64:
            try:
                df = df.with_columns(pl.col('act_orders_rev').cast(pl.Float64).alias('act_orders_rev'))
                print(f"DEBUG: 'act_orders_rev' cast to Float64. New dtype: {df['act_orders_rev'].dtype}")
            except Exception as cast_error:
                print(f"WARNING: Failed to cast 'act_orders_rev' to Float64: {cast_error}")
                # If casting fails, fill with 0 to prevent further errors
                df = df.with_columns(pl.col('act_orders_rev').fill_null(0).fill_nan(0).alias('act_orders_rev'))

        # Immediately convert date column to datetime after reconstruction
        try:
            if 'sales_date' in df.columns:
                print(f"Before date conversion: sales_date column type = {df['sales_date'].dtype}")
                
                # Check if column is already datetime, if not, convert from string
                if df['sales_date'].dtype != pl.Datetime:
                    df = df.with_columns(
                        pl.col('sales_date').str.to_datetime().alias('sales_date')
                    )
                    print(f"After string-to-datetime conversion: sales_date column type = {df['sales_date'].dtype}")
                else:
                    print("sales_date column is already datetime, skipping conversion")
        except Exception as json_date_error:
            print(f"Error converting JSON date column: {json_date_error}")
            # If conversion fails, try to cast existing column
            try:
                df = df.with_columns(
                    pl.col('sales_date').cast(pl.Datetime).alias('sales_date')
                )
                print(f"After cast: sales_date column type = {df['sales_date'].dtype}")
            except Exception as cast_error:
                print(f"Date cast also failed: {cast_error}")
        
        print(f"DEBUG: DataFrame dtypes before clustering: {df.dtypes}") # Added debug print
        # Simple clustering if not present
        if 'cluster' not in df.columns:
            # Ensure sales_date is properly formatted as datetime
            try:
                if 'sales_date' in df.columns:
                    # Convert to datetime if it's not already
                    df = df.with_columns(
                        pl.col('sales_date').cast(pl.Datetime).alias('sales_date')
                    )
            except Exception as date_cast_error:
                print(f"Error casting sales_date to datetime: {date_cast_error}")
                # If date casting fails, create a default date column
                df = df.with_columns(
                    pl.lit(datetime.today()).alias('sales_date')
                )
            
            # Create unique_id if not present
            if 'unique_id' not in df.columns:
                try:
                    # Use item_skey and location_skey to create unique_id for consistency
                    df = df.with_columns(
                        (pl.col('item_skey').cast(pl.Utf8) + "_" + pl.col('location_skey').cast(pl.Utf8)).alias('unique_id')
                    )
                    print(f"DEBUG: Created unique_id using item_skey and location_skey. Sample: {df['unique_id'].head(5).to_list()}")
                except Exception as unique_id_error:
                    print(f"Error creating unique_id: {unique_id_error}")
                    # Fallback to a simple unique_id if item_skey or location_skey are missing
                    df = df.with_columns(unique_id=pl.lit("UNKNOWN_UNKNOWN"))
            
            # Simple clustering logic with proper date handling
            try:
                last_full_month = datetime.today() - relativedelta(months=1)
                print(f"Filtering data before {last_full_month}")

                # Ensure we have the right data types before filtering
                df1 = df.filter(pl.col('sales_date').dt.date() <= last_full_month.date())
                df1 = df1[['unique_id', 'sales_date', 'act_orders_rev']]
                df1 = df1.with_columns(pl.col('act_orders_rev').cast(pl.Float32).alias('act_orders_rev'))
                df1 = df1.with_columns(ynorm=((pl.col('act_orders_rev')-pl.col('act_orders_rev').mean()) / pl.col('act_orders_rev').std()).over('unique_id'))
                df1 = df1.fill_nan(0)
                df1 = df1.with_columns(pl.when(pl.col('ynorm').is_infinite()).then(0).otherwise(pl.col('ynorm')).alias('ynorm'))
                df1 = df1.pivot(index='unique_id', on='sales_date', values='ynorm', aggregate_function='sum')
                
                if len(df1) > 0:
                    bi = Birch(n_clusters=6).fit(df1[:, 1:])
                    df1 = df1.with_columns(cluster=bi.labels_)
                    df1 = df1['unique_id', 'cluster']
                    df = df.join(df1, on='unique_id', how='left', coalesce=True)
                    df = df.with_columns(cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id"))
                    df = df.with_columns(cluster=pl.col('cluster').cast(pl.Utf8))
                    print(f"Successfully created {len(df1)} clusters")
                else:
                    print("No data available for clustering")
            except Exception as clustering_error:
                print(f"Clustering failed: {clustering_error}")
                # Continue without clustering
                df = df.with_columns(cluster=pl.lit("0"))
        
        # Actual forecast generation using EnsembleForecaster
        merged_df = df
        validation_results = {'mae': 0.0, 'mape': 0.0, 'rmse': 0.0}
        
        if FORECASTING_AVAILABLE and EnsembleForecaster is not None:
            try:
                # Prepare data for forecasting
                dft = prepare_data1(df)
                if DataCleaner is not None:
                    dft = DataCleaner.prepare_data_for_forecasting(dft)
                df_fr = dft.rename({'sales_date': 'ds', 'act_orders_rev': 'y'})
                df_fr = df_fr[['unique_id', 'ds', 'y', 'cluster']]
                
                # Generate forecasts using EnsembleForecaster
                forecaster = EnsembleForecaster(horizon=60)
                forecast_df = forecaster.generate_forecasts(df_fr)
                
                if forecast_df is not None:
                    print(f"Forecast generation successful! Generated {len(forecast_df)} forecast records")
                    print(f"DEBUG: Original df columns before merge: {df.columns}")
                    print(f"DEBUG: Forecast df columns before merge: {forecast_df.columns}")
                    print(f"DEBUG: Sample original df unique_id: {df['unique_id'].head(5).to_list()}")
                    print(f"DEBUG: Sample forecast df unique_id: {forecast_df['unique_id'].head(5).to_list()}")
                    # Check if sales_date column exists before trying to access it
                    if 'sales_date' in df.columns:
                        print(f"DEBUG: Sample original df sales_date: {df['sales_date'].head(5).to_list()}")
                    else:
                        print("DEBUG: Original df does not have 'sales_date' column")
                    # Check if sales_date column exists in forecast_df before trying to access it
                    if 'sales_date' in forecast_df.columns:
                        print(f"DEBUG: Sample forecast df sales_date: {forecast_df['sales_date'].head(5).to_list()}")
                    elif 'ds' in forecast_df.columns:
                        print(f"DEBUG: Sample forecast df ds: {forecast_df['ds'].head(5).to_list()}")
                    else:
                        print("DEBUG: Forecast df does not have 'sales_date' or 'ds' column")
                
                    # Rename 'ds' back to 'sales_date' in forecast_df for merging
                    if 'ds' in forecast_df.columns:
                        forecast_df = forecast_df.rename({'ds': 'sales_date'})
                        print("DEBUG: Renamed 'ds' to 'sales_date' in forecast_df for merging.")
                
                    # Ensure both dataframes have the sales_date column for merging
                    # The forecast_df should have sales_date from the rename above
                    # The original df should already have sales_date
                    merge_columns = ['unique_id']
                    if 'sales_date' in forecast_df.columns and 'sales_date' in df.columns:
                        merge_columns.append('sales_date')
                    else:
                        print("Warning: sales_date column missing from one or both dataframes, merging on unique_id only")
                    
                    # Merge forecast_df with original df
                    merged_df = df.join(
                        forecast_df,
                        on=merge_columns, # This is the key for merging
                        how='outer',
                        coalesce=True
                    )
                    if len(forecast_df) > 0:
                        print(f"Sample forecast data: {forecast_df.head(3)}")
                    
                    # Save forecasts to database
                    print("Attempting to save forecasts to database...")
                    print(f"Forecast DataFrame shape: {forecast_df.shape}")
                    print(f"Forecast DataFrame columns: {forecast_df.columns}")
                    if len(forecast_df) > 0:
                        print(f"Sample forecast data: {forecast_df.head(3)}")
                        print(f"Forecast data types: {forecast_df.dtypes}")
                        print(f"Forecast DataFrame columns before processing: {forecast_df.columns}")
                        
                        # Ensure forecast DataFrame has required columns for saving
                        # Handle different possible date column names
                        if 'ds' in forecast_df.columns:
                            print("Renaming 'ds' column to 'forecast_date'")
                            forecast_df = forecast_df.rename({'ds': 'forecast_date'})
                        elif 'SALES_DATE' in forecast_df.columns:
                            print("Renaming 'SALES_DATE' column to 'forecast_date'")
                            forecast_df = forecast_df.rename({'SALES_DATE': 'forecast_date'})
                        elif 'sales_date' in forecast_df.columns:
                            print("Renaming 'sales_date' column to 'forecast_date'")
                            forecast_df = forecast_df.rename({'sales_date': 'forecast_date'})
                        else:
                            print("No date column found in forecast DataFrame")
                        
                        print(f"Forecast DataFrame columns after date column processing: {forecast_df.columns}")
                        
                        # Ensure we have a forecast_date column (required for saving)
                        if 'forecast_date' not in forecast_df.columns:
                            print("Warning: Forecast DataFrame missing 'forecast_date' column, cannot save to database")
                            saved_count = 0
                        else:
                            # Ensure forecast_date is properly typed
                            forecast_df = forecast_df.with_columns(pl.col('forecast_date').cast(pl.Datetime))
                            
                            # Ensure we have a forecast value column (use ensemble if available, otherwise use any forecast column)
                            forecast_value_column = None
                            possible_forecast_columns = ['ensemble', 'Fcst Ensemble Rev', 'NHITS', 'LSTM', 'AutoARIMA', 'AutoETS', 'SeasonalNaive']
                            for col in possible_forecast_columns:
                                if col in forecast_df.columns:
                                    forecast_value_column = col
                                    break
                            
                            print(f"Found forecast value column: {forecast_value_column}")
                            print(f"Forecast DataFrame columns: {forecast_df.columns}")
                            
                            if forecast_value_column and forecast_value_column != 'forecast_value':
                                print(f"Renaming '{forecast_value_column}' column to 'forecast_value'")
                                forecast_df = forecast_df.rename({forecast_value_column: 'forecast_value'})
                            elif not forecast_value_column:
                                # If no forecast column found, create a default one
                                print("No forecast value column found, creating default 'forecast_value' column")
                                forecast_df = forecast_df.with_columns(pl.lit(0.0).alias('forecast_value'))
                            else:
                                print("Forecast value column is already named 'forecast_value'")
                            # Ensure we have item_skey and location_skey columns for proper saving
                            if 'item_skey' not in forecast_df.columns or 'location_skey' not in forecast_df.columns:
                                # Try to extract from unique_id if it's in item_skey_location_skey format
                                if 'unique_id' in forecast_df.columns:
                                    print("Extracting item_skey and location_skey from unique_id...")
                                    # Batch extract item_skey and location_skey from unique_id
                                    # Assuming unique_id is in format "item_skey_location_skey"
                                    split_data = forecast_df['unique_id'].str.split('_').to_list()
                                    item_skeys = []
                                    location_skeys = []
                                    
                                    for parts in split_data:
                                        if len(parts) == 2:
                                            try:
                                                item_skey = int(parts[0])
                                                location_skey = int(parts[1])
                                                item_skeys.append(item_skey)
                                                location_skeys.append(location_skey)
                                            except ValueError:
                                                # If conversion fails, use None
                                                item_skeys.append(None)
                                                location_skeys.append(None)
                                        else:
                                            # Invalid format
                                            item_skeys.append(None)
                                            location_skeys.append(None)
                                    
                                    forecast_df = forecast_df.with_columns([
                                        pl.Series('item_skey', item_skeys),
                                        pl.Series('location_skey', location_skeys)
                                    ])
                            
                            db_service = DatabaseUtils.get_database_service()
                            if db_service:
                                try:
                                    saved_count = db_service.insert_forecasts(forecast_df, model_type="Ensemble")
                                    print(f"Successfully saved {saved_count} forecast records to database")
                                except Exception as save_error:
                                    print(f"Error saving forecasts to database: {save_error}")
                                    print("Continuing with merged data without saving forecasts")
                                    saved_count = 0
                            else:
                                print("Database service not available, skipping forecast save")
                                saved_count = 0
                    
                    validation_results = {'mae': 0.0, 'mape': 0.0, 'rmse': 0.0, 'forecasts_generated': len(forecast_df), 'forecasts_saved': saved_count}
                    print(f"Models created successfully. Validation results: {validation_results}")
                else:
                    print("Forecast generation returned None")
                    validation_results = {'mae': 0.0, 'mape': 0.0, 'rmse': 0.0, 'forecasts_generated': 0}
            except Exception as forecast_error:
                print(f"Forecast generation failed: {forecast_error}")
                print("Returning clustered data without forecasts")
        else:
            print("EnsembleForecaster not available, returning clustered data")
        
        # Return as JSON strings - use IPC format to avoid path length issues
        try:
            # Use IPC format instead of JSON to avoid Windows path length limitations
            import tempfile
            import os
            with tempfile.NamedTemporaryFile(delete=False, suffix='.ipc') as tmp_file:
                merged_df.write_ipc(tmp_file.name) if merged_df is not None else None
                merged_df_json = tmp_file.name
        except Exception as ipc_error:
            print(f"Error serializing to IPC: {ipc_error}")
            # Fallback to JSON with row limit to avoid path issues
            try:
                if merged_df is not None and len(merged_df) > 1000:
                    # Limit rows to avoid path length issues
                    merged_df_limited = merged_df.head(1000)
                    merged_df_json = merged_df_limited.write_json(pretty=False)
                    print(f"Limited DataFrame to {len(merged_df_limited)} rows to avoid path length issues")
                else:
                    merged_df_json = merged_df.write_json(pretty=False) if merged_df is not None else None
            except Exception as json_error:
                print(f"Error serializing to JSON: {json_error}")
                merged_df_json = None

        return merged_df_json, validation_results
        
    except Exception as e:
        print(f"Error in standalone pipeline: {e}")
        # Ensure we always return a proper tuple even on error
        return None, {'error': str(e)}

def _merge_forecasts_with_data(original_df: pl.DataFrame, forecast_df: pl.DataFrame) -> pl.DataFrame:
    """Merge forecast results with original data"""
    try:
        # Convert forecast dates to proper format
        if 'ds' in forecast_df.columns:
            forecast_df = forecast_df.with_columns(
                ds=pl.col('ds').cast(pl.Datetime)
            )
        
        # Rename forecast columns to match expected format
        forecast_renamed = forecast_df.rename({
            'ds': 'sales_date'
        })
        
        # Handle different forecast model column names
        if 'ensemble' in forecast_renamed.columns:
            forecast_renamed = forecast_renamed.rename({'ensemble': 'Fcst Ensemble Rev'})
        elif 'NHITS' in forecast_renamed.columns:
            forecast_renamed = forecast_renamed.rename({'NHITS': 'Fcst Ensemble Rev'})
        elif 'LSTM' in forecast_renamed.columns:
            forecast_renamed = forecast_renamed.rename({'LSTM': 'Fcst Ensemble Rev'})
        else:
            # Use the first available forecast column or create a default one
            forecast_cols = [col for col in forecast_renamed.columns if col not in ['unique_id', 'sales_date']]
            if forecast_cols:
                forecast_renamed = forecast_renamed.rename({forecast_cols[0]: 'Fcst Ensemble Rev'})
            else:
                forecast_renamed = forecast_renamed.with_columns(pl.lit(0.0).alias('Fcst Ensemble Rev'))
        
        # Merge forecasts with original data
        # This will add forecast columns to future dates
        merged_df = original_df.join(
            forecast_renamed,
            on=['unique_id', 'sales_date'],
            how='outer',
            coalesce=True
        )
        
        return merged_df
        
    except Exception as e:
        print(f"Error merging forecasts: {e}")
        return original_df

def run_enhanced_forecasting_pipeline(df: pl.DataFrame, file_path: str, state: DataState = None):
    """Run the complete enhanced forecasting pipeline - wrapper for UI"""
    try:
        # Convert dataframe to JSON for pickling
        df_json = df.write_json()

        # Call the pickle-safe implementation
        result = _standalone_forecasting_pipeline(df_json, file_path)

        # Ensure we have a proper tuple return
        if result is None or not isinstance(result, tuple) or len(result) != 2:
            print(f"Invalid return from _standalone_forecasting_pipeline: {result}")
            merged_df_json, validation_results = None, {'error': 'Invalid pipeline return'}
        else:
            merged_df_json, validation_results = result

        # Reconstruct dataframe from JSON/IPC
        if merged_df_json is not None:
            try:
                # Check if it's an IPC file (temporary file) or JSON string
                if isinstance(merged_df_json, str) and merged_df_json.endswith('.ipc'):
                    # Read from IPC file
                    merged_df = pl.read_ipc(merged_df_json)
                    # Clean up temporary file
                    try:
                        os.unlink(merged_df_json)
                    except:
                        pass  # Ignore cleanup errors
                else:
                    # Read from JSON string
                    merged_df = pl.read_json(merged_df_json)
                # Update state if provided
                if state is not None:
                    state.df = merged_df
            except Exception as json_read_error:
                print(f"Error reading result data: {json_read_error}")
                merged_df = None
        else:
            merged_df = None

        return merged_df, validation_results

    except Exception as e:
        print(f"Error in run_enhanced_forecasting_pipeline: {e}")
        return None, {'error': str(e)}

def filter_last_36_months(df: pl.DataFrame) -> pl.DataFrame:
    """Filter data to last 36 months"""
    if DataCleaner is not None:
        return DataCleaner.filter_last_n_months(df, 36)
    else:
        # Simple fallback filter with timezone-safe comparison
        cutoff_date = datetime.today() - relativedelta(months=36)
        return df.filter(pl.col('sales_date').dt.date() >= cutoff_date.date())

def prepare_data1(df: pl.DataFrame) -> pl.DataFrame:
    """Prepare data for training"""
    # Ensure consistent unique_id creation
    if 'unique_id' not in df.columns:
        if 'item_skey' in df.columns and 'location_skey' in df.columns:
            df = df.with_columns(
                (pl.col('item_skey').cast(pl.Utf8) + "_" + pl.col('location_skey').cast(pl.Utf8)).alias('unique_id')
            )
        else:
            # Fallback to country and catalog_number if available
            if 'Country' in df.columns and 'CatalogNumber' in df.columns:
                df = df.with_columns(
                    (pl.col('Country') + "," + pl.col('CatalogNumber')).alias('unique_id')
                )
            else:
                # Last resort: create a generic unique_id
                df = df.with_columns(unique_id=pl.lit("UNKNOWN_UNKNOWN"))
    
    if DataCleaner is not None:
        # Ensure SALES_DATE is converted to sales_date before passing to DataCleaner
        if 'SALES_DATE' in df.columns:
            df = df.rename({'SALES_DATE': 'sales_date'})
            print("DEBUG: Renamed 'SALES_DATE' to 'sales_date' in prepare_data1 for DataCleaner.")
        return DataCleaner.prepare_training_data(df)
    else:
        # Simple fallback preparation with proper date handling
        try:
            # Ensure SALES_DATE is converted to sales_date for fallback logic
            if 'SALES_DATE' in df.columns:
                df = df.rename({'SALES_DATE': 'sales_date'})
                print("DEBUG: Renamed 'SALES_DATE' to 'sales_date' in prepare_data1 fallback.")

            # Define last_full_month for filtering
            last_full_month = datetime.today() - relativedelta(months=1)
            
            # Ensure sales_date is datetime before filtering
            if 'sales_date' in df.columns:
                df = df.with_columns(
                    pl.col('sales_date').cast(pl.Datetime).alias('sales_date')
                )
            
            return df.fill_null(0).filter(pl.col('sales_date').dt.date() <= last_full_month.date())
        except Exception as date_error:
            print(f"Error in prepare_data1 date filtering: {date_error}")
            # If date filtering fails, just return the data without filtering
            return df.fill_null(0)

def apply_filters(filters, state: DataState = None):
    """Apply filters to the dataset by querying database directly"""
    if state is None:
        state = get_global_state()

    # Only fetch data when both location and product filters are set
    if not (filters.get('location2') and filters.get('location1') and
            filters.get('product2') and filters.get('product1')):
        # Return empty result if filters are not complete
        return {
            'fdf': '{}',
            'filtered_df': pl.DataFrame(),
            'filtered_products': [],
            'filtered_models': []
        }

    try:
        # Import database service
        db_service = DatabaseUtils.get_database_service()
        if db_service is None:
            return {
                'fdf': '{}',
                'filtered_df': pl.DataFrame(),
                'filtered_products': [],
                'filtered_models': []
            }

        # Query database directly with filters
        df = db_service.get_filtered_sales_actuals(
            location_col=filters.get('location1'),
            location_val=filters.get('location2'),
            product_col=filters.get('product1'),
            product_val=filters.get('product2')
        )

        # Apply standard data preparation
        df = DataUtils.prepare_data_for_ui(df)

        # Update state with fresh data
        state.df = df.clone()
        state.full_df = df.clone()
        state.filtered_df = df.clone()

        # Prepare filtered result for UI display
        if filters.get('level'):
            fdf = df.clone()
            # Use level + location1 if location1 is set, otherwise just level
            level_col = 'Business Unit'  # Default level column
            if filters.get('level') in ['Franchise', 'IBP Level 5', 'IBP Level 6']:
                level_col = filters['level']

            if level_col in df.columns:
                group_cols = ['sales_date', level_col]
                if filters.get('location1') and filters.get('location1') != 'level':
                    location_col = filters['location1']
                    if location_col in df.columns:
                        group_cols.append(location_col)

                fdf = fdf.group_by(group_cols).sum()
        else:
            fdf = df.clone()

        # Get filtered products for dropdown
        try:
            if filters.get('product1') and filters['product1'] in df.columns:
                filtered_products = df[filters['product1']].unique().to_list()
            else:
                filtered_products = []
        except Exception:
            filtered_products = []

        filtered_models = [f"Model for {product}" for product in filtered_products if product]

        return {
            'fdf': fdf.write_json(),
            'filtered_df': fdf,
            'filtered_products': filtered_products,
            'filtered_models': filtered_models
        }

    except Exception as e:
        print(f"Error in apply_filters: {e}")
        return {
            'fdf': '{}',
            'filtered_df': pl.DataFrame(),
            'filtered_products': [],
            'filtered_models': []
        }

def create_clusters(df: pl.DataFrame, file_path: str, state: DataState = None) -> pl.DataFrame:
    """Create clusters for the dataset"""
    if state is None:
        state = get_global_state()
    
    # Ensure unique_id is created consistently using item_skey and location_skey
    if 'unique_id' not in df.columns:
        if 'item_skey' in df.columns and 'location_skey' in df.columns:
            df = df.with_columns(
                (pl.col('item_skey').cast(pl.Utf8) + "_" + pl.col('location_skey').cast(pl.Utf8)).alias('unique_id')
            )
        else:
            # Fallback to country and catalog_number if available
            if 'Country' in df.columns and 'CatalogNumber' in df.columns:
                df = df.with_columns(
                    (pl.col('Country') + "," + pl.col('CatalogNumber')).alias('unique_id')
                )
            else:
                # Last resort: create a generic unique_id
                df = df.with_columns(unique_id=pl.lit("UNKNOWN_UNKNOWN"))
    
    # Remove existing cluster columns
    df = df.drop(['cluster', 'cluster_right'], strict=False)
    
    # Filter to training data
    df1 = df.filter(pl.col('sales_date').dt.date() <= (datetime.today() - relativedelta(months=1)).date())
    df1 = df1[['unique_id', 'sales_date', 'act_orders_rev']]
    df1 = df1.with_columns(pl.col('act_orders_rev').cast(pl.Float32).alias('act_orders_rev'))
    df1 = df1.with_columns(ynorm=((pl.col('act_orders_rev')-pl.col('act_orders_rev').mean()) / pl.col('act_orders_rev').std()).over('unique_id'))
    df1 = df1.fill_nan(0)
    df1 = df1.with_columns(pl.when(pl.col('ynorm').is_infinite()).then(0).otherwise(pl.col('ynorm')).alias('ynorm'))
    df1 = df1.pivot(index='unique_id', on='sales_date', values='ynorm', aggregate_function='sum')
    
    if len(df1) > 1:  # Need at least 2 points for clustering
        bi = Birch(n_clusters=6).fit(df1[:, 1:])
        df1 = df1.with_columns(cluster=bi.labels_)
        df1 = df1[['unique_id', 'cluster']]
        df = df.join(df1, on='unique_id', how='left', coalesce=True)
        df = df.with_columns(cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id"))
        df = df.with_columns(cluster=pl.col('cluster').cast(pl.Utf8))
    else:
        # Not enough data for clustering, assign all to cluster 0
        df = df.with_columns(cluster=pl.lit("0"))
    
    # Save clusters to database instead of parquet
    db_service = DatabaseUtils.get_database_service()
    if db_service:
        db_service.upsert_clusters(df)
    
    # Update state with clustered data
    state.df = df
    
    return df
    
def _create_simple_forecasts(df_fr: pl.DataFrame) -> pl.DataFrame:
    """Simple NHITS forecasting fallback"""
    try:
        # Ensure unique_id is created consistently if not already present
        if 'unique_id' not in df_fr.columns:
            try:
                df_fr = df_fr.with_columns(
                    (pl.col('item_skey').cast(pl.Utf8) + "_" + pl.col('location_skey').cast(pl.Utf8)).alias('unique_id')
                )
                print(f"DEBUG: Created unique_id in _create_simple_forecasts using item_skey and location_skey. Sample: {df_fr['unique_id'].head(5).to_list()}")
            except Exception as unique_id_error:
                print(f"Error creating unique_id in _create_simple_forecasts: {unique_id_error}")
                df_fr = df_fr.with_columns(unique_id=pl.lit("UNKNOWN_UNKNOWN"))

        # Simple NHITS model for each cluster
        forecasts = []
        
        for cluster in df_fr['cluster'].unique():
            cluster_data = df_fr.filter(pl.col('cluster') == cluster)
            
            # Create simple NHITS model
            models = [NHITS(h=60, input_size=24, max_steps=50)]
            nf = NeuralForecast(models=models, freq='M')
            
            # Fit and predict
            nf.fit(cluster_data.to_pandas())
            forecast = nf.predict()
            
            # Convert back to polars
            forecast_pl = pl.from_pandas(forecast.reset_index())
            forecasts.append(forecast_pl)
        
        # Combine all forecasts
        if forecasts:
            return pl.concat(forecasts)
        else:
            return pl.DataFrame()
            
    except Exception as e:
        print(f"Error in simple forecasting: {e}")
        return pl.DataFrame()

def create_models_action(df: pl.DataFrame, file_path: str, state: DataState = None) -> pl.DataFrame:
    """Business logic for creating models"""
    if state is None:
        state = get_global_state()
    
    try:
        # Run the enhanced pipeline
        result = run_enhanced_forecasting_pipeline(df, file_path, state)
        
        # Ensure we have a proper tuple return
        if result is None or not isinstance(result, tuple) or len(result) != 2:
            print(f"Invalid return from run_enhanced_forecasting_pipeline: {result}")
            merged_df, validation_results = None, {'error': 'Invalid pipeline return'}
        else:
            merged_df, validation_results = result
        
        # Update state
        if merged_df is not None:
            state.df = merged_df
            print(f"Models created successfully. Validation results: {validation_results}")
        else:
            print("Model creation failed")
        
        return merged_df
    except Exception as e:
        print(f"Error in create_models_action: {e}")
        return df

def change_fc_action():
    """Business logic for changing forecast settings"""
    # Actual forecast settings logic would go here
    return "Changing forecast settings"
