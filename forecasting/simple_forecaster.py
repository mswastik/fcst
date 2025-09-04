"""
Simple forecasting models for single model training.
Extracted from create_models_action for better separation of concerns.
"""
import polars as pl
from typing import List, Tuple
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS
from neuralforecast.losses.pytorch import RMSE
from forecasting.data_processor import DataCleaner, ForecastDataProcessor


class SimpleModelConfiguration:
    """Configuration for simple NHITS model."""
    
    def __init__(self, horizon: int = 56, input_size: int = 56, stacks: int = 3):
        self.horizon = horizon
        self.input_size = input_size
        self.stacks = stacks
        self.max_steps = 700
        self.learning_rate = 1e-3
        self.random_seed = 1
        self.batch_size = 36
        self.windows_batch_size = 35
        self.val_check_steps = 100


class SimpleNHITSForecaster:
    """Simple NHITS forecaster for single model training."""
    
    def __init__(self, config: SimpleModelConfiguration = None):
        self.config = config or SimpleModelConfiguration()
    
    def create_nhits_model(self) -> NHITS:
        """Create and configure a single NHITS model."""
        return NHITS(
            h=self.config.horizon,
            input_size=self.config.input_size,
            max_steps=self.config.max_steps,
            stack_types=['identity'] * self.config.stacks,
            n_blocks=[3] * self.config.stacks,
            mlp_units=[[256, 256, 128]] * self.config.stacks,
            n_pool_kernel_size=[2, 4, 6],
            n_freq_downsample=[2, 4, 6],
            interpolation_mode='nearest',
            activation='ReLU',
            dropout_prob_theta=0.3,
            scaler_type='robust',
            loss=RMSE(),
            valid_loss=RMSE(),
            batch_size=self.config.batch_size,
            windows_batch_size=self.config.windows_batch_size,
            random_seed=self.config.random_seed,
            start_padding_enabled=True,
            learning_rate=self.config.learning_rate,
            val_check_steps=self.config.val_check_steps
        )
    
    def train_and_forecast(self, df_fr: pl.DataFrame) -> pl.DataFrame:
        """Train NHITS model and generate forecasts."""
        models = [self.create_nhits_model()]
        
        nf = NeuralForecast(models=models, freq='1mo')
        nf.fit(df=df_fr.fill_nan(0).fill_null(0))
        
        return nf.predict()


class SimpleModelPipeline:
    """Complete pipeline for simple model training and forecasting."""
    
    def __init__(self, config: SimpleModelConfiguration = None):
        self.forecaster = SimpleNHITSForecaster(config)
        self.data_processor = ForecastDataProcessor()
    
    def run_pipeline(self, df: pl.DataFrame, file_path: str) -> pl.DataFrame:
        """Run the complete simple forecasting pipeline."""
        # Step 1: Prepare data
        prepared_data = self._prepare_data(df)
        
        # Step 2: Train and forecast
        forecasts = self.forecaster.train_and_forecast(prepared_data)
        
        # Step 3: Process and integrate results
        return self._process_and_integrate_results(forecasts, df, file_path)
    
    def _prepare_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Prepare data for training."""
        dft = DataCleaner.prepare_training_data(df)
        df_fr = dft.rename({'SALES_DATE': 'ds', '`Act Orders Rev': 'y'})
        return df_fr[['unique_id', 'ds', 'y']]
    
    def _process_and_integrate_results(self, forecasts: pl.DataFrame, 
                                     original_df: pl.DataFrame, file_path: str) -> pl.DataFrame:
        """Process forecasts and integrate with original data."""
        # Split unique_id and add hierarchy data
        forecasts = self._split_unique_id_and_add_hierarchy(forecasts)
        
        # Filter by region if needed
        forecasts = self._filter_by_region(forecasts, file_path)
        
        # Merge with original data
        return self._merge_with_original_data(forecasts, original_df, file_path)
    
    def _split_unique_id_and_add_hierarchy(self, forecasts: pl.DataFrame) -> pl.DataFrame:
        """Split unique_id and join with hierarchy data."""
        # Split unique_id into Country and CatalogNumber
        unique_id_columns = ["Country", "CatalogNumber"]
        forecasts = forecasts.with_columns(
            pl.col('unique_id').str.split_exact(",", 1)
            .struct.rename_fields(unique_id_columns)
            .alias("fields")
        ).unnest("fields")
        
        # Rename ds to SALES_DATE
        forecasts = forecasts.rename({'ds': 'SALES_DATE'})
        
        # Join with hierarchy data
        ph = pl.read_parquet('data/phierarchy.parquet')
        try:
            lh = pl.read_parquet('data/lhierarchy.parquet').drop('Selling Division').unique()
        except Exception:
            lh = pl.read_parquet('data/lhierarchy.parquet')
        
        forecasts = forecasts.join(ph, on='CatalogNumber', how='left')
        forecasts = forecasts.join(lh, on='Country', how='left')
        
        return forecasts
    
    def _filter_by_region(self, forecasts: pl.DataFrame, file_path: str) -> pl.DataFrame:
        """Filter forecasts by the same region as original data."""
        try:
            original_df_polars = pl.read_parquet(f"data/{file_path}").unique()
            region = original_df_polars['Stryker Group Region'].unique()[0]
            return forecasts.filter(pl.col('Stryker Group Region') == region)
        except Exception:
            return forecasts
    
    def _merge_with_original_data(self, forecasts: pl.DataFrame, 
                                original_df: pl.DataFrame, file_path: str) -> pl.DataFrame:
        """Merge forecasts with original dataframe."""
        # Read original data from file
        original_df_polars = pl.read_parquet(f"data/{file_path}").unique()
        
        # Add NHITS column if not present
        if 'NHITS' not in original_df_polars.columns:
            original_df_polars = original_df_polars.with_columns(NHITS=0)
        
        # Define join columns
        join_columns = [
            'SALES_DATE', 'CatalogNumber', 'Country', 'Area', 'Stryker Group Region',
            'Region', 'Business Sector', 'Business Unit', 'Franchise', 'Product Line',
            'IBP Level 5', 'IBP Level 6', 'IBP Level 7', 'unique_id'
        ]
        
        # Merge forecasts with original data
        merged_df_polars = original_df_polars.filter(
            pl.col('unique_id').is_in(forecasts['unique_id'].unique())
        ).drop('NHITS').join(
            forecasts, on=join_columns, how='outer', coalesce=True
        )
        
        # Combine with non-forecasted data
        non_forecasted = original_df_polars.filter(
            ~pl.col('unique_id').is_in(forecasts['unique_id'].unique())
        )
        
        merged_df_polars = pl.concat([merged_df_polars, non_forecasted], how='diagonal_relaxed')
        
        # Forward fill birch column if present
        if 'birch' in merged_df_polars.columns:
            merged_df_polars = merged_df_polars.with_columns(
                pl.col('birch').forward_fill().over('unique_id')
            )
        
        # Save results
        merged_df_polars.write_parquet(f"data/{file_path}")
        
        return merged_df_polars
