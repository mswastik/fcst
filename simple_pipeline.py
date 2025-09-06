"""
Simple, standalone forecasting pipeline module.
This module has minimal dependencies to ensure pickle compatibility.
"""

def standalone_forecasting_pipeline(df_dict: dict) -> tuple:
    """Completely standalone forecasting pipeline - guaranteed pickle-safe"""
    import polars as pl
    import numpy as np
    from sklearn.cluster import Birch
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    
    try:
        # Reconstruct dataframe from dictionary
        df = pl.DataFrame(df_dict)
        
        # Simple clustering if not present
        if 'cluster' not in df.columns:
            # Create unique_id if not present
            if 'unique_id' not in df.columns:
                df = df.with_columns(unique_id = pl.col('Country') + "," + pl.col('CatalogNumber'))
            
            # Simple clustering logic
            last_full_month = datetime.today() - relativedelta(months=1)
            df1 = df.filter(pl.col('SALES_DATE') <= last_full_month)
            df1 = df1[['unique_id', 'SALES_DATE', 'Act Orders Rev']]
            df1 = df1.with_columns(pl.col('Act Orders Rev').cast(pl.Float32).alias('Act Orders Rev'))
            df1 = df1.with_columns(ynorm=((pl.col('Act Orders Rev')-pl.col('Act Orders Rev').mean()) / pl.col('Act Orders Rev').std()).over('unique_id'))
            df1 = df1.fill_nan(0)
            df1 = df1.with_columns(pl.when(pl.col('ynorm').is_infinite()).then(0).otherwise(pl.col('ynorm')).alias('ynorm'))
            df1 = df1.pivot(index='unique_id', on='SALES_DATE', values='ynorm', aggregate_function='sum')
            
            if len(df1) > 0:
                bi = Birch(n_clusters=6).fit(df1[:, 1:])
                df1 = df1.with_columns(cluster=bi.labels_)
                df1 = df1['unique_id', 'cluster']
                df = df.join(df1, on='unique_id', how='left', coalesce=True)
                df = df.with_columns(cluster=pl.col("cluster").forward_fill().backward_fill().over("unique_id"))
                df = df.with_columns(cluster=pl.col('cluster').cast(pl.Utf8))
        
        # Simple forecast generation (placeholder)
        # For now, just return the clustered data
        merged_df = df
        validation_results = {'mae': 0.0, 'mape': 0.0, 'rmse': 0.0}
        
        # Return as dictionary to avoid filename issues
        merged_df_dict = merged_df.to_dict(as_series=False) if merged_df is not None else None
        return merged_df_dict, validation_results
        
    except Exception as e:
        print(f"Error in standalone pipeline: {e}")
        return None, {'error': str(e)}
