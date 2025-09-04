"""
State management for the FCST application.
Replaces global variables with proper state management.
"""
from typing import Optional, List, Dict, Any
import polars as pl
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DataState:
    """Centralized state management for application data."""
    
    # Main dataframes
    df: Optional[pl.DataFrame] = None
    filtered_df: Optional[pl.DataFrame] = None
    
    # Filter state
    filtered_products: List[str] = field(default_factory=list)
    filtered_models: List[str] = field(default_factory=list)
    
    # UI state
    by_month: bool = False
    
    # Constants
    products: List[str] = field(default_factory=lambda: [
        "Franchise", "IBP Level 5", "IBP Level 6", "CatalogNumber"
    ])
    locations: List[str] = field(default_factory=lambda: [
        'Area', 'Region', 'Country'
    ])
    levels: List[str] = field(default_factory=lambda: [
        "Franchise", "IBP Level 5", "IBP Level 6", "CatalogNumber"
    ])
    
    # Hierarchy data
    products_filt: Optional[pl.DataFrame] = None
    locations_filt: Optional[pl.DataFrame] = None
    
    def __post_init__(self):
        """Initialize hierarchy data after object creation."""
        try:
            self.products_filt = pl.read_parquet('data/phierarchy.parquet')
            self.locations_filt = pl.read_parquet('data/lhierarchy.parquet')
        except Exception:
            self.products_filt = pl.DataFrame()
            self.locations_filt = pl.DataFrame()
    
    def initialize_data(self) -> None:
        """Initialize the application data."""
        # Reset dataframes
        self.df = None
        self.filtered_df = None
    
    def load_sample_data(self, path: str) -> pl.DataFrame:
        """Load sample data from parquet file."""
        try:
            self.df = pl.read_parquet(path)
            self.df = self.df.with_columns(
                pl.col('`Act Orders Rev', '`Fcst Stat Prelim Rev', '`Fcst Stat Final Rev', 
                       'L2 Stat Final Rev', '`Fcst DF Final Rev', 'L2 DF Final Rev').cast(pl.Float32)
            )
            self.filtered_df = self.df.clone()
            return self.df
        except Exception as e:
            raise ValueError(f"Failed to load data from {path}: {e}")
    
    def get_filter_options(self, prod: str = None, loc: str = None) -> Dict[str, Any]:
        """Return filter options for UI dropdowns."""
        prod = prod or self.products[0]
        loc = loc or self.locations[0]
        
        try:
            if self.df is not None:
                return {
                    'products_filt': self.df[prod].unique().to_list(),
                    'locations_filt': self.df[loc].unique().to_list(),
                    'products': self.products,
                    'locations': self.locations,
                    'levels': self.levels
                }
        except Exception:
            pass
        
        return {
            'products_filt': [],
            'locations_filt': [],
            'products': self.products,
            'locations': self.locations,
            'levels': self.levels
        }
    
    def update_filtered_data(self, new_filtered_df: pl.DataFrame) -> None:
        """Update the filtered dataframe and related state."""
        self.filtered_df = new_filtered_df
        
        # Update filtered products and models based on new data
        if new_filtered_df is not None and len(new_filtered_df) > 0:
            try:
                # Extract unique products from filtered data
                if 'CatalogNumber' in new_filtered_df.columns:
                    self.filtered_products = new_filtered_df['CatalogNumber'].unique().to_list()
                else:
                    self.filtered_products = []
                
                # Generate model names (placeholder for real model data)
                self.filtered_models = [f"Model for {product}" for product in self.filtered_products]
            except Exception:
                self.filtered_products = []
                self.filtered_models = []
    
    def get_chart_data(self, chart_type: str) -> Optional[Dict[str, Any]]:
        """Get data formatted for charts."""
        if self.filtered_df is None or len(self.filtered_df) == 0:
            return None
        
        filtered_df = pl.DataFrame(self.filtered_df)
        chart_data = filtered_df.clone()
        
        # Group by month if toggle is active
        if self.by_month:
            chart_data = chart_data.with_columns(
                group=pl.col('SALES_DATE').dt.strftime('%b')
            )
        else:
            chart_data = chart_data.with_columns(
                group=pl.col('SALES_DATE')
            )
        
        # Prepare data based on chart type
        if chart_type == 'column':
            return self._get_column_chart_data(chart_data)
        elif chart_type == 'line':
            return self._get_line_chart_data(chart_data)
        
        return None
    
    def _get_column_chart_data(self, chart_data: pl.DataFrame) -> Dict[str, Any]:
        """Generate column chart data."""
        chart_data = chart_data.with_columns(Month=pl.col('SALES_DATE').dt.strftime('%b'))
        chart_data = chart_data.with_columns(Year=pl.col('SALES_DATE').dt.year())
        
        # Group by Year and Month for actuals
        agg_actuals = chart_data.group_by(['Year', 'Month']).sum()['Year', 'Month', '`Act Orders Rev']
        
        # Group by Year and Month for forecasts (only if NHITS exists)
        agg_forecasts = None
        if 'NHITS' in chart_data.columns:
            agg_forecasts = chart_data.group_by(['Year', 'Month']).sum()['Year', 'Month', 'NHITS']
        
        # Merge actuals and forecasts
        if agg_forecasts is not None:
            agg_data = agg_actuals.join(agg_forecasts, on=['Year', 'Month'], how='outer')
        else:
            agg_data = agg_actuals.with_columns(pl.lit(None).alias('NHITS'))
        
        # Sort by Year and then by Month
        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        agg_data = agg_data.with_columns(
            pl.col('Month').map_elements(
                lambda x: month_order.index(x), 
                return_dtype=pl.Int32
            ).alias('MonthOrder')
        )
        agg_data = agg_data.sort(['Year', 'MonthOrder'])
        
        unique_years = sorted(agg_data['Year'].unique().to_list())
        series_data = []
        colors = ['#5470C6', '#91CC75', '#EE6666', '#73C0DE', 
                 '#3BA272', '#FC8452', '#9A60B4', '#EA7CCC']
        
        for i, year in enumerate(unique_years):
            year_data = agg_data.filter(pl.col('Year') == year)
            
            actual_values = []
            forecast_values = []
            for month in month_order:
                month_row = year_data.filter(pl.col('Month') == month)
                if len(month_row) > 0:
                    actual_values.append(month_row['`Act Orders Rev'][0])
                    forecast_values.append(
                        month_row['NHITS'][0] if 'NHITS' in month_row.columns else None
                    )
                else:
                    actual_values.append(None)
                    forecast_values.append(None)
            
            current_color = colors[i % len(colors)]
            
            series_data.append({
                'name': f'{year} - Actual',
                'type': 'bar',
                'data': actual_values,
                'color': current_color
            })
            
            if any(fv is not None for fv in forecast_values):
                series_data.append({
                    'name': f'{year} - Forecast',
                    'type': 'line',
                    'data': forecast_values,
                    'color': current_color,
                    'lineStyle': {'type': 'dashed'}
                })
        
        return {
            'months': month_order,
            'series': series_data
        }
    
    def _get_line_chart_data(self, chart_data: pl.DataFrame) -> Dict[str, Any]:
        """Generate line chart data."""
        chart_data = chart_data.sort('group')
        
        if self.by_month:
            agg_data = chart_data.group_by('group').sum()['`Act Orders Rev']
            forecast_values = []
            if 'NHITS' in chart_data.columns:
                agg_data = agg_data.with_columns(
                    chart_data.group_by('group').sum()['NHITS']
                )
                forecast_values = agg_data['NHITS'].to_list()
            x_values = agg_data['group'].to_list()
        else:
            # Aggregate by date for line chart
            agg_data = chart_data.group_by('group').sum()['group', '`Act Orders Rev']
            forecast_values = []
            if 'NHITS' in chart_data.columns:
                agg_data = agg_data.with_columns(
                    chart_data.group_by('group').sum()['NHITS']
                )
                forecast_values = agg_data['NHITS'].to_list()
            x_values = agg_data['group'].to_list()
        
        return {
            'categories': x_values,
            'values': agg_data['`Act Orders Rev'].to_list(),
            'forecast_values': forecast_values
        }


# Global instance for backward compatibility during transition
_global_state = DataState()

def get_global_state() -> DataState:
    """Get the global state instance."""
    return _global_state

def initialize_global_state() -> None:
    """Initialize the global state."""
    _global_state.initialize_data()
