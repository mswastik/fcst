-- DuckDB Schema for Forecasting Application
-- Optimized for time series forecasting with product and location hierarchies

-- Product Hierarchy Table (based on phierarchy function in sql.py)
CREATE TABLE da.product_hierarchy (
    demantra_item_skey BIGINT PRIMARY KEY,
    business_sector VARCHAR,
    business_unit VARCHAR,
    franchise VARCHAR,
    product_line VARCHAR,
    ibp_level_5 VARCHAR,
    ibp_level_6 VARCHAR,
    ibp_level_7 VARCHAR,
    catalog_number VARCHAR NOT NULL,
    uom VARCHAR,
    pack_content VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Location Hierarchy Table (based on lhierarchy function in sql.py)
CREATE TABLE da.location_hierarchy (
    location_skey BIGINT PRIMARY KEY,
    selling_division VARCHAR,
    area VARCHAR,
    stryker_group_region VARCHAR,
    region VARCHAR,
    country VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main Fact Table for Historical Data (based on sales_actuals function in sql.py)
CREATE TABLE da.sales_actuals (
    id BIGINT PRIMARY KEY,
    item_skey BIGINT NOT NULL,
    location_skey BIGINT NOT NULL,
    sales_date DATE NOT NULL,
    asp_final_rev DECIMAL(38,8),
    act_orders_rev DECIMAL(38,8),
    act_orders_rev_val DECIMAL(38,8),
    fcst_df_final_rev DECIMAL(38,8),
    l0_df_final_rev DECIMAL(38,8),
    l1_df_final_rev DECIMAL(38,8),
    l2_df_final_rev DECIMAL(38,8),
    fcst_df_final_rev_val DECIMAL(38,8),
    fcst_stat_prelim_rev DECIMAL(38,8),
    fcst_stat_final_rev DECIMAL(38,8),
    l0_stat_final_rev DECIMAL(38,8),
    l1_stat_final_rev DECIMAL(38,8),
    l2_stat_final_rev DECIMAL(38,8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (item_skey) REFERENCES da.product_hierarchy(demantra_item_skey),
    FOREIGN KEY (location_skey) REFERENCES da.location_hierarchy(location_skey)
);

-- Clustering Results Table
CREATE TABLE da.product_clusters (
    cluster_id VARCHAR PRIMARY KEY,
    item_skey BIGINT NOT NULL,
    location_skey BIGINT NOT NULL,
    cluster_number INTEGER NOT NULL,
    cluster_features JSON, -- Store feature vector as JSON
    silhouette_score DECIMAL(15,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (item_skey) REFERENCES da.product_hierarchy(demantra_item_skey),
    FOREIGN KEY (location_skey) REFERENCES da.location_hierarchy(location_skey),
    UNIQUE(item_skey, location_skey) -- One cluster per product-location combination
);

-- Forecast Results Table
CREATE TABLE da.forecasts (
    forecast_id BIGINT PRIMARY KEY,
    item_skey BIGINT NOT NULL,
    location_skey BIGINT NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_horizon INTEGER NOT NULL, -- months ahead
    model_type VARCHAR NOT NULL, -- 'NHITS', 'Ensemble', etc.
    forecast_value DECIMAL(15,2) NOT NULL,
    confidence_lower DECIMAL(15,2),
    confidence_upper DECIMAL(15,2),
    model_version VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (item_skey) REFERENCES da.product_hierarchy(demantra_item_skey),
    FOREIGN KEY (location_skey) REFERENCES da.location_hierarchy(location_skey)
);

-- Model Performance Metrics Table
CREATE TABLE da.model_validation (
    validation_id BIGINT PRIMARY KEY,
    model_type VARCHAR NOT NULL,
    validation_date DATE NOT NULL,
    validation_period_months INTEGER NOT NULL,
    mae DECIMAL(10,4),
    mape DECIMAL(10,4),
    rmse DECIMAL(10,4),
    accuracy_percentage DECIMAL(5,2),
    forecast_bias DECIMAL(10,4),
    silhouette_score DECIMAL(15,6),
    validation_details JSON, -- Store detailed validation results
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Performance
CREATE INDEX idx_sales_actuals_date ON da.sales_actuals(sales_date);
CREATE INDEX idx_sales_actuals_item_location ON da.sales_actuals(item_skey, location_skey);
CREATE INDEX idx_sales_actuals_date_item ON da.sales_actuals(sales_date, item_skey);
CREATE INDEX idx_forecasts_date ON da.forecasts(forecast_date);
CREATE INDEX idx_forecasts_item_location ON da.forecasts(item_skey, location_skey);
CREATE INDEX idx_forecasts_model_type ON da.forecasts(model_type);

-- Additional performance indexes for filtering
CREATE INDEX idx_product_hierarchy_franchise ON da.product_hierarchy(franchise);
CREATE INDEX idx_product_hierarchy_ibp_level_5 ON da.product_hierarchy(ibp_level_5);
CREATE INDEX idx_product_hierarchy_ibp_level_6 ON da.product_hierarchy(ibp_level_6);
CREATE INDEX idx_product_hierarchy_business_unit ON da.product_hierarchy(business_unit);
CREATE INDEX idx_location_hierarchy_region ON da.location_hierarchy(region);
CREATE INDEX idx_location_hierarchy_country ON da.location_hierarchy(country);
CREATE INDEX idx_location_hierarchy_area ON da.location_hierarchy(area);

-- Composite indexes for common filter combinations
CREATE INDEX idx_sales_product_location_date ON da.sales_actuals(item_skey, location_skey, sales_date);
CREATE INDEX idx_product_location_franchise_region ON da.product_hierarchy(franchise), da.location_hierarchy(region);

-- Views for Common Queries
CREATE VIEW v_product_location_summary AS
SELECT 
    p.catalog_number,
    p.franchise,
    p.ibp_level_5,
    p.ibp_level_6,
    p.business_sector,
    p.business_unit,
    p.product_line,
    l.country,
    l.region,
    l.area,
    l.stryker_group_region,
    l.selling_division,
    s.item_skey,
    s.location_skey,
    COUNT(*) as data_points,
    MIN(s.sales_date) as first_date,
    MAX(s.sales_date) as last_date,
    AVG(s.act_orders_rev) as avg_revenue,
    SUM(s.act_orders_rev) as total_revenue
FROM da.sales_actuals s
JOIN da.product_hierarchy p ON s.item_skey = p.demantra_item_skey
JOIN da.location_hierarchy l ON s.location_skey = l.location_skey
GROUP BY p.catalog_number, p.franchise, p.ibp_level_5, p.ibp_level_6,
         p.business_sector, p.business_unit, p.product_line,
         l.country, l.region, l.area, l.stryker_group_region, l.selling_division,
         s.item_skey, s.location_skey;

-- View for Forecast vs Actual Comparison
CREATE VIEW v_forecast_accuracy AS
SELECT 
    f.item_skey,
    f.location_skey,
    f.forecast_date,
    f.model_type,
    f.forecast_value,
    s.act_orders_rev as actual_value,
    ABS(f.forecast_value - s.act_orders_rev) as absolute_error,
    CASE 
        WHEN s.act_orders_rev != 0 
        THEN ABS(f.forecast_value - s.act_orders_rev) / ABS(s.act_orders_rev) * 100
        ELSE NULL 
    END as percentage_error
FROM da.forecasts f
JOIN da.sales_actuals s ON f.item_skey = s.item_skey 
    AND f.location_skey = s.location_skey 
    AND f.forecast_date = s.sales_date
WHERE f.forecast_horizon = 1; -- 1-month ahead forecasts

-- View for Time Series with Clusters
CREATE VIEW v_time_series_clustered AS
SELECT 
    s.item_skey,
    s.location_skey,
    s.sales_date,
    s.act_orders_rev,
    s.act_orders_rev_val,
    s.fcst_df_final_rev,
    s.fcst_df_final_rev_val,
    p.catalog_number,
    p.franchise,
    p.business_sector,
    p.business_unit,
    p.product_line,
    p.ibp_level_5,
    p.ibp_level_6,
    p.ibp_level_7,
    l.country,
    l.region,
    l.area,
    l.stryker_group_region,
    l.selling_division,
    c.cluster_number,
    CONCAT(l.country, ',', p.catalog_number) as unique_id
FROM da.sales_actuals s
JOIN da.product_hierarchy p ON s.item_skey = p.demantra_item_skey
JOIN da.location_hierarchy l ON s.location_skey = l.location_skey
LEFT JOIN da.product_clusters c ON s.item_skey = c.item_skey 
    AND s.location_skey = c.location_skey
ORDER BY s.item_skey, s.location_skey, s.sales_date;
