-- DuckDB Schema for Forecasting Application
-- Optimized for time series forecasting with product and location hierarchies

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS da;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS da.product_clusters;
DROP TABLE IF EXISTS da.forecasts;
DROP TABLE IF EXISTS da.model_validation;

-- Product Hierarchy Table (based on phierarchy function in sql.py)
CREATE TABLE IF NOT EXISTS da.product_hierarchy (
    demantra_item_skey BIGINT,
    business_sector VARCHAR(100),
    business_unit VARCHAR(100),
    franchise VARCHAR(100),
    product_line VARCHAR(100),
    ibp_level_5 VARCHAR(100),
    ibp_level_6 VARCHAR(100),
    ibp_level_7 VARCHAR(100),
    catalog_number VARCHAR(50) NOT NULL,
    uom VARCHAR(20),
    pack_content VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Location Hierarchy Table (based on lhierarchy function in sql.py)
CREATE TABLE IF NOT EXISTS da.location_hierarchy (
    location_skey BIGINT,
    selling_division VARCHAR(100),
    area VARCHAR(100),
    stryker_group_region VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main Fact Table for Historical Data (based on sales_actuals function in sql.py)
CREATE TABLE IF NOT EXISTS da.sales_actuals (
    id BIGINT,
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
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Clustering Results Table
CREATE TABLE IF NOT EXISTS da.product_clusters (
    cluster_id VARCHAR(255),
    item_skey BIGINT NOT NULL,
    location_skey BIGINT NOT NULL,
    cluster_number INTEGER NOT NULL,
    cluster_features VARCHAR, -- Use VARCHAR for JSON in DuckDB
    silhouette_score DECIMAL(15,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Forecast Results Table
CREATE TABLE IF NOT EXISTS da.forecasts (
    forecast_id BIGINT,
    item_skey BIGINT NOT NULL,
    location_skey BIGINT NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_horizon INTEGER NOT NULL, -- months ahead
    model_type VARCHAR(50) NOT NULL, -- 'NHITS', 'Ensemble', etc.
    forecast_value DECIMAL(15,2) NOT NULL,
    confidence_lower DECIMAL(15,2),
    confidence_upper DECIMAL(15,2),
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Model Performance Metrics Table
CREATE TABLE IF NOT EXISTS da.model_validation (
    validation_id BIGINT,
    model_type VARCHAR(50) NOT NULL,
    validation_date DATE NOT NULL,
    validation_period_months INTEGER NOT NULL,
    mae DECIMAL(10,4),
    mape DECIMAL(10,4),
    rmse DECIMAL(10,4),
    accuracy_percentage DECIMAL(5,2),
    forecast_bias DECIMAL(10,4),
    silhouette_score DECIMAL(15,6),
    validation_details VARCHAR, -- Use VARCHAR for JSON in DuckDB
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Final Forecasts Table (for approved forecasts only)
CREATE TABLE IF NOT EXISTS da.final_forecasts (
    final_forecast_id BIGINT,
    -- Original forecast data
    forecast_id BIGINT, -- Reference to original forecast (can be NULL)
    item_skey BIGINT NOT NULL,
    location_skey BIGINT NOT NULL,
    forecast_date DATE NOT NULL,
    forecast_horizon INTEGER NOT NULL,
    forecast_value DECIMAL(15,2) NOT NULL,
    -- Final forecast metadata
    forecast_cycle_month DATE NOT NULL, -- Month when forecast was generated
    model_type VARCHAR(50) NOT NULL, -- Final model used ('Ensemble', 'NHITS', etc.)
    model_version VARCHAR(50),
    -- Approval and lifecycle
    approved_by VARCHAR(100) NOT NULL,
    is_current BOOLEAN DEFAULT TRUE, -- Flag for active forecasts
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

-- Unique indexes to replace PRIMARY KEY and FOREIGN KEY constraints
CREATE UNIQUE INDEX idx_product_hierarchy_pk ON da.product_hierarchy(demantra_item_skey);
CREATE UNIQUE INDEX idx_location_hierarchy_pk ON da.location_hierarchy(location_skey);
CREATE UNIQUE INDEX idx_sales_actuals_pk ON da.sales_actuals(id);
CREATE UNIQUE INDEX idx_product_clusters_pk ON da.product_clusters(cluster_id);
CREATE UNIQUE INDEX idx_forecasts_pk ON da.forecasts(forecast_id);
CREATE UNIQUE INDEX idx_model_validation_pk ON da.model_validation(validation_id);
CREATE UNIQUE INDEX idx_final_forecasts_pk ON da.final_forecasts(final_forecast_id);

-- Relationship indexes to maintain referential integrity
CREATE INDEX idx_sales_actuals_item_fk ON da.sales_actuals(item_skey);
CREATE INDEX idx_sales_actuals_location_fk ON da.sales_actuals(location_skey);
CREATE INDEX idx_product_clusters_item_fk ON da.product_clusters(item_skey);
CREATE INDEX idx_product_clusters_location_fk ON da.product_clusters(location_skey);
CREATE INDEX idx_forecasts_item_fk ON da.forecasts(item_skey);
CREATE INDEX idx_forecasts_location_fk ON da.forecasts(location_skey);
CREATE INDEX idx_final_forecasts_item_fk ON da.final_forecasts(item_skey);
CREATE INDEX idx_final_forecasts_location_fk ON da.final_forecasts(location_skey);

-- Unique index to replace UNIQUE constraint
CREATE UNIQUE INDEX idx_product_clusters_unique ON da.product_clusters(item_skey, location_skey);

-- Composite indexes for common filter combinations
CREATE INDEX idx_sales_product_location_date ON da.sales_actuals(item_skey, location_skey, sales_date);

-- Views for Common Queries
CREATE OR REPLACE VIEW v_product_location_summary AS
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
CREATE OR REPLACE VIEW v_forecast_accuracy AS
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
CREATE OR REPLACE VIEW v_time_series_clustered AS
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

-- Initialize default tables if they don't exist with sample data
-- This can be used for first-time setup
INSERT INTO da.product_hierarchy (demantra_item_skey, business_sector, business_unit, franchise, product_line, ibp_level_5, ibp_level_6, ibp_level_7, catalog_number, uom, pack_content) 
SELECT 1, 'Orthopedics', 'Knee', 'Knee Solutions', 'Knee Replacement', 'Knee Solutions', 'Knee Replacement', 'Knee Implant', 'KNEE001', 'EA', 'Single Pack'
WHERE NOT EXISTS (SELECT 1 FROM da.product_hierarchy WHERE demantra_item_skey = 1);

INSERT INTO da.location_hierarchy (location_skey, selling_division, area, stryker_group_region, region, country) 
SELECT 1, 'US', 'North America', 'NA', 'US', 'USA'
WHERE NOT EXISTS (SELECT 1 FROM da.location_hierarchy WHERE location_skey = 1);
