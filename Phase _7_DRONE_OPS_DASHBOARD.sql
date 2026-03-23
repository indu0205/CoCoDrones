-- Prerequisites Created --

-- Helper procedure to upload Python files to Snowflake stages
CREATE OR REPLACE PROCEDURE DRONE_DELIVERY_DB.ANALYTICS.STAGE_FILE(
    STAGE_PATH STRING, FILE_NAME STRING, FILE_CONTENT STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
def run(session, stage_path, file_name, file_content):
    import os
    local_path = '/tmp/' + file_name
    with open(local_path, 'w') as f:
        f.write(file_content)
    session.file.put(local_path, stage_path, auto_compress=False, overwrite=True)
    os.remove(local_path)
    return 'Uploaded ' + file_name + ' to ' + stage_path
$$;


--  Deployment Commands (In Order)  --  

-- 1. Create dedicated stage
CREATE STAGE IF NOT EXISTS DRONE_DELIVERY_DB.ANALYTICS.STREAMLIT_V2_STAGE;

-- 2. Upload streamlit_app.py (292 lines) via stored procedure
CALL DRONE_DELIVERY_DB.ANALYTICS.STAGE_FILE(
  '@DRONE_DELIVERY_DB.ANALYTICS.STREAMLIT_V2_STAGE',
  'streamlit_app.py',
  $$<full app code — see Section 5>$$
);

-- 3. Create the Streamlit app object
CREATE OR REPLACE STREAMLIT DRONE_DELIVERY_DB.ANALYTICS.DRONE_OPS_DASHBOARD
  ROOT_LOCATION = '@DRONE_DELIVERY_DB.ANALYTICS.STREAMLIT_V2_STAGE'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = COMPUTE_WH
  COMMENT = 'Phase 7 v2: Full-featured drone ops dashboard with 5 pages, filters, sparklines, AI summary';

-- 4. Grant access
GRANT USAGE ON STREAMLIT DRONE_DELIVERY_DB.ANALYTICS.DRONE_OPS_DASHBOARD
  TO ROLE DRONE_ANALYST;




--  Dashboard Architecture --

from snowflake.snowpark.context import get_active_session
session = get_active_session()


-- Sidebar Filters --

# Data freshness indicator
st.sidebar.caption(f"Data refreshed: {MAX(_DBT_LOADED_AT)}")

# Date range (two date inputs)
start_d = st.sidebar.date_input("Start date", value=min_date)
end_d = st.sidebar.date_input("End date", value=max_date)

# Depot selector — cascades to aircraft and deliveries
sel_depot = st.sidebar.selectbox("Depot", ["All"] + depot_names)

# Zone selector
sel_zone = st.sidebar.selectbox("Delivery zone", ["All"] + zone_names)




-- KPI Cards with Sparklines --

def spark(df, x, y, color):
    return alt.Chart(df).mark_area(
        color=color, opacity=0.3,
        line={"color": color, "strokeWidth": 1.5}
    ).encode(
        x=alt.X(f"{x}:T", axis=None),
        y=alt.Y(f"{y}:Q", axis=None, scale=alt.Scale(zero=False))
    ).properties(height=50)




-- AI Daily Summary --  

SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
    'You are a drone delivery operations analyst...' ||
    'Date=' || REPORT_DATE::STRING ||
    ' Orders=' || TOTAL_ORDERS::STRING ||
    ' SuccessRate=' || ROUND(DELIVERY_SUCCESS_RATE_PCT,1)::STRING || '%' ||
    ...
) AS S
FROM DRONE_DELIVERY_DB.GOLD.KPI_DELIVERY_PERFORMANCE
WHERE REPORT_DATE = (SELECT MAX(REPORT_DATE) FROM ...)



-- Data Freshness Indicator -- 

SELECT MAX(_DBT_LOADED_AT) AS TS
FROM DRONE_DELIVERY_DB.BRONZE.BRONZE_ORDERS


-- Snowflake ObjDRONE_DELIVERY_DB.ANALYTICS --

-- ├── STREAMLIT_V2_STAGE          (internal stage)
-- │   └── streamlit_app.py        (292 lines, 14KB)
-- ├── DRONE_OPS_DASHBOARD         (STREAMLIT object)
-- │   ├── ROOT_LOCATION: @STREAMLIT_V2_STAGE
-- │   ├── MAIN_FILE: streamlit_app.py
-- │   └── QUERY_WAREHOUSE: COMPUTE_WH
-- └── STAGE_FILE                  (helper procedure)
-- ects --



--  How to Redeploy --

-- Update the app code
CALL DRONE_DELIVERY_DB.ANALYTICS.STAGE_FILE(
  '@DRONE_DELIVERY_DB.ANALYTICS.STREAMLIT_V2_STAGE',
  'streamlit_app.py',
  $$<updated code>$$
);

-- Recreate the app to pick up changes
CREATE OR REPLACE STREAMLIT DRONE_DELIVERY_DB.ANALYTICS.DRONE_OPS_DASHBOARD
  ROOT_LOCATION = '@DRONE_DELIVERY_DB.ANALYTICS.STREAMLIT_V2_STAGE'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = COMPUTE_WH;


-- Validation Queries Executed (Read-Only) --

-- 1. Row count reconciliation (Raw vs Bronze)
SELECT 'MISSIONS' AS entity,
    (SELECT COUNT(*) FROM DRONE_DELIVERY_DB.API.MISSIONS) AS raw_ct,
    (SELECT COUNT(*) FROM DRONE_DELIVERY_DB.BRONZE.BRONZE_MISSIONS) AS bronze_ct
UNION ALL ... (all 16 entities);

-- 2. Silver row counts
SELECT 'silver_customers', COUNT(*) FROM DRONE_DELIVERY_DB.SILVER.SILVER_CUSTOMERS
UNION ALL ... (10 Silver views);

-- 3. Gold row counts
SELECT 'dim_customers', COUNT(*) FROM DRONE_DELIVERY_DB.GOLD.DIM_CUSTOMERS
UNION ALL ... (all 8 Gold tables);

-- 4. CDC stream health
SHOW STREAMS IN SCHEMA DRONE_DELIVERY_DB.BRONZE;

-- 5. RBAC grants (all 4 roles)
SHOW GRANTS TO ROLE DRONE_LOADER;
SHOW GRANTS TO ROLE DRONE_ADMIN;
SHOW GRANTS TO ROLE DRONE_ENGINEER;
SHOW GRANTS TO ROLE DRONE_ANALYST;

-- 6. Data quality (12 checks)
SELECT 'fct_deliveries.order_id NULL', COUNT(*)
  FROM DRONE_DELIVERY_DB.GOLD.FCT_DELIVERIES WHERE order_id IS NULL
UNION ALL ... (nulls, ranges, referential integrity);

-- 7. KPI date continuity
SELECT MIN(report_date), MAX(report_date), COUNT(*),
       DATEDIFF('day', MIN(report_date), MAX(report_date)) + 1
FROM DRONE_DELIVERY_DB.GOLD.KPI_DELIVERY_PERFORMANCE;

-- 8. Object inventory
SELECT table_schema, table_type, COUNT(*)
FROM DRONE_DELIVERY_DB.INFORMATION_SCHEMA.TABLES
GROUP BY table_schema, table_type;

-- 9. Performance baseline
SELECT COUNT(*), AVG(delivery_time_min), AVG(feedback_rating)
  FROM DRONE_DELIVERY_DB.GOLD.FCT_DELIVERIES;
SELECT COUNT(*), SUM(anomaly_count), AVG(avg_speed_kmh)
  FROM DRONE_DELIVERY_DB.GOLD.FCT_FLIGHT_TELEMETRY;

