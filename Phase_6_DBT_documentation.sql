-- Feature Implementation --

-- 6.1 Sidebar Filters

-- # Data freshness indicator
-- st.sidebar.caption(f"Data refreshed: {MAX(_DBT_LOADED_AT)}")

-- # Date range (two date inputs)
-- start_d = st.sidebar.date_input("Start date", value=min_date)
-- end_d = st.sidebar.date_input("End date", value=max_date)

-- # Depot selector — cascades to aircraft and deliveries
-- sel_depot = st.sidebar.selectbox("Depot", ["All"] + depot_names)

-- # Zone selector
-- sel_zone = st.sidebar.selectbox("Delivery zone", ["All"] + zone_names)




-- 6.2 KPI Cards with Sparklines --

def spark(df, x, y, color):
    return alt.Chart(df).mark_area(
        color=color, opacity=0.3,
        line={"color": color, "strokeWidth": 1.5}
    ).encode(
        x=alt.X(f"{x}:T", axis=None),
        y=alt.Y(f"{y}:Q", axis=None, scale=alt.Scale(zero=False))
    ).properties(height=50)





-- 6.3 AI Daily Summary ---

SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
    'You are a drone delivery operations analyst...' ||
    'Date=' || REPORT_DATE::STRING ||
    ' Orders=' || TOTAL_ORDERS::STRING ||
    ' SuccessRate=' || ROUND(DELIVERY_SUCCESS_RATE_PCT,1)::STRING || '%' ||
    ...
) AS S
FROM DRONE_DELIVERY_DB.GOLD.KPI_DELIVERY_PERFORMANCE
WHERE REPORT_DATE = (SELECT MAX(REPORT_DATE) FROM ...)



-- 6.4 Data Freshness Indicator --

SELECT MAX(_DBT_LOADED_AT) AS TS
FROM DRONE_DELIVERY_DB.BRONZE.BRONZE_ORDERS
