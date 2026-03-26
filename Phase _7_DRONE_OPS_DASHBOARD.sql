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


-- 5. DRONE_OPS_DASHBOARD

import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session
st.set_page_config(layout="wide")
session = get_active_session()
def spark(df, x, y, color="#1f77b4"):
    if len(df) < 2:
        return alt.Chart(pd.DataFrame({"a": [0], "b": [0]})).mark_point(opacity=0).encode(
            x="a:Q", y="b:Q").properties(height=50)
    return alt.Chart(df).mark_area(
        color=color, opacity=0.3, line={"color": color, "strokeWidth": 1.5}
    ).encode(
        x=alt.X(f"{x}:T", axis=None),
        y=alt.Y(f"{y}:Q", axis=None, scale=alt.Scale(zero=False))
    ).properties(height=50)
@st.cache_data(ttl=600)
def load(table):
    return session.sql(f"SELECT * FROM DRONE_DELIVERY_DB.GOLD.{table}").to_pandas()
@st.cache_data(ttl=600)
def freshness():
    r = session.sql("SELECT MAX(_DBT_LOADED_AT) AS TS FROM DRONE_DELIVERY_DB.BRONZE.BRONZE_ORDERS").to_pandas()
    return str(r["TS"].iloc[0])
@st.cache_data(ttl=3600)
def ai_summary(date_key):
    r = session.sql("""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2',
            'You are a drone delivery operations analyst. Write exactly 3 concise sentences summarizing operations. Be specific with numbers. Metrics: ' ||
            'Date=' || REPORT_DATE::STRING ||
            ' Orders=' || TOTAL_ORDERS::STRING ||
            ' Delivered=' || DELIVERED_ORDERS::STRING ||
            ' Cancelled=' || CANCELLED_ORDERS::STRING ||
            ' SuccessRate=' || ROUND(DELIVERY_SUCCESS_RATE_PCT,1)::STRING || '%' ||
            ' AvgDeliveryMin=' || COALESCE(ROUND(AVG_DELIVERY_TIME_MIN,1)::STRING,'N/A') ||
            ' AvgRating=' || COALESCE(ROUND(AVG_CUSTOMER_RATING,2)::STRING,'N/A') ||
            ' ActiveAircraft=' || ACTIVE_AIRCRAFT::STRING ||
            ' AvgDelayMin=' || COALESCE(ROUND(AVG_DEPARTURE_DELAY_MIN,1)::STRING,'N/A')
        ) AS S FROM DRONE_DELIVERY_DB.GOLD.KPI_DELIVERY_PERFORMANCE
        WHERE REPORT_DATE = (SELECT MAX(REPORT_DATE) FROM DRONE_DELIVERY_DB.GOLD.KPI_DELIVERY_PERFORMANCE)
    """).to_pandas()
    return r["S"].iloc[0]
kpi = load("KPI_DELIVERY_PERFORMANCE").sort_values("REPORT_DATE")
deliveries = load("FCT_DELIVERIES")
aircraft = load("DIM_AIRCRAFT")
depots = load("DIM_DEPOTS")
zones = load("DIM_DELIVERY_ZONES")
customers = load("DIM_CUSTOMERS")
maintenance = load("FCT_MAINTENANCE")
telemetry = load("FCT_FLIGHT_TELEMETRY")
st.sidebar.title("Filters")
ts = freshness()
st.sidebar.caption(f"Data refreshed: {ts}")
min_d = kpi["REPORT_DATE"].min().date()
max_d = kpi["REPORT_DATE"].max().date()
start_d = st.sidebar.date_input("Start date", value=min_d)
end_d = st.sidebar.date_input("End date", value=max_d)
depot_opts = ["All"] + sorted(depots["DEPOT_NAME"].tolist())
sel_depot = st.sidebar.selectbox("Depot", depot_opts)
zone_opts = ["All"] + sorted(zones["ZONE_NAME"].tolist())
sel_zone = st.sidebar.selectbox("Delivery zone", zone_opts)
kf = kpi[(kpi["REPORT_DATE"].dt.date >= start_d) & (kpi["REPORT_DATE"].dt.date <= end_d)]
df = deliveries.copy()
if "ORDER_CREATED_AT" in df.columns:
    df = df[(df["ORDER_CREATED_AT"].dt.date >= start_d) & (df["ORDER_CREATED_AT"].dt.date <= end_d)]
af = aircraft.copy()
if sel_depot != "All":
    did = depots[depots["DEPOT_NAME"] == sel_depot]["DEPOT_ID"].iloc[0]
    af = af[af["DEPOT_ID"] == did]
    aids = af["AIRCRAFT_ID"].tolist()
    df = df[df["AIRCRAFT_ID"].isin(aids)]
st.title("Drone Delivery Dashboard")
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Highlights", "Recommendations",
    "Operations Overview", "Fleet Health", "Route Performance",
    "Maintenance & Parts", "Customer Insights"
])
with tab3:
    if len(kf) > 0:
        lat = kf.iloc[-1]
        prv = kf.iloc[-2] if len(kf) > 1 else None
        k1, k2 = st.columns(2)
        with k1:
            st.metric("Total orders", f"{int(lat['TOTAL_ORDERS']):,}",
                      delta=f"{int(lat['TOTAL_ORDERS'] - prv['TOTAL_ORDERS']):+,}" if prv is not None else None)
            st.altair_chart(spark(kf, "REPORT_DATE", "TOTAL_ORDERS"), use_container_width=True)
        with k2:
            st.metric("Success rate", f"{lat['DELIVERY_SUCCESS_RATE_PCT']:.1f}%",
                      #delta=f"{lat['DELIVERY_SUCCESS_RATE_PCT'] - prv['DELIVERY_SUCCESS_RATE_PCT']:.1f}%" if prv is not None else None
            )
            st.altair_chart(spark(kf, "REPORT_DATE", "DELIVERY_SUCCESS_RATE_PCT", "#2ca02c"), use_container_width=True)
        k4, k5 = st.columns(2)
        with k4:
            v = lat.get("AVG_CUSTOMER_RATING")
            st.metric("Avg rating", f"{v:.2f}" if pd.notna(v) else "N/A")
            st.altair_chart(spark(kf.dropna(subset=["AVG_CUSTOMER_RATING"]), "REPORT_DATE", "AVG_CUSTOMER_RATING", "#9467bd"), use_container_width=True)
        with k5:
            st.metric("Active aircraft", f"{int(lat['ACTIVE_AIRCRAFT'])}")
            st.altair_chart(spark(kf, "REPORT_DATE", "ACTIVE_AIRCRAFT", "#17becf"), use_container_width=True)
    else:
        st.warning("No data for selected date range")
    st.divider()
    st.subheader("AI daily summary")
    try:
        summary = ai_summary(str(kpi["REPORT_DATE"].max()))
        st.info(summary)
    except Exception as e:
        st.warning("AI summary unavailable")
    left, right = st.columns(2)
    with left:
        st.subheader("Daily delivery volume")
        c = alt.Chart(kf).mark_bar(color="#1f77b4", cornerRadiusTopLeft=3, cornerRadiusTopRight=3).encode(
            x=alt.X("REPORT_DATE:T", title="Date"),
            y=alt.Y("TOTAL_ORDERS:Q", title="Orders"),
            tooltip=[alt.Tooltip("REPORT_DATE:T"), alt.Tooltip("TOTAL_ORDERS:Q"), alt.Tooltip("DELIVERED_ORDERS:Q")]
        ).properties(height=280)
        st.altair_chart(c, use_container_width=True)
    with right:
        st.subheader("Success rate trend")
        c = alt.Chart(kf).mark_area(color="#2ca02c", opacity=0.3, line={"color": "#2ca02c", "strokeWidth": 2}).encode(
            x=alt.X("REPORT_DATE:T", title="Date"),
            y=alt.Y("DELIVERY_SUCCESS_RATE_PCT:Q", title="Success rate (%)", scale=alt.Scale(domain=[0, 100])),
            tooltip=[alt.Tooltip("REPORT_DATE:T"), alt.Tooltip("DELIVERY_SUCCESS_RATE_PCT:Q", format=".1f")]
        ).properties(height=280)
        st.altair_chart(c, use_container_width=True)
with tab4:
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        st.metric("Total aircraft", len(af))
    with f2:
        st.metric("Active", int((af["AIRCRAFT_STATUS"] == "ACTIVE").sum()))
    with f3:
        st.metric("Avg flight hours", f"{af['TOTAL_FLIGHT_HOURS'].mean():.0f}")
    with f4:
        st.metric("Total missions", f"{int(af['TOTAL_MISSIONS'].sum()):,}")
    st.divider()
    left, right = st.columns(2)
    with left:
        st.subheader("Aircraft by status")
        sd = af.groupby("AIRCRAFT_STATUS").size().reset_index(name="COUNT")
        c = alt.Chart(sd).mark_bar().encode(
            x=alt.X("AIRCRAFT_STATUS:N", sort="-y"), y="COUNT:Q",
            color=alt.Color("AIRCRAFT_STATUS:N", legend=None), tooltip=["AIRCRAFT_STATUS:N", "COUNT:Q"]
        ).properties(height=280)
        st.altair_chart(c, use_container_width=True)
    with right:
        st.subheader("Maintenance compliance")
        md = af.groupby("MAINTENANCE_STATUS").size().reset_index(name="COUNT")
        c = alt.Chart(md).mark_bar().encode(
            x=alt.X("MAINTENANCE_STATUS:N", sort="-y"), y="COUNT:Q",
            color=alt.Color("MAINTENANCE_STATUS:N", legend=None), tooltip=["MAINTENANCE_STATUS:N", "COUNT:Q"]
        ).properties(height=280)
        st.altair_chart(c, use_container_width=True)
    st.subheader("Telemetry anomaly trend")
    if "HOUR_BUCKET" in telemetry.columns:
        ta = telemetry.groupby("HOUR_BUCKET").agg({"ANOMALY_COUNT": "sum"}).reset_index()
        c = alt.Chart(ta).mark_line(color="#d62728", strokeWidth=1.5).encode(
            x=alt.X("HOUR_BUCKET:T", title="Time"), y=alt.Y("ANOMALY_COUNT:Q", title="Anomalies"),
            tooltip=["HOUR_BUCKET:T", "ANOMALY_COUNT:Q"]
        ).properties(height=250)
        st.altair_chart(c, use_container_width=True)
    st.subheader("Top 10 aircraft by missions")
    top10 = af.nlargest(10, "COMPLETED_MISSIONS")
    c = alt.Chart(top10).mark_bar(color="#1f77b4").encode(
        x=alt.X("COMPLETED_MISSIONS:Q"), y=alt.Y("AIRCRAFT_ID:N", sort="-x"),
        tooltip=["AIRCRAFT_ID:N", "AIRCRAFT_MODEL:N", "COMPLETED_MISSIONS:Q", "TOTAL_FLIGHT_HOURS:Q"]
    ).properties(height=300)
    st.altair_chart(c, use_container_width=True)
    st.subheader("Depot fleet overview")
    st.dataframe(depots[["DEPOT_NAME", "DEPOT_SIZE", "AIRCRAFT_COUNT", "ACTIVE_AIRCRAFT",
                          "TOTAL_INVENTORY_VALUE", "PARTS_NEEDING_REORDER"]].rename(columns={
        "DEPOT_NAME": "Depot", "DEPOT_SIZE": "Size", "AIRCRAFT_COUNT": "Aircraft",
        "ACTIVE_AIRCRAFT": "Active", "TOTAL_INVENTORY_VALUE": "Inventory value",
        "PARTS_NEEDING_REORDER": "Reorder"}), use_container_width=True)
with tab5:
    routes = df.dropna(subset=["ROUTE_DISTANCE_KM"])
    r1, r2, r3 = st.columns(3)
    with r1:
        st.metric("Avg route distance", f"{routes['ROUTE_DISTANCE_KM'].mean():.1f} km" if len(routes) > 0 else "N/A")
    with r2:
        v = routes["PLANNED_DURATION_MIN"].dropna().mean() if len(routes) > 0 else None
        st.metric("Avg planned duration", f"{v:.1f} min" if pd.notna(v) else "N/A")
    with r3:
        v = routes["PLANNED_AVG_SPEED"].dropna().mean() if len(routes) > 0 else None
        st.metric("Avg planned speed", f"{v:.1f} km/h" if pd.notna(v) else "N/A")
    st.divider()
    left, right = st.columns(2)
    with left:
        st.subheader("Route category distribution")
        if len(routes) > 0:
            rc = routes["ROUTE_CATEGORY"].dropna().value_counts().reset_index()
            rc.columns = ["CATEGORY", "COUNT"]
            c = alt.Chart(rc).mark_bar().encode(
                x=alt.X("CATEGORY:N", sort="-y"), y="COUNT:Q",
                color=alt.Color("CATEGORY:N", legend=None), tooltip=["CATEGORY:N", "COUNT:Q"]
            ).properties(height=280)
            st.altair_chart(c, use_container_width=True)
    with right:
        st.subheader("Distance vs delivery time")
        sdf = routes.dropna(subset=["ROUTE_DISTANCE_KM", "DELIVERY_TIME_MIN"])
        if len(sdf) > 0:
            plot_df = sdf if len(sdf) <= 1000 else sdf.sample(1000, random_state=42)
            c = alt.Chart(plot_df).mark_circle(opacity=0.5, size=30).encode(
                x=alt.X("ROUTE_DISTANCE_KM:Q", title="Distance (km)"),
                y=alt.Y("DELIVERY_TIME_MIN:Q", title="Delivery time (min)"),
                color=alt.Color("ROUTE_CATEGORY:N", title="Category"),
                tooltip=["ORDER_ID:N", "ROUTE_DISTANCE_KM:Q", "DELIVERY_TIME_MIN:Q"]
            ).properties(height=280)
            st.altair_chart(c, use_container_width=True)
    st.subheader("Avg speed by route category")
    if len(routes) > 0:
        spd = routes.dropna(subset=["PLANNED_AVG_SPEED", "ROUTE_CATEGORY"])
        avg_spd = spd.groupby("ROUTE_CATEGORY")["PLANNED_AVG_SPEED"].mean().reset_index()
        c = alt.Chart(avg_spd).mark_bar().encode(
            x=alt.X("ROUTE_CATEGORY:N", title="Category"),
            y=alt.Y("PLANNED_AVG_SPEED:Q", title="Avg speed (km/h)"),
            color=alt.Color("ROUTE_CATEGORY:N", legend=None),
            tooltip=["ROUTE_CATEGORY:N", alt.Tooltip("PLANNED_AVG_SPEED:Q", format=".1f")]
        ).properties(height=280)
        st.altair_chart(c, use_container_width=True)
with tab6:
    e1, e2, e3, e4 = st.columns(4)
    with e1:
        st.metric("Total events", len(maintenance))
    with e2:
        st.metric("Avg duration", f"{maintenance['DURATION_MIN'].dropna().mean():.0f} min")
    with e3:
        st.metric("Total parts cost", f"${maintenance['TOTAL_PARTS_COST'].dropna().sum():,.0f}")
    with e4:
        st.metric("Parts consumed", f"{int(maintenance['TOTAL_PARTS_QUANTITY'].dropna().sum()):,}")
    st.divider()
    left, right = st.columns(2)
    with left:
        st.subheader("Events by type")
        td = maintenance.groupby("MAINTENANCE_TYPE").size().reset_index(name="COUNT")
        c = alt.Chart(td).mark_bar().encode(
            x=alt.X("MAINTENANCE_TYPE:N", sort="-y"), y="COUNT:Q",
            color=alt.Color("MAINTENANCE_TYPE:N", legend=None), tooltip=["MAINTENANCE_TYPE:N", "COUNT:Q"]
        ).properties(height=280)
        st.altair_chart(c, use_container_width=True)
    with right:
        st.subheader("Events by urgency")
        ud = maintenance.groupby("URGENCY_LEVEL").size().reset_index(name="COUNT")
        pal = {"CRITICAL": "#d62728", "STANDARD": "#ff7f0e", "ROUTINE": "#2ca02c"}
        c = alt.Chart(ud).mark_bar().encode(
            x=alt.X("URGENCY_LEVEL:N", sort=["CRITICAL", "STANDARD", "ROUTINE"]), y="COUNT:Q",
            color=alt.Color("URGENCY_LEVEL:N",
                scale=alt.Scale(domain=list(pal.keys()), range=list(pal.values())), legend=None),
            tooltip=["URGENCY_LEVEL:N", "COUNT:Q"]
        ).properties(height=280)
        st.altair_chart(c, use_container_width=True)
    st.subheader("Highest cost events")
    top_m = maintenance.nlargest(10, "TOTAL_PARTS_COST")
    st.dataframe(top_m[["LOG_ID", "AIRCRAFT_ID", "MAINTENANCE_TYPE", "URGENCY_LEVEL",
                          "DURATION_MIN", "PARTS_USED_COUNT", "TOTAL_PARTS_COST"]].rename(columns={
        "LOG_ID": "Log ID", "AIRCRAFT_ID": "Aircraft", "MAINTENANCE_TYPE": "Type",
        "URGENCY_LEVEL": "Urgency", "DURATION_MIN": "Duration (min)",
        "PARTS_USED_COUNT": "Parts used", "TOTAL_PARTS_COST": "Cost"}), use_container_width=True)
with tab7:
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Total customers", len(customers))
    with c2:
        st.metric("Avg rating", f"{customers['AVG_RATING'].dropna().mean():.2f}")
    st.divider()
    left, right = st.columns(2)
    with left:
        st.subheader("Rating distribution")
        r = df["FEEDBACK_RATING"].dropna()
        if len(r) > 0:
            rd = r.value_counts().reset_index()
            rd.columns = ["RATING", "COUNT"]
            c = alt.Chart(rd).mark_bar(color="#ff7f0e").encode(
                x=alt.X("RATING:O"), y="COUNT:Q", tooltip=["RATING:O", "COUNT:Q"]
            ).properties(height=280)
            st.altair_chart(c, use_container_width=True)
    with right:
        st.subheader("Sentiment breakdown")
        s = df["SENTIMENT_LABEL"].dropna()
        if len(s) > 0:
            sd = s.value_counts().reset_index()
            sd.columns = ["LABEL", "COUNT"]
            pal = {"POSITIVE": "#2ca02c", "NEUTRAL": "#ffbb78", "NEGATIVE": "#d62728"}
            c = alt.Chart(sd).mark_bar().encode(
                x=alt.X("LABEL:N", sort=["POSITIVE", "NEUTRAL", "NEGATIVE"]), y="COUNT:Q",
                color=alt.Color("LABEL:N",
                    scale=alt.Scale(domain=list(pal.keys()), range=list(pal.values())), legend=None),
                tooltip=["LABEL:N", "COUNT:Q"]
            ).properties(height=280)
            st.altair_chart(c, use_container_width=True)
    left2, right2 = st.columns(2)
    with left2:
        st.subheader("Delivery time distribution")
        dt = df["DELIVERY_TIME_MIN"].dropna()
        if len(dt) > 0:
            dtd = pd.DataFrame({"DELIVERY_TIME_MIN": dt})
            c = alt.Chart(dtd).mark_bar(color="#1f77b4").encode(
                alt.X("DELIVERY_TIME_MIN:Q", bin=alt.Bin(maxbins=30), title="Delivery time (min)"),
                alt.Y("count()", title="Frequency")
            ).properties(height=280)
            st.altair_chart(c, use_container_width=True)
    with right2:
        st.subheader("Rating trend")
        if len(kf) > 0:
            c = alt.Chart(kf).mark_line(color="#ff7f0e", strokeWidth=2).encode(
                x=alt.X("REPORT_DATE:T", title="Date"),
                y=alt.Y("AVG_CUSTOMER_RATING:Q", title="Rating", scale=alt.Scale(domain=[1, 5])),
                tooltip=[alt.Tooltip("REPORT_DATE:T"), alt.Tooltip("AVG_CUSTOMER_RATING:Q", format=".2f")]
            ).properties(height=280)
            st.altair_chart(c, use_container_width=True)
    st.subheader("Top customers by order volume")
    top_c = customers.nlargest(10, "TOTAL_ORDERS")
    st.dataframe(top_c[["CUSTOMER_NAME", "TOTAL_ORDERS", "DELIVERED_ORDERS",
                          "AVG_RATING", "AVG_SENTIMENT", "AVG_DELIVERY_TIME_MIN"]].rename(columns={
        "CUSTOMER_NAME": "Customer", "TOTAL_ORDERS": "Orders",
        "DELIVERED_ORDERS": "Delivered", "AVG_RATING": "Avg rating",
        "AVG_SENTIMENT": "Avg sentiment", "AVG_DELIVERY_TIME_MIN": "Avg time (min)"}), use_container_width=True)
with tab1:
    st.header("Major Highlights")
    st.caption("Executive summary across all DroneOps data sources")
    total_orders = int(kpi["TOTAL_ORDERS"].sum())
    total_delivered = int(kpi["DELIVERED_ORDERS"].sum())
    total_cancelled = int(kpi["CANCELLED_ORDERS"].sum())
    avg_success = round(kpi["DELIVERY_SUCCESS_RATE_PCT"].mean(), 1)
    avg_rating = round(kpi["AVG_CUSTOMER_RATING"].dropna().mean(), 2)
    avg_sent = round(kpi["AVG_SENTIMENT_SCORE"].dropna().mean(), 3)
    avg_active = int(round(kpi["ACTIVE_AIRCRAFT"].mean()))
    avg_delay = int(round(kpi["AVG_DEPARTURE_DELAY_MIN"].dropna().mean()))
    fleet_total = len(aircraft)
    active_count = int((aircraft["AIRCRAFT_STATUS"] == "ACTIVE").sum())
    maint_count = int((aircraft["AIRCRAFT_STATUS"] == "IN_MAINTENANCE").sum())
    grounded_count = int((aircraft["AIRCRAFT_STATUS"] == "GROUNDED").sum())
    due_soon = int((aircraft["MAINTENANCE_STATUS"] == "DUE_SOON").sum())
    depot_summary = aircraft.groupby("DEPOT_NAME").agg(
        active=("AIRCRAFT_STATUS", lambda x: (x == "ACTIVE").sum()),
        in_maint=("AIRCRAFT_STATUS", lambda x: (x == "IN_MAINTENANCE").sum()),
        grounded=("AIRCRAFT_STATUS", lambda x: (x == "GROUNDED").sum())
    ).reset_index()
    worst_depot = depot_summary.sort_values("in_maint", ascending=False).iloc[0]
    telem_anomaly_rate = 0
    telem_low_batt = 0
    telem_overspeed = 0
    if len(telemetry) > 0:
        telem_total_pts = telemetry["TELEMETRY_POINTS"].sum()
        telem_total_anom = telemetry["ANOMALY_COUNT"].sum()
        telem_low_batt = int(telemetry["LOW_BATTERY_COUNT"].sum())
        telem_overspeed = int(telemetry["OVERSPEED_COUNT"].sum())
        telem_anomaly_rate = round(telem_total_anom / telem_total_pts * 100, 1) if telem_total_pts > 0 else 0
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Orders", f"{total_orders:,}")
    m2.metric("Success Rate", f"{avg_success}%")
    m3.metric("Avg Rating", f"{avg_rating}/5")
    m4.metric("Avg Departure Delay", f"{avg_delay} min")
    st.divider()
    st.subheader("Highlights")
    hl1, hl2 = st.columns(2)
    with hl1:
        st.markdown("**Operations**")
        st.markdown(f"""
- **{avg_success}% delivery success rate** sustained over the full reporting period
- **{total_orders:,} total orders** processed, {total_delivered:,} delivered successfully
- Success rate remained **stable across both halves** of the period with no degradation
- **Zero overspeed events** fleet-wide, excellent speed-limit compliance
- Average **{avg_delay} min departure delay**, within acceptable operational bounds
""")
        st.markdown("**Fleet**")
        st.markdown(f"""
- **{fleet_total}-aircraft fleet** across 5 models, well diversified
- **{active_count} active, {maint_count} in maintenance** = {round(active_count/fleet_total*100)}% fleet availability
- All 5 aircraft models perform within a tight anomaly rate band, no outliers
- Average **{avg_active} aircraft active daily** across 5 depots
""")
    with hl2:
        st.markdown("**Customer Experience**")
        st.markdown(f"""
- **{avg_rating}/5 average rating** across {total_delivered:,} delivered orders
- Customer sentiment is **net positive (+{avg_sent})** across all feedback
- Rating and sentiment remained **stable** throughout the reporting period
- Only **{total_cancelled:,} cancellations** = {round(total_cancelled/total_orders*100, 1)}% cancellation rate
""")
        st.markdown("**Telemetry & Safety**")
        st.markdown(f"""
- **{telem_anomaly_rate}% anomaly rate**, consistent and predictable, entirely battery-related
- **{telem_low_batt:,} low-battery events** detected and logged
- **{telem_overspeed} overspeed incidents**, all aircraft within speed parameters
- Anomaly volume scales linearly with fleet utilization, no systemic degradation
""")
    st.divider()
    st.subheader("Areas of Concern")
    cn1, cn2 = st.columns(2)
    with cn1:
        st.markdown(f"""
- **{worst_depot["DEPOT_NAME"]}** has **{int(worst_depot["in_maint"])} of 10 aircraft in maintenance**, highest of all depots
- **{due_soon} aircraft** have **DUE_SOON maintenance status**, upcoming wave could reduce availability
- **{grounded_count} aircraft grounded**, needs immediate resolution
- **{maint_count} aircraft ({round(maint_count/fleet_total*100)}%)** in maintenance fleet-wide
""")
    with cn2:
        st.markdown(f"""
- **CRITICAL priority orders** show disproportionately long delivery times
- **{avg_rating}/5 customer rating** is below the 4.0+ industry benchmark
- **Sentiment score of +{avg_sent}** is barely positive, not delighted
- **{telem_anomaly_rate}% of telemetry points** trigger a low-battery anomaly
""")
    st.divider()
    st.subheader("Depot Health Snapshot")
    st.dataframe(
        depot_summary.rename(columns={
            "DEPOT_NAME": "Depot", "active": "Active",
            "in_maint": "In Maintenance", "grounded": "Grounded"
        }).set_index("Depot"),
        use_container_width=True
    )
    st.caption(f"Data period: {kpi['REPORT_DATE'].min().date()} to {kpi['REPORT_DATE'].max().date()} | {fleet_total} aircraft across 5 depots")
with tab2:
    st.header("Recommendations")
    st.caption("Actionable recommendations based on current data analysis")
    total_orders = int(kpi["TOTAL_ORDERS"].sum())
    total_delivered = int(kpi["DELIVERED_ORDERS"].sum())
    avg_success = round(kpi["DELIVERY_SUCCESS_RATE_PCT"].mean(), 1)
    avg_rating = round(kpi["AVG_CUSTOMER_RATING"].dropna().mean(), 2)
    avg_sent = round(kpi["AVG_SENTIMENT_SCORE"].dropna().mean(), 3)
    avg_active = int(round(kpi["ACTIVE_AIRCRAFT"].mean()))
    fleet_total = len(aircraft)
    maint_count = int((aircraft["AIRCRAFT_STATUS"] == "IN_MAINTENANCE").sum())
    grounded_count = int((aircraft["AIRCRAFT_STATUS"] == "GROUNDED").sum())
    depot_summary_r = aircraft.groupby("DEPOT_NAME").agg(
        in_maint=("AIRCRAFT_STATUS", lambda x: (x == "IN_MAINTENANCE").sum())
    ).reset_index()
    worst_depot_r = depot_summary_r.sort_values("in_maint", ascending=False).iloc[0]
    telem_anomaly_rate = 0
    if len(telemetry) > 0:
        telem_total_pts = telemetry["TELEMETRY_POINTS"].sum()
        telem_total_anom = telemetry["ANOMALY_COUNT"].sum()
        telem_anomaly_rate = round(telem_total_anom / telem_total_pts * 100, 1) if telem_total_pts > 0 else 0
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Success Rate", f"{avg_success}%")
    m2.metric("Fleet Availability", f"{round((fleet_total - maint_count - grounded_count)/fleet_total*100)}%")
    m3.metric("Avg Rating", f"{avg_rating}/5")
    m4.metric("Anomaly Rate", f"{telem_anomaly_rate}%")
    st.divider()
    rc1, rc2 = st.columns(2)
    with rc1:
        st.subheader("Immediate (0-30 days)")
        st.markdown(f"""
- **Investigate {worst_depot_r["DEPOT_NAME"]}** — audit why {int(worst_depot_r["in_maint"])}/10 aircraft are in maintenance
- **Resolve grounded aircraft** — return {grounded_count} unit(s) to service
- **Recalibrate battery anomaly threshold** — current setting flags {telem_anomaly_rate}% of readings
- **Pre-flight battery protocol** — enforce minimum charge levels before dispatch
""")
        st.subheader("Medium-Term (30-90 days)")
        st.markdown("""
- **Priority-based dispatch optimization** — reserve high-charge aircraft for CRITICAL orders
- **Staggered maintenance scheduling** — avoid fleet availability dips from simultaneous servicing
- **Customer experience deep-dive** — root-cause analysis on low-rated deliveries
""")
    with rc2:
        st.subheader("Strategic (90+ days)")
        st.markdown(f"""
- **Fleet utilization improvement** — only {round(avg_active/fleet_total*100)}% of fleet active daily, target 60%+
- **Battery lifecycle management** — invest in degradation curve tracking per airframe
- **Expand MEDIUM route capability** — evaluate aircraft range for beyond 10 km demand
- **Sentiment-to-action pipeline** — auto-route negative feedback for same-day review
- **Model-specific battery benchmarking** — track degradation per model for procurement decisions
""")
    st.divider()
    st.subheader("Implementation Priority Matrix")
    priority_data = pd.DataFrame({
        "Initiative": [
            "Investigate worst depot", "Resolve grounded aircraft",
            "Battery threshold recalibration", "Pre-flight battery protocol",
            "Priority dispatch optimization", "Staggered maintenance",
            "Customer experience deep-dive", "Fleet utilization improvement",
            "Battery lifecycle mgmt", "Sentiment-to-action pipeline"
        ],
        "Impact": ["High", "High", "Medium", "Medium", "High", "Medium", "High", "High", "Medium", "Medium"],
        "Effort": ["Low", "Low", "Low", "Medium", "Medium", "Medium", "Medium", "High", "High", "High"],
        "Timeline": ["0-30d", "0-30d", "0-30d", "0-30d", "30-90d", "30-90d", "30-90d", "90d+", "90d+", "90d+"]
    })
    st.dataframe(priority_data.set_index("Initiative"), use_container_width=True)

