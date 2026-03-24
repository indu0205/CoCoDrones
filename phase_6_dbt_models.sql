-- ============================================================================
-- PHASE 6: DBT MODEL SQL FILES
-- Drone Delivery Platform - Medallion Architecture (Bronze → Silver → Gold)
-- ============================================================================

-- ============================================================================
-- SECTION A: BRONZE STAGING MODELS (Source → Bronze)
-- These models copy raw source data with CDC metadata columns
-- Config: transient tables in BRONZE schema
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_aircraft.sql
-- Source: DRONE_DELIVERY_DB.DEPOT.AIRCRAFT
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_AIRCRAFT') }}

SELECT
    aircraft_id,
    model,
    serial_number,
    status,
    depot_id,
    total_flight_hours,
    last_maintenance_date,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'DEPOT.AIRCRAFT' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.DEPOT.AIRCRAFT;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_customers.sql
-- Source: DRONE_DELIVERY_DB.DELIVERY.CUSTOMERS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_CUSTOMERS') }}

SELECT
    customer_id,
    name,
    email,
    phone,
    address,
    created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'DELIVERY.CUSTOMERS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.DELIVERY.CUSTOMERS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_delivery_feedback.sql
-- Source: DRONE_DELIVERY_DB.DELIVERY.DELIVERY_FEEDBACK
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_DELIVERY_FEEDBACK') }}

SELECT
    feedback_id,
    order_id,
    customer_id,
    rating,
    comments,
    feedback_channel,
    created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'DELIVERY.DELIVERY_FEEDBACK' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.DELIVERY.DELIVERY_FEEDBACK;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_delivery_zones.sql
-- Source: DRONE_DELIVERY_DB.GEO.DELIVERY_ZONES
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_DELIVERY_ZONES') }}

SELECT
    zone_id,
    zone_name,
    center_lat,
    center_lon,
    radius_km,
    zone_type,
    is_active,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'GEO.DELIVERY_ZONES' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.GEO.DELIVERY_ZONES;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_depots.sql
-- Source: DRONE_DELIVERY_DB.DEPOT.DEPOTS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_DEPOTS') }}

SELECT
    depot_id,
    depot_name,
    lat,
    lon,
    capacity,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'DEPOT.DEPOTS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.DEPOT.DEPOTS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_flight_logs.sql
-- Source: DRONE_DELIVERY_DB.FLV6.FLIGHT_LOGS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_FLIGHT_LOGS') }}

SELECT
    flight_log_id,
    aircraft_id,
    recorded_at,
    lat,
    lon,
    altitude_m,
    speed_kmh,
    battery_pct,
    heading_deg,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'FLV6.FLIGHT_LOGS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.FLV6.FLIGHT_LOGS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_maintenance_logs.sql
-- Source: DRONE_DELIVERY_DB.RECORDS.MAINTENANCE_LOGS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_MAINTENANCE_LOGS') }}

SELECT
    log_id,
    aircraft_id,
    maintenance_type,
    description,
    technician,
    started_at,
    completed_at,
    status,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'RECORDS.MAINTENANCE_LOGS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.RECORDS.MAINTENANCE_LOGS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_missions.sql
-- Source: DRONE_DELIVERY_DB.API.MISSIONS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_MISSIONS') }}

SELECT
    mission_id,
    order_id,
    aircraft_id,
    status,
    planned_departure,
    actual_departure,
    planned_arrival,
    actual_arrival,
    notes,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'API.MISSIONS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.API.MISSIONS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_mission_events.sql
-- Source: DRONE_DELIVERY_DB.API.MISSION_EVENTS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_MISSION_EVENTS') }}

SELECT
    event_id,
    mission_id,
    event_type,
    event_timestamp,
    description,
    lat,
    lon,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'API.MISSION_EVENTS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.API.MISSION_EVENTS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_no_fly_zones.sql
-- Source: DRONE_DELIVERY_DB.GEO.NO_FLY_ZONES
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_NO_FLY_ZONES') }}

SELECT
    nfz_id,
    zone_name,
    center_lat,
    center_lon,
    radius_km,
    restriction_type,
    effective_from,
    effective_to,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'GEO.NO_FLY_ZONES' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.GEO.NO_FLY_ZONES;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_orders.sql
-- Source: DRONE_DELIVERY_DB.DELIVERY.ORDERS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_ORDERS') }}

SELECT
    order_id,
    customer_id,
    pickup_lat,
    pickup_lon,
    dropoff_lat,
    dropoff_lon,
    package_weight_kg,
    priority,
    status,
    created_at,
    delivered_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'DELIVERY.ORDERS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.DELIVERY.ORDERS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_parts_inventory.sql
-- Source: DRONE_DELIVERY_DB.DEPOT.PARTS_INVENTORY
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_PARTS_INVENTORY') }}

SELECT
    part_id,
    part_name,
    category,
    quantity_on_hand,
    reorder_level,
    unit_cost,
    depot_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'DEPOT.PARTS_INVENTORY' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.DEPOT.PARTS_INVENTORY;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_parts_used.sql
-- Source: DRONE_DELIVERY_DB.RECORDS.PARTS_USED
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_PARTS_USED') }}

SELECT
    usage_id,
    log_id,
    part_id,
    quantity_used,
    cost,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'RECORDS.PARTS_USED' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.RECORDS.PARTS_USED;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_routes.sql
-- Source: DRONE_DELIVERY_DB.OSRM.ROUTES
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_ROUTES') }}

SELECT
    route_id,
    mission_id,
    origin_lat,
    origin_lon,
    dest_lat,
    dest_lon,
    distance_km,
    duration_min,
    waypoints,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'OSRM.ROUTES' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.OSRM.ROUTES;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_route_segments.sql
-- Source: DRONE_DELIVERY_DB.OSRM.ROUTE_SEGMENTS
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_ROUTE_SEGMENTS') }}

SELECT
    segment_id,
    route_id,
    segment_seq,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    distance_km,
    altitude_m,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'OSRM.ROUTE_SEGMENTS' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.OSRM.ROUTE_SEGMENTS;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: stg_bronze_weather_data.sql
-- Source: DRONE_DELIVERY_DB.FLV6.WEATHER_DATA
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='BRONZE', alias='BRONZE_WEATHER_DATA') }}

SELECT
    weather_id,
    station_id,
    recorded_at,
    temp_c,
    wind_speed_kmh,
    wind_direction_deg,
    visibility_km,
    precipitation_mm,
    conditions,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS _loaded_at,
    'FLV6.WEATHER_DATA' AS _source_file,
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM DRONE_DELIVERY_DB.FLV6.WEATHER_DATA;


-- ============================================================================
-- SECTION B: SILVER TRANSFORMATION MODELS (Bronze → Silver)
-- These models clean, deduplicate, and enrich raw data
-- Config: views in SILVER schema
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_aircraft.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    aircraft_id,
    TRIM(model) AS aircraft_model,
    serial_number,
    UPPER(TRIM(status)) AS aircraft_status,
    depot_id,
    ROUND(total_flight_hours, 1) AS total_flight_hours,
    last_maintenance_date,
    CASE
        WHEN last_maintenance_date IS NULL THEN 'UNKNOWN'
        WHEN DATEDIFF('day', last_maintenance_date, CURRENT_DATE()) > 90 THEN 'OVERDUE'
        WHEN DATEDIFF('day', last_maintenance_date, CURRENT_DATE()) > 60 THEN 'DUE_SOON'
        ELSE 'CURRENT'
    END AS maintenance_status,
    CASE
        WHEN total_flight_hours > 1000 THEN 'HIGH'
        WHEN total_flight_hours > 500 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS usage_tier,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_aircraft') }}
WHERE aircraft_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_customers.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    customer_id,
    TRIM(name) AS customer_name,
    LOWER(TRIM(email)) AS email,
    REGEXP_REPLACE(phone, '[^0-9+]', '') AS phone_clean,
    TRIM(address) AS address,
    created_at,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_customers') }}
WHERE customer_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_delivery_feedback.sql
-- Uses Snowflake Cortex SENTIMENT for AI-powered sentiment analysis
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

WITH base AS (
    SELECT
        feedback_id,
        order_id,
        customer_id,
        rating,
        TRIM(comments) AS comments,
        UPPER(TRIM(feedback_channel)) AS feedback_channel,
        created_at,
        _dbt_loaded_at,
        SNOWFLAKE.CORTEX.SENTIMENT(comments) AS sentiment_score
    FROM {{ ref('stg_bronze_delivery_feedback') }}
    WHERE feedback_id IS NOT NULL
      AND comments IS NOT NULL
)

SELECT
    feedback_id,
    order_id,
    customer_id,
    rating,
    comments,
    feedback_channel,
    created_at,
    sentiment_score,
    CASE
        WHEN sentiment_score >= 0.3 THEN 'POSITIVE'
        WHEN sentiment_score <= -0.3 THEN 'NEGATIVE'
        ELSE 'NEUTRAL'
    END AS sentiment_label,
    _dbt_loaded_at
FROM base;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_delivery_zones.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    zone_id,
    TRIM(zone_name) AS zone_name,
    center_lat,
    center_lon,
    ROUND(radius_km, 2) AS radius_km,
    UPPER(TRIM(zone_type)) AS zone_type,
    COALESCE(is_active, TRUE) AS is_active,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_delivery_zones') }}
WHERE zone_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_depots.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    depot_id,
    TRIM(depot_name) AS depot_name,
    lat,
    lon,
    capacity,
    CASE
        WHEN capacity >= 20 THEN 'LARGE'
        WHEN capacity >= 10 THEN 'MEDIUM'
        ELSE 'SMALL'
    END AS depot_size,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_depots') }}
WHERE depot_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_flight_logs.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    flight_log_id,
    aircraft_id,
    recorded_at,
    lat,
    lon,
    ROUND(altitude_m, 1) AS altitude_m,
    ROUND(speed_kmh, 1) AS speed_kmh,
    ROUND(battery_pct, 1) AS battery_pct,
    ROUND(heading_deg, 1) AS heading_deg,
    CASE
        WHEN battery_pct < 10 THEN TRUE
        ELSE FALSE
    END AS is_low_battery,
    CASE
        WHEN speed_kmh > 120 THEN TRUE
        ELSE FALSE
    END AS is_overspeed,
    CASE
        WHEN altitude_m > 400 THEN TRUE
        ELSE FALSE
    END AS is_high_altitude,
    CASE
        WHEN battery_pct < 10 OR speed_kmh > 120 OR altitude_m > 400 THEN TRUE
        ELSE FALSE
    END AS has_anomaly,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_flight_logs') }}
WHERE flight_log_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_maintenance_logs.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    log_id,
    aircraft_id,
    UPPER(TRIM(maintenance_type)) AS maintenance_type,
    TRIM(description) AS description,
    TRIM(technician) AS technician,
    started_at,
    completed_at,
    UPPER(TRIM(status)) AS maintenance_status,
    CASE
        WHEN completed_at IS NOT NULL AND started_at IS NOT NULL
        THEN DATEDIFF('minute', started_at, completed_at)
    END AS duration_min,
    CASE
        WHEN UPPER(TRIM(maintenance_type)) IN ('ENGINE_OVERHAUL', 'STRUCTURAL_REPAIR', 'EMERGENCY') THEN 'CRITICAL'
        WHEN UPPER(TRIM(maintenance_type)) IN ('INSPECTION', 'CALIBRATION', 'SCHEDULED') THEN 'ROUTINE'
        ELSE 'STANDARD'
    END AS urgency_level,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_maintenance_logs') }}
WHERE log_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_missions.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    mission_id,
    order_id,
    aircraft_id,
    UPPER(TRIM(status)) AS mission_status,
    planned_departure,
    actual_departure,
    planned_arrival,
    actual_arrival,
    TRIM(notes) AS notes,
    CASE
        WHEN actual_departure IS NOT NULL AND actual_arrival IS NOT NULL
        THEN DATEDIFF('minute', actual_departure, actual_arrival)
    END AS actual_duration_min,
    CASE
        WHEN planned_departure IS NOT NULL AND planned_arrival IS NOT NULL
        THEN DATEDIFF('minute', planned_departure, planned_arrival)
    END AS planned_duration_min,
    CASE
        WHEN actual_departure IS NOT NULL AND planned_departure IS NOT NULL
        THEN DATEDIFF('minute', planned_departure, actual_departure)
    END AS departure_delay_min,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_missions') }}
WHERE mission_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_mission_events.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    event_id,
    mission_id,
    UPPER(TRIM(event_type)) AS event_type,
    event_timestamp,
    TRIM(description) AS description,
    lat,
    lon,
    CASE
        WHEN UPPER(TRIM(event_type)) IN ('CRASH', 'EMERGENCY_LANDING', 'COLLISION', 'SYSTEM_FAILURE') THEN 'CRITICAL'
        WHEN UPPER(TRIM(event_type)) IN ('LOW_BATTERY', 'WEATHER_ABORT', 'REROUTE', 'SIGNAL_LOST') THEN 'WARNING'
        WHEN UPPER(TRIM(event_type)) IN ('TAKEOFF', 'LANDING', 'WAYPOINT', 'DELIVERED') THEN 'INFO'
        ELSE 'UNKNOWN'
    END AS severity,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_mission_events') }}
WHERE event_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_mission_timeline.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    me.mission_id,
    me.event_type,
    me.event_timestamp,
    me.severity,
    me.description,
    me.lat,
    me.lon,
    m.aircraft_id,
    m.order_id,
    m.mission_status,
    ROW_NUMBER() OVER (
        PARTITION BY me.mission_id
        ORDER BY me.event_timestamp
    ) AS event_sequence,
    LAG(me.event_timestamp) OVER (
        PARTITION BY me.mission_id
        ORDER BY me.event_timestamp
    ) AS prev_event_timestamp,
    DATEDIFF('second',
        LAG(me.event_timestamp) OVER (
            PARTITION BY me.mission_id
            ORDER BY me.event_timestamp
        ),
        me.event_timestamp
    ) AS seconds_since_prev_event
FROM {{ ref('silver_mission_events') }} me
INNER JOIN {{ ref('silver_missions') }} m
    ON me.mission_id = m.mission_id;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_no_fly_zones.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    nfz_id,
    TRIM(zone_name) AS zone_name,
    center_lat,
    center_lon,
    ROUND(radius_km, 2) AS radius_km,
    UPPER(TRIM(restriction_type)) AS restriction_type,
    effective_from,
    effective_to,
    CASE
        WHEN effective_to IS NULL OR effective_to > CURRENT_TIMESTAMP() THEN TRUE
        ELSE FALSE
    END AS is_currently_active,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_no_fly_zones') }}
WHERE nfz_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_orders.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    order_id,
    customer_id,
    pickup_lat,
    pickup_lon,
    dropoff_lat,
    dropoff_lon,
    ROUND(package_weight_kg, 2) AS package_weight_kg,
    COALESCE(UPPER(TRIM(priority)), 'STANDARD') AS priority,
    UPPER(TRIM(status)) AS order_status,
    created_at,
    delivered_at,
    CASE
        WHEN delivered_at IS NOT NULL AND created_at IS NOT NULL
        THEN DATEDIFF('minute', created_at, delivered_at)
    END AS delivery_time_min,
    ROUND(
        HAVERSINE(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon),
        2
    ) AS straight_line_distance_km,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_orders') }}
WHERE order_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_parts_inventory.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    part_id,
    TRIM(part_name) AS part_name,
    UPPER(TRIM(category)) AS category,
    quantity_on_hand,
    reorder_level,
    ROUND(unit_cost, 2) AS unit_cost,
    depot_id,
    CASE
        WHEN quantity_on_hand <= reorder_level THEN 'REORDER_NEEDED'
        WHEN quantity_on_hand <= reorder_level * 1.5 THEN 'LOW_STOCK'
        ELSE 'IN_STOCK'
    END AS stock_status,
    ROUND(quantity_on_hand * unit_cost, 2) AS inventory_value,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_parts_inventory') }}
WHERE part_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_parts_used.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    usage_id,
    log_id,
    part_id,
    quantity_used,
    ROUND(cost, 2) AS cost,
    ROUND(cost / NULLIF(quantity_used, 0), 2) AS cost_per_unit,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_parts_used') }}
WHERE usage_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_routes.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    route_id,
    mission_id,
    origin_lat,
    origin_lon,
    dest_lat,
    dest_lon,
    ROUND(distance_km, 2) AS distance_km,
    ROUND(duration_min, 1) AS duration_min,
    waypoints,
    CASE
        WHEN distance_km > 0 AND duration_min > 0
        THEN ROUND(distance_km / (duration_min / 60), 1)
        ELSE NULL
    END AS avg_speed_kmh,
    CASE
        WHEN distance_km > 50 THEN 'LONG'
        WHEN distance_km > 20 THEN 'MEDIUM'
        ELSE 'SHORT'
    END AS route_category,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_routes') }}
WHERE route_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_route_segments.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    segment_id,
    route_id,
    segment_seq,
    start_lat,
    start_lon,
    end_lat,
    end_lon,
    ROUND(distance_km, 3) AS distance_km,
    ROUND(altitude_m, 1) AS altitude_m,
    ROUND(
        HAVERSINE(start_lat, start_lon, end_lat, end_lon),
        3
    ) AS straight_line_km,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_route_segments') }}
WHERE segment_id IS NOT NULL;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: silver_weather_data.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='view', schema='SILVER') }}

SELECT
    weather_id,
    station_id,
    recorded_at,
    ROUND(temp_c, 1) AS temp_c,
    ROUND(wind_speed_kmh, 1) AS wind_speed_kmh,
    ROUND(wind_direction_deg, 0) AS wind_direction_deg,
    ROUND(visibility_km, 1) AS visibility_km,
    ROUND(precipitation_mm, 1) AS precipitation_mm,
    UPPER(TRIM(conditions)) AS conditions,
    CASE
        WHEN wind_speed_kmh > 50 OR visibility_km < 1 OR precipitation_mm > 20 THEN 'SEVERE'
        WHEN wind_speed_kmh > 30 OR visibility_km < 3 OR precipitation_mm > 10 THEN 'MODERATE'
        WHEN wind_speed_kmh > 15 OR visibility_km < 5 OR precipitation_mm > 5 THEN 'MILD'
        ELSE 'CLEAR'
    END AS weather_severity,
    CASE
        WHEN wind_speed_kmh > 50 OR visibility_km < 1 THEN FALSE
        ELSE TRUE
    END AS is_flyable,
    _dbt_loaded_at
FROM {{ ref('stg_bronze_weather_data') }}
WHERE weather_id IS NOT NULL;


-- ============================================================================
-- SECTION C: GOLD AGGREGATION MODELS (Silver → Gold)
-- Business-level fact and dimension tables
-- Config: transient tables in GOLD schema
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- Model: dim_aircraft.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

WITH aircraft_missions AS (
    SELECT
        aircraft_id,
        COUNT(*) AS total_missions,
        SUM(CASE WHEN mission_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_missions,
        MAX(actual_departure) AS last_mission_at
    FROM {{ ref('silver_missions') }}
    GROUP BY aircraft_id
),

aircraft_maintenance AS (
    SELECT
        aircraft_id,
        COUNT(*) AS total_maintenance_events
    FROM {{ ref('silver_maintenance_logs') }}
    GROUP BY aircraft_id
)

SELECT
    a.aircraft_id,
    a.aircraft_model,
    a.serial_number,
    a.aircraft_status,
    a.depot_id,
    d.depot_name,
    a.total_flight_hours,
    a.last_maintenance_date,
    a.maintenance_status,
    a.usage_tier,
    COALESCE(am.total_missions, 0) AS total_missions,
    COALESCE(am.completed_missions, 0) AS completed_missions,
    COALESCE(mt.total_maintenance_events, 0) AS total_maintenance_events,
    am.last_mission_at
FROM {{ ref('silver_aircraft') }} a
LEFT JOIN {{ ref('silver_depots') }} d ON a.depot_id = d.depot_id
LEFT JOIN aircraft_missions am ON a.aircraft_id = am.aircraft_id
LEFT JOIN aircraft_maintenance mt ON a.aircraft_id = mt.aircraft_id;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: dim_customers.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
        MAX(created_at) AS last_order_at,
        AVG(delivery_time_min) AS avg_delivery_time_min
    FROM {{ ref('silver_orders') }}
    GROUP BY customer_id
),

customer_feedback AS (
    SELECT
        customer_id,
        AVG(rating) AS avg_rating,
        AVG(sentiment_score) AS avg_sentiment
    FROM {{ ref('silver_delivery_feedback') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone_clean AS phone,
    c.address,
    c.created_at AS customer_since,
    COALESCE(co.total_orders, 0) AS total_orders,
    COALESCE(co.delivered_orders, 0) AS delivered_orders,
    COALESCE(cf.avg_rating, 0) AS avg_rating,
    COALESCE(cf.avg_sentiment, 0) AS avg_sentiment,
    co.last_order_at,
    COALESCE(co.avg_delivery_time_min, 0) AS avg_delivery_time_min
FROM {{ ref('silver_customers') }} c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
LEFT JOIN customer_feedback cf ON c.customer_id = cf.customer_id;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: dim_delivery_zones.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

SELECT
    zone_id,
    zone_name,
    center_lat,
    center_lon,
    radius_km,
    zone_type,
    is_active
FROM {{ ref('silver_delivery_zones') }};

-- ────────────────────────────────────────────────────────────────────────────
-- Model: dim_depots.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

WITH depot_aircraft AS (
    SELECT
        depot_id,
        COUNT(*) AS aircraft_count,
        SUM(CASE WHEN aircraft_status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_aircraft
    FROM {{ ref('silver_aircraft') }}
    GROUP BY depot_id
),

depot_inventory AS (
    SELECT
        depot_id,
        SUM(inventory_value) AS total_inventory_value,
        SUM(CASE WHEN stock_status = 'REORDER_NEEDED' THEN 1 ELSE 0 END) AS parts_needing_reorder
    FROM {{ ref('silver_parts_inventory') }}
    GROUP BY depot_id
)

SELECT
    d.depot_id,
    d.depot_name,
    d.lat,
    d.lon,
    d.capacity,
    d.depot_size,
    COALESCE(da.aircraft_count, 0) AS aircraft_count,
    COALESCE(da.active_aircraft, 0) AS active_aircraft,
    COALESCE(di.total_inventory_value, 0) AS total_inventory_value,
    COALESCE(di.parts_needing_reorder, 0) AS parts_needing_reorder
FROM {{ ref('silver_depots') }} d
LEFT JOIN depot_aircraft da ON d.depot_id = da.depot_id
LEFT JOIN depot_inventory di ON d.depot_id = di.depot_id;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: fct_deliveries.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

SELECT
    o.order_id,
    o.customer_id,
    m.mission_id,
    m.aircraft_id,
    o.priority,
    o.order_status,
    m.mission_status,
    o.package_weight_kg,
    o.straight_line_distance_km,
    r.distance_km AS route_distance_km,
    r.duration_min AS planned_duration_min,
    r.avg_speed_kmh AS planned_avg_speed,
    r.route_category,
    o.created_at AS order_created_at,
    m.actual_departure,
    m.actual_arrival,
    o.delivered_at,
    o.delivery_time_min,
    m.actual_duration_min AS flight_duration_min,
    m.departure_delay_min,
    f.rating AS feedback_rating,
    f.sentiment_score,
    f.sentiment_label
FROM {{ ref('silver_orders') }} o
LEFT JOIN {{ ref('silver_missions') }} m ON o.order_id = m.order_id
LEFT JOIN {{ ref('silver_routes') }} r ON m.mission_id = r.mission_id
LEFT JOIN {{ ref('silver_delivery_feedback') }} f ON o.order_id = f.order_id;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: fct_flight_telemetry.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

SELECT
    aircraft_id,
    DATE_TRUNC('hour', recorded_at) AS hour_bucket,
    COUNT(*) AS telemetry_points,
    AVG(altitude_m) AS avg_altitude_m,
    MAX(altitude_m) AS max_altitude_m,
    AVG(speed_kmh) AS avg_speed_kmh,
    MAX(speed_kmh) AS max_speed_kmh,
    MIN(battery_pct) AS min_battery_pct,
    AVG(battery_pct) AS avg_battery_pct,
    SUM(CASE WHEN has_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
    SUM(CASE WHEN is_low_battery THEN 1 ELSE 0 END) AS low_battery_count,
    SUM(CASE WHEN is_overspeed THEN 1 ELSE 0 END) AS overspeed_count
FROM {{ ref('silver_flight_logs') }}
GROUP BY aircraft_id, DATE_TRUNC('hour', recorded_at);

-- ────────────────────────────────────────────────────────────────────────────
-- Model: fct_maintenance.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

WITH parts_agg AS (
    SELECT
        log_id,
        COUNT(*) AS parts_used_count,
        SUM(quantity_used) AS total_parts_quantity,
        SUM(cost) AS total_parts_cost
    FROM {{ ref('silver_parts_used') }}
    GROUP BY log_id
)

SELECT
    ml.log_id,
    ml.aircraft_id,
    ml.maintenance_type,
    ml.description,
    ml.technician,
    ml.started_at,
    ml.completed_at,
    ml.maintenance_status,
    ml.duration_min,
    ml.urgency_level,
    COALESCE(p.parts_used_count, 0) AS parts_used_count,
    COALESCE(p.total_parts_quantity, 0) AS total_parts_quantity,
    COALESCE(p.total_parts_cost, 0) AS total_parts_cost
FROM {{ ref('silver_maintenance_logs') }} ml
LEFT JOIN parts_agg p ON ml.log_id = p.log_id;

-- ────────────────────────────────────────────────────────────────────────────
-- Model: kpi_delivery_performance.sql
-- ────────────────────────────────────────────────────────────────────────────
-- {{ config(materialized='table', transient=true, schema='GOLD') }}

SELECT
    DATE_TRUNC('day', order_created_at) AS report_date,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_orders,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders,
    ROUND(
        SUM(CASE WHEN order_status = 'DELIVERED' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0),
        1
    ) AS delivery_success_rate_pct,
    AVG(delivery_time_min) AS avg_delivery_time_min,
    MEDIAN(delivery_time_min) AS median_delivery_time_min,
    AVG(route_distance_km) AS avg_route_distance_km,
    AVG(feedback_rating) AS avg_customer_rating,
    AVG(sentiment_score) AS avg_sentiment_score,
    COUNT(DISTINCT aircraft_id) AS active_aircraft,
    AVG(departure_delay_min) AS avg_departure_delay_min
FROM {{ ref('fct_deliveries') }}
GROUP BY DATE_TRUNC('day', order_created_at);
