-- 4.0 — Context Setup--

sql
USE ROLE DRONE_ADMIN;
USE DATABASE DRONE_DELIVERY_DB;
USE WAREHOUSE DRONE_INGEST_WH;



-- 4.1 — Foundation Tables (No Dependencies) --

DEPOT.DEPOTS (5 rows)
sql
INSERT INTO DEPOT.DEPOTS (DEPOT_ID, DEPOT_NAME, LAT, LON, CAPACITY)
VALUES
    ('DEP-001', 'Austin Hub',         30.2672, -97.7431,  15),
    ('DEP-002', 'San Francisco Hub',  37.7749, -122.4194, 12),
    ('DEP-003', 'Denver Hub',         39.7392, -104.9903, 10),
    ('DEP-004', 'Miami Hub',          25.7617, -80.1918,  12),
    ('DEP-005', 'Seattle Hub',        47.6062, -122.3321, 10);
DELIVERY.CUSTOMERS (1,000 rows)
sql
INSERT INTO DELIVERY.CUSTOMERS (CUSTOMER_ID, NAME, EMAIL, PHONE, ADDRESS, CREATED_AT)
SELECT
    UUID_STRING(),
    ARRAY_CONSTRUCT('James','Maria','Robert','Linda','David','Sarah','Michael','Jennifer','William','Jessica',
                    'Carlos','Emily','Daniel','Ashley','Thomas','Amanda','Alex','Sophia','Ryan','Olivia')
        [MOD(SEQ4(), 20)]::VARCHAR || ' ' ||
    ARRAY_CONSTRUCT('Smith','Garcia','Johnson','Williams','Brown','Jones','Miller','Davis','Martinez','Wilson',
                    'Anderson','Taylor','Thomas','Moore','Jackson','Martin','Lee','Harris','Clark','Lewis')
        [MOD(FLOOR(SEQ4()/20), 20)]::VARCHAR,
    LOWER(
        ARRAY_CONSTRUCT('james','maria','robert','linda','david','sarah','michael','jennifer','william','jessica',
                        'carlos','emily','daniel','ashley','thomas','amanda','alex','sophia','ryan','olivia')
            [MOD(SEQ4(), 20)]::VARCHAR || '.' ||
        ARRAY_CONSTRUCT('smith','garcia','johnson','williams','brown','jones','miller','davis','martinez','wilson',
                        'anderson','taylor','thomas','moore','jackson','martin','lee','harris','clark','lewis')
            [MOD(FLOOR(SEQ4()/20), 20)]::VARCHAR
    ) || SEQ4()::VARCHAR || '@example.com',
    '+1-' || LPAD((UNIFORM(200, 999, RANDOM()))::VARCHAR, 3, '0') || '-' ||
             LPAD((UNIFORM(100, 999, RANDOM()))::VARCHAR, 3, '0') || '-' ||
             LPAD((UNIFORM(1000, 9999, RANDOM()))::VARCHAR, 4, '0'),
    (UNIFORM(100, 9999, RANDOM()))::VARCHAR || ' ' ||
    ARRAY_CONSTRUCT('Oak','Maple','Cedar','Pine','Elm','Birch','Walnut','Spruce','Willow','Ash')
        [MOD(SEQ4(), 10)]::VARCHAR || ' ' ||
    ARRAY_CONSTRUCT('St','Ave','Blvd','Dr','Ln','Way','Ct','Pl','Rd','Cir')
        [MOD(FLOOR(SEQ4()/10), 10)]::VARCHAR || ', ' ||
    ARRAY_CONSTRUCT('Austin TX','San Francisco CA','Denver CO','Miami FL','Seattle WA')
        [MOD(SEQ4(), 5)]::VARCHAR,
    DATEADD('day', -UNIFORM(1, 365, RANDOM()), '2025-09-19'::TIMESTAMP_NTZ)
FROM TABLE(GENERATOR(ROWCOUNT => 1000));
GEO.DELIVERY_ZONES (10 rows)
sql
INSERT INTO GEO.DELIVERY_ZONES (ZONE_ID, ZONE_NAME, CENTER_LAT, CENTER_LON, RADIUS_KM, ZONE_TYPE, IS_ACTIVE)
VALUES
    ('DZ-001', 'Downtown Austin',       30.2672, -97.7431,  8.0, 'COMMERCIAL',  TRUE),
    ('DZ-002', 'South Austin',          30.2100, -97.7700, 10.0, 'RESIDENTIAL', TRUE),
    ('DZ-003', 'SF Financial District', 37.7900, -122.4000, 5.0, 'COMMERCIAL',  TRUE),
    ('DZ-004', 'SF Mission Bay',        37.7700, -122.3900, 6.0, 'MIXED',       TRUE),
    ('DZ-005', 'Denver Downtown',       39.7400, -104.9900, 7.0, 'COMMERCIAL',  TRUE),
    ('DZ-006', 'Denver Tech Center',    39.6500, -104.8900, 8.0, 'COMMERCIAL',  TRUE),
    ('DZ-007', 'Miami Beach',           25.7900, -80.1300,  6.0, 'RESIDENTIAL', TRUE),
    ('DZ-008', 'Brickell Miami',        25.7600, -80.1900,  5.0, 'COMMERCIAL',  TRUE),
    ('DZ-009', 'Seattle Downtown',      47.6100, -122.3400, 6.0, 'COMMERCIAL',  TRUE),
    ('DZ-010', 'Capitol Hill Seattle',  47.6200, -122.3200, 7.0, 'RESIDENTIAL', TRUE);
GEO.NO_FLY_ZONES (5 rows)
sql
INSERT INTO GEO.NO_FLY_ZONES (NFZ_ID, ZONE_NAME, CENTER_LAT, CENTER_LON, RADIUS_KM, RESTRICTION_TYPE, EFFECTIVE_FROM, EFFECTIVE_TO)
VALUES
    ('NFZ-001', 'Austin-Bergstrom Airport',  30.1975, -97.6664,  5.0, 'PERMANENT',  '2020-01-01'::TIMESTAMP_NTZ, NULL),
    ('NFZ-002', 'SFO International Airport', 37.6213, -122.3790, 5.0, 'PERMANENT',  '2020-01-01'::TIMESTAMP_NTZ, NULL),
    ('NFZ-003', 'Denver International',      39.8561, -104.6737, 8.0, 'PERMANENT',  '2020-01-01'::TIMESTAMP_NTZ, NULL),
    ('NFZ-004', 'Miami Military Base',       25.7500, -80.3800,  3.0, 'PERMANENT',  '2020-01-01'::TIMESTAMP_NTZ, NULL),
    ('NFZ-005', 'Seattle Stadium District',  47.5914, -122.3316, 1.5, 'TEMPORARY',  '2025-06-01'::TIMESTAMP_NTZ, '2025-12-31'::TIMESTAMP_NTZ);




-- 4.2 — Dependent on Depots --

DEPOT.AIRCRAFT (50 rows)
sql
INSERT INTO DEPOT.AIRCRAFT (AIRCRAFT_ID, MODEL, SERIAL_NUMBER, STATUS, DEPOT_ID, TOTAL_FLIGHT_HOURS, LAST_MAINTENANCE_DATE)
SELECT
    'AC-' || LPAD(SEQ4()::VARCHAR, 3, '0'),
    ARRAY_CONSTRUCT('DJI FlyCart 30','Wingcopter 198','Zipline P2','Matternet M2','EHang 216')
        [MOD(SEQ4(), 5)]::VARCHAR,
    'SN-' || LPAD((10000 + SEQ4())::VARCHAR, 6, '0'),
    CASE
        WHEN UNIFORM(1, 100, RANDOM()) <= 70 THEN 'ACTIVE'
        WHEN UNIFORM(1, 100, RANDOM()) <= 85 THEN 'IN_MAINTENANCE'
        ELSE 'GROUNDED'
    END,
    'DEP-' || LPAD((MOD(SEQ4(), 5) + 1)::VARCHAR, 3, '0'),
    ROUND(UNIFORM(50, 2000, RANDOM())::FLOAT, 1),
    DATEADD('day', -UNIFORM(1, 90, RANDOM()), CURRENT_DATE())
FROM TABLE(GENERATOR(ROWCOUNT => 50));
DEPOT.PARTS_INVENTORY (200 rows)
sql
INSERT INTO DEPOT.PARTS_INVENTORY (PART_ID, PART_NAME, CATEGORY, QUANTITY_ON_HAND, REORDER_LEVEL, UNIT_COST, DEPOT_ID)
SELECT
    'PRT-' || LPAD(SEQ4()::VARCHAR, 4, '0'),
    ARRAY_CONSTRUCT(
        'Propeller Blade A','Propeller Blade B','Brushless Motor 2205','Brushless Motor 2212',
        'LiPo Battery 6S','LiPo Battery 4S','ESC 30A','ESC 40A','Flight Controller FC7',
        'GPS Module V3','Compass Sensor','Barometer BMP390','IMU MPU6050','Camera Module 4K',
        'Landing Gear Set','Carbon Frame Arm','LED Navigation Light','Payload Release Mechanism',
        'Antenna 5.8GHz','FPV Transmitter','Receiver Module RX8','Servo Motor SG90',
        'Vibration Dampener','Heat Sink Assembly','Cooling Fan 40mm','Wiring Harness Kit',
        'USB-C Charge Port','Power Distribution Board','Voltage Regulator 5V','Propeller Guard',
        'Battery Connector XT60','Motor Mount Bracket','Gimbal Stabilizer','Parachute Recovery System',
        'Ultrasonic Sensor','LIDAR Module','Obstacle Avoidance Sensor','Thermal Camera Module',
        'Firmware Chip v3.2','Signal Amplifier'
    )[MOD(SEQ4(), 40)]::VARCHAR,
    ARRAY_CONSTRUCT('PROPULSION','POWER','ELECTRONICS','SENSORS','STRUCTURAL','SAFETY','COMMUNICATION','PAYLOAD')
        [MOD(FLOOR(SEQ4()/25), 8)]::VARCHAR,
    UNIFORM(0, 50, RANDOM()),
    UNIFORM(3, 15, RANDOM()),
    ROUND(UNIFORM(5, 500, RANDOM())::FLOAT + UNIFORM(0, 99, RANDOM())::FLOAT / 100, 2),
    'DEP-' || LPAD((MOD(SEQ4(), 5) + 1)::VARCHAR, 3, '0')
FROM TABLE(GENERATOR(ROWCOUNT => 200));




-- 4.3 — Orders (Depends on Customers) --

DELIVERY.ORDERS (5,620 rows)
sql
INSERT INTO DELIVERY.ORDERS (ORDER_ID, CUSTOMER_ID, PICKUP_LAT, PICKUP_LON, DROPOFF_LAT, DROPOFF_LON, PACKAGE_WEIGHT_KG, PRIORITY, STATUS, CREATED_AT, DELIVERED_AT)
WITH cust AS (
    SELECT CUSTOMER_ID, ROW_NUMBER() OVER (ORDER BY CUSTOMER_ID) - 1 AS rn,
           COUNT(*) OVER () AS total
    FROM DELIVERY.CUSTOMERS
),
depot_coords AS (
    SELECT * FROM VALUES
        (0, 30.2672, -97.7431),  (1, 37.7749, -122.4194),
        (2, 39.7392, -104.9903), (3, 25.7617, -80.1918),
        (4, 47.6062, -122.3321)
    AS t(idx, lat, lon)
)
SELECT
    UUID_STRING(),
    c.CUSTOMER_ID,
    d.lat + (UNIFORM(-50, 50, RANDOM())::FLOAT / 1000),
    d.lon + (UNIFORM(-50, 50, RANDOM())::FLOAT / 1000),
    d.lat + (UNIFORM(-80, 80, RANDOM())::FLOAT / 1000),
    d.lon + (UNIFORM(-80, 80, RANDOM())::FLOAT / 1000),
    ROUND(UNIFORM(1, 150, RANDOM())::FLOAT / 10, 1),
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 70 THEN 'STANDARD'
         WHEN UNIFORM(1, 100, RANDOM()) <= 90 THEN 'EXPRESS' ELSE 'CRITICAL' END,
    CASE UNIFORM(1, 100, RANDOM())
        WHEN  1 THEN 'PENDING'    WHEN  2 THEN 'PENDING'    WHEN  3 THEN 'PENDING'
        WHEN  4 THEN 'CANCELLED'  WHEN  5 THEN 'CANCELLED'
        WHEN  6 THEN 'FAILED'
        ELSE 'DELIVERED' END,
    DATEADD('minute', -UNIFORM(1, 262800, RANDOM()), '2025-09-19'::TIMESTAMP_NTZ),
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 93
         THEN DATEADD('minute',
              -UNIFORM(1, 262800, RANDOM()) + UNIFORM(15, 120, RANDOM()),
              '2025-09-19'::TIMESTAMP_NTZ)
         ELSE NULL END
FROM TABLE(GENERATOR(ROWCOUNT => 5000)) g
JOIN cust c ON MOD(SEQ4(), c.total) = c.rn
JOIN depot_coords d ON MOD(SEQ4(), 5) = d.idx;

4.4 — Missions (Depends on Orders + Aircraft)
API.MISSIONS (5,620 rows)
sql
INSERT INTO API.MISSIONS (MISSION_ID, ORDER_ID, AIRCRAFT_ID, STATUS, PLANNED_DEPARTURE, ACTUAL_DEPARTURE, PLANNED_ARRIVAL, ACTUAL_ARRIVAL, NOTES)
WITH aircraft_list AS (
    SELECT AIRCRAFT_ID, ROW_NUMBER() OVER (ORDER BY AIRCRAFT_ID) - 1 AS rn,
           COUNT(*) OVER () AS total
    FROM DEPOT.AIRCRAFT
),
notes_arr AS (
    SELECT ARRAY_CONSTRUCT(
        'Standard delivery route, clear skies expected',
        'Express priority delivery, minimize transit time',
        'Rerouted due to temporary restricted airspace near downtown area',
        'Heavy package load, monitor battery consumption closely',
        'Weather conditions marginal with light rain, proceed with caution',
        'Customer requested specific delivery window between 2-4 PM',
        'Multiple delivery zone crossing required, watch airspace boundaries',
        'High wind advisory in effect, maintain low altitude approach',
        'Night delivery operation, ensure all navigation lights functional',
        'VIP customer order, priority handling required',
        'Replacement delivery after previous failed attempt',
        'Medical supply delivery, time-critical mission',
        'Fragile package contents, reduce speed during descent',
        'Test flight for newly serviced aircraft, monitor all telemetry',
        'Batch delivery route with two stops planned'
    ) AS notes
)
SELECT
    UUID_STRING(),
    o.ORDER_ID,
    a.AIRCRAFT_ID,
    CASE o.STATUS
        WHEN 'DELIVERED' THEN 'COMPLETED'
        WHEN 'CANCELLED' THEN 'ABORTED'
        WHEN 'FAILED' THEN 'FAILED'
        WHEN 'PENDING' THEN
            CASE WHEN UNIFORM(1,2,RANDOM())=1 THEN 'ASSIGNED' ELSE 'IN_FLIGHT' END
        ELSE 'COMPLETED'
    END,
    DATEADD('minute', -UNIFORM(5, 30, RANDOM()), o.CREATED_AT),
    DATEADD('minute', UNIFORM(0, 15, RANDOM()), o.CREATED_AT),
    DATEADD('minute', UNIFORM(20, 60, RANDOM()), o.CREATED_AT),
    CASE WHEN o.DELIVERED_AT IS NOT NULL THEN o.DELIVERED_AT ELSE NULL END,
    n.notes[MOD(ABS(HASH(o.ORDER_ID)), 15)]::VARCHAR
FROM DELIVERY.ORDERS o
JOIN aircraft_list a ON MOD(ABS(HASH(o.ORDER_ID)), a.total) = a.rn
CROSS JOIN notes_arr n;




-- 4.5 — Delivery Feedback (Depends on Orders + Customers)-- 

DELIVERY.DELIVERY_FEEDBACK (5,261 rows)
sql
INSERT INTO DELIVERY.DELIVERY_FEEDBACK (FEEDBACK_ID, ORDER_ID, CUSTOMER_ID, RATING, COMMENTS, FEEDBACK_CHANNEL, CREATED_AT)
WITH feedback_text AS (
    SELECT ARRAY_CONSTRUCT(
        'Delivery was incredibly fast! Package arrived in perfect condition. Will definitely use again.',
        'The drone was extremely noisy and scared my dog. Package was fine but the experience was unpleasant.',
        'Package arrived damaged with a dent on one corner. Very disappointed with the handling.',
        'Excellent service! Delivered 10 minutes ahead of schedule. Love the tracking updates.',
        'Waited over an hour past the estimated delivery time. Not acceptable for express shipping.',
        'Smooth and quiet delivery. The drone landed perfectly on my porch. Great technology!',
        'Package was left in the rain because the drone dropped it in the wrong spot. Contents got wet.',
        'Amazing speed of delivery. Ordered at noon, received by 12:30. The future is here!',
        'Average experience. Delivery was on time but no special care for fragile items.',
        'The drone hovered for 5 minutes before landing. Wasted battery and made neighbors nervous.',
        'Perfect delivery every single time. This is my 10th order and never had an issue.',
        'Terrible experience. Drone crashed into my fence and damaged it. Need compensation.',
        'Good service overall. Minor delay due to weather but communication was excellent.',
        'The package was not secured properly and items inside shifted during flight.',
        'Absolutely love this service! Fast, reliable, and eco-friendly. Five stars!',
        'Delivery was fine but the app notifications were delayed. Improve the tracking system.',
        'Received someone elses package. Very frustrating. Customer support was helpful though.',
        'Quick and efficient. The drone delivery beats traditional shipping by miles.',
        'Not impressed. The delivery window was too wide and I had to wait around all afternoon.',
        'Outstanding service for medical supplies. Fast and careful handling. Literally a lifesaver.'
    ) AS comments
)
SELECT
    UUID_STRING(),
    o.ORDER_ID,
    o.CUSTOMER_ID,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 15 THEN UNIFORM(1, 2, RANDOM())
        WHEN UNIFORM(1,100,RANDOM()) <= 35 THEN 3
        WHEN UNIFORM(1,100,RANDOM()) <= 75 THEN 4
        ELSE 5
    END,
    f.comments[MOD(ABS(HASH(o.ORDER_ID)), 20)]::VARCHAR,
    ARRAY_CONSTRUCT('APP','EMAIL','SMS','WEB','PHONE')[MOD(ABS(HASH(o.CUSTOMER_ID)), 5)]::VARCHAR,
    DATEADD('minute', UNIFORM(30, 1440, RANDOM()), COALESCE(o.DELIVERED_AT, o.CREATED_AT))
FROM DELIVERY.ORDERS o
CROSS JOIN feedback_text f
WHERE o.STATUS = 'DELIVERED';




-- 4.6 — Mission Events (Depends on Missions)--

API.MISSION_EVENTS (44,960 rows)
sql
INSERT INTO API.MISSION_EVENTS (EVENT_ID, MISSION_ID, EVENT_TYPE, EVENT_TIMESTAMP, DESCRIPTION, LAT, LON)
WITH event_templates AS (
    SELECT column1 AS event_seq, column2 AS event_type, column3 AS offset_pct, column4 AS description
    FROM VALUES
        (1, 'PREFLIGHT_CHECK',    0.00, 'Pre-flight systems check completed. All sensors nominal. Battery fully charged.'),
        (2, 'LAUNCHED',           0.05, 'Aircraft launched from depot pad. Vertical ascent to cruising altitude initiated.'),
        (3, 'CRUISING',          0.15, 'Reached cruising altitude. En route to pickup location at optimal speed.'),
        (4, 'APPROACHING_PICKUP', 0.30, 'Approaching pickup zone. Reducing altitude and speed for landing sequence.'),
        (5, 'PACKAGE_LOADED',     0.35, 'Package secured in payload bay. Weight verified. Resuming flight to delivery.'),
        (6, 'EN_ROUTE_DELIVERY',  0.50, 'En route to delivery point. Maintaining stable altitude and heading.'),
        (7, 'PACKAGE_DELIVERED',  0.85, 'Package successfully delivered at drop-off location. Customer notified.'),
        (8, 'RETURNED_TO_DEPOT',  1.00, 'Aircraft returned to home depot. Landing sequence completed successfully.')
)
SELECT
    UUID_STRING(),
    m.MISSION_ID,
    e.event_type,
    DATEADD('second',
        (DATEDIFF('second', m.ACTUAL_DEPARTURE,
            COALESCE(m.ACTUAL_ARRIVAL, DATEADD('minute', 45, m.ACTUAL_DEPARTURE))) * e.offset_pct)::INT,
        m.ACTUAL_DEPARTURE),
    e.description,
    o.PICKUP_LAT + (o.DROPOFF_LAT - o.PICKUP_LAT) * e.offset_pct + (UNIFORM(-10,10,RANDOM())::FLOAT/10000),
    o.PICKUP_LON + (o.DROPOFF_LON - o.PICKUP_LON) * e.offset_pct + (UNIFORM(-10,10,RANDOM())::FLOAT/10000)
FROM API.MISSIONS m
JOIN DELIVERY.ORDERS o ON m.ORDER_ID = o.ORDER_ID
CROSS JOIN event_templates e
WHERE m.ACTUAL_DEPARTURE IS NOT NULL;




-- 4.7 — Routes + Route Segments (Depends on Missions) --

OSRM.ROUTES (5,620 rows)
sql
INSERT INTO OSRM.ROUTES (ROUTE_ID, MISSION_ID, ORIGIN_LAT, ORIGIN_LON, DEST_LAT, DEST_LON, DISTANCE_KM, DURATION_MIN, WAYPOINTS)
SELECT
    UUID_STRING(),
    m.MISSION_ID,
    o.PICKUP_LAT,
    o.PICKUP_LON,
    o.DROPOFF_LAT,
    o.DROPOFF_LON,
    ROUND(SQRT(POWER((o.DROPOFF_LAT - o.PICKUP_LAT)*111, 2) + POWER((o.DROPOFF_LON - o.PICKUP_LON)*85, 2))
          * UNIFORM(110, 140, RANDOM())::FLOAT/100, 2),
    ROUND(UNIFORM(8, 55, RANDOM())::FLOAT + UNIFORM(0, 99, RANDOM())::FLOAT/100, 1),
    PARSE_JSON('[' ||
        '{"lat":' || (o.PICKUP_LAT + (o.DROPOFF_LAT-o.PICKUP_LAT)*0.25 + UNIFORM(-5,5,RANDOM())::FLOAT/10000)::VARCHAR ||
        ',"lon":' || (o.PICKUP_LON + (o.DROPOFF_LON-o.PICKUP_LON)*0.25 + UNIFORM(-5,5,RANDOM())::FLOAT/10000)::VARCHAR || '},' ||
        '{"lat":' || (o.PICKUP_LAT + (o.DROPOFF_LAT-o.PICKUP_LAT)*0.50 + UNIFORM(-5,5,RANDOM())::FLOAT/10000)::VARCHAR ||
        ',"lon":' || (o.PICKUP_LON + (o.DROPOFF_LON-o.PICKUP_LON)*0.50 + UNIFORM(-5,5,RANDOM())::FLOAT/10000)::VARCHAR || '},' ||
        '{"lat":' || (o.PICKUP_LAT + (o.DROPOFF_LAT-o.PICKUP_LAT)*0.75 + UNIFORM(-5,5,RANDOM())::FLOAT/10000)::VARCHAR ||
        ',"lon":' || (o.PICKUP_LON + (o.DROPOFF_LON-o.PICKUP_LON)*0.75 + UNIFORM(-5,5,RANDOM())::FLOAT/10000)::VARCHAR || '}]')
FROM API.MISSIONS m
JOIN DELIVERY.ORDERS o ON m.ORDER_ID = o.ORDER_ID;
OSRM.ROUTE_SEGMENTS (22,480 rows)
sql
INSERT INTO OSRM.ROUTE_SEGMENTS (SEGMENT_ID, ROUTE_ID, SEGMENT_SEQ, START_LAT, START_LON, END_LAT, END_LON, DISTANCE_KM, ALTITUDE_M)
WITH seg_seq AS (
    SELECT column1 AS seg_num, column2 AS start_pct, column3 AS end_pct
    FROM VALUES (1, 0.00, 0.25), (2, 0.25, 0.50), (3, 0.50, 0.75), (4, 0.75, 1.00)
)
SELECT
    UUID_STRING(),
    r.ROUTE_ID,
    s.seg_num,
    r.ORIGIN_LAT + (r.DEST_LAT - r.ORIGIN_LAT) * s.start_pct,
    r.ORIGIN_LON + (r.DEST_LON - r.ORIGIN_LON) * s.start_pct,
    r.ORIGIN_LAT + (r.DEST_LAT - r.ORIGIN_LAT) * s.end_pct,
    r.ORIGIN_LON + (r.DEST_LON - r.ORIGIN_LON) * s.end_pct,
    ROUND(r.DISTANCE_KM / 4 * (UNIFORM(80, 120, RANDOM())::FLOAT / 100), 2),
    ROUND(UNIFORM(30, 120, RANDOM())::FLOAT + UNIFORM(0, 9, RANDOM())::FLOAT / 10, 1)
FROM OSRM.ROUTES r
CROSS JOIN seg_seq s;




-- 4.8 — Flight Logs (Depends on Missions + Aircraft) --

FLV6.FLIGHT_LOGS (562,000 rows — 2 batches)
sql
-- Batch 1: pings 0-49 (281,000 rows)
INSERT INTO FLV6.FLIGHT_LOGS (FLIGHT_LOG_ID, AIRCRAFT_ID, RECORDED_AT, LAT, LON, ALTITUDE_M, SPEED_KMH, BATTERY_PCT, HEADING_DEG)
WITH ping_seq AS (
    SELECT SEQ4() AS ping_num FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
mission_data AS (
    SELECT m.MISSION_ID, m.AIRCRAFT_ID, m.ACTUAL_DEPARTURE, m.ACTUAL_ARRIVAL,
           o.PICKUP_LAT, o.PICKUP_LON, o.DROPOFF_LAT, o.DROPOFF_LON
    FROM API.MISSIONS m
    JOIN DELIVERY.ORDERS o ON m.ORDER_ID = o.ORDER_ID
    WHERE m.ACTUAL_DEPARTURE IS NOT NULL
)
SELECT
    UUID_STRING(),
    md.AIRCRAFT_ID,
    DATEADD('second', p.ping_num * 30, md.ACTUAL_DEPARTURE),
    md.PICKUP_LAT + (md.DROPOFF_LAT - md.PICKUP_LAT) * (p.ping_num / 100.0)
        + (UNIFORM(-20, 20, RANDOM())::FLOAT / 100000),
    md.PICKUP_LON + (md.DROPOFF_LON - md.PICKUP_LON) * (p.ping_num / 100.0)
        + (UNIFORM(-20, 20, RANDOM())::FLOAT / 100000),
    CASE
        WHEN p.ping_num < 5 THEN UNIFORM(5, 50, RANDOM())::FLOAT
        WHEN p.ping_num > 45 THEN UNIFORM(5, 50, RANDOM())::FLOAT
        ELSE UNIFORM(50, 120, RANDOM())::FLOAT
    END,
    CASE
        WHEN p.ping_num < 5 THEN UNIFORM(10, 40, RANDOM())::FLOAT
        WHEN p.ping_num > 45 THEN UNIFORM(10, 40, RANDOM())::FLOAT
        ELSE UNIFORM(40, 80, RANDOM())::FLOAT
    END,
    GREATEST(5.0, 100.0 - (p.ping_num * UNIFORM(5, 15, RANDOM())::FLOAT / 10.0)),
    UNIFORM(0, 359, RANDOM())::FLOAT
FROM mission_data md
CROSS JOIN ping_seq p;

-- Batch 2: pings 50-99 (281,000 rows)
INSERT INTO FLV6.FLIGHT_LOGS (FLIGHT_LOG_ID, AIRCRAFT_ID, RECORDED_AT, LAT, LON, ALTITUDE_M, SPEED_KMH, BATTERY_PCT, HEADING_DEG)
WITH ping_seq AS (
    SELECT SEQ4() + 50 AS ping_num FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
mission_data AS (
    SELECT m.MISSION_ID, m.AIRCRAFT_ID, m.ACTUAL_DEPARTURE, m.ACTUAL_ARRIVAL,
           o.PICKUP_LAT, o.PICKUP_LON, o.DROPOFF_LAT, o.DROPOFF_LON
    FROM API.MISSIONS m
    JOIN DELIVERY.ORDERS o ON m.ORDER_ID = o.ORDER_ID
    WHERE m.ACTUAL_DEPARTURE IS NOT NULL
)
SELECT
    UUID_STRING(),
    md.AIRCRAFT_ID,
    DATEADD('second', p.ping_num * 30, md.ACTUAL_DEPARTURE),
    md.PICKUP_LAT + (md.DROPOFF_LAT - md.PICKUP_LAT) * (p.ping_num / 100.0)
        + (UNIFORM(-20, 20, RANDOM())::FLOAT / 100000),
    md.PICKUP_LON + (md.DROPOFF_LON - md.PICKUP_LON) * (p.ping_num / 100.0)
        + (UNIFORM(-20, 20, RANDOM())::FLOAT / 100000),
    CASE
        WHEN p.ping_num > 90 THEN UNIFORM(5, 40, RANDOM())::FLOAT
        ELSE UNIFORM(50, 120, RANDOM())::FLOAT
    END,
    CASE
        WHEN p.ping_num > 90 THEN UNIFORM(10, 35, RANDOM())::FLOAT
        ELSE UNIFORM(40, 80, RANDOM())::FLOAT
    END,
    GREATEST(5.0, 100.0 - (p.ping_num * UNIFORM(5, 15, RANDOM())::FLOAT / 10.0)),
    UNIFORM(0, 359, RANDOM())::FLOAT
FROM mission_data md
CROSS JOIN ping_seq p;




-- 4.9 — Weather Data (Time-Aligned with Flights) --

FLV6.WEATHER_DATA (4,500 rows)
sql
INSERT INTO FLV6.WEATHER_DATA (WEATHER_ID, STATION_ID, RECORDED_AT, TEMP_C, WIND_SPEED_KMH, WIND_DIRECTION_DEG, VISIBILITY_KM, PRECIPITATION_MM, CONDITIONS)
WITH hours AS (
    SELECT DATEADD('hour', -SEQ4(), '2025-09-19'::TIMESTAMP_NTZ) AS hour_ts
    FROM TABLE(GENERATOR(ROWCOUNT => 900))
),
stations AS (
    SELECT column1 AS station_id, column2 AS base_temp, column3 AS city
    FROM VALUES
        ('WS-ATX', 28.0, 'Austin'), ('WS-SFO', 18.0, 'SanFrancisco'),
        ('WS-DEN', 15.0, 'Denver'),  ('WS-MIA', 30.0, 'Miami'),
        ('WS-SEA', 12.0, 'Seattle')
)
SELECT
    UUID_STRING(),
    s.station_id,
    h.hour_ts,
    ROUND(s.base_temp + UNIFORM(-10, 10, RANDOM())::FLOAT +
          5 * SIN(2 * 3.14159 * HOUR(h.hour_ts) / 24.0), 1),
    ROUND(UNIFORM(0, 45, RANDOM())::FLOAT, 1),
    UNIFORM(0, 359, RANDOM())::FLOAT,
    ROUND(GREATEST(0.5, UNIFORM(2, 20, RANDOM())::FLOAT), 1),
    CASE WHEN UNIFORM(1, 100, RANDOM()) <= 25
         THEN ROUND(UNIFORM(0, 15, RANDOM())::FLOAT, 1) ELSE 0.0 END,
    ARRAY_CONSTRUCT('CLEAR','PARTLY_CLOUDY','CLOUDY','LIGHT_RAIN','HEAVY_RAIN',
                    'FOG','THUNDERSTORM','SNOW','WINDY','HAZE')
        [CASE
            WHEN UNIFORM(1,100,RANDOM()) <= 40 THEN 0
            WHEN UNIFORM(1,100,RANDOM()) <= 60 THEN 1
            WHEN UNIFORM(1,100,RANDOM()) <= 75 THEN 2
            WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN 3
            WHEN UNIFORM(1,100,RANDOM()) <= 90 THEN 4
            WHEN UNIFORM(1,100,RANDOM()) <= 93 THEN 5
            WHEN UNIFORM(1,100,RANDOM()) <= 96 THEN 6
            WHEN UNIFORM(1,100,RANDOM()) <= 98 THEN 7
            WHEN UNIFORM(1,100,RANDOM()) <= 99 THEN 8
            ELSE 9
        END]::VARCHAR
FROM hours h
CROSS JOIN stations s;




-- 4.10 — Maintenance + Parts Used --

RECORDS.MAINTENANCE_LOGS (500 rows)
sql
INSERT INTO RECORDS.MAINTENANCE_LOGS (LOG_ID, AIRCRAFT_ID, MAINTENANCE_TYPE, DESCRIPTION, TECHNICIAN, STARTED_AT, COMPLETED_AT, STATUS)
WITH aircraft_arr AS (
    SELECT ARRAY_AGG(AIRCRAFT_ID) WITHIN GROUP (ORDER BY AIRCRAFT_ID) AS ids,
           COUNT(*) AS total
    FROM DEPOT.AIRCRAFT
)
SELECT
    'ML-' || LPAD(SEQ4()::VARCHAR, 4, '0'),
    a.ids[MOD(SEQ4(), a.total)]::VARCHAR,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 40 THEN 'PREVENTIVE'
        WHEN UNIFORM(1,100,RANDOM()) <= 80 THEN 'CORRECTIVE'
        ELSE 'EMERGENCY'
    END,
    ARRAY_CONSTRUCT(
        'Replaced left rear motor due to abnormal bearing noise during pre-flight check. Recalibrated IMU and compass after motor swap. Root cause: bearing wear from extended high-RPM operations.',
        'Routine 100-hour inspection completed. All systems nominal. Lubricated landing gear joints, checked propeller balance, verified firmware version. No issues found.',
        'Battery cells degraded below 80 percent capacity after 200 charge cycles. Full battery pack replacement performed. Old battery sent for recycling. Root cause: normal cell degradation.',
        'Propeller blade cracked during hard landing in high winds. Emergency replacement of all four propellers as precaution. Root cause: excessive stress from crosswind landing.',
        'Firmware update to v3.2 applied. Compass recalibration completed. GPS module antenna connector was loose causing intermittent signal loss. Tightened and secured with thread lock.',
        'ESC failure on motor 3 caused mid-flight power loss. Emergency landing activated successfully. Replaced ESC and motor. Root cause: voltage spike from damaged power distribution board.',
        'Payload release mechanism jammed during delivery attempt. Disassembled servo assembly, found corroded contacts. Cleaned and replaced servo motor. Root cause: moisture ingress.',
        'Scheduled quarterly maintenance. Replaced worn landing gear dampeners, updated obstacle avoidance sensor firmware, cleaned camera lens. All flight tests passed.',
        'GPS module intermittent failure traced to faulty antenna cable. Replaced cable and GPS module as precaution. Full navigation test completed. Root cause: cable chafing against frame.',
        'Structural inspection revealed hairline crack in carbon fiber frame arm number 2. Arm replaced and stress tested. Root cause: accumulated fatigue from repeated takeoff vibrations.',
        'LiDAR sensor returning inaccurate readings below 5 meters altitude. Sensor recalibrated and lens cleaned. Issue persisted so full sensor replacement performed. Root cause: lens coating damage.',
        'Complete overhaul after 500 flight hours. All motors replaced, new propellers installed, battery system refreshed, firmware updated to latest version. Aircraft returned to service.',
        'Cooling fan failure caused flight controller overheating warning. Fan replaced and thermal paste reapplied to heat sink. Root cause: fan motor bearing seized due to dust accumulation.',
        'Navigation light malfunction detected during night operations check. LED driver board replaced. Root cause: water damage to circuit board from inadequate weatherproofing.',
        'Vibration dampener degradation causing unstable hover. All four dampeners replaced with upgraded silicone version. Root cause: UV degradation of rubber dampeners from outdoor storage.'
    )[MOD(SEQ4(), 15)]::VARCHAR,
    ARRAY_CONSTRUCT('Mike Chen','Sarah Rodriguez','James Wilson','Priya Patel','Tom Anderson',
                    'Lisa Kim','David Martinez','Amy Thompson','Carlos Rivera','Rachel Green')
        [MOD(SEQ4(), 10)]::VARCHAR,
    DATEADD('day', -UNIFORM(1, 180, RANDOM()), '2025-09-19'::TIMESTAMP_NTZ),
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 90
         THEN DATEADD('hour', UNIFORM(2, 48, RANDOM()),
              DATEADD('day', -UNIFORM(1, 180, RANDOM()), '2025-09-19'::TIMESTAMP_NTZ))
         ELSE NULL END,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 90 THEN 'COMPLETED'
         WHEN UNIFORM(1,100,RANDOM()) <= 95 THEN 'IN_PROGRESS'
         ELSE 'SCHEDULED' END
FROM TABLE(GENERATOR(ROWCOUNT => 500))
CROSS JOIN aircraft_arr a;
RECORDS.PARTS_USED (1,374 rows)
sql
INSERT INTO RECORDS.PARTS_USED (USAGE_ID, LOG_ID, PART_ID, QUANTITY_USED, COST)
WITH parts_arr AS (
    SELECT ARRAY_AGG(PART_ID) WITHIN GROUP (ORDER BY PART_ID) AS ids,
           ARRAY_AGG(UNIT_COST) WITHIN GROUP (ORDER BY PART_ID) AS costs,
           COUNT(*) AS total
    FROM DEPOT.PARTS_INVENTORY
),
usage_seq AS (
    SELECT column1 AS seq_num FROM VALUES (1), (2), (3)
)
SELECT
    UUID_STRING(),
    ml.LOG_ID,
    p.ids[MOD(ABS(HASH(ml.LOG_ID || u.seq_num::VARCHAR)), p.total)]::VARCHAR,
    UNIFORM(1, 4, RANDOM()),
    ROUND(p.costs[MOD(ABS(HASH(ml.LOG_ID || u.seq_num::VARCHAR)), p.total)]::FLOAT
          * UNIFORM(1, 4, RANDOM()), 2)
FROM RECORDS.MAINTENANCE_LOGS ml
CROSS JOIN usage_seq u
CROSS JOIN parts_arr p
WHERE ml.STATUS = 'COMPLETED';
