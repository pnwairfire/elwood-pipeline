-- Unified AQ feed for elwood outlier detection.
-- One row per active unit across the 5 tileserver source tables. Columns that
-- don't exist upstream (e.g. corrected_pm25 outside of PurpleAir) are NULL.
WITH joined_units AS (
    -- PurpleAir: only source with corrected_pm25
    SELECT
        unit_id::varchar        AS unit_id,
        ST_Y(geom)::numeric     AS latitude,
        ST_X(geom)::numeric     AS longitude,
        aqi,
        nowcast,
        raw_pm25,
        corrected_pm25,
        'PurpleAir'             AS unit_type,
        geom
    FROM pwfsl_map.purple_air
    WHERE status = 0

    UNION ALL

    -- AirNow low-cost sensors: split into SensOR / SensWA via instrument column
    SELECT
        unit_id::varchar        AS unit_id,
        ST_Y(geom)::numeric     AS latitude,
        ST_X(geom)::numeric     AS longitude,
        aqi,
        nowcast,
        raw_pm25,
        NULL::numeric           AS corrected_pm25,
        instrument              AS unit_type,
        geom
    FROM pwfsl_map.airnow_sensors
    WHERE status = 0

    UNION ALL

    -- Clarity sensors
    SELECT
        unit_id::varchar        AS unit_id,
        ST_Y(geom)::numeric     AS latitude,
        ST_X(geom)::numeric     AS longitude,
        aqi,
        nowcast,
        raw_pm25,
        NULL::numeric           AS corrected_pm25,
        'Clarity'               AS unit_type,
        geom
    FROM pwfsl_map.clarity_sensors
    WHERE status = 0

    UNION ALL

    -- Permanent (regulatory) monitors
    SELECT
        unit_id::varchar        AS unit_id,
        ST_Y(geom)::numeric     AS latitude,
        ST_X(geom)::numeric     AS longitude,
        aqi,
        nowcast,
        raw_pm25,
        NULL::numeric           AS corrected_pm25,
        'permanent_monitor'     AS unit_type,
        geom
    FROM pwfsl_map.permanent_monitors
    WHERE status = 0

    UNION ALL

    -- Mobile (AIRSIS / WRCC) monitors
    SELECT
        unit_id::varchar        AS unit_id,
        ST_Y(geom)::numeric     AS latitude,
        ST_X(geom)::numeric     AS longitude,
        aqi,
        nowcast,
        raw_pm25,
        NULL::numeric           AS corrected_pm25,
        'mobile_monitor'        AS unit_type,
        geom
    FROM pwfsl_map.mobile_monitors
    WHERE status = 0
)
SELECT * FROM joined_units;
