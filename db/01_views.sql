-- =====================================================================
-- 01_views.sql
-- Purpose:
--   * Create a tiny state table used as a watermark for incremental
--     syncing to Google Sheets (the sink reads only rows with
--     last_updated > last_sync_time).
--   * Create a curated, analysis-friendly view that joins the fact table
--     to its dimensions so Google Sheets / Tableau get tidy columns.
-- What it does:
--   1) Ensures gsheet_sync_state exists and is initialized (id=1).
--   2) Defines vw_flights_for_sheet_fact with denormalized columns:
--      flight identifiers/status + airline + dep/arr airport fields
--      + key timestamps/delays + last_updated for incremental pulls.
-- Notes:
--   * Keep column names stable—your Sheets sink relies on them.
--   * last_updated comes from fact_flight_status and drives increments.
-- =====================================================================

-- ---------------------------------------------------------------------
-- Incremental watermark table for the Sheets sink:
--   - Single-row table keyed by id=1.
--   - The sink updates last_sync_time after each successful append.
--   - If NULL, the sink will export everything on first run.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gsheet_sync_state(
  id INT PRIMARY KEY DEFAULT 1,
  last_sync_time TIMESTAMPTZ
);

-- Seed the row once; future runs won't overwrite it.
INSERT INTO gsheet_sync_state(id, last_sync_time)
VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------
-- Curated view exposed to Google Sheets / Tableau.
--   - Denormalizes facts with dimensions:
--       * Airline (IATA + name)
--       * Route → Departure/Arrival airports (IATA/ICAO/name)
--   - Keeps scheduling/actual timestamps and delay metrics.
--   - Includes last_updated so the sink can do incremental reads.
--   - LEFT JOINs to tolerate missing dim rows while the loader catches up.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_flights_for_sheet_fact AS
SELECT
  -- Core identifiers & status
  f.flight_key,
  f.flight_date,
  f.status,

  -- Airline (from dim_airline)
  da.iata AS airline_iata,
  da.airline_name,

  -- Departure timeline & delay
  f.dep_scheduled,
  f.dep_estimated,
  f.dep_actual,
  f.dep_delay_min,

  -- Arrival timeline & delay
  f.arr_scheduled,
  f.arr_estimated,
  f.arr_actual,
  f.arr_delay_min,

  -- Departure airport attributes (from dim_airport via dim_route)
  dep.iata         AS dep_iata,
  dep.icao         AS dep_icao,
  dep.airport_name AS dep_airport,

  -- Arrival airport attributes (from dim_airport via dim_route)
  arr.iata         AS arr_iata,
  arr.icao         AS arr_icao,
  arr.airport_name AS arr_airport,

  -- Drives incremental sync; updated by loader’s upsert
  f.last_updated
FROM fact_flight_status f
LEFT JOIN dim_airline da  ON da.airline_id = f.airline_id
LEFT JOIN dim_route   r   ON r.route_id    = f.route_id
LEFT JOIN dim_airport dep ON dep.airport_id = r.dep_airport_id
LEFT JOIN dim_airport arr ON arr.airport_id = r.arr_airport_id;
