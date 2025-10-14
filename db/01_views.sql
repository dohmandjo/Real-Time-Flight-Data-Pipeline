-- Watermark for Sheets incremental sync
CREATE TABLE IF NOT EXISTS gsheet_sync_state(
  id INT PRIMARY KEY DEFAULT 1,
  last_sync_time TIMESTAMPTZ
);
INSERT INTO gsheet_sync_state(id, last_sync_time)
VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;

-- Curated view for Sheets/Tableau
CREATE OR REPLACE VIEW vw_flights_for_sheet_fact AS
SELECT
  f.flight_key,
  f.flight_date,
  f.status,
  da.iata AS airline_iata,
  da.airline_name,
  f.dep_scheduled, f.dep_estimated, f.dep_actual, f.dep_delay_min,
  f.arr_scheduled, f.arr_estimated, f.arr_actual, f.arr_delay_min,
  dep.iata AS dep_iata, dep.icao AS dep_icao, dep.airport_name AS dep_airport,
  arr.iata AS arr_iata, arr.icao AS arr_icao, arr.airport_name AS arr_airport,
  f.last_updated
FROM fact_flight_status f
LEFT JOIN dim_airline da ON da.airline_id = f.airline_id
LEFT JOIN dim_route r ON r.route_id = f.route_id
LEFT JOIN dim_airport dep ON dep.airport_id = r.dep_airport_id
LEFT JOIN dim_airport arr ON arr.airport_id = r.arr_airport_id;
