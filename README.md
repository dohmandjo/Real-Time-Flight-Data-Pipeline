# Real-Time-Flight-Data-Pipeline

A fully containerized, streaming data pipeline that ingests live flight data from the **Aviationstack** API into **Kafka**, cleans and filters it with **Spark Structured Streaming (PySpark)**, loads it into a **Postgres** warehouse (staging + star schema), mirrors curated facts to **Google Sheets**, and visualizes everything in **Tableau Public**.

---

## What you get

- **Streaming ingest** from Aviationstack → **Kafka** topic (`flight_live`)
- **PySpark** stream:
  - Robust timestamp normalization (fixes odd seconds, timezones, millis)
  - Filters to **arrived** or **en-route** flights only
  - Keeps rolling **last 3 days**
- **Postgres** warehouse:
  - `fact_flight_status_staging` (stream landing)
  - Dimensions: `dim_airline`, `dim_airport`, `dim_route`
  - Fact: `fact_flight_status` with `last_updated`
  - Curated view: `vw_flights_for_sheet_fact`
- **Google Sheets Sink**:
  - Incremental sync using a watermark (`gsheet_sync_state.last_sync_time`)
  - Header row, frozen/bold, auto-size, **numeric** delay columns
- **Tableau Public** dashboards on top of the sheet (refreshable)

---

## 🧱 Architecture

Aviationstack API ──(REST)──> Producer (Python) ──> Kafka (topic: flight_live)
│
▼
Spark Structured Streaming (PySpark)
normalize + filter + last 3 days + write to Postgres (staging)
│
▼
Loader (Python, SQL upserts) ──> Star schema in Postgres
│
▼
Google Sheets Sink (incremental by last_updated watermark)
│
▼
Tableau Public (analysis & dashboards)

---

## Repo layout

├─ docker-compose.yml
├─ .env.example
├─ apps/
│ ├─ producer/
│ │ ├─ run_producer.py
│ │ └─ Dockerfile
│ ├─ spark_app/
│ │ ├─ flight_stream.py
│ │ └─ Dockerfile
│ ├─ loader/
│ │ ├─ load_warehouse.py
│ │ └─ Dockerfile
│ └─ sheets_sink/
│ ├─ sheets_sink.py
│ └─ Dockerfile
└─ db/
├─ 00_warehouse.sql
└─ 01_views.sql


---

## Prerequisites

- **Docker** + **Docker Compose**
- **Aviationstack API key**
- **Google Cloud Service Account (SA)** with **Google Sheets API** enabled  
  - Download SA JSON key file
  - Share your target Google Sheet with the SA **email address**
- **Tableau Public** (desktop)

---

## Environment variables

Copy `.env.example` → `.env` and fill:

| Key | Example | Notes |
|---|---|---|
| `AVIATIONSTACK_KEY` | `xxxxxxxx` | Aviationstack API key |
| `KAFKA_TOPIC` | `flight_live` | Must match producer & spark |
| `PGHOST` | `flight_postgres` | Docker DNS |
| `PGPORT` | `5432` | Postgres port |
| `PGDATABASE` | `flight_pipeline` | DB name |
| `PGUSER` / `PGPASSWORD` | `flight_user` / `Password123` | Credentials |
| `GSHEET_ID` | `1xWJ3yV1Q4...` | Sheet ID (middle of the URL) |
| `GSHEET_TAB` | `flight_curated` | Tab written by sink |
| `GSHEET_BATCH_SIZE` | `300` | Rows per append |
| `GSHEET_INTERVAL_SEC` | `30` | Sleep when no rows |
| `CHECKPOINT_DIR` | `/tmp/chk/flights_stream` | Spark checkpoints |
| `HOST_KAFKA_PORT` | `9094` | Host Kafka port mapping |

> Place your SA key at: `${HOME}/gsa/gsa-keys.json` and **share the Sheet** with the SA email.

---

## Run

```bash
# Build and start everything
docker compose up --build -d

# Tail logs (examples)
docker compose logs -f producer
docker compose logs -f spark
docker compose logs -f loader
docker compose logs -f sheets-sink\
```
# Verification Checklist

### Spark
- Prints schema and preview rows per micro-batch.

### Postgres
Check that data exists:
```bash
docker exec -it flight_postgres psql -U flight_user -d flight_pipeline -c \
  "SELECT COUNT(*) FROM fact_flight_status_staging;"
```

## Google Sheets

- Has header and new rows.
- Delay columns (`dep_delay_min`, `arr_delay_min`) are in **numeric** format.

---

## Data Model

### Staging

**fact_flight_status_staging** — Flat stream intake from Spark.

---

### Dimensions

| Table         | Columns                                      | Notes                    |
|---------------|----------------------------------------------|--------------------------|
| `dim_airline` | airline_id, iata (UNIQUE), icao, airline_name |                          |
| `dim_airport` | airport_id, iata (UNIQUE), icao (UNIQUE), airport_name |                |
| `dim_route`   | route_id, dep_airport_id, arr_airport_id     | Unique dep–arr pair      |

---

## Fact Table

### `fact_flight_status`
Stores the latest flight data per flight key.

- `flight_key` (Primary Key)
- `status`
- `dep_*` (e.g., scheduled, actual, delay, gate, terminal)
- `arr_*` (e.g., scheduled, actual, delay, gate, terminal)
- `last_updated`

---

## Curated View (for Google Sheets & Tableau)

### `vw_flights_for_sheet_fact`
A view that joins the fact and dimension tables. Fields exposed:

- `flight_key`
- `flight_date`
- `status`
- **Airline**: `iata`, `airline_name`
- **Departure**:
  - timestamps: `dep_scheduled`, `dep_actual`, etc.
  - delays: `dep_delay_min`
  - codes: `dep_airport_iata`, `dep_airport_name`
- **Arrival**:
  - timestamps: `arr_scheduled`, `arr_actual`, etc.
  - delays: `arr_delay_min`
  - codes: `arr_airport_iata`, `arr_airport_name`
- `last_updated`

---

## Watermark Table

### `gsheet_sync_state`
Controls incremental pushes to Google Sheets.

- `id = 1`
- `last_sync_time` — Only rows with `last_updated > last_sync_time` are appended to Sheets.

---

## Spark Stream Logic (Highlights)

### Timestamp Cleaning — `clean_ts()`
Standardizes inconsistent or malformed timestamp formats:
- `'Z'` → `'+00:00'`
- `+0000` → `+00:00`
- Trim milliseconds to **3 digits**
- Pad single-digit seconds (`...:2+00:00` → `...:02+00:00`)
- Trim any 3-digit seconds
- Append `+00:00` if timezone info is missing

---

### Filtering & Write Logic

- Keep only statuses:
  - `active`, `landed`, `arrived`, `en-route`, `enroute`
- Time filter:
  - Only include flights within the **last 3 days**
    - based on `dep_scheduled`, `dep_actual`, `arr_scheduled`, `arr_actual`
- Skip empty micro-batches to avoid JDBC overhead
- Write each valid micro-batch to **Postgres staging** via JDBC

---

## Loader Logic

- **Upserts** into dimension tables:
  - `dim_airline`, `dim_airport`  
  - IATA codes preferred; ICAO as fallback if IATA missing
- **Builds routes** in `dim_route` using dep/arr airport references
- **Upserts latest flight record** per `flight_key` into `fact_flight_status`
  - Sets the `last_updated` timestamp for tracking freshness
- **Deletes processed staging rows** up to the current micro-batch cutoff time

---

## Sheets Sink (Google Sheets Writer)

- Ensures header row is present, **bold**, **frozen**, and **auto-sized**
- Formats `dep_delay_min` and `arr_delay_min` columns as **numeric**
- Appends only rows where `last_updated > watermark` from `gsheet_sync_state`
- **Updates the watermark** table after each successful sync

---

---

## Tableau Public

### Connect Tableau to Google Sheets

1. Go to 
    **Data → Google Sheets**
2. Select your connected Sheet
3. Choose the `flight_curated` tab (backed by `vw_flights_for_sheet_fact`)

---

### Starter Calculations

#### OnTime_Arrival_Flag
```tableau
IF NOT ISNULL([arr_delay_min]) THEN
  IF [arr_delay_min] <= 5 THEN "On-time" ELSE "Late" END
END
```

#### OnTime_Arrival_Flag_Count
```tableau
IF NOT ISNULL([arr_delay_min]) THEN
  IF [arr_delay_min] <= 5 THEN 1 ELSE 0
  END
ELSE 0
END
```

#### Tip:
Use AVG([OnTime_Arrival_Flag_Count]) over time to measure on-time performance trends.

## Example Visuals

| Visualization | Description |
|----------------|--------------|
| **On-time Trend by Flight Date (Line Chart)** | Plots average on-time rate over `flight_date`. |
| **Average Arrival Delay by Airline (Bar Chart)** | Shows average `arr_delay_min` per airline, sorted descending. |
| **Worst Routes by Delay (Heatmap)** | Uses a label like `dep_airport_code → arr_airport_code` to visualize route performance. |

#### Refresh

In authoring mode:
Data → Refresh

Republishing automatically updates Tableau Public.

**Google Sheets-backed sources** auto-refresh whenever the Sheet updates.

#### Tip

You can combine the visuals above into an interactive Flight Performance Dashboard to showcase:

Airline punctuality

Route reliability

Trends over time

Perfect for portfolio or demo presentations.

---

---
## Operations

### Rebuild After Code Edits
```bash
docker compose build producer spark loader sheets-sink
docker compose up -d
```

### Quick DB Checks
```SQL
SELECT COUNT(*) FROM fact_flight_status;
SELECT * FROM vw_flights_for_sheet_fact ORDER BY last_updated DESC LIMIT 10;
SELECT * FROM gsheet_sync_state;
```
---


---

## Troubleshooting

| Issue                     | Check / Fix                                                                 |
|---------------------------|------------------------------------------------------------------------------|
| **Spark parse errors**    | Handled via `clean_ts()` — malformed values are safely cast to `NULL`       |
| **No data in Postgres**   | Check:  
  - Producer logs (API quota exceeded?)  
  - Kafka topic name  
  - Spark logs for schema or connection issues |
| **Loader connection refused** | Postgres might still be starting. The loader retries with backoff logic |
| **Sheets not updating**   | Ensure:  
  - Sheet is **shared** with the Service Account email  
  - `GSHEET_ID` and tab name are correct  
  - `sheets-sink` logs show no quota or auth errors |
| **Delays appear as text** | Sink enforces numeric format. If column formatting was manually changed, clear it and let the sink rewrite |

---

## Security & Quotas

- **Never commit `.env` files or Service Account (SA) keys** to GitHub or version control
- **Aviationstack (free tier)**  
  - API rate-limited → set a safe fetch interval in the producer
- **Google Sheets API**  
  - Write quota applies → tune `BATCH_SIZE` and `INTERVAL` in the sink

---

---

## Ideas to Extend

- **Add airport geolocation** data and build interactive **maps** in Tableau (e.g., flight paths, density plots)
- **Materialize daily aggregates** (e.g., delay trends, airport-level stats) to accelerate BI dashboards
- **Test PySpark logic** using `pytest` and [`chispa`](https://github.com/MrPowers/chispa) for transform validation

---

## Resume Blurb

> Built a Dockerized streaming pipeline with Kafka → PySpark → Postgres (star schema) and a Google Sheets → Tableau Public analytics layer.  
> Implemented robust timestamp normalization, incremental sync to Sheets, and idempotent warehouse upserts.

---