# Dashboard app

## What it does

`dashboard/app.py` is the read-only Streamlit telemetry dashboard. It shows the
latest temperature, humidity, and device, followed by a table of recent
telemetry rows.

## Data flow

The page reads `POSTGRES_DSN` through the shared settings, opens a PostgreSQL
connection, and runs one parameterized query against the `telemetry` table.
The selected row limit is passed to the query, and Streamlit renders the
returned rows. Refreshing the page runs the query again.

## Important parts

- `QUERY` selects the newest records by `received_at`.
- The sidebar controls how many rows are returned.
- The database error handler displays a useful message instead of crashing the
  page.
- The first returned row supplies the three summary values.

## Dependencies

The script depends on Streamlit, Psycopg, `shared/config.py`, a reachable
PostgreSQL database, and the `telemetry` table created by `database/init.sql`.

## How to run it

The normal Docker Compose command is:

```powershell
docker compose up -d dashboard
```

Open `http://localhost:8501` after PostgreSQL and the dashboard are running.
For direct development with the Python dependencies installed, use:

```powershell
streamlit run dashboard/app.py
```

## What to understand

The dashboard reads PostgreSQL directly and never changes telemetry. This keeps
the read path visible and avoids adding an unnecessary analytics service.
