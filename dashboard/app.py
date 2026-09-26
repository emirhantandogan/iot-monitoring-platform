import psycopg
import streamlit as st
from psycopg.rows import dict_row

from shared.config import load_settings


settings = load_settings()

st.set_page_config(page_title="IoT Monitoring", layout="wide")
st.title("IoT Monitoring Dashboard")
st.caption("Phase 1: recent telemetry stored in PostgreSQL")

row_limit = st.sidebar.number_input(
    "Rows to show",
    min_value=10,
    max_value=1000,
    value=100,
    step=10,
)
st.sidebar.button("Refresh")

QUERY = """
    SELECT
        device_id,
        temperature,
        humidity,
        recorded_at,
        ingested_at,
        received_at
    FROM telemetry
    ORDER BY received_at DESC
    LIMIT %s
"""

try:
    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as connection:
        rows = connection.execute(QUERY, (row_limit,)).fetchall()
except psycopg.Error as error:
    st.error(f"Could not read telemetry from PostgreSQL: {error}")
    st.stop()

if not rows:
    st.info("No telemetry has been stored yet. Start the simulator.")
    st.stop()

latest = rows[0]
temperature_column, humidity_column, device_column = st.columns(3)
temperature_column.metric("Latest temperature", f"{latest['temperature']:.1f} °C")
humidity_column.metric("Latest humidity", f"{latest['humidity']:.1f} %")
device_column.metric("Latest device", latest["device_id"])

st.dataframe(rows, width="stretch", hide_index=True)
