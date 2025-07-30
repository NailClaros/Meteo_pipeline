import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")

st.set_page_config(layout="wide")
st.title("🌤️ Nail's Weather Dashboard")
st.write("Live hourly weather metrics from North Carolina cities.")

# Connect to NeonDB
conn = psycopg2.connect(DB_URL)
query = """
    SELECT * FROM "WeatherData"."formatted_weather_data"
    WHERE time >= NOW() - INTERVAL '24 hours'
    ORDER BY time ASC;
"""
df = pd.read_sql_query(query, conn)
conn.close()

# Ensure time is in datetime format
df['time'] = pd.to_datetime(df['time'])
df['hour'] = df['time'].dt.hour

# Discover real location_id values from your data
available_locations = df['location_id'].unique()

# Rename columns for human readability
COLUMN_RENAMES = {
    "temp_F": "Temperature (°F)",
    "cloud_cover_perc": "Cloud Cover (%)",
    "surface_pressure": "Surface Pressure (hPa)",
    "wind_speed_80m_mph": "Wind Speed @80m (mph)",
    "wind_direction_80m_deg": "Wind Direction @80m (°)"
}
REVERSE_RENAMES = {v: k for k, v in COLUMN_RENAMES.items()}

# Sidebar filters
st.sidebar.header("Select Weather Metrics")
selected_labels = st.sidebar.multiselect(
    "Choose metrics to show:",
    options=list(COLUMN_RENAMES.values()),
    default=["Temperature (°F)"]
)
selected_vars = [REVERSE_RENAMES[label] for label in selected_labels]

# Layout for charts
columns = st.columns(3)

for i, loc_id in enumerate(available_locations):
    city_df = df[df["location_id"] == loc_id]
    if city_df.empty:
        with columns[i % 3]:
            st.subheader(f"{loc_id} (No Data)")
            st.warning("⚠️ No data available.")
    else:
        chart_data = city_df.groupby("hour")[selected_vars].mean()
        chart_data.rename(columns=COLUMN_RENAMES, inplace=True)

        with columns[i % 3]:
            st.subheader(f"📍 {loc_id}")
            st.line_chart(chart_data)

