import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_URL = os.getenv("DB_URL")

st.set_page_config(layout="wide")
st.title("🌤️ Weather Dashboard")
st.write("Live hourly weather metrics from Charlotte, Raleigh, and Greensboro, NC.")

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

# Sidebar filters
st.sidebar.header("Filter Weather Variables")
selected_vars = st.sidebar.multiselect(
    "Select metrics to show:",
    ["temp_F", "cloud_cover_perc", "surface_pressure", "wind_speed_80m_mph", "wind_direction_80m_deg"],
    default=["temp_F"]
)

locations = {
    "CLT": "Charlotte, NC",
    "RAL": "Raleigh, NC",
    "GSB": "Greensboro, NC"
}

col1, col2, col3 = st.columns(3)

for i, (loc_id, loc_name) in enumerate(locations.items()):
    city_df = df[df['location_id'] == loc_id]

    if city_df.empty:
        chart = "No data for " + loc_name
    else:
        chart_data = city_df.pivot_table(index='hour', values=selected_vars, aggfunc='mean')
        chart = st.line_chart(chart_data)

    with [col1, col2, col3][i]:
        st.subheader(loc_name)
        if isinstance(chart, str):
            st.write(chart)
        else:
            st.line_chart(chart_data)
