import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DB_URL")

st.title("🌤️ Weather Dashboard")
st.write("Latest weather data from the pipeline.")

# Connect to NeonDB
conn = psycopg2.connect(DB_URL)
query = """
    SELECT * FROM "WeatherData"."formatted_weather_data"
    ORDER BY time DESC
    LIMIT 72;
"""
df = pd.read_sql_query(query, conn)
conn.close()

# Show data
st.dataframe(df)

# Optional: Plot
st.line_chart(df[['temp_F']])