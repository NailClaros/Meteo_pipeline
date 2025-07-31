import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import altair as alt
from datetime import datetime

load_dotenv()
DB_URL = os.getenv("DB_URL")

st.set_page_config(layout="wide")
st.title("🌤️ Nail's Weather Dashboard")
st.write(f"Live hourly weather metrics from North Carolina cities.  Date: {datetime.now():%Y-%m-%d}")

# Fetch data
conn = psycopg2.connect(DB_URL)
query = """
    SELECT * FROM "WeatherData"."formatted_weather_data"
    WHERE time >= NOW() - INTERVAL '24 hours'
    ORDER BY time ASC
    LIMIT 72;
"""
df = pd.read_sql_query(query, conn)
conn.close()

# Prepare time/hour labels
df['time'] = pd.to_datetime(df['time'])
df['hour'] = df['time'].dt.hour
df['hour_label'] = df['time'].dt.strftime('%-I%p')  # e.g., 1AM, 2PM

# Clean location_id
df['location_id'] = df['location_id'].astype(str).str.strip()
available_locations = df['location_id'].dropna().unique()

# Metric definitions: key -> (display name, unit)
METRICS = {
    "temp_F": ("Temperature", "°F"),
    "cloud_cover_perc": ("Cloud Cover", "%"),
    "surface_pressure": ("Surface Pressure", "hPa"),
    "wind_speed_80m_mph": ("Wind Speed @80m", "mph"),
    "wind_direction_80m_deg": ("Wind Direction @80m", "°")
}

# Sidebar selector with friendly labels
metric_options = [f"{name} ({unit})" for name, unit in (METRICS[m] for m in METRICS)]
# Map friendly label back to metric key
label_to_key = {f"{METRICS[k][0]} ({METRICS[k][1]})": k for k in METRICS}

selected_labels = st.sidebar.multiselect(
    "Select metrics to show:",
    options=metric_options,
    default=[f"{METRICS['temp_F'][0]} ({METRICS['temp_F'][1]})"]
)
selected_metrics = [label_to_key[label] for label in selected_labels if label in label_to_key]

if not selected_metrics:
    st.sidebar.warning("No metrics selected; please choose at least one.")

# Layout: 1x3 grid
cols = st.columns(3)

for i, loc_id in enumerate(available_locations):
    loc_df = df[df["location_id"] == loc_id]
    with cols[i % 3]:
        st.subheader(f"📍 {loc_id}")
        if loc_df.empty:
            st.warning("No data available.")
            continue

        # Build long-form data: one row per hour per metric
        records = []
        for metric in selected_metrics:
            display_name, unit = METRICS[metric]
            # Use raw chronological values; if multiple per hour you could aggregate (e.g., last or mean)
            agg = (
                loc_df.groupby(["hour", "hour_label"])[metric]
                .mean()  # collapse duplicates per hour; remove if you want full timestamp series
                .reset_index()
                .sort_values("hour")
            )
            if agg.empty:
                continue
            agg = agg.rename(columns={metric: "value"})
            agg["metric"] = f"{display_name} ({unit})"
            records.append(agg[["hour", "hour_label", "metric", "value"]])

        # st.write(records) ## TEST

        if not records:
            st.info("No metric data to display for selected filters.")
            continue

        long_df = pd.concat(records, ignore_index=True)

        # st.write(long_df) ## TEST

        # Selection for hover nearest hour
        hover = alt.selection_point(
        fields=["hour_label"],
        nearest=True,
        on="mouseover",
        empty="none",
        clear="mouseout"
        )

        # Base lines: one line per metric
        lines = (
            alt.Chart(long_df)
            .mark_line()
            .encode(
                x=alt.X(
                    "hour:O",
                    title="Hour of Day",
                    axis=alt.Axis(
                        labelExpr="""
                            datum.value == 0 ? '12AM' :
                            datum.value < 12 ? datum.value + 'AM' :
                            datum.value == 12 ? '12PM' :
                            (datum.value - 12) + 'PM'
                        """,
                        labelAngle=0
                    ),
                ),
                y=alt.Y("value:Q", title="Metric"),
                color=alt.Color("metric:N", title="Metric"),
            )
        )

        # Points on hovered hour across all metrics
        points = (
            alt.Chart(long_df)
            .transform_filter(hover)
            .mark_circle(size=80)
            .encode(
                x=alt.X("hour:O"),
                y=alt.Y("value:Q"),
                color=alt.Color("metric:N", title="Metric"),
                tooltip=[
                    alt.Tooltip("hour_label:N", title="Hour"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("value:Q", title="Value", format=".2f"),
                ],
            )
        )

        # Vertical rule at hovered hour with tooltip
        rule = (
            alt.Chart(long_df)
            .mark_rule(color="gray")
            .encode(
                x=alt.X("hour:O"),
                opacity=alt.condition(hover, alt.value(1), alt.value(0)),
                tooltip=[
                    alt.Tooltip("hour_label:N", title="Hour"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("value:Q", title="Value", format=".2f"),
                ]
            )
            .add_params(hover)
        )

        # Compose layered chart; independent y-scales so mixed units don’t squash
        final_chart = (
            alt.layer(lines, points, rule)
            .resolve_scale(y="shared")
            .properties(width=350, height=350)
        )
        st.altair_chart(final_chart, use_container_width=True)
