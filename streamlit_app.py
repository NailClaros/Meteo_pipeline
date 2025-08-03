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
    "Select metrics to show for the 3 graphs:",
    options=metric_options,
    default=[f"{METRICS['temp_F'][0]} ({METRICS['temp_F'][1]})",
             f"{METRICS['cloud_cover_perc'][0]} ({METRICS['cloud_cover_perc'][1]})"]
)
selected_metrics = [label_to_key[label] for label in selected_labels if label in label_to_key]

if not selected_metrics:
    st.sidebar.warning("No metrics selected; please choose at least one.")

# Layout: 1x3 grid
cols = st.columns(3)

for i, loc_id in enumerate(available_locations):
    loc_df = df[df["location_id"] == loc_id]
    with cols[i % 3]:
        st.subheader(f"📍 {loc_id}", anchor= False)
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
        fields=["hour"],
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
                y=alt.Y("value:Q", title=None),
                        color=alt.Color(
                            "metric:N",
                            title="Metric",
                            scale=alt.Scale(scheme="category10"),
                            legend=alt.Legend(orient="top", titleFontSize=12, labelFontSize=11),
                        ),
            )
        )

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
        summary = (
                    long_df
                    .groupby(["hour", "hour_label"], sort=False)
                    .apply(
                        lambda g: ", ".join(f"{m}: {v:.1f}" for m, v in zip(g["metric"], g["value"])),
                        include_groups=False
                    )
                    .reset_index(name="summary")
                )
        
        rule = (
            alt.Chart(summary)
            .mark_rule(color="gray")
            .encode(
                x=alt.X("hour:O"),
                opacity=alt.condition(hover, alt.value(1), alt.value(0)),
                tooltip=[
                    alt.Tooltip("hour_label:N", title="Hour"),
                    alt.Tooltip("summary:N", title="Values")
                ]
            )
            .add_params(hover)
        )

        final_chart = (
            alt.layer(lines, points, rule)
            .resolve_scale(y="shared")
            .properties(width=350, height=350)
            .interactive()
        )
        st.altair_chart(final_chart, use_container_width=True)


# Mapping location IDs to friendly city names and reverse
CITY_MAP = {
    "CLT": "Charlotte",
    "RAL": "Raleigh",
    "GSB": "Greensboro"
}
REVERSE_CITY_MAP = {v: k for k, v in CITY_MAP.items()}

st.markdown("---")
st.header("📈 Weekly History", anchor= False)

# Sidebar-like selectors (you can also place them inline)
col_main, col_controls = st.columns([3, 1])

with col_controls:
    # 1. City selector (friendly name)
    city_friendly = st.selectbox(
        "Select City",
        options=list(CITY_MAP.values()),
        index=list(CITY_MAP.values()).index("Charlotte"),
        key="weekly_city"
    )
    selected_location_id = REVERSE_CITY_MAP[city_friendly]

    # 2. Metrics selector (friendly)
    metric_options = [f"{METRICS[k][0]} ({METRICS[k][1]})" for k in METRICS]
    label_to_key = {f"{METRICS[k][0]} ({METRICS[k][1]})": k for k in METRICS}
    default_metrics = [
        f"{METRICS['temp_F'][0]} ({METRICS['temp_F'][1]})",
        f"{METRICS['cloud_cover_perc'][0]} ({METRICS['cloud_cover_perc'][1]})",
        f"{METRICS['wind_speed_80m_mph'][0]} ({METRICS['wind_speed_80m_mph'][1]})",
    ]
    selected_labels_week = st.multiselect(
        "Select metrics",
        options=metric_options,
        default=default_metrics,
        key="weekly_metrics"
    )
    selected_metrics_week = [label_to_key[lbl] for lbl in selected_labels_week if lbl in label_to_key]
    if not selected_metrics_week:
        st.warning("Pick at least one metric for weekly history.")
        st.stop()

with col_main:
    st.subheader(f"Past 7 Days — {city_friendly}", anchor= False)

    # Query fresh weekly data for the selected city
    try:
        conn = psycopg2.connect(DB_URL)
        query = """
            SELECT *
            FROM "WeatherData"."formatted_weather_data"
            WHERE location_id = %s
            AND time >= (date_trunc('day', now() AT TIME ZONE 'UTC') - INTERVAL '6 days')
            AND time <  (date_trunc('day', now() AT TIME ZONE 'UTC') + INTERVAL '1 day')
            ORDER BY time desc
            LIMIT 168;
        """
        week_df = pd.read_sql_query(query, conn, params=((CITY_MAP[selected_location_id],)))
        conn.close()
    except Exception as e:
        st.error(f"Failed to fetch weekly data: {e}")
        week_df = pd.DataFrame()  # fallback

    if week_df.empty:
        st.info("No weekly data available for that selection.")

    else:
        # Prepare for plotting
        week_df["time"] = pd.to_datetime(week_df["time"])
        week_df["hour_label"] = week_df["time"].dt.strftime("%-I%p")
        # Build long-form for selected metrics
        records = []
        for metric in selected_metrics_week:
            display_name, unit = METRICS[metric]
            tmp = week_df[["time", metric]].copy()
            tmp = tmp.rename(columns={metric: "value"})
            tmp["metric"] = f"{display_name} ({unit})"
            # Keep raw time-series (no collapsing)
            records.append(tmp[["time", "metric", "value"]])

        long_week = pd.concat(records, ignore_index=True)

        summary_week = (
            long_week
            .groupby(["time"], sort=False)
            .apply(
                lambda g: ", ".join(f"{m}: {v:.1f}" for m, v in zip(g["metric"], g["value"])),
                include_groups=False
            )
            .reset_index(name="summary")
        )


        # Hover selection on time (nearest timestamp)
        hover_time = alt.selection_point(
            fields=["time"],
            nearest=True,
            on="mouseover",
            empty="none",
            clear="mouseout"
        )

        # Line layer
        lines_week = (
            alt.Chart(long_week)
            .mark_line()
            .encode(
                x=alt.X("time:T", title="Time", axis=alt.Axis(format="%b %d %I %p")),
                y=alt.Y("value:Q", title=None),
                color=alt.Color(
                    "metric:N", 
                    title="Metric", 
                    scale=alt.Scale(scheme="category10"),
                    legend=alt.Legend(orient="top", titleFontSize=12, labelFontSize=11)),
                tooltip=[
                    alt.Tooltip("time:T", title="Time", format="%Y-%m-%d %I:%M %p"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("value:Q", title="Value", format=".2f"),
                ],
            )
        )

        # Points at hovered time
        points_week = (
            alt.Chart(long_week)
            .transform_filter(hover_time)
            .mark_circle(size=80)
            .encode(
                x="time:T",
                y="value:Q",
                color=alt.Color("metric:N", scale=alt.Scale(scheme="category10"), legend=None),
                tooltip=[
                    alt.Tooltip("time:T", title="Time", format="%Y-%m-%d %I:%M %p"),
                    alt.Tooltip("metric:N", title="Metric"),
                    alt.Tooltip("value:Q", title="Value", format=".2f"),
                ],
            )
        )

        # Rule with combined tooltip from summary_week
        rule_week = (
            alt.Chart(summary_week)
            .mark_rule(color="gray")
            .encode(
                x=alt.X("time:T"),
                opacity=alt.condition(hover_time, alt.value(1), alt.value(0)),
                tooltip=[
                    alt.Tooltip("time:T", title="Time", format="%Y-%m-%d %I:%M %p"),
                    alt.Tooltip("summary:N", title="All metrics"),
                ],
            )
            .add_params(hover_time)
        )

        # Compose with independent y-scales
        big_chart = (
            alt.layer(lines_week, points_week, rule_week)
            .resolve_scale(y="shared")
            .properties(height=500)
            .interactive()
        )

        st.altair_chart(big_chart, use_container_width=True)