import streamlit as st
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import altair as alt
from datetime import datetime, date
import time
from streamlit_autorefresh import st_autorefresh
import redis
import uuid

# This will cause the script to rerun every 1 second, enabling a live countdown.
st_autorefresh(interval=5000, key="live_cooldown", limit=100)

load_dotenv()
DB_URL = os.getenv("DB_URL")
COOLDOWN = 30 #seconds
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=os.getenv("REDIS_PORT"),
    decode_responses=True,
    username=os.getenv("REDIS_UN"),
    password=os.getenv("REDIS_PWD")
)

def can_refresh(key: str) -> bool:
    now = int(time.time())
    next_allowed = r.get(key)
    if next_allowed is not None and now < int(next_allowed):
        return False  # still cooling down
    # set next_allowed with slight buffer
    r.set(key, now + COOLDOWN, ex=COOLDOWN + 2)
    return True

def get_cooldown_remaining(key: str) -> int:
    now = int(time.time())
    next_allowed = r.get(key)
    if next_allowed is None:
        return 0
    remaining = int(next_allowed) - now
    return max(0, remaining)

# --- persistent client identifier (survives refresh) ---
if "client_key" not in st.session_state:
    st.session_state.client_key = str(uuid.uuid4())
if "refresh_bust" not in st.session_state:
    st.session_state.refresh_bust = 0

client_key = st.session_state.client_key

def make_redis_key(client_key: str) -> str:
    return f"cooldown:{client_key}"

@st.cache_data(show_spinner=False)
def fetch_today_data(db_url: str, today: date, bust: int):
    try:
        conn = psycopg2.connect(db_url)
        query = """
            SELECT * FROM "WeatherData"."formatted_weather_data"
            WHERE time >= NOW() - INTERVAL '24 hours'
            ORDER BY time ASC
            LIMIT 72;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.warning(f"fetch_today_data failed: {e}")
        return pd.DataFrame()  # safe empty fallback
    finally:
        conn.close()

@st.cache_data(show_spinner=False)
def fetch_weekly_data(db_url: str, location_id: str, today: date, bust: int):
    try:
        conn = psycopg2.connect(db_url)
        query = """
            SELECT *
            FROM "WeatherData"."formatted_weather_data"
            WHERE location_id = %s
              AND time >= (date_trunc('day', now() AT TIME ZONE 'UTC') - INTERVAL '6 days')
              AND time <  (date_trunc('day', now() AT TIME ZONE 'UTC') + INTERVAL '1 day')
            ORDER BY time ASC;
        """
        df = pd.read_sql_query(query, conn, params=(location_id,))
        conn.close()
        return df
    except Exception as e:
        st.warning(f"fetch_weekly_data failed: {e}")
        return pd.DataFrame()
    finally:
        conn.close()


redis_key = make_redis_key(client_key)
remaining = get_cooldown_remaining(redis_key)

col1, col2 = st.columns([1, 3])
with col1:
    if remaining > 0:
        st.button(f"🔄 Refresh ({remaining}s)", disabled=True)
    else:
        if st.button("🔄 Refresh"):
            if can_refresh(redis_key):
                st.session_state.refresh_bust = int(time.time())  # trigger cache bust
            else:
                # race or concurrent check fallback
                st_autorefresh(interval=3000, key="live_cooldown", limit=11)
                st.warning("Cooldown still active.",  width=250)

with col1:
    if remaining > 0:
        st.info(f"Cooldown: {remaining}s until next refresh")
    else:
        st.success("Refresh available incase of bad/outdated data", width=200)




st.set_page_config(layout="wide")
st.title("🌤️ Nail's Weather Dashboard", anchor=False)
st.write(f"Live hourly weather metrics from North Carolina cities.  Date: {datetime.now():%Y-%m-%d}")

df = fetch_today_data(DB_URL, date.today(), st.session_state.refresh_bust)



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
    
    week_df = fetch_weekly_data(DB_URL, (CITY_MAP[selected_location_id],), 
                                date.today(), st.session_state.refresh_bust)

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

with st.expander("ℹ️ About Me & System Architecture", expanded=True):
    st.markdown("""
## ℹ️ About Me & System Architecture

### Who I Am  
My name is Nail Claros. I am a Computer Science graduate from the University of North Carolina at Charlotte with a passion for backend and data engineering. I built this dashboard as a hands-on learning project to explore and demonstrate real-world skills involved in designing and deploying a full data pipeline—from raw data ingestion to interactive visualization.

### Project Purpose & Learning Goals  
This app was created to internalize and showcase the entire data engineering workflow and backend system integration. Key objectives included:

- Mastering the data pipeline lifecycle: ingestion, transformation, cloud storage, caching, and visualization.
- Applying backend engineering best practices such as reliable API integration, rate-limiting with Redis, session and state management, and seamless cloud service integration.
- Building a production-style interactive dashboard without relying on heavy frontend frameworks, instead leveraging Streamlit and Altair for rapid development and elegant visualization.

### Core Pipeline (End-to-End Flow)  

**Data Ingestion:**  
The system pulls hourly weather data from an external API (Open-Meteo), targeting multiple cities in North Carolina and retrieving key metrics like temperature, wind speed, and cloud cover.

**Staging & Transformation:**  
Raw data is staged temporarily, then cleaned, normalized (including UTC timestamp standardization), and transformed into a long-form format that supports flexible and dynamic visualization.

**Cloud Integration:**  
Processed data flows through AWS S3 as a data lake and is ultimately persisted in a PostgreSQL database hosted by Neon—a cloud-native, scalable solution optimized for analytical querying.

**Caching & Rate Control:**  
To improve performance and protect backend resources:  
- Streamlit’s `@st.cache_data` decorator is used to memoize data fetches scoped to daily intervals.  
- Redis manages cooldown periods and session refresh states at the server level to enforce rate limits and prevent request spamming.  
*Note:* Because Streamlit’s session state is not fully persistent across all deployment scenarios, Redis remains the authoritative source for cooldown and session tracking. This design ensures backend integrity even if the frontend session lifecycle is volatile.

**Presentation:**  
The frontend delivers:  
- Three compact, real-time metric panels for current-hour weather data.  
- A comprehensive historical view that supports city and metric filtering, per-hour hover tooltips, and independently scaled y-axes for clarity across diverse units.

---

## 🛠 Skills and Impact

This project is a tangible demonstration of advanced data engineering and backend engineering capabilities, including:

**Data Engineering:**  
- Constructed and maintained an end-to-end automated data pipeline integrating API ingestion, data transformation, cloud staging, and persistent storage.  
- Designed data models using long-form structures for scalable and flexible multidimensional analytics.  
- Applied effective caching strategies both in-memory (Streamlit) and distributed (Redis) to optimize performance and reduce redundant database queries.  
- Implemented time-partitioned queries and rolling window analytics to enable efficient historical data exploration.

**Backend Engineering:**  
- Developed rate-limiting and cooldown mechanisms using Redis to safeguard backend services from overload and abuse.  
- Navigated the complexities of session and client-state tracking in a stateless frontend framework, reconciling UI-level volatility with server-side persistence.  
- Crafted secure, parameterized database queries and API endpoints emphasizing maintainability and scalability.  
- Delivered a full-stack solution using Python without a dedicated frontend framework, showcasing rapid prototyping and production-readiness with Streamlit and Altair.

**System Integration & Observability:**  
- Orchestrated seamless interactions between multiple cloud services, including external APIs, AWS storage, database hosting, and Redis caching.  
- Built a resilient system with robust logging, error handling, and user feedback mechanisms (cooldowns, tooltips, fallback messaging) to enhance usability and reliability.

This dashboard is much more than a visualization tool — it embodies a comprehensive data-driven backend system engineered to ingest, process, protect, and deliver actionable insights to end users.
 
                """)
