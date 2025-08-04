
# 🌦 NC Weather Data Dashboard

An **end-to-end Data Engineering + Backend Engineering** project that ingests live weather data, stores it in the cloud, and visualizes it with **interactive charts**.

Built by **Nail Claros**, a Computer Science graduate from UNC Charlotte, as a way to **learn, practice, and showcase** real-world backend and data engineering skills.

---

## 📌 Features

- **Real-time Weather Data** – Hourly weather metrics for Charlotte, Raleigh, and Greensboro.
- **Historical View** – Switch between cities and metrics to see the **last 7 days** of data.
- **Interactive Charts** – Hover for detailed tooltips, compare metrics, and explore without page reloads.
- **Custom Cooldown Logic** – Prevents excessive database queries using Redis-backed rate limiting.
- **Daily Caching** – Automatically caches results for the day to reduce redundant calls.
- **Fully Cloud-Integrated** – Uses Neon PostgreSQL for persistent storage and AWS for intermediate file storage.
- **Backend-Focused Architecture** – Demonstrates API integration, caching, and scalable querying patterns.

---

## 🛠 Architecture Overview

This app isn’t just charts — it’s a **full pipeline**:

1. **Data Ingestion**
   - Pulls raw weather data from an **external weather API**.
   - Targets multiple cities with hourly granularity.
   - Fetches core metrics:  
     - 🌡 Temperature  
     - ☁ Cloud Cover %  
     - 🌬 Wind Speed  
     - (Expandable for more)

2. **Transformation**
   - Normalizes timestamps into **UTC**.
   - Renames and formats metrics into a **long-form** structure for better plotting flexibility.
   - Adds human-readable labels (`12AM`, `3PM`, etc.).

3. **Cloud Storage**
   - Intermediate files sent to **AWS**.
   - Persistent storage in **PostgreSQL hosted via Neon** for:
     - Fast analytical queries
     - Rolling window historical views

4. **Caching & Rate Limiting**
   - **Streamlit `@st.cache_data`**  
     - Stores results for **the current day**, avoiding duplicate fetches when possible.
   - **Redis Cooldown System**  
     - Enforces a **minimum refresh interval** (e.g., 30 seconds) to protect the database.
     - Stores the true session refresh state in Redis.
     - _Note:_ Due to Streamlit's session limitations, cooldown persistence is fully enforced server-side, but the countdown timer may reset visually when the app refreshes.

5. **Visualization**
   - Uses **Altair** for responsive, interactive visualizations.
   - Features:
     - Multiple line charts
     - Per-hour hover summaries
     - Independent y-axis scaling per metric
     - Legend placement for clarity
   - Separate views:
     - **Three small panels** for real-time metrics
     - **One large panel** for weekly historical data

---

## 🧠 Advanced Concepts Used

### 1. **Long-Form Data for Visualization**
Most charting libraries prefer a **long** structure:
```text
time           | metric           | value
2025-08-04 01:00 | Temperature (°F) | 85.2
2025-08-04 01:00 | Wind Speed (mph) | 5.6
