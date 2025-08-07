
# 🌦 NC Weather Data Dashboard

An **end-to-end Data Engineering + Backend Engineering** project that ingests live weather data, stores it in the cloud, and visualizes it with **interactive charts**.

Built by **Nail Claros**, a Computer Science graduate from UNC Charlotte, as a way to **learn, practice, and showcase** real-world backend and data engineering skills.
---

# Weather Data Pipeline and Dashboard

This project is a complete end-to-end weather data pipeline and live dashboard that ingests hourly weather data from the Open-Meteo API, processes and stores it using AWS and PostgreSQL, and visualizes it in an interactive frontend built with Streamlit and Altair.

---

## Overview

* Collects real-time weather data from the Open-Meteo API for cities like Charlotte, Raleigh, and Greensboro
* Schedules and automates ingestion using AWS Lambda and EventBridge
* Stores raw data in Amazon S3 and final processed data in PostgreSQL (Neon)
* Provides a live interactive dashboard built with Streamlit and Altair
* Includes caching and simulated session-based cooldown logic using Redis

---

## Architecture

1. **Data Ingestion**

   * AWS Lambda function triggered by EventBridge
   * Pulls hourly weather data every day (temperature, wind speed, cloud cover, etc.)

2. **Data Processing**

   * Normalizes and reformats data into long-form structure
   * Cleans missing values and handles API edge cases

3. **Storage**

   * Raw data temporarily stored in S3
   * Final processed data stored in PostgreSQL via Neon

4. **Deployment & Compatibility**

   * Developed locally with Python 3.13.5
   * Used Docker to simulate AWS Lambda’s Linux environment
   * Created Lambda Layers for managing external dependencies

5. **Visualization**

   * Streamlit + Altair dashboard
   * Allows city and metric switching in real-time
   * Responsive Altair charts with interactive tooltips and independent y-axes

6. **Performance Optimization**

   * Streamlit `@st.cache_data` used to locally cache daily and weekly data
   * Redis integrated to simulate a server-side cooldown system per city refresh
   * Although Streamlit doesn’t support persistent sessions, Redis logic was built anyway to understand production-ready session tracking and rate limiting

---

## Key Features

* **Fully Serverless**: Powered by AWS Lambda and EventBridge
* **Real-Time Data**: Ingests and updates weather data hourly
* **Cloud-Native Storage**: Combines S3 and PostgreSQL (Neon) for scalability
* **Cross-Platform Development**: Docker used for compatibility between Windows and Lambda
* **Robust Error Handling**: Logs and manages API failures or missing values
* **Simulated Session Management**: Redis logic included despite Streamlit limitations
* **Interactive Dashboard**: Built with Streamlit and Altair for seamless data exploration

---

## Lessons Learned

* Gained experience deploying Python projects to AWS Lambda
* Learned how to package and manage dependencies with Lambda Layers and various Python versions
* Improved understanding of Docker for cross-environment development
* Practiced building a fault-tolerant ETL pipeline
* Experimented with Redis and theoretical session management in a stateless frontend
* Designed a user-friendly dashboard with high-performance visualizations

---

## Stack

**Languages**: Python 3.13.5 + Python 3.12.x
**Cloud**: AWS Lambda, Amazon S3, EventBridge
**Database**: PostgreSQL (Neon)
**Visualization**: Streamlit, Altair
**Caching**: Redis, Streamlit `@st.cache_data`
**Containerization**: Docker

---

## Future Improvements... One day

* Host frontend with persistent session support (e.g., FastAPI + Redis)
* Store full historical weather records for trend analysis (I have my AWS bucket to hold a weeks worth of data for the free tier)
* Add forecast vs. actual weather comparisons
* Implement email or Slack alerts for extreme weather events

---

## Running Locally

1. Clone the repo
2. Set up a `.env` file with your API keys and Redis/PostgreSQL credentials
3. Run the Streamlit app with:

```bash
streamlit run app.py
```

## To get a similar result for aws lambda, I made a zip folder that lambda will accept in case you would like to try it at home as well. have fun!
