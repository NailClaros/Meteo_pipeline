from datetime import datetime
import os
from awsfuncs import file_exists_in_s3
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from dotenv import load_dotenv

load_dotenv()

LAKE_BUCKET = os.getenv("BUCKET_NAME")

def fetch_and_save_weather_data(date=None, forecast_length=1, past_days=0):
    # Create data folder if it doesn't exist
    os.makedirs("data", exist_ok=True)    

    if date is None:
        date = datetime.now()
        date = date.strftime("%Y-%m-%d")
    
    filename = f"weather_{date}.csv"
    output_path = os.path.join("data", filename)

    file_exists_local = os.path.exists(output_path)
    file_exists_cloud = file_exists_in_s3(bucket_name=LAKE_BUCKET, key=f"weather_{date}.csv")

    if file_exists_local or file_exists_cloud:
        print(f"Data for {date} already exists locally or in S3. Skipping fetch.")
        print(f"Local file exists: {file_exists_local}\nCloud file exists: {file_exists_cloud}")
        return

    # Setup request caching and retries
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Define locations
    locations = [
        {"location_id": "Charlotte", "latitude": 35.216976, "longitude": -80.83189},
        {"location_id": "Raleigh", "latitude": 35.77436, "longitude": -78.64127},
        {"location_id": "Greensboro", "latitude": 36.071556, "longitude": -79.78957}
    ]

    # API request parameters
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": [loc["latitude"] for loc in locations],
        "longitude": [loc["longitude"] for loc in locations],
        "hourly": ["temperature_2m", "cloud_cover", "surface_pressure", "wind_speed_80m", "wind_direction_80m"],
        "timezone": "auto",
        "forecast_days": forecast_length,
        "past_days": past_days,
        "wind_speed_unit": "mph",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch",
    }

    # Fetch data
    responses = openmeteo.weather_api(url, params=params)

    all_dataframes = []

    for i, response in enumerate(responses):
        loc = locations[i]
        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(1).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(2).ValuesAsNumpy()
        hourly_wind_speed_80m = hourly.Variables(3).ValuesAsNumpy()
        hourly_wind_direction_80m = hourly.Variables(4).ValuesAsNumpy()

        time_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

        df = pd.DataFrame({
            "location_id": loc["location_id"],
            "time": time_range,
            "temperature (°F)": hourly_temperature_2m,
            "cloud cover (%)": hourly_cloud_cover,
            "surface pressure (hPa)": hourly_surface_pressure,
            "wind speed (80m elevation) (mph)": hourly_wind_speed_80m,
            "wind direction (80m elevation) (°)": hourly_wind_direction_80m
        })

        all_dataframes.append(df)

    # Combine and save the DataFrame
    final_df = pd.concat(all_dataframes, ignore_index=True)
    final_df.to_csv(output_path, index=False)
    print(f"Weather data saved to '{output_path}'")