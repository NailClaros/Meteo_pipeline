def test_example():
    assert True

import pytest
import pandas as pd
from io import StringIO
from db import file_already_uploaded, upload_weather_data_to_db

# -----------------------------
# Helper fixture to insert dummy CSV into S3-like mock
# -----------------------------
@pytest.fixture
def sample_weather_csv():
    csv_content = """location_id,temperature (°F),cloud cover (%),surface pressure (hPa),wind speed (80m elevation) (mph),wind direction (80m elevation) (°),time
LOC1,70,50,1012,5,180,2025-07-20 12:00:00
LOC2,75,20,1010,3,90,2025-07-20 13:00:00
"""
    return StringIO(csv_content)