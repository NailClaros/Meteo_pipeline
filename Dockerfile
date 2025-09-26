# Use official Python base image
FROM python:3.12-slim

# Set environment variables (no pyc, UTF-8)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set work directory
WORKDIR /app

# Install system deps (e.g. psycopg2 needs libpq)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the code (except what's in .dockerignore)
COPY . .

# Default command (can override in docker-compose)
CMD ["python", "master.py"]
