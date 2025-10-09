# Use official Airflow base image
FROM apache/airflow:2.9.3

# Switch to root to install system dependencies
USER root

# Install any system-level dependencies (optional)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user before pip install
USER airflow

# Copy requirements and install them as airflow user
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
