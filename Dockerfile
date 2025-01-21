FROM apache/airflow:2.6.1

USER root

# Install any additional packages you need
RUN apt-get update && apt-get install -y --no-install-recommends vim

USER airflow

# Copy requirements file if you have any additional Python dependencies
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt