# Use the official Airflow image as base
FROM apache/airflow:slim-latest-python3.11

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME

USER airflow

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip

# Copy the requirements file into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt