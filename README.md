# Setting Up Shopsmart ETL

This guide will walk you through setting up a data pipeline to load data into PostgreSQL using Airflow and Apache Spark. This project are use Apache Airflow to Orchestration tool manage and monitor pipeline is call dags. In processing are use Apache Spark to receive spark-submit from airflow are use distributed data processing. Database are use PostgresSql to keep data in relational Database.

## Step 1: Clone the Repository
Clone the repository containing the necessary files for the setup:

```bash
git clone https://github.com/natthakitnga/shopsmart_etl.git
```

## Step 2: Build custom airflow image and spark image

#### build custom airflow image
pull airflow image latest version of Airflow from Dockerhub [apache/airflow official](https://hub.docker.com/r/apache/airflow/tags)
```bash
docker pull apache/airflow:latest
```
then build the custom airflow image. Use build airflow images name airflow_local

```bash
cd ./docker_airflow
docker build -t airflow_local .
```
#### build custom spark image
pull spark image from bitnami
```bash
docker pull bitnami/spark:latest
```
then build the custom spark image. Use build spark images name spark_local

```bash
cd ./docker_spark
docker build -t spark_local .
```

## Step 3: Start Docker Containers with Docker Compose
back to shopsmart_etl dir. This repos packing service airflow , postgres and spark inside docker-compose. After prepare custom images can running docker-compose
```bash
docker-compose up -d --build
```
all of service are just starting in docker

## Step 4: run pipeline ingestion
in this Repos has two dags in ./dags use to ingest data to postgres. after running all of service can find airflow ui to monitor and trigger pipeline using this link [localhost:8080](http://localhost:8080/)