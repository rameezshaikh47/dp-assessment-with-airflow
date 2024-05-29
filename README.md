# dp-assessment-with-airflow

## Introduction 

This repository contains code to read csv files from the local directtroy, apply data validation rules and store the data in PostgreSQL database. 
The input csv files are expected to be in the format as given [here](https://www.kaggle.com/datasets/atulanandjha/temperature-readings-iot-devices/data).
Apache Airflow is used as an orchestration tool. 

## Prerequisites
As a prerequisite to run this code, you will need the following tools installed on your local machine

1. [Docker Desktop](https://www.docker.com/products/docker-desktop/) - To build and run docker containers
2. [PgAdmin](https://www.pgadmin.org/download/) - To browse and query data
3. Optinally and IDE such as PyCharm or Visual Studio Code


## Environment Setup

To setup the environment, follow the steps given below

1. Clone the git repository and navigate to the project directory `dp-assessment-with-airflow`
   ```
   git clone git@github.com:rameezshaikh47/dp-assessment-with-airflow.git
   ```
   ```
   cd dp-assessment-with-airflow
   ```

2. The project directory has 2 files `Dockerfile` and `docker-compose.yaml` which are used to build and run the docker images for Apache Airflow and PostgreSQL.
   Execute the below commands in your terminal to build the image
   ```
   docker build . --tag local_extending_airflow:latest
   ```
3. Once the image is build successfully, run below command to initailze the airflow's metadata database 
   ```
   docker compose up airflow-init
   ```
4. Run Apache Airflow and PostgreSQL services
   ```
   docker-compose up
   ```

Airflow web UI can now be accessed using the URL: http://localhost:8080/
**Username:** airflow
**Password:** airflow


5. Create a connection from Airflow to PostgreSQL 

   
   


