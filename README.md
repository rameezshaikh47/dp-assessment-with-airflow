# dp-assessment-with-airflow

## Introduction 

This repository contains code to read csv files from the local directory, apply data validation rules and store the data in PostgreSQL database. 
The input csv files are expected to be in the format as given [here](https://www.kaggle.com/datasets/atulanandjha/temperature-readings-iot-devices/data).
Apache Airflow is used as an orchestration tool. 

## Prerequisites
As a prerequisite to run this code, you will need the following tools installed on your local machine

1. [Docker Desktop](https://www.docker.com/products/docker-desktop/) - To build and run docker containers
2. [PgAdmin](https://www.pgadmin.org/download/) - To browse and query data
3. Optionally and IDE such as PyCharm or Visual Studio Code


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


5. Create a new database in PostgreSQL

   Connect to PostgreSQL DB using PgAdmin with below credentials

   **Host Name:** localhost
   
   **Username:** airflow

   **Password:** airflow

   **Port:** 5432

   **Maintenance Database:** airflow
   
   Once connected, create a database by name `data_platform`. This database will be used to store the processed data and to create a connection in Airflow.
   ```
   CREATE DATABASE data_platform;
   ```

7. Create Airflow connection to PostgreSQL DB

   Login to Airflow using the URL and credentials given in step (4)

   Once logged in, Click in Admin >> Connections

   <img width="921" alt="image" src="https://github.com/rameezshaikh47/dp-assessment-with-airflow/assets/24712733/109e6126-f5e9-46b8-ae19-6cdc4ac92f77">

   Create a new connection by clicking on <img width="31" alt="image" src="https://github.com/rameezshaikh47/dp-assessment-with-airflow/assets/24712733/88fda944-3ed4-47b3-b035-6ebc61394527"> icon. This will open a new window to create the connection. Enter following details

   **Connection Id:** postgresql-data-platform
   
   **Connection Type:** Postgres
   
   **Host:** postgres
   
   **Schema:** data_platform
   
   **Login:** airflow
   
   **Password:** airflow
   
   **Port:** 5432

   Test the connection and save

   <img width="1067" alt="image" src="https://github.com/rameezshaikh47/dp-assessment-with-airflow/assets/24712733/d879c53b-a655-461c-ba0f-b714ec73a65b">

   
## Create Schema and Tables 

Once the environment is setup, the next step is to create the required Schema and Tables 
This is done using an Airflow DAG named. [schema_creation_dag](http://localhost:8080/dags/schema_creation_dag/grid)

* Ensure the DAG is enabled by clicking on the toggle button highlighted in the left side of the screenshot below
* Execute the DAG by clicking on the play button highlighted in the right side of the screenshot below
* This DAG is supposed to be executed only for the first time after the environment is setup
* The second task `create_database` in DAG is expected to fail as it tries to create the database `data_platform` which is already created in earlier steps

<img width="1440" alt="image" src="https://github.com/rameezshaikh47/dp-assessment-with-airflow/assets/24712733/2bb81b64-ce9a-4293-a262-48af71c6e51e">

## Executing the Data Pipeline 

**Note:** The `/dp-assessment-with-airflow` of the cloned repository is mounted to the docker containers when the containers are deployed. Any code changes done to local cloned repository or files added to this directory will be accessible in docker pods. This is done to share files and code between local machine and docker containers.

1. To read the csv files, apply data validation and store the data to Postgres table - DAG [process_iot_temp_files](http://localhost:8080/dags/process_iot_temp_files/graph) is used
2. The files to be processed should be placed in the directory `/dp-assessment-with-airflow/data/input` of the cloned repository. The file should have an extension `.csv`.
3. Now open the DAG [process_iot_temp_files](http://localhost:8080/dags/process_iot_temp_files/graph) and ensure it is enabled and execute it by clicking on the play button
4. If the file in not in the correct format - That is the column names and order of columns are not correct then the file will be moved to  `/dp-assessment-with-airflow/data/rejected` directory
5. If the file is in correct format then it will be processed further.
6. Once the DAG is successfully executed for valid files. The logs of the `log_processed_record_counts` task will show a summary of data that has been processed. Click on this task to view the logs. In a real world scenario this summary can be sent out as a notification or a slack message. 

**Logging Task**

<img width="1431" alt="image" src="https://github.com/rameezshaikh47/dp-assessment-with-airflow/assets/24712733/21f923bc-d933-4b6d-bd15-12071207da4c">

**Log Summary**

<img width="1431" alt="image" src="https://github.com/rameezshaikh47/dp-assessment-with-airflow/assets/24712733/1ee1fdb7-6f18-4e23-8433-d4776f30f512">


