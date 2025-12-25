# 🌦️ Weather ETL Pipeline with Airflow, Docker & PostgreSQL

This project implements a production-style ETL pipeline that takes in real-time weather data from WeatherAPI.com, transforms it, then stores it in a PostgreSQL database using Apache Airflow. The entire system is containerized using Docker and orchestrated via the Astro runtime.

This project is designed to demonstrate data engineering skills including ETL design, workflow orchestration, containerization, and database integration that are applicable to areas in Quantitative Finance and Data S

---

## Architecture Overview

Extract:
- Pulls live weather data from a public Weather API using city-based queries

Transform:
- Cleans and structures raw JSON data
- Selects relevant metrics such as temperature, humidity, wind speed, and conditions
- Normalizes timestamps into a database-friendly format

Load:
- Inserts transformed data into a PostgreSQL table (`weather_log`)
- Automatically creates the table if it does not exist

Orchestration:
- Apache Airflow DAG manages task dependencies and execution

Infrastructure:
- Docker containers for Airflow services and PostgreSQL
- Astro CLI for local development

---

## Tech Stack

- Python 3.12
- Apache Airflow
- PostgreSQL
- Docker & Docker Compose
- Astro CLI
- Pandas
- SQLAlchemy
- psycopg2

---

## Project Structure

.<img width="254" height="231" alt="Screenshot 2025-12-25 at 1 01 47 AM" src="https://github.com/user-attachments/assets/bc150dab-262f-4b2c-bee2-1b608e16860f" />


<img width="254" height="265" alt="Screenshot 2025-12-25 at 1 03 03 AM" src="https://github.com/user-attachments/assets/9a7ffcc7-1251-4de9-bdf2-0f2559c31a36" />



## How to Run

1. Clone the repository

2. Start Airflow and PostgreSQL

astro dev start

Airflow UI will be available at:
http://localhost:8080

---

3. Trigger the DAG

- Open the Airflow UI
- Enable the `weather_etl_pipeline` DAG
- Trigger it manually or let it run on its schedule

---

4. View the Data

Connect to PostgreSQL:

psql postgresql://weather_user:password123@localhost:5433/weather_db

Query the data:

SELECT *
FROM weather_log
ORDER BY timestamp DESC
LIMIT 10;

---

<img width="712" height="191" alt="Screenshot 2025-12-25 at 1 03 36 AM" src="https://github.com/user-attachments/assets/50295cb0-06cc-4008-aa85-193e0cfbb30d" />


## Key Concepts Demonstrated

- ETL pipeline design
- Apache Airflow DAG development
- Task dependency management
- XCom-safe data passing
- Containerized data infrastructure
- PostgreSQL schema management
- Debugging distributed workflows

---

## Future Improvements

- Historical backfilling
- Table partitioning by date
- Data quality checks
- Integration with Snowflake or BigQuery
- Cloud deployment (AWS ECS / RDS)
- dbt integration for transformations

---

## Author

Nohim Jayasinghe  
Simon Fraser University  
Data Science
