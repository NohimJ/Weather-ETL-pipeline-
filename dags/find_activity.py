from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime
import requests
import pandas as pd
import logging
from sqlalchemy import create_engine, Table, Column, String, Float, MetaData

# Airflow Variables
API_KEY = Variable.get("weatherapi_key")
CITY = Variable.get("weather_city", default_var="Vancouver")
DB_URI = Variable.get(
    "weather_db_uri",
    default_var="postgresql+psycopg2://weather_user:password123@host.docker.internal:5433/weather_db"
)

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    tags=["etl", "weather"],
    catchup=False,
)
def weather_etl_pipeline():

    @task
    def extract_weather():
        """Extract current weather from WeatherAPI.com"""
        url = "https://api.weatherapi.com/v1/current.json"
        params = {"key": API_KEY, "q": CITY, "aqi": "no"}
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            logging.info("Successfully extracted weather data")
            return data
        except Exception as e:
            logging.error(f"Weather API request failed: {e}")
            raise

    @task
    def transform_weather(data: dict):
        """Transform raw API data into JSON-serializable dict"""
        location = data.get("location", {})
        current = data.get("current", {})
        transformed = {
            "city": location.get("name"),
            "country": location.get("country"),
            "temp_c": current.get("temp_c"),
            "condition": current.get("condition", {}).get("text"),
            "humidity": current.get("humidity"),
            "wind_kph": current.get("wind_kph"),
            "timestamp": current.get("last_updated")  # keep as string
        }
        logging.info(f"Transformed data: {transformed}")
        return transformed

    @task
    def load_weather(data: dict):
        """Load data into PostgreSQL and create table if it doesn't exist"""
        try:
            engine = create_engine(DB_URI)
            metadata = MetaData(bind=engine)

            # Define table
            weather_table = Table(
                "weather_log",
                metadata,
                Column("city", String),
                Column("country", String),
                Column("temp_c", Float),
                Column("condition", String),
                Column("humidity", Float),
                Column("wind_kph", Float),
                Column("timestamp", String)  # store as string for simplicity
            )

            # Create table if it doesn't exist
            metadata.create_all(engine)

            # Insert new row
            df = pd.DataFrame([data])
            df.to_sql("weather_log", engine, if_exists="append", index=False)
            logging.info("Data loaded into PostgreSQL successfully.")
        except Exception as e:
            logging.error(f"Failed to load data into PostgreSQL: {e}")
            raise

    @task
    def analytics():
        """Compute basic analytics: 7-day average temperature"""
        try:
            engine = create_engine(DB_URI)
            df = pd.read_sql("SELECT * FROM weather_log", engine)

            # Convert timestamp string back to datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            last_7_days = df[df['timestamp'] >= pd.Timestamp.now() - pd.Timedelta(days=7)]
            avg_temp = last_7_days['temp_c'].mean()
            logging.info(f"Average temperature over last 7 days in {CITY}: {avg_temp:.2f}°C")
            return avg_temp
        except Exception as e:
            logging.error(f"Analytics task failed: {e}")
            raise

    # ETL workflow
    raw = extract_weather()
    transformed = transform_weather(raw)
    load_weather(transformed)
    analytics()

weather_etl_pipeline_dag = weather_etl_pipeline()
