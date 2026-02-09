import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime

DATA_PATH = "data/yellow_tripdata_2025-01.parquet"

DB_USER = "postgres"
DB_PASS = "4121"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "nyc_taxi"
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TABLE_NAME = "yellow_taxi_trips"

logging.basicConfig(filename='taxi.log',
                    level=logging.INFO,
                    format = '%(asctime)s  - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract():
    logger.info("Reading Parquet File")
    df = pd.read_parquet(DATA_PATH)
    logger.info(f"Loaded {len(df):,} rows")
    return df

def transform(df: pd.DataFrame):
    logger.info("Cleaning & Transforming Data")
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                           "trip_distance"])
    df["passenger_count"] = df["passenger_count"].fillna(df["passenger_count"].median())
    df = df[df["trip_distance"] >= 0.01]
    df = df[df["total_amount"] >= 0]
    df["trip_duration_minutes"] = ((df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).
                                   dt.total_seconds() / 60)
    df["avg_speed_mph"] = df["trip_distance"] / (df["trip_duration_minutes"] / 60 + 1e-6)
    df = df[df["trip_duration_minutes"].between(1, 360)]
    df = df[df["avg_speed_mph"].between(2, 80)]
    float_cols = df.select_dtypes(include="float").columns
    df[float_cols] = df[float_cols].round(2)
    logger.info(f"After Cleaning: {len(df):,} rows remain")
    return df

def load(df: pd.DataFrame):
    logger.info(f"Loading to {DB_URL} table: {TABLE_NAME}")
    engine = create_engine(DB_URL)
    df.to_sql(TABLE_NAME, engine, if_exists="append", index=False, chunksize=
              100000, method="multi")
    logger.info("Load Finished")

def basic_validation():
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        row_count = pd.read_sql(f"SELECT COUNT(*) FROM {TABLE_NAME}", conn).iloc[0, 0]
        sample = pd.read_sql(f"SELECT * FROM {TABLE_NAME} LIMIT 5", conn)
    logger.info(f"Rows in database: {row_count:,}")
    logger.info("\nSample rows:\n" + sample.to_string(index=False))

if __name__ == "__main__":
    start_time = datetime.now()
    raw_df = extract()
    cleaned_df = transform(raw_df)
    load(cleaned_df)
    basic_validation()
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"ETL Completed in {elapsed:.1f} seconds")


