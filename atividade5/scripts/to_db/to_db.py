from pathlib import Path

from airflow.sdk import task
import polars as pl

@task()
def to_db():
    print("Starting to_db")

    base_path = Path(__file__).resolve().parents[2] / "datalake"
    delivery_path = base_path / f"delivery/delivery.parquet"
    print(f"Processing data: \n\tfrom: {base_path} \n\tto: {delivery_path}")
    
    print("lambda start")
    DB_HOST = "atividade5-postgres-db"
    DB_NAME = "atividade5"
    DB_PORT = "5432"
    DB_USER = "atividade5"
    TABLE_NAME = "atividade5"
    print("secret value got from secrets mananger")

    # Read parquet file into DataFrame
    df = pl.read_parquet(delivery_path)
    print(f"data shape: {df.shape}")

    db_uri = f"postgresql://{DB_USER}:mypassword@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    df.write_database(
        table_name=TABLE_NAME,
        connection=db_uri,
        if_table_exists="replace",
        engine="adbc" # Or "sqlalchemy" if preferred, with corresponding setup
    )

    print("Finishing to_db")

if __name__ == "__main__":
    to_db()
