from pathlib import Path

from airflow.sdk import task
import polars as pl

@task()
def source_to_raw(source_date: str):
    print("Starting to raw script")

    base_path = Path(__file__).resolve().parents[2] / "datalake"
    source_path = base_path / f"source/{source_date}/"
    raw_path = base_path / f"raw/{source_date}/"
    raw_path.mkdir(parents=True, exist_ok=True)
    print(f"Coping data: \n\tfrom: {source_path} \n\tto: {raw_path}")

    if source_date == "bancos":
        df = pl.scan_csv(f"{source_path}/*.tsv", separator="\t")
    elif source_date == "glassdoor":
        dfs = [pl.scan_csv(path, separator="|") for path in source_path.iterdir()]
        df = pl.concat(dfs, how="diagonal")
    elif source_date == "reclamacoes":
        df = pl.scan_csv(f"{source_path}/*.csv", separator=";")
    else:
        raise Exception(f"The is no source_date: {source_date}")
    print("Data scanned from the source")
    
    df.sink_parquet(f"{raw_path}/{source_date}.parquet")
    print("Data sink in the target")

    print("Finishing to raw script")
