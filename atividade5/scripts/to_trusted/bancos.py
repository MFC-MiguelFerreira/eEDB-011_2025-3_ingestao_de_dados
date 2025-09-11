from pathlib import Path

from airflow.sdk import task
import polars as pl

@task()
def to_trusted_bancos():
    print("Starting to_trusted banco")

    source_date = "bancos"
    base_path = Path(__file__).resolve().parents[2] / "datalake"
    raw_path = base_path / f"raw/{source_date}/"
    trusted_path = base_path / f"trusted/{source_date}/"
    trusted_path.mkdir(parents=True, exist_ok=True)
    print(f"Processing data: \n\tfrom: {raw_path} \n\tto: {trusted_path}")

    bancos = pl.read_parquet(f"{raw_path}/{source_date}.parquet")
    print(f"Data read from raw: {bancos.shape}")

    column_rename = {
        "CNPJ": "cnpj",
        "Nome": "name",
        "Segmento": "segment",
    }
    bancos_treated = bancos \
        .rename(column_rename) \
        .with_columns(
            pl.col("cnpj")
                .cast(pl.Utf8)
                .str.strip_chars(),
            pl.col("name")
                .str.strip_chars()
                .str.to_lowercase()
                .str.replace(r"\s*-\s*prudencial$|s\.a\.?\s*-\s*prudencial$|s\.a\.?$|s\/a$|ltda\.?$", "")
                .str.strip_chars()
        )

    bancos_treated.write_parquet(f"{trusted_path}/{source_date}.parquet")
    print(f"Data wrote on trusted")

    print("Finishing to_trusted banco")

if __name__ == "__main__":
    to_trusted_bancos()