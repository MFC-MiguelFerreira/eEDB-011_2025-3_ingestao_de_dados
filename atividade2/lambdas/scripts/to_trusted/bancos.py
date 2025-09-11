import os

import polars as pl

raw_bucket_name = os.environ.get("raw_bucket_name")
trusted_bucket_name = os.environ.get("trusted_bucket_name")

def lambda_handler(event, context):
    print("Starting lambda banco")

    raw_s3_path = f"s3://{raw_bucket_name}/bancos/*.tsv"
    bancos = pl.read_csv(raw_s3_path, separator="\t", infer_schema=False)
    print(f"Data read from raw: {raw_s3_path}")

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

    trusted_s3_path = f"s3://{trusted_bucket_name}/bancos/bancos.parquet.snappy"
    bancos_treated.write_parquet(trusted_s3_path, compression="snappy")
    print(f"Data wrote on trusted: {trusted_bucket_name}")

    print("Finishing lambda banco")