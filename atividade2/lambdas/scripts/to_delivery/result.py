import os

import awswrangler as wr
import pandas as pd

trusted_bucket_name = os.environ.get("trusted_bucket_name")
delivery_bucket_name = os.environ.get("delivery_bucket_name")

def lambda_handler(event, context):
    print("Starting lambda banco")

    bancos = wr.s3.read_parquet(f"s3://{trusted_bucket_name}/bancos/")
    glassdoor = wr.s3.read_parquet(f"s3://{trusted_bucket_name}/glassdoor/")
    reclamacoes = wr.s3.read_parquet(f"s3://{trusted_bucket_name}/reclamacoes/")

    # merge glassdoor bancos
    result = glassdoor.merge(
        right=bancos,
        how="left",
        on="cnpj",
        suffixes=("", "_bancos1")
    ).merge(
        right=bancos,
        how="left",
        on="name",
        suffixes=("", "_bancos2")
    )
    result["cnpj"] = result["cnpj"].combine_first(result["cnpj_bancos2"])
    result["segment"] = result["segment"].combine_first(result["segment_bancos1"]).combine_first(result["segment_bancos2"])
    result = result.drop(columns=["segment_bancos1", "name_bancos1", "segment_bancos2", "cnpj_bancos2"]).drop_duplicates()

    # merge reclamacoes
    result = pd.concat(
        [
            result.merge(right=reclamacoes, how="left", on="cnpj", suffixes=("", "_reclamacoes")),
            result.merge(right=reclamacoes, how="left", on="name", suffixes=("", "_reclamacoes")),
        ]
    ).drop_duplicates()

    delivery_s3_path = f"s3://{delivery_bucket_name}/result/result.parquet.snappy"
    wr.s3.to_parquet(df=result, path=delivery_s3_path, compression="snappy")
    print(f"Data wrote on delivery: {delivery_bucket_name}")

    print("Finishing lambda banco")