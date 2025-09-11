import os
import glob

import polars as pl
import boto3

raw_bucket_name = os.environ.get("raw_bucket_name")
trusted_bucket_name = os.environ.get("trusted_bucket_name")

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    print("Starting lambda banco")

    response = s3_client.list_objects_v2(Bucket=raw_bucket_name, Prefix="empregados")
    raw_s3_path = [f"s3://{raw_bucket_name}/{obj['Key']}" for obj in response.get('Contents')]
    dataframes = [pl.scan_csv(path, separator="|").collect() for path in raw_s3_path]
    empregados = pl.concat(dataframes, how="diagonal")
    print(f"Data read from raw: {raw_s3_path}")

    column_rename = {
        "CNPJ": "cnpj",
        "Nome": "name",
        "Segmento": "segment",
        "employer-revenue": "revenue",
        "reviews_count": "reviews_count",
        "culture_count": "culture_count",
        "salaries_count": "salaries_count",
        "benefits_count": "benefits_count",
        "Geral": "general_score",
        "Cultura e valores": "culture_score",
        "Diversidade e inclusão": "diversity_score",
        "Qualidade de vida": "quality_of_life_score",
        "Alta liderança": "leadership_score",
        "Remuneração e benefícios": "compensation_score",
        "Oportunidades de carreira": "career_opportunities_score",
        "Recomendam para outras pessoas(%)": "recommend_percent",
        "Perspectiva positiva da empresa(%)": "positive_outlook_percent",
        "match_percent": "match_percent"
    }
    empregados_treated = empregados \
        .rename(column_rename) \
        .select(column_rename.values()) \
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
    print("Columns renamed and selected")

    string_lastest_cols = ["cnpj", "segment", "revenue"]
    numeric_sum_cols = ["reviews_count", "culture_count", "salaries_count", "benefits_count"]
    numeric_mean_cols = ["culture_score", "diversity_score", "quality_of_life_score", "leadership_score", "compensation_score", "career_opportunities_score", "recommend_percent", "positive_outlook_percent", "match_percent"]
    aggregations = (
        [pl.col(c).forward_fill().last().alias(c) for c in string_lastest_cols] +
        [pl.col(c).sum().alias(c) for c in numeric_sum_cols] +
        [pl.col(c).mean().alias(c) for c in numeric_mean_cols]
    )
    empregados_filtered = empregados_treated.group_by("name").agg(aggregations)
    print("Data group by name and filtered")

    trusted_s3_path = f"s3://{trusted_bucket_name}/glassdoor/glassdoor.parquet.snappy"
    empregados_filtered.write_parquet(trusted_s3_path, compression="snappy")
    print(f"Data wrote on trusted: {trusted_bucket_name}")

    print("Finishing lambda banco")