from pathlib import Path

from airflow.sdk import task
import polars as pl

@task()
def to_trusted_glassdoor():
    print("Starting to_trusted banco")

    source_date = "glassdoor"
    base_path = Path(__file__).resolve().parents[2] / "datalake"
    raw_path = base_path / f"raw/{source_date}/"
    trusted_path = base_path / f"trusted/{source_date}/"
    trusted_path.mkdir(parents=True, exist_ok=True)
    print(f"Processing data: \n\tfrom: {raw_path} \n\tto: {trusted_path}")

    glassdoor = pl.read_parquet(f"{raw_path}/{source_date}.parquet")
    print(f"Data read from raw: {glassdoor.shape}")

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
    glassdoor_treated = glassdoor \
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
    glassdoor_treated = glassdoor_treated.group_by("name").agg(aggregations)
    print(f"Data group by name and filtered: {glassdoor.shape}")

    glassdoor_treated.write_parquet(f"{trusted_path}/{source_date}.parquet")
    print(f"Data wrote on trusted")

    print("Finishing to_trusted banco")

if __name__ == "__main__":
    to_trusted_glassdoor()