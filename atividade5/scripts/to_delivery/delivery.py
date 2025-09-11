from pathlib import Path

from airflow.sdk import task
import polars as pl

@task()
def to_delivery():
    print("Starting to_delivery")

    base_path = Path(__file__).resolve().parents[2] / "datalake"
    trusted_bancos_path = base_path / f"trusted/bancos/bancos.parquet"
    trusted_glassdoor_path = base_path / f"trusted/glassdoor/glassdoor.parquet"
    trusted_reclamacoes_path = base_path / f"trusted/reclamacoes/reclamacoes.parquet"
    delivery_path = base_path / f"delivery/"
    delivery_path.mkdir(parents=True, exist_ok=True)
    print(f"Processing data: \n\tfrom: {base_path} \n\tto: {delivery_path}")

    bancos = pl.read_parquet(f"{trusted_bancos_path}")
    print(f"bancos data read from trusted: {bancos.shape}")
    glassdoor = pl.read_parquet(f"{trusted_glassdoor_path}")
    print(f"glassdoor data read from trusted: {glassdoor.shape}")
    reclamacoes = pl.read_parquet(f"{trusted_reclamacoes_path}")
    print(f"reclamacoes data read from trusted: {reclamacoes.shape}")

    # merge glassdoor bancos
    result = glassdoor.join(
        bancos,
        on="cnpj",
        how="left",
        suffix="_bancos1"
    ).join(
        bancos,
        on="name",
        how="left",
        suffix="_bancos2"
    )
    result = result.with_columns([
        pl.coalesce([pl.col("cnpj"), pl.col("cnpj_bancos2")]).alias("cnpj"),
        pl.coalesce([pl.col("segment"), pl.col("segment_bancos1"), pl.col("segment_bancos2")]).alias("segment")
    ])
    result = result.drop(["segment_bancos1", "name_bancos1", "segment_bancos2", "cnpj_bancos2"]).unique()

    # merge reclamacoes
    result_cnpj = result.join(
        reclamacoes,
        on="cnpj",
        how="left",
        suffix="_reclamacoes"
    )
    result_name = result.join(
        reclamacoes,
        on="name",
        how="left",
        suffix="_reclamacoes"
    )
    result = pl.concat([result_cnpj, result_name], how="diagonal").unique()
    print(f"Result data treated trusted: {result.shape}")

    result.write_parquet(f"{delivery_path}/delivery.parquet")
    print(f"Data wrote on trusted")

    print("Finishing to_delivery")

if __name__ == "__main__":
    to_delivery()