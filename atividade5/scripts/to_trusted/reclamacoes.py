from pathlib import Path

from airflow.sdk import task
import polars as pl

@task()
def to_trusted_reclamacoes():
    print("Starting to_trusted reclamacoes")

    source_date = "reclamacoes"
    base_path = Path(__file__).resolve().parents[2] / "datalake"
    raw_path = base_path / f"raw/{source_date}/"
    trusted_path = base_path / f"trusted/{source_date}/"
    trusted_path.mkdir(parents=True, exist_ok=True)
    print(f"Processing data: \n\tfrom: {raw_path} \n\tto: {trusted_path}")

    reclamacoes = pl.read_parquet(f"{raw_path}/{source_date}.parquet")
    print(f"Data read from raw: {reclamacoes.shape}")

    column_rename = {
        "CNPJ IF": "cnpj",
        "Instituição financeira": "name",
        "Categoria": "category",
        "Tipo": "type",
        "Ano": "year",
        "Trimestre": "quarter",
        "Índice": "complaint_index",
        "Quantidade de reclamações reguladas procedentes": "regulated_complaints_upheld",
        "Quantidade de reclamações reguladas - outras": "regulated_complaints_other",
        "Quantidade de reclamações não reguladas": "unregulated_complaints",
        "Quantidade total de reclamações": "total_complaints",
        "Quantidade total de clientes – CCS e SCR": "total_clients_ccs_scr",
        "Quantidade de clientes – CCS": "clients_ccs",
        "Quantidade de clientes – SCR": "clients_scr",
    }
    reclamacoes_schema = {
        "cnpj": pl.String(),
        "name": pl.String(),
        "category": pl.String(),
        "type": pl.String(),
        "quarter": pl.String(),
        "year": pl.Int32(),
        "complaint_index": pl.Float32(),
        "regulated_complaints_upheld": pl.Int32(),
        "regulated_complaints_other": pl.Int32(),
        "unregulated_complaints": pl.Int32(),
        "total_complaints": pl.Int32(),
        "total_clients_ccs_scr": pl.Int32(),
        "clients_ccs": pl.Int32(),
        "clients_scr": pl.Int32(),
    }
    reclamacoes_treated = reclamacoes \
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
        ) \
        .cast(reclamacoes_schema, strict=False) \
        .select(reclamacoes_schema.keys())

    reclamacoes_treated.write_parquet(f"{trusted_path}/{source_date}.parquet")
    print(f"Data wrote on trusted")

    print("Finishing to_trusted reclamacoes")

if __name__ == "__main__":
    to_trusted_reclamacoes()