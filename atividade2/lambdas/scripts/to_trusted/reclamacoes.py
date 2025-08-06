import os

import awswrangler as wr
import numpy as np

raw_bucket_name = os.environ.get("raw_bucket_name")
trusted_bucket_name = os.environ.get("trusted_bucket_name")

def lambda_handler(event, context):
    print("Starting lambda banco")

    reclamacoes = wr.s3.read_csv(path=f"s3://{raw_bucket_name}/reclamacoes/", sep=";", encoding="latin-1")
    print(f"Data read from raw {reclamacoes.shape}")

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
        "Quantidade total de clientes \x96 CCS e SCR": "total_clients_ccs_scr",
        "Quantidade de clientes \x96 CCS": "clients_ccs",
        "Quantidade de clientes \x96 SCR": "clients_scr",
    }

    reclamacoes_schema = {
        "cnpj": "string",
        "name": "string",
        "category": "string",
        "type": "string",
        "year": "string",
        "quarter": "string",
        "complaint_index": "string",
        "regulated_complaints_upheld": "string",
        "regulated_complaints_other": "string",
        "unregulated_complaints": "string",
        "total_complaints": "string",
        "total_clients_ccs_scr": "string",
        "clients_ccs": "string",
        "clients_scr": "string",
    }

    reclamacoes_treated = reclamacoes.rename(columns=column_rename)[column_rename.values()]
    reclamacoes_treated["cnpj"] = reclamacoes_treated["cnpj"] \
        .str.strip() \
        .str.lower()
    reclamacoes_treated["name"] = reclamacoes_treated["name"] \
        .str.strip() \
        .str.lower() \
        .str.replace(r"\s*\(conglomerado\)$|\s*s\.a\.?|\s*s\/a|ltda\.?$", "", regex=True) \
        .str.strip()
    reclamacoes_treated = reclamacoes_treated.astype(reclamacoes_schema)

    trusted_s3_path = f"s3://{trusted_bucket_name}/reclamacoes/reclamacoes.parquet.snappy"
    wr.s3.to_parquet(df=reclamacoes_treated, path=trusted_s3_path, compression="snappy")
    print(f"Data wrote on trusted: {trusted_bucket_name}")

    print("Finishing lambda banco")