import os
import json

import awswrangler as wr
import boto3
import pandas as pd

BUCKET = os.environ.get("raw_bucket_name", "atividade7-533267025636")
SQS_URL = os.environ.get("sqs_url", "https://sqs.us-east-1.amazonaws.com/533267025636/atividade7-producer-queue")

def handler(event, context):
    print("starting the lambda")
    s3_folder = f"s3://{BUCKET}/reclamacoes/"
    queue_url = f"{SQS_URL}"
    sqs = boto3.client("sqs")
    print(f"s3: {s3_folder} and sqs: {queue_url}")

    # List all CSV files in the folder
    files = wr.s3.list_objects(s3_folder, suffix=".csv")
    for file_path in files:
        print(f"reading the data from {file_path}")

        # Read CSV into DataFrame
        df = wr.s3.read_csv(file_path, sep=";", encoding="utf-8")
        print(f"date read {df.shape}")

        column_rename = {
            "CNPJ IF": "cnpj",
            "Instituição financeira": "name",
            "Categoria": "category",
            "Tipo": "type",
            "Trimestre": "quarter",
            "Ano": "year",
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
            "cnpj": "string",
            "name": "string",
            "category": "string",
            "type": "string",
            "quarter": "string",
            "year": "string",
            "complaint_index": "string",
            "regulated_complaints_upheld": "string",
            "regulated_complaints_other": "string",
            "unregulated_complaints": "string",
            "total_complaints": "string",
            "total_clients_ccs_scr": "string",
            "clients_ccs": "string",
            "clients_scr": "string",
        }
        reclamacoes_treated = df.rename(columns=column_rename)[column_rename.values()]
        reclamacoes_treated["cnpj"] = reclamacoes_treated["cnpj"] \
            .str.strip() \
            .str.lower()
        reclamacoes_treated["cnpj"] = reclamacoes_treated["cnpj"].replace("", pd.NA)
        reclamacoes_treated["name"] = reclamacoes_treated["name"] \
            .str.strip() \
            .str.lower() \
            .str.replace(r"\s*\(conglomerado\)$|\s*s\.a\.?|\s*s\/a|ltda\.?$", "", regex=True) \
            .str.strip()
        reclamacoes_treated = reclamacoes_treated.astype(reclamacoes_schema)
        print(f"data amount {reclamacoes_treated.shape}")

        # Send each row as a JSON message to SQS
        for _, row in reclamacoes_treated.iterrows():
            message_body = json.dumps(row.to_dict(), default=str)
            sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
        print(f"data sent to {queue_url}")