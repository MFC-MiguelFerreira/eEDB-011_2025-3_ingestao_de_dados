import json
import uuid
import boto3
import awswrangler as wr
import pandas as pd
import os

DATABASE = "atividade7"
OUTPUT_BUCKET = os.environ.get("raw_bucket_name", "atividade7-533267025636")

def handler(event, context):
    print(event)
    results = []

    for record in event["Records"]:
        body = json.loads(record["body"])
        cnpj = body.get("cnpj")
        print(f"working with CNPJ {cnpj}")

        if not cnpj or cnpj == '':
            df_bancos = pd.DataFrame(columns=["name", "cnpj", "segment"])
            print("not retrieving from bancos")
        else:
            query = f"SELECT * FROM {DATABASE}.bancos WHERE cnpj = '{cnpj}'"
            df_bancos = wr.athena.read_sql_query(
                sql=query,
                database=DATABASE,
                ctas_approach=False,
            )
            print(f"data retrieved from bancos info\n{df_bancos.shape}")
        print(f"banco record found for CNPJ {df_bancos.shape}")

        df_event = pd.DataFrame([body])
        df_joined = df_event.merge(df_bancos[["cnpj", "segment"]], on="cnpj", how="left", suffixes=("", "_right"))
        results.append(df_joined)
        print(f"result appended: {len(results)}")

    if results:
        final_df = pd.concat(results, ignore_index=True)
        schema = {
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
            "segment": "string"
        }
        final_df = final_df.astype(schema)[schema.keys()]
        final_df = final_df.drop_duplicates(subset=["cnpj", "name"], keep="last")
        print(f"final dataframe info:\n{final_df.shape}")

        wr.athena.to_iceberg(
            df=final_df,
            database=DATABASE,
            table="reclamacoes_augmented",
            merge_cols=["cnpj", "name"], 
            merge_condition="update",
            merge_match_nulls=False,
            schema_evolution=False,
            mode="append",
            keep_files=False,  # Set to False to prevent duplicates from retained temp files
            temp_path=f"s3://{OUTPUT_BUCKET}/athena-staging/{uuid.uuid4()}",
            s3_output=f"s3://{OUTPUT_BUCKET}/atividade5/"
        )
        print(f"inserted {len(final_df)} records into reclamacoes_augmented")
    else:
        print("no records to insert")

    return {"status": "done"}
