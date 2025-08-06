from io import BytesIO
import json
import os

import boto3
import polars as pl
import psycopg

S3_BUCKET = os.environ.get("delivery_bucket_name")
S3_KEY = "result/result.parquet.snappy"

def lambda_handler(event, context):
    
    print("lambda start")
    DB_HOST = "database-1.cow15th1wubu.us-east-1.rds.amazonaws.com"
    DB_NAME = "public"
    DB_PORT = "5432"
    DB_USER = "postgres"
    TABLE_NAME = "atividade2"
    
    secretsmanager = boto3.client('secretsmanager')
    get_secret_values_response = secretsmanager.get_secret_value(SecretId="rds!db-1e52058e-a532-4edd-9725-f5179ed6e2b2")
    secret_value = json.loads(get_secret_values_response['SecretString'])
    DB_PASSWORD = secret_value["password"]
    print("secret value got from secrets mananger")

    # Read parquet file into DataFrame
    print(f"{S3_BUCKET} / {S3_KEY}")
    df = pl.read_parquet(f"s3://{S3_BUCKET}/{S3_KEY}")
    print(f"data shape: {df.shape}")

    try:
        # Connect to Aurora PostgreSQL using psycopg3
        conn = psycopg.connect(
            host=DB_HOST,
            dbname=DB_NAME,  # <-- corrected here
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("Connection successful with database")

        # Insert rows
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"

        index = 0
        for row in df.rows():
            cur.execute(insert_query, tuple(row))
            print(f"Data inserted: {index}")
            index += 1

        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("Connection closed")


    return {
        'statusCode': 200,
        'body': f'Successfully inserted {len(df)} rows into {TABLE_NAME}.'
    }
