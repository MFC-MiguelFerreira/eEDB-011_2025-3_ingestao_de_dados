import boto3
import awswrangler as wr
import json

def handler(event, context):
    print("starting the lambda")
    s3_folder = "s3://atividade7/reclamacoes/"
    queue_url = "https://sqs.us-east-1.amazonaws.com/472916995593/atividade7-producer-queue"
    sqs = boto3.client("sqs")

    # List all CSV files in the folder
    files = wr.s3.list_objects(s3_folder, suffix=".csv")
    for file_path in files:
        print(f"reading the data from {file_path}")
        # Read CSV into DataFrame
        df = wr.s3.read_csv(file_path, sep=";", encoding="latin-1")
        print(f"data amount {df.shape}")
        # Send each row as a JSON message to SQS
        for _, row in df.iterrows():
            message_body = json.dumps(row.to_dict(), default=str)
            sqs.send_message(QueueUrl=queue_url, MessageBody=message_body)
        print(f"data sent to {queue_url}")