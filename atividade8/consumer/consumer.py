import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, coalesce, current_timestamp, lit, broadcast
from pyspark.sql.types import MapType, StringType

# ENV
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9094")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "reclamacoes")

OUTPUT_DIR     = os.getenv("OUTPUT_DIR", "./data/saida")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "atividade8/consumer/checkpoints/reclamacoes")

DB_HOST   =  os.getenv("DB_HOST", "host.docker.internal")
DB_PORT   =  os.getenv("DB_PORT", "5432")
DB_NAME   =  os.getenv("DB_NAME", "ingestao")
DB_USER   =  os.getenv("DB_USER", "postgres")
DB_PASS   =  os.getenv("DB_PASS", "postgres")
DB_TABLE  =  os.getenv("DB_TABLE", "bancos")

DRIVER = "org.postgresql.Driver"

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("load_delivery_to_db")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )

    ## 1) Lê dimensão (estática) do Postgres
    bancos_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", DB_TABLE)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", DRIVER)
        .load()
    )

    bancos_df = broadcast(bancos_df)  # pequena dimensão: broadcast para join mais barato

    ## 2) Lê o tópico Kafka (stream)
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    ## 3) Parse da mensagem 
    parsed = kafka_df.selectExpr("CAST(value AS STRING) AS json", "timestamp as kafka_ts") \
        .select(from_json(col("json"), MapType(StringType(), StringType())).alias("data"),
                col("kafka_ts"))
    
    ## 4) Expande o Map em colunas individuais
    expanded = parsed.select(
    col("kafka_ts"),
    col("data")["CNPJ IF"].alias("cnpj"),
    col("data")["Ano"].alias("year"),
    col("data")["Trimestre"].alias("quarter"),
    col("data")["Categoria"].alias("category"),
    col("data")["Tipo"].alias("type"),
    col("data")["Instituição financeira"].alias("financial_institution_name"),
    col("data")["Índice"].alias("complaint_index"),
    col("data")["Quantidade de reclamações reguladas procedentes"].alias("regulated_complaints_upheld"),
    col("data")["Quantidade de reclamações reguladas - outras"].alias("regulated_complaints_other"),
    col("data")["Quantidade de reclamações não reguladas"].alias("unregulated_complaints"),
    col("data")["Quantidade total de reclamações"].alias("total_complaints"),
    col("data")["Quantidade total de clientes – CCS e SCR"].alias("total_clients_ccs_scr"),
    col("data")["Quantidade de clientes – CCS"].alias("clients_ccs"),
    col("data")["Quantidade de clientes – SCR"].alias("clients_scr")
    )   

    ## 5) Enriquecimento (left join)
    enriched = (
        expanded.join(bancos_df, on="cnpj", how="left")
                .withColumn("ingestion_ts", current_timestamp())
    )

    # 6) Escrita contínua em JSON (pasta local)
    query = (
        enriched.writeStream
        .format("json")
        .outputMode("append")
        .option("path", OUTPUT_DIR)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print(f"Gravando em {OUTPUT_DIR} (checkpoint: {CHECKPOINT_DIR})… Ctrl+C para parar.")
    query.awaitTermination()

    spark.stop()
    print("Concluído.")