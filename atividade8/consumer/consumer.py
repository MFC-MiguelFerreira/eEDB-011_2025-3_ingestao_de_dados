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

PG_HOST   = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT   = os.getenv("POSTGRES_PORT", "5432")
PG_DB     = os.getenv("POSTGRES_DB", "financas")
PG_USER   = os.getenv("POSTGRES_USER", "admin")
PG_PASS   = os.getenv("POSTGRES_PASSWORD", "admin")
PG_TABLE  = os.getenv("POSTGRES_TABLE", "bancos")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

spark = (
    SparkSession.builder
    .appName("atividade8-consumer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Lê dimensão (estática) do Postgres
bancos_df = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", PG_TABLE)
    .option("user", PG_USER)
    .option("password", PG_PASS)
    .option("driver", "org.postgresql.Driver")
    .load()
)

# Se o nome da coluna no Postgres não for 'cnpj', ajuste aqui:
if "cnpj" not in bancos_df.columns:
    # tente normalizar alguns nomes comuns (ajuste conforme seu init.sql)
    for candidate in ["CNPJ", "cnpj_basico", "cnpj_banco"]:
        if candidate in bancos_df.columns:
            bancos_df = bancos_df.withColumnRenamed(candidate, "cnpj")
            break

bancos_df = broadcast(bancos_df)  # pequena dimensão: broadcast para join mais barato

# 2) Lê o tópico Kafka (stream)
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# 3) Parse da mensagem (JSON genérico → Map<String,String>)
parsed = kafka_df.selectExpr("CAST(value AS STRING) AS json", "timestamp as kafka_ts") \
    .select(from_json(col("json"), MapType(StringType(), StringType())).alias("data"),
            col("kafka_ts"))

# 4) Extrai a coluna de junção 'cnpj' (tente variações)
with_key = parsed.withColumn("cnpj",
    coalesce(col("data")["cnpj"], col("data")["CNPJ"])
)

# 5) Enriquecimento (left join)
enriched = (
    with_key.join(bancos_df, on="cnpj", how="left")
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