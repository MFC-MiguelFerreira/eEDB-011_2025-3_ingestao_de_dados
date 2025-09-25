from pyspark.sql import SparkSession
from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parents[2]  # .../ <repo>
BANCOS_DIR = BASE_DIR / "data" / "trusted" / "bancos"

def get_cfg():
    load_dotenv(BASE_DIR / ".env")
    return {
        "host": os.getenv("DB_HOST", "host.docker.internal"),
        "port": os.getenv("DB_PORT", "5432"),
        "db": os.getenv("DB_NAME", "ingestao"),
        "user": os.getenv("DB_USER", "postgres"),
        "pwd": os.getenv("DB_PASSWORD", "postgres"),
        "mode": os.getenv("DB_MODE", "overwrite"),
        "batchsize": os.getenv("BATCHSIZE", "10000"),
    }

if __name__ == "__main__":
    cfg = get_cfg()

    url = f"jdbc:postgresql://{cfg['host']}:{cfg['port']}/{cfg['db']}"
    driver = "org.postgresql.Driver"

    spark = (
        SparkSession.builder
        .appName("load_bancos_to_db")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .getOrCreate()
    )

    props = {
        "user": cfg["user"],
        "password": cfg["pwd"],
        "driver": driver,
        "batchsize": cfg["batchsize"],
    }

    if not BANCOS_DIR.exists():
        raise FileNotFoundError(f"Pasta bancos não encontrada: {BANCOS_DIR}")

    # Se a delivery tiver subpastas (ex.: glassdoor/, reclamacoes/), cada uma vira uma tabela.
    # Se for uma única pasta com part-*.parquet (sem subpastas), vira tabela "delivery".
    table = "bancos"
    print(f"[LOAD] {table} ← {BANCOS_DIR}")
    df = spark.read.parquet(str(BANCOS_DIR))
    df.write.jdbc(url=url, table=table, mode=cfg["mode"], properties=props)
    print(f"[OK] {table}")

    spark.stop()
    print("Concluído.")
