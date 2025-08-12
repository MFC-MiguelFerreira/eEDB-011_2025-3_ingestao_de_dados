from pyspark.sql import SparkSession
from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parents[2]  # .../ <repo>
DELIVERY_DIR = BASE_DIR / "data" / "delivery"

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
        .appName("load_delivery_to_db")
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

    if not DELIVERY_DIR.exists():
        raise FileNotFoundError(f"Pasta delivery não encontrada: {DELIVERY_DIR}")

    # Se a delivery tiver subpastas (ex.: glassdoor/, reclamacoes/), cada uma vira uma tabela.
    # Se for uma única pasta com part-*.parquet (sem subpastas), vira tabela "delivery".
    subdirs = [p for p in DELIVERY_DIR.iterdir() if p.is_dir()]
    if subdirs:
        for entry in sorted(subdirs):
            table = entry.name.lower()
            print(f"[LOAD] {table} ← {entry}")
            df = spark.read.parquet(str(entry))
            df.write.jdbc(url=url, table=table, mode=cfg["mode"], properties=props)
            print(f"[OK] {table}")
    else:
        table = "delivery"
        print(f"[LOAD] {table} ← {DELIVERY_DIR}")
        df = spark.read.parquet(str(DELIVERY_DIR))
        df.write.jdbc(url=url, table=table, mode=cfg["mode"], properties=props)
        print(f"[OK] {table}")

    spark.stop()
    print("Concluído.")
