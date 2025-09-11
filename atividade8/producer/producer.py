import os
import time
import json
import glob
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv

# carrega .env da raiz do projeto ou da pasta atual
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv()

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9094")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC", "reclamacoes")
DATA_DIR      = os.getenv("DATA_DIR", "./data/reclamacoes")
RATE_PER_SEC  = float(os.getenv("RATE_PER_SEC", "10"))

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=20,
        retries=5,
        acks="all",
    )

    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not files:
        print(f"Nenhum CSV encontrado em {DATA_DIR}")
        return

    delay = 1.0 / RATE_PER_SEC if RATE_PER_SEC > 0 else 0
    sent = 0

    for f in files:
        print(f"Publicando arquivo: {f}")
        df = pd.read_csv(f, dtype=str, sep=",", encoding="utf-8", engine="python")
        df = df.fillna("")  # evita NaN
        for record in df.to_dict(orient="records"):
            producer.send(KAFKA_TOPIC, value=record)
            sent += 1
            if delay:
                time.sleep(delay)

        producer.flush()
        print(f"✔ Enviado: {sent} mensagens no total (após {os.path.basename(f)})")

    print("Finalizado.")
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()