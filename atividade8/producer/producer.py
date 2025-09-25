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

#KAFKA_TOPIC = {"bancos","empregados","reclamacoes"}
#DATA_DIR = {"./atividade8/data/source/bancos","./atividade8/data/source/empregados","./atividade8/data/source/reclamacoes"}

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
RATE_PER_SEC  = float(os.getenv("RATE_PER_SEC"))

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=20,
        retries=5,
        acks="all",
    )

    delay = 1.0 / RATE_PER_SEC if RATE_PER_SEC > 0 else 0
    sent = 0
 
    # processamento reclamecoes
    DATA_DIR = "./atividade8/data/source/reclamacoes"
    KAFKA_TOPIC = "reclamacoes"

    print(f"Iniciando publicação para o tópico '{KAFKA_TOPIC}'...")

    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv"))) 
    if not files:
        print(f"Nenhum CSV encontrado em {DATA_DIR}")
        return

    for f in files:
        print(f"Publicando arquivo: {f}")
        df = pd.read_csv(f, dtype=str, sep=";", encoding="utf-8", engine="python")
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