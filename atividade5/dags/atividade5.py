import sys

from airflow.sdk import dag

sys.path.append("/opt/airflow/")
from scripts.to_raw.source_to_raw import source_to_raw

@dag(
    dag_id="atividade5"
)
def atividade5():
    source_to_raw("bancos")
    source_to_raw("glassdoor")
    source_to_raw("reclamacoes")

atividade5()
