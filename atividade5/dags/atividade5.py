import sys

from airflow.sdk import dag

sys.path.append("/opt/airflow/")
from scripts.to_raw.source_to_raw import source_to_raw
from scripts.to_trusted.bancos import to_trusted_bancos

@dag(
    dag_id="atividade5"
)
def atividade5():
    t1 = source_to_raw.override(task_id="source_to_raw_bancos")("bancos")
    t2 = source_to_raw.override(task_id="source_to_raw_glassdoor")("glassdoor")
    t3 = source_to_raw.override(task_id="source_to_raw_reclamacoes")("reclamacoes")
    t4 = to_trusted_bancos.override(task_id="to_trusted_bancos")()

    [t1, t2, t3] >> t4

atividade5()
