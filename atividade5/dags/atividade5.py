import sys

from airflow.sdk import dag

sys.path.append("/opt/airflow/")
from scripts.to_raw.source_to_raw import source_to_raw
from scripts.to_trusted.bancos import to_trusted_bancos
from scripts.to_trusted.glassdoor import to_trusted_glassdoor
from scripts.to_trusted.reclamacoes import to_trusted_reclamacoes
from scripts.to_delivery.delivery import to_delivery

@dag(
    dag_id="atividade5"
)
def atividade5():
    bancos_src_to_raw = source_to_raw.override(task_id="source_to_raw_bancos")("bancos")
    glassdoor_src_to_raw = source_to_raw.override(task_id="source_to_raw_glassdoor")("glassdoor")
    reclamacoes_src_to_raw = source_to_raw.override(task_id="source_to_raw_reclamacoes")("reclamacoes")
    
    bancos_raw_to_trs = to_trusted_bancos.override(task_id="to_trusted_bancos")()
    glassdoor_raw_to_trs = to_trusted_glassdoor.override(task_id="to_trusted_glassdoor")()
    reclamacoes_raw_to_trs = to_trusted_reclamacoes.override(task_id="to_trusted_reclamacoes")()
    
    final_trs_dlv = to_delivery.override(task_id="to_delivery")()

    bancos_src_to_raw >> bancos_raw_to_trs
    glassdoor_src_to_raw >> glassdoor_raw_to_trs
    reclamacoes_src_to_raw >> reclamacoes_raw_to_trs

    [bancos_raw_to_trs, glassdoor_raw_to_trs, reclamacoes_raw_to_trs] >> final_trs_dlv

atividade5()
