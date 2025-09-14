CREATE TABLE atividade7.reclamacoes_augmented (
  cnpj string,
  name string,
  category string,
  type string,
  quarter string,
  year string,
  complaint_index string,
  regulated_complaints_upheld string,
  regulated_complaints_other string,
  unregulated_complaints string,
  total_complaints string,
  total_clients_ccs_scr string,
  clients_ccs string,
  clients_scr string,
  segment string
)
LOCATION 's3://atividade7-753251897225/reclamacoes_augmented/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG'
);