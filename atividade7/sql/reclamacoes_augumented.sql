CREATE TABLE atividade7.reclamacoes_augmented (
  cnpj string,
  name string,
  category string,
  type string,
  quarter string,
  year bigint,
  complaint_index string,
  regulated_complaints_upheld bigint,
  regulated_complaints_other bigint,
  unregulated_complaints bigint,
  total_complaints bigint,
  total_clients_ccs_scr string,
  clients_ccs string,
  clients_scr string
)
LOCATION 's3://atividade7/reclamacoes_augmented/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG'
);