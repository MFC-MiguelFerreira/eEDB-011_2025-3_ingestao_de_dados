CREATE SCHEMA IF NOT EXISTS public;

DROP TABLE IF EXISTS bancos;
CREATE TABLE bancos (
  cnpj VARCHAR PRIMARY KEY,
  nome_banco TEXT,
  segmento   TEXT
);

COPY bancos (Segmento, CNPJ, Nome)
FROM '/dados/bancos/EnquadramentoInicia_v2.tsv'
WITH (FORMAT csv, DELIMITER E'\t', HEADER true);