# Atividade 4

## Configuração do Ambiente

1. **Instale as dependências**
   
   Execute o comando abaixo para instalar os requisitos do projeto, incluindo o `dbt-duckdb`:
   ```bash
   pip install -r requirements.txt
   ```

2. **Inicialize o projeto dbt**
   
   Execute o comando para rodar o projeto dbt com DuckDB:
   ```bash
   dbt run
   ```

   Arquivo `.dbt/profiles.yml`:

   ```yml
   atividade4:
    outputs:
        duckdb:
        type: duckdb
        path: dev.duckdb
        threads: 1
        extensions:
            - postgres

    target: duckdb
   ```

## Executando o PostgreSQL com Docker

1. **Inicie o container PostgreSQL**
   
   Use o arquivo `docker-compose.yml` para subir o banco de dados:
   ```bash
   docker-compose up -d
   ```

## Exportando dados para o PostgreSQL

1. **Exporte os dados usando dbt**
   
   Execute o comando abaixo para exportar o modelo DuckDB para a tabela `atividade4` no PostgreSQL:
   ```bash
   dbt run-operation export_to_postgres --args '{"model_name": "to_db.to_db", "pg_table": "atividade4"}'
   ```

Certifique-se de que o container PostgreSQL está rodando antes de exportar os dados.