{% macro export_to_postgres(model_name, pg_table, conn_str=var("postgres_connection")) %}
    {% set sql %}
        INSTALL postgres;
        LOAD postgres;
        ATTACH '{{ conn_str }}' AS pg_db (TYPE POSTGRES);

        DROP TABLE IF EXISTS pg_db.public.{{ pg_table }};
        CREATE TABLE IF NOT EXISTS pg_db.public.{{ pg_table }} AS
        SELECT * FROM {{ model_name }};
    {% endset %}

    {{ log("Exporting " ~ model_name ~ " to Postgres table " ~ pg_table, info=True) }}
    {{ run_query(sql) }}
{% endmacro %}
