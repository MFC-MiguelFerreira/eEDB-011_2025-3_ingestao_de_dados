from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

if __name__ == "__main__":
    
    # Spark Session 
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("dataframes") \
        .enableHiveSupport() \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()
    
    # Bancos
    ## Reading
    bancos = spark.read.csv(
        path="../../data/source/bancos/"
        , sep="\t"
        , encoding="utf-8"
        , header=True
    )

    bancos.printSchema()
    
    ## Writing
    bancos.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("../../data/raw/bancos/")
    
    # Glassdoor
    ## Reading
    glassdoor = spark.read.csv(
        path="../../data/source/empregados/glassdoor_consolidado_join_match_less_v2.csv"
        , sep="|"
        , encoding="utf-8"
        , header=True
    ).unionByName(
        spark.read.csv(
            path="../../data/source/empregados/glassdoor_consolidado_join_match_v2.csv"
            , sep="|"
            , encoding="utf-8"
            , header=True
        )
        , allowMissingColumns=True
    )
    
    glassdoor = glassdoor.select(*[
        col("employer_name").cast("string"),
        col("reviews_count").cast("integer"),
        col("culture_count").cast("integer"),
        col("salaries_count").cast("integer"),
        col("benefits_count").cast("integer"),
        col("employer-website").cast("string"),
        col("employer-headquarters").cast("string"),
        col("employer-founded").cast("integer"),
        col("employer-industry").cast("string"),
        col("employer-revenue").cast("string"),
        col("url").cast("string"),
        col("Geral").cast("float"),
        col("Cultura e valores").cast("float"),
        col("Diversidade e inclusão").cast("float"),
        col("Qualidade de vida").cast("float"),
        col("Alta liderança").cast("float"),
        col("Remuneração e benefícios").cast("float"),
        col("Oportunidades de carreira").cast("float"),
        col("Recomendam para outras pessoas(%)").cast("float"),
        col("Perspectiva positiva da empresa(%)").cast("float"),
        col("CNPJ").cast("string"),
        col("Nome").cast("string"),
        col("match_percent").cast("float"),
        col("Segmento").cast("string"),
    ])

    glassdoor.printSchema()

    ## Writing
    glassdoor.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("../../data/raw/glassdoor/")

    # Reclamacoes
    ## Reading
    reclamacoes = spark.read.csv(
        path="../../data/source/reclamacoes/"
        , sep=";"
        , encoding="ISO-8859-1"
        , header=True
    )
    
    reclamacoes = reclamacoes.select(*[
            col("Ano").cast("string"),
            col("Trimestre").cast("string"),
            col("Categoria").cast("string"),
            col("Tipo").cast("string"),
            col("CNPJ IF").cast("string"),
            col("Instituição financeira").cast("string"),
            col("Índice").cast("float"),
            col("Quantidade de reclamações reguladas procedentes").cast("integer"),
            col("Quantidade de reclamações reguladas - outras").cast("integer"),
            col("Quantidade de reclamações não reguladas").cast("integer"),
            col("Quantidade total de reclamações").cast("integer"),
            col("Quantidade total de clientes \x96 CCS e SCR").cast("integer"),
            col("Quantidade de clientes \x96 CCS").cast("integer"),
            col("Quantidade de clientes \x96 SCR").cast("integer")
        ])
    
    reclamacoes.printSchema()

    ## Writing
    reclamacoes.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("../../data/raw/reclamacoes/")

    spark.stop()