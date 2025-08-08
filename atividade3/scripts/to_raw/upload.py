from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

if __name__ == "__main__":
    
    # Spark Session 
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("dataframes") \
        .getOrCreate()
    
    # Bancos
    ## Reading
    bancos = spark.read.csv(
        path="../../data/source/bancos/"
        , sep="\t"
        , encoding="utf-8"
        , header=True
        , schema=StructType([
            StructField("Segmento", StringType(), True),
            StructField("CNPJ", StringType(), True),
            StructField("Nome", StringType(), True),
        ])
    )
    bancos.printSchema()
    
    ## Writing
    bancos.write.parquet(
        path="../../data/raw/bancos/"
        , mode="overwrite"
    )
    
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
    ).select(*[
        col("employer_name").cast(StringType()).alias("employer_name"),
        col("reviews_count").cast(IntegerType()).alias("reviews_count"),
        col("culture_count").cast(IntegerType()).alias("culture_count"),
        col("salaries_count").cast(IntegerType()).alias("salaries_count"),
        col("benefits_count").cast(IntegerType()).alias("benefits_count"),
        col("employer-website").cast(StringType()).alias("employer-website"),
        col("employer-headquarters").cast(StringType()).alias("employer-headquarters"),
        col("employer-founded").cast(StringType()).alias("employer-founded"),
        col("employer-industry").cast(StringType()).alias("employer-industry"),
        col("employer-revenue").cast(StringType()).alias("employer-revenue"),
        col("url").cast(StringType()).alias("url"),
        col("Geral").cast(FloatType()).alias("Geral"),
        col("Cultura e valores").cast(FloatType()).alias("Cultura e valores"),
        col("Diversidade e inclusão").cast(FloatType()).alias("Diversidade e inclusão"),
        col("Qualidade de vida").cast(FloatType()).alias("Qualidade de vida"),
        col("Alta liderança").cast(FloatType()).alias("Alta liderança"),
        col("Remuneração e benefícios").cast(FloatType()).alias("Remuneração e benefícios"),
        col("Oportunidades de carreira").cast(FloatType()).alias("Oportunidades de carreira"),
        col("Recomendam para outras pessoas(%)").cast(FloatType()).alias("Recomendam para outras pessoas(%)"),
        col("Perspectiva positiva da empresa(%)").cast(FloatType()).alias("Perspectiva positiva da empresa(%)"),
        col("CNPJ").cast(StringType()).alias("CNPJ"),
        col("Nome").cast(StringType()).alias("Nome"),
        col("match_percent").cast(FloatType()).alias("match_percent"),
        col("Segmento").cast(StringType()).alias("Segmento"),
    ])
    glassdoor.printSchema()

    ## Writing
    glassdoor.write.parquet(
        path="../../data/raw/glassdoor/"
        , mode="overwrite"
    )

    # Reclamacoes
    ## Reading
    reclamacoes = spark.read.csv(
        path="../../data/source/reclamacoes/"
        , sep=";"
        , encoding="ISO-8859-1"
        , header=True
        , schema=StructType([
            StructField("Ano", StringType(), True),
            StructField("Trimestre", StringType(), True),
            StructField("Categoria", StringType(), True),
            StructField("Tipo", StringType(), True),
            StructField("CNPJ IF", StringType(), True),
            StructField("Instituição financeira", StringType(), True),
            StructField("Índice", FloatType(), True),
            StructField("Quantidade de reclamações reguladas procedentes", IntegerType(), True),
            StructField("Quantidade de reclamações reguladas - outras", IntegerType(), True),
            StructField("Quantidade de reclamações não reguladas", IntegerType(), True),
            StructField("Quantidade total de reclamações", IntegerType(), True),
            StructField("Quantidade total de clientes \x96 CCS e SCR", IntegerType(), True),
            StructField("Quantidade de clientes \x96 CCS", IntegerType(), True),
            StructField("Quantidade de clientes \x96 SCR", IntegerType(), True),
            StructField("Empty", StringType(), True),
        ])
    )
    reclamacoes.printSchema()

    ## Writing
    reclamacoes.write.parquet(
        path="../../data/raw/reclamacoes/"
        , mode="overwrite"
    )

    spark.stop()