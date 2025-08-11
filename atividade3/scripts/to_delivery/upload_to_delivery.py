from pyspark.sql import SparkSession
import logging
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO
    )
    log = logging.getLogger(__name__)   
    
    # Spark Session 
    spark = (
        SparkSession.builder.master("local[1]") 
        .appName("upload_to_trusted")
        .enableHiveSupport() 
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") 
        .getOrCreate()
    )

    ## Reading files 
    log.info("Processing started...")

    log.info(f"Reading bancos files...")

    df_bancos = spark.read.parquet(f'../../data/trusted/bancos/*.parquet')
    #df_bancos.printSchema()

    log.info(f"Reading glassdoor files...")

    df_glassdoor = spark.read.parquet(f'../../data/trusted/glassdoor/*.parquet')
    #df_glassdoor.printSchema()

    log.info(f"Reading reclamacoes files...")

    df_reclamacoes = spark.read.parquet(f'../../data/trusted/reclamacoes/*.parquet')
    #df_reclamacoes.printSchema()

    ##Transforming 

    log.info(f"Transforming trusted files...")

    log.info(f"Joining dataframes...")

    df_bancos_glassdoor = (
        df_glassdoor.alias("g")
        .join(
        df_bancos.alias("b"),
        (f.col("b.cnpj") == f.col("g.cnpj")) | (f.col("b.name") == f.col("g.name")),
        "inner",
        )
        .drop(f.col("b.cnpj"), f.col("b.name"),f.col("b.segment"))
        .dropDuplicates()
    )

    df_bancos_glassdoor_reclamacoes = (
        df_bancos_glassdoor.alias("bg")
        .join(
        df_reclamacoes.alias("r"),
        (f.col("bg.cnpj") == f.col("r.cnpj")) | (f.col("bg.name") == f.col("r.name")),
        "inner",
        )
        .drop(f.col("r.cnpj"), f.col("r.name"),f.col("r.segment"))
        .dropDuplicates()
    )

    ##Writing output files

    log.info(f"Writing delivery file...")

    df_bancos_glassdoor_reclamacoes.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("../../data/delivery/")


