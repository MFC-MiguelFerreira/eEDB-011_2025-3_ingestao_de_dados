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
    df_bancos = spark.read.parquet(f'../../data/raw/bancos/*.parquet')

    log.info(f"Reading glassdoor files...")
    df_glassdoor = spark.read.parquet(f'../../data/raw/glassdoor/*.parquet')

    log.info(f"Reading reclamacoes files...")
    df_reclamacoes = spark.read.parquet(f'../../data/raw/reclamacoes/*.parquet')

    ##Transforming 

    log.info(f"Transforming bancos files...")

    log.info(f"Renaming bancos columns...")

    df_bancos_renamed = (df_bancos
                .withColumnRenamed("Segmento", "segment")
                .withColumnRenamed("CNPJ", "cnpj")
                .withColumnRenamed("Nome", "name")
    )

    log.info(f"Bancos column formatting...")

    df_bancos_final = (df_bancos_renamed
                .withColumn("name", lower(col("name")))
                .withColumn("name", trim(col("name")))
                .withColumn("name", regexp_replace("name", r"\s*-\s*prudencial$|s\.a\.?\s*-\s*prudencial$|s\.a\.?$|s\/a$|ltda\.?$", ""))
                .withColumn("cnpj", trim(col("cnpj")))
                .withColumn("segment", trim(col("segment")))
    )

    log.info(f"Transforming glassdoor files...")

    log.info(f"Renaming glassdoor columns...")

    df_glassdoor_renamed = (df_glassdoor
                .withColumnRenamed("employer-website", "employer_website")
                .withColumnRenamed("employer-headquarters", "employer_headquarters")
                .withColumnRenamed("employer-founded", "employer_founded")
                .withColumnRenamed("employer-industry", "employer_industry")
                .withColumnRenamed("employer-revenue", "employer_revenue")
                .withColumnRenamed("Geral", "general_score")
                .withColumnRenamed("Cultura e valores", "culture_score")
                .withColumnRenamed("Diversidade e inclusão", "diversity_score")
                .withColumnRenamed("Qualidade de vida", "quality_of_life_score")
                .withColumnRenamed("Alta liderança", "leadership_score")
                .withColumnRenamed("Remuneração e benefícios", "compensation_score")
                .withColumnRenamed("Oportunidades de carreira", "career_opportunities_score")
                .withColumnRenamed("Recomendam para outras pessoas(%)", "recommendation_percentage")
                .withColumnRenamed("Perspectiva positiva da empresa(%)", "positive_outlook_percentage")
                .withColumnRenamed("match_percent", "match_percentage")
                .withColumnRenamed("Segmento", "segment")
                .withColumnRenamed("CNPJ", "cnpj")
                .withColumnRenamed("Nome", "name")

    )

    log.info(f"Glassdoor column formatting...")

    df_glassdoor_treated = (df_glassdoor_renamed
                .withColumn("name", lower(col("name")))
                .withColumn("name", trim(col("name")))
                .withColumn("name", regexp_replace("name", r"\s*-\s*prudencial$|s\.a\.?\s*-\s*prudencial$|s\.a\.?$|s\/a$|ltda\.?$", ""))
                .withColumn("cnpj", trim(col("cnpj")))
                .withColumn("segment", trim(col("segment")))

    ) 

    df_glassdoor_final = (df_glassdoor_treated
                        .groupBy(
                            f.col("name"),
                            f.col("cnpj"),
                            f.col("segment"),
                            f.col("employer_website"),
                            f.col("employer_headquarters"),
                            f.col("employer_founded"),
                            f.col("employer_industry"),
                            f.col("employer_revenue")
                        )
                        .agg(
                            avg("general_score").alias("genaral_score"),  
                            avg("culture_score").alias("culture_score"),
                            avg("diversity_score").alias("diversity_score"),
                            avg("quality_of_life_score").alias("quality_of_life_score"),
                            avg("leadership_score").alias("leadership_score"),
                            avg("compensation_score").alias("compensation_score"),
                            avg("career_opportunities_score").alias("career_opportunities_score"),
                            avg("recommendation_percentage").alias("recommendation_percentage"),
                            avg("positive_outlook_percentage").alias("positive_outlook_percentage"),
                            avg("match_percentage").alias("match_percentage"),
                            sum("reviews_count").alias("reviews_count"),
                            sum("culture_count").alias("culture_count"),
                            sum("salaries_count").alias("salaries_count"),
                            sum("benefits_count").alias("benefits_count")
                        )
                        .dropDuplicates(["name"])               
    )

    log.info(f"Transforming reclamacoes files...")

    log.info(f"Renaming reclamacoes columns...")

    df_reclamacoes_renamed = (df_reclamacoes
                .withColumnRenamed("CNPJ IF", "cnpj")
                .withColumnRenamed("Instituição financeira", "name")
                .withColumnRenamed("Categoria", "category")
                .withColumnRenamed("Tipo", "type")
                .withColumnRenamed("Ano", "year")
                .withColumnRenamed("Trimestre", "quarter")
                .withColumnRenamed("Índice", "complaint_index")
                .withColumnRenamed("Quantidade de reclamações reguladas procedentes", "regulated_complaints_upheld")
                .withColumnRenamed("Quantidade de reclamações reguladas - outras", "regulated_complaints_other")
                .withColumnRenamed("Quantidade de reclamações não reguladas", "unregulated_complaints")
                .withColumnRenamed("Quantidade total de reclamações", "total_complaints")
                .withColumnRenamed("Quantidade total de clientes \x96 CCS e SCR", "total_clients_ccs_scr")
                .withColumnRenamed("Quantidade de clientes \x96 CCS", "clients_ccs")
                .withColumnRenamed("Quantidade de clientes \x96 SCR", "clients_scr")
    )

    log.info(f"Reclamacoes column formatting...")

    df_reclamacoes_final = (df_reclamacoes_renamed
                .withColumn("name", lower(col("name")))
                .withColumn("name", trim(col("name")))
                .withColumn("name", regexp_replace("name", r"\s*\(conglomerado\)$|\s*s\.a\.?|\s*s\/a|ltda\.?$", ""))
                .withColumn("cnpj", trim(col("cnpj")))
                .withColumn("category", lower(col("category")))
                .withColumn("type", lower(col("type")))  
                .withColumn("quarter", regexp_replace("quarter", r"º", ""))
    )

    ##Writing output files
    log.info(f"Writing bancos trusted file...")

    df_bancos_final.coalesce(1) \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save("../../data/trusted/bancos/")

    log.info(f"Writing glassdoor trusted file...")

    df_glassdoor_final.coalesce(1) \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save("../../data/trusted/glassdoor/")

    log.info(f"Writing reclamacoes trusted file...")

    df_reclamacoes_final.coalesce(1) \
            .write \
            .format("parquet") \
            .mode("overwrite") \
            .save("../../data/trusted/reclamacoes/")
    
    spark.stop()
 