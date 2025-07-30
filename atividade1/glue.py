import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node bancos
bancos_node1753639927076 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": "\t", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://raw-eedb-011-2025-3-472916995593/bancos/"], "recurse": True}, transformation_ctx="bancos_node1753639927076")

# Script generated for node empregados
empregados_node1753640035165 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": "|", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://raw-eedb-011-2025-3-472916995593/empregados/"], "recurse": True}, transformation_ctx="empregados_node1753640035165")

# Script generated for node empregados renamed columns
empregadosrenamedcolumns_node1753640568980 = ApplyMapping.apply(frame=empregados_node1753640035165, mappings=[("employer_name", "string", "employer_name", "string"), ("reviews_count", "string", "reviews_count", "string"), ("culture_count", "string", "culture_count", "string"), ("salaries_count", "string", "salaries_count", "string"), ("benefits_count", "string", "benefits_count", "string"), ("employer-website", "string", "employer_website", "string"), ("employer-headquarters", "string", "employer_headquarters", "string"), ("employer-founded", "string", "employer_founded", "string"), ("employer-industry", "string", "employer_industry", "string"), ("employer-revenue", "string", "employer_revenue", "string"), ("url", "string", "url", "string"), ("geral", "string", "geral", "string"), ("cultura e valores", "string", "cultura_e_valores", "string"), ("diversidade e inclusão", "string", "diversidade_e_inclusao", "string"), ("qualidade de vida", "string", "qualidade_de_vida", "string"), ("alta liderança", "string", "right_alta lideranca", "string"), ("remuneração e benefícios", "string", "remuneracao_e_beneficios", "string"), ("oportunidades de carreira", "string", "oportunidades_de_carreira", "string"), ("`recomendam para outras pessoas(%)`", "string", "recomendam_para_outras_pessoas", "string"), ("`perspectiva positiva da empresa(%)`", "string", "perspectiva_positiva_da_empresa", "string"), ("cnpj", "string", "empregados_cnpj", "string"), ("nome", "string", "empregados_nome", "string"), ("match_percent", "string", "right_match_percent", "string"), ("segmento", "string", "right_segmento", "string")], transformation_ctx="empregadosrenamedcolumns_node1753640568980")

# Script generated for node banco_empregados
bancos_node1753639927076DF = bancos_node1753639927076.toDF()
empregadosrenamedcolumns_node1753640568980DF = empregadosrenamedcolumns_node1753640568980.toDF()
banco_empregados_node1753640446240 = DynamicFrame.fromDF(bancos_node1753639927076DF.join(empregadosrenamedcolumns_node1753640568980DF, (bancos_node1753639927076DF['cnpj'] == empregadosrenamedcolumns_node1753640568980DF['empregados_cnpj']) & (bancos_node1753639927076DF['nome'] == empregadosrenamedcolumns_node1753640568980DF['empregados_nome']), "outer"), glueContext, "banco_empregados_node1753640446240")

# Script generated for node Amazon S3
AmazonS3_node1753644420559 = glueContext.getSink(path="s3://raw-eedb-011-2025-3-472916995593/resultado/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1753644420559")
AmazonS3_node1753644420559.setCatalogInfo(catalogDatabase="ingestao_de_dados",catalogTableName="atividade1")
AmazonS3_node1753644420559.setFormat("glueparquet", compression="snappy")
AmazonS3_node1753644420559.writeFrame(banco_empregados_node1753640446240)
job.commit()