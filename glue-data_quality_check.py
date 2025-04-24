import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, count, when, isnan, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# nicialização do glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# caminhos dos buckets
trusted_base_path = "s3://health-insurance-trusted/"
delivery_path = "s3://health-insurance-delivery/data_quality_checks/"

# tabelas e colunas-chave por dimensão
tabelas_config = {
    "Rate.csv": {
        "path": f"{trusted_base_path}Rate.csv/",
        "completude": ["PlanId", "StateCode", "Age", "IndividualRate"],
        "acuracia": [("IndividualRate", lambda df: df.filter((col("IndividualRate") <= 0) | col("IndividualRate").isNull()).count())],
        "consistencia": [("PlanId", "BenefitsCostSharing.csv")]  # Deve existir em outra tabela
    },
    "BenefitsCostSharing.csv": {
        "path": f"{trusted_base_path}BenefitsCostSharing.csv/",
        "completude": ["PlanId", "BenefitName", "IsCovered"],
    },
    "PlanAttributes.csv": {
        "path": f"{trusted_base_path}PlanAttributes.csv/",
        "completude": ["PlanId", "StateCode", "MetalLevel"],
    }

}

# lista para resultados
checks = []

# função para aplicar verificações de qualidade
def aplicar_checks(tabela_nome, config):
    df = spark.read.option("header", "true").csv(config["path"])
    total = df.count()

    # completude
    for col_name in config.get("completude", []):
        nulos = df.filter(col(col_name).isNull() | (col(col_name) == "")).count()
        porcentagem_nula = (nulos / total) * 100 if total > 0 else 0
        checks.append((tabela_nome, col_name, "completude", f"{porcentagem_nula:.2f}%", "OK" if nulos == 0 else "FALHA"))

    # acurácia
    for col_name, func in config.get("acuracia", []):
        erros = func(df)
        checks.append((tabela_nome, col_name, "acurácia", f"{erros} registros inválidos", "OK" if erros == 0 else "FALHA"))

    # consistência (verifica existência cruzada de chaves)
    for col_name, ref_table in config.get("consistencia", []):
        df_ref = spark.read.option("header", "true").csv(f"{trusted_base_path}{ref_table}")
        ref_keys = df_ref.select(col_name).distinct()
        origem_keys = df.select(col_name).distinct()
        faltantes = origem_keys.join(ref_keys, col_name, "left_anti").count()
        checks.append((tabela_nome, col_name, "consistência", f"{faltantes} valores não encontrados em {ref_table}", "OK" if faltantes == 0 else "FALHA"))

# executa os checks para todas as tabelas configuradas
for tabela, cfg in tabelas_config.items():
    aplicar_checks(tabela, cfg)

# criação do DataFrame com resultados
schema = StructType([
    StructField("Tabela", StringType(), True),
    StructField("Campo", StringType(), True),
    StructField("Dimensao", StringType(), True),
    StructField("Descricao", StringType(), True),
    StructField("Status", StringType(), True),
])
df_resultados = spark.createDataFrame(checks, schema) \
    .withColumn("TimestampExecucao", current_timestamp()) \
    .withColumn("JobName", lit(args["JOB_NAME"]))

# escrita na camada delivery
#df_resultados.write.mode("overwrite").option("header", "true").csv(delivery_path)
df_resultados.coalesce(1) \
             .write.mode("overwrite") \
             .option("header", "true") \
             .csv(delivery_path)

job.commit()
