import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, count, avg

# Inicialização do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos S3 da camada trusted
rate_path = "s3://health-insurance-trusted/Rate.csv/"
benefit_path = "s3://health-insurance-trusted/BenefitsCostSharing.csv/"
output_path = "s3://health-insurance-delivery2/rate_by_benefit_count/"

# Leitura dos arquivos CSV
df_rate = spark.read.option("header", "true").csv(rate_path) \
    .select("PlanId", "IndividualRate") \
    .dropna(subset=["PlanId", "IndividualRate"]) \
    .withColumn("IndividualRate", col("IndividualRate").cast("double"))

df_benefit = spark.read.option("header", "true").csv(benefit_path) \
    .select("PlanId", "BenefitName") \
    .dropna(subset=["PlanId", "BenefitName"])

# Agregações simples
df_benefit_count = df_benefit.groupBy("PlanId") \
    .agg(count("BenefitName").alias("BenefitCount"))

df_rate_avg = df_rate.groupBy("PlanId") \
    .agg(avg("IndividualRate").alias("AvgRate"))

# Junção (mantendo o máximo possível de dados com outer join opcional)
df_result = df_benefit_count.join(df_rate_avg, on="PlanId", how="outer")

# Escrita no S3 (CSV)
if output_path.strip() != "":
    print(f"📤 Salvando resultado em: {output_path}")
    df_result.write.mode("overwrite").option("header", "true").csv(output_path)
else:
    raise Exception("Caminho de saída está vazio!")

# Finalização
job.commit()
