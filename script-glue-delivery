from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import avg
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos
rate_path = "s3://health-insurance-trusted/Rate.csv/"
benefit_path = "s3://health-insurance-trusted/BenefitsCostSharing.csv/"
delivery_output = "s3://health-insurance-delivery/"

# 1. Leitura de Rate.csv
df_rate = spark.read.option("header", "true").csv(rate_path)
df_rate = df_rate.select("BusinessYear", "StateCode", "Age", "IndividualRate", "PlanId") \
                 .dropna(subset=["IndividualRate", "Age", "PlanId"])

df_rate = df_rate.withColumn("IndividualRate", df_rate["IndividualRate"].cast("double"))
df_rate = df_rate.withColumn("Age", df_rate["Age"].cast("int"))
df_rate.cache()

# 2. Média de preço por estado e idade
df_rate_summary = df_rate.groupBy("StateCode", "Age").agg(avg("IndividualRate").alias("AvgRate"))
df_rate_summary.write.mode("overwrite").option("header", "true").csv(delivery_output + "avg_rate_by_age_state/")

# 3. Leitura de BenefitsCostSharing.csv (pasta com vários arquivos)
df_benefit = spark.read.option("header", "true").csv(benefit_path)
df_benefit = df_benefit.select("PlanId").dropna(subset=["PlanId"]).dropDuplicates()
df_benefit.cache()

# 4. Número de benefícios por plano
df_benefit_count = df_benefit.groupBy("PlanId").count().withColumnRenamed("count", "NumBenefits")
df_benefit_count.write.mode("overwrite").option("header", "true").csv(delivery_output + "num_benefits_by_plan/")

# 5. Cruzamento: média de preço por número de benefícios
df_rate_plan = df_rate.select("PlanId", "IndividualRate")
df_join = df_rate_plan.join(df_benefit_count, on="PlanId", how="inner")
df_join_summary = df_join.groupBy("NumBenefits").agg(avg("IndividualRate").alias("AvgRate"))

df_join_summary.write.mode("overwrite").option("header", "true").csv(delivery_output + "rate_by_benefit_count/")

job.commit()
