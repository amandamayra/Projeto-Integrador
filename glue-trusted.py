import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, trim
from pyspark.sql import DataFrame

# Inicialização
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos
bucket_raw = "s3://health-insurance-raw/"
bucket_trusted = "s3://health-insurance-trusted/"

files_to_process = [
    "ServiceArea.csv",
    "Rate.csv",
    "PlanAttributes.csv",
    "Network.csv",
    "Crosswalk2016.csv",
    "Crosswalk2015.csv",
    "BusinessRules.csv",
    "BenefitsCostSharing.csv"
]

# Função de limpeza
def clean_csv_spark(df: DataFrame) -> DataFrame:
    # Remove colunas com todos os valores nulos
    df_clean = df.dropna(how='all', subset=df.columns)

    # Remove linhas onde todas as colunas são nulas
    df_clean = df_clean.dropna(how='all')

    # Remove espaços em branco de strings
    for column in df_clean.columns:
        df_clean = df_clean.withColumn(column, trim(col(column)))
    
    # Substitui nulos por string vazia
    df_clean = df_clean.fillna("")

    return df_clean

# Processa cada arquivo
for file_name in files_to_process:
    source_path = bucket_raw + file_name
    target_path = bucket_trusted + file_name.replace(".csv", "-cleaned")

    try:
        # Lê o CSV do bucket raw
        df = spark.read.option("header", "true").csv(source_path)

        # Limpeza
        df_clean = clean_csv_spark(df)

        # Grava o CSV limpo no bucket trusted
        df_clean.write.mode("overwrite").option("header", "true").csv(target_path)

        print(f"✔️ Arquivo processado: {file_name}")
    
    except Exception as e:
        print(f"Erro ao processar {file_name}: {e}")
        continue

job.commit()
