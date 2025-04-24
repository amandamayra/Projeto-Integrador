import pandas as pd
import boto3
import s3fs
import mysql.connector
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Configurações
bucket_name = "health-insurance-delivery"
prefix = "athena_output/"
rds_host = "projeto-health-insurance.cyg3bgki6wn8.us-east-1.rds.amazonaws.com"
rds_user = "admin"
rds_password = "(Z-<q#Q768nAYxZSS3FzS>TFiv>A"
rds_db = "health_insurance"

# Conexão com RDS
engine = create_engine(
    f"mysql+mysqlconnector://{rds_user}:{quote_plus(rds_password)}@{rds_host}/{rds_db}"
)
conn = engine.connect()

# Cliente boto3 + s3fs
s3 = boto3.client("s3")
fs = s3fs.S3FileSystem()

# Lista todos os CSVs nas subpastas do bucket
paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

csv_keys = []
for page in pages:
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            csv_keys.append(obj["Key"])

print(f"📁 {len(csv_keys)} arquivos CSV encontrados\n")

# Processa cada arquivo individualmente
for key in csv_keys:
    table_name = key.replace(prefix, "").split("/")[0].replace("-", "_")
    s3_path = f"s3://{bucket_name}/{key}"

    print(f"➡️ Lendo {s3_path} → tabela `{table_name}`")

    try:
        df = pd.read_csv(s3_path)
        if not df.empty:
            # Drop da tabela se existir
            conn.execute(text(f"DROP TABLE IF EXISTS `{table_name}`"))
            conn.commit()

            # Criação da nova tabela
            df.to_sql(table_name, con=engine, if_exists="replace", index=False)
            print(f"✅ Tabela `{table_name}` recriada com {len(df)} linhas\n")
        else:
            print(f"CSV vazio — tabela `{table_name}` não foi criada\n")
    except Exception as e:
        print(f"Erro ao processar `{key}`: {e}\n")

conn.close()
