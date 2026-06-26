"""
Job de validação: confirma que o Spark consegue ler e gravar no MinIO via S3A.
Roda com:
  docker exec spark-master spark-submit /jobs/processamento/test_spark_conexao.py
"""
import os

from pyspark.sql import SparkSession

MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASS = os.environ["MINIO_ROOT_PASSWORD"]
BUCKET = os.getenv("DEFAULT_BUCKET", "bronze")

spark = (
    SparkSession.builder
    .appName("test-conexao-minio")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Cria um DataFrame simples e grava no MinIO
df = spark.createDataFrame(
    [("sensor-001", 22.5), ("sensor-002", 18.3)],
    ["sensor_id", "temperatura"],
)

caminho = f"s3a://{BUCKET}/test/validacao_spark"
df.write.mode("overwrite").parquet(caminho)
print(f"[OK] Escrita em {caminho}")

# Lê de volta e mostra
df_lido = spark.read.parquet(caminho)
df_lido.show()
print(f"[OK] Leitura ok - {df_lido.count()} linhas")

spark.stop()
