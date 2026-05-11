# Importa a SparkSession (entrada principal do Spark)
from pyspark.sql import SparkSession

# Importa funções do Spark para manipulação de dados
from pyspark.sql.functions import col, when, round

# Cria a sessão Spark (no Glue isso já roda, mas aqui garante compatibilidade)
spark = SparkSession.builder.appName("Churn_Bronze_to_Silver").getOrCreate()

# Caminhos no S3 (ajuste com seu bucket real)
path_bronze = "s3://SEU-BUCKET/Bronze/WA_Fn-UseC_-Telco-Customer-Churn.csv"
path_silver = "s3://SEU-BUCKET/Silver/churn_refinado"

# Lê o CSV do S3 e já tenta inferir tipos automaticamente
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load(path_bronze)

# Remove registros duplicados com base no customerID
df = df.dropDuplicates(["customerID"])

# Remove registros com valores nulos nas colunas críticas
df = df.na.drop(subset=["MonthlyCharges", "tenure"])

# Remove a coluna TotalCharges (problema de tipagem)
df = df.drop("TotalCharges")

# Lista de colunas de serviços
colunas_servicos = [
    "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity",
    "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies"
]

# Cria coluna Qtd_Servicos somando quantos "Yes" existem nas colunas
df = df.withColumn(
    "Qtd_Servicos",
    sum([when(col(c) == "Yes", 1).otherwise(0) for c in colunas_servicos])
)

# Cria coluna de faturamento total baseada no cálculo correto
df = df.withColumn(
    "TotalCharges_Pipeline",
    round(col("MonthlyCharges") * col("tenure"), 2)
)

# Remove possíveis nulos após o cálculo
df = df.na.drop(subset=["TotalCharges_Pipeline"])

# Escreve no S3 em formato parquet (modo overwrite evita duplicação)
df.write.mode("overwrite").parquet(path_silver)
