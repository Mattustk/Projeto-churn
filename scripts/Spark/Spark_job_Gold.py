# Importa SparkSession
from pyspark.sql import SparkSession

# Importa funções de agregação
from pyspark.sql.functions import count, avg, sum as spark_sum, round

# Cria sessão Spark
spark = SparkSession.builder.appName("Churn_Silver_to_Gold").getOrCreate()

# Caminho da camada Silver
path_silver = "s3://SEU-BUCKET/Silver/churn_refinado"

# Lê os dados parquet já tratados
df_silver = spark.read.parquet(path_silver)

# --- 1. ANÁLISE DE CONTRATOS ---
df_gold = df_silver.groupBy("Contract", "Churn").agg(
    count("gender").alias("Total_Clientes"), # Conta clientes
    round(avg("MonthlyCharges"), 2).alias("Media_Mensalidade"), # Média mensal
    spark_sum("TotalCharges_Pipeline").alias("Faturamento_Total"), # Soma faturamento
    round(avg("tenure"), 2).alias("Media_Meses_Fidelidade") # Média de tempo
)

# --- 2. ANÁLISE DE INTERNET ---
Internet = df_silver.groupBy("InternetService", "Churn").agg(
    count("gender").alias("Total_Clientes"),
    round(avg("MonthlyCharges"), 2).alias("Media_Mensalidade")
)

# --- 3. ANÁLISE DE PAGAMENTO ---
Pagamentos = df_silver.groupBy("PaymentMethod", "Churn").agg(
    count("gender").alias("Total_Clientes"),
    round(avg("MonthlyCharges"), 2).alias("Ticket_Medio"),
    spark_sum("TotalCharges_Pipeline").alias("Faturamento_Total")
)

# --- 4. PERFIL VIP ---
Vip = df_silver.groupBy("Partner", "Dependents", "Churn").agg(
    count("gender").alias("Total_Clientes"),
    round(avg("MonthlyCharges"), 2).alias("Ticket_Medio"),
    round(avg("tenure"), 2).alias("Tempo_Medio")
)

# --- 5. STREAMING ---
Streaming_Familia = df_silver.groupBy(
    "Partner", "Dependents", "StreamingTV", "StreamingMovies", "Churn"
).agg(
    count("gender").alias("Total_Clientes"),
    round(avg("tenure"), 2).alias("Tempo_Medio")
)

# Dicionário com tabelas finais
tabelas_gold = {
    "analise_internet": Internet,
    "analise_pagamento": Pagamentos,
    "analise_fidelidade": df_gold,
    "analise_perfil_vip": Vip,
    "analise_streaming": Streaming_Familia
}

# Loop para salvar cada tabela no S3
for nome, df in tabelas_gold.items():

    path = f"s3://SEU-BUCKET/Gold/{nome}"

    # Escreve cada DataFrame em parquet no S3
    df.write.mode("overwrite").parquet(path)
