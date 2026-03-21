# ==========================================
# IMPORTAÇÃO DE BIBLIOTECAS
# ==========================================
import boto3
import awswrangler as wr
import pandas as pd

# ==========================================
# CONFIGURAÇÃO DE SESSÃO AWS
# ==========================================
session = boto3.Session()
path_s3 = "s3://projetochurnaws/Bronze/WA_Fn-UseC_-Telco-Customer-Churn.csv"
path_silver = "s3://projetochurnaws/Silver/churn_refinado.parquet"

# ==========================================
# EXTRAÇÃO DO DATAFRAME (CAMADA BRONZE)
# ==========================================
df = wr.s3.read_csv(path=path_s3, boto3_session=session)

# ==========================================
# QUALIDADE: REMOVE REGISTROS DUPLICADOS E INVÁLIDOS
# ==========================================
df = df.drop_duplicates(subset=['customerID'])
df = df.dropna(subset=['MonthlyCharges', 'tenure'])

# ==========================================
# CRIAÇÃO DE FEATURES (ENGENHARIA DE ATRIBUTOS)
# ==========================================
colunas_servicos = ['PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 
                    'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies']

df['Qtd_Servicos'] = (df[colunas_servicos] == 'Yes').sum(axis=1)
df['TotalCharges_Pipeline'] = (df['MonthlyCharges'] * df['tenure']).round(2)

# ==========================================
# REMOÇÃO DA COLUNA ID PARA PRESERVAR A QUALIDADE DO DATASET
# ==========================================
df = df.drop(columns=['customerID'])

# ==========================================
# CARGA: TRANSFORMAÇÃO DO ARQUIVO CSV PARA PARQUET (SILVER)
# ==========================================
wr.s3.to_parquet(
    df=df,
    path=path_silver,
    dataset=True,
    mode="overwrite",
    boto3_session=sessi
