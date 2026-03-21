import awswrangler as wr
import pandas as pd


path_bronze = 's3://projetochurnaws/Bronze/WA_Fn-UseC_-Telco-Customer-Churn.csv'
path_silver = 's3://projetochurnaws/Silver/churn_refinado.parquet'


df = wr.s3.read_csv(path=path_bronze)


df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
df = df.dropna(subset=['TotalCharges'])


colunas_servicos = [
    'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 
    'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies'
]

df['Qtd_Servicos'] = (df[colunas_servicos] == 'Yes').sum(axis=1)


df['TotalCharges_Pipeline'] = (df['MonthlyCharges'] * df['tenure']).round(2)


wr.s3.to_parquet(
    df=df,
    path=path_silver,
    dataset=True,
    mode="
