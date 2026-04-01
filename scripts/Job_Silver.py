# Importação de Blibiotecas
import awswrangler as wr
import pandas as pd

#Salvando o caminho do bucket S3 em path
path_bronze = 's3://SEU-BUCKET/Bronze/WA_Fn-UseC_-Telco-Customer-Churn.csv'
path_silver = 's3://SEU-BUCKET/Silver/churn_refinado.parquet'

#Criação do dataFrame usando a camada Bronze do S3
df = wr.s3.read_csv(path=path_bronze)


df = df.drop_duplicates(subset=['customerID']) #Impede que entre duplicatas no dataset           
df = df.dropna(subset=['MonthlyCharges','tenure']) #Deleta valores inválidos de entrarem no DataSet

df = df.drop(columns=['TotalCharges']) #Apaguei a tabela TotalCharges por estar com inconsistências na tipagem

colunas_servicos = [
    'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 
    'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies' ] #criação da tabela de serviços para a criação de uma nova tabela no DF

df['Qtd_Servicos'] = (df[colunas_servicos] == 'Yes').sum(axis=1) #usei o "axis =1" para somar realizada no eixo das colunas (horizontal) para contabilizar serviços por clientes


df['TotalCharges_Pipeline'] = (df['MonthlyCharges'] * df['tenure']).round(2) #Criação da nova tabela de Total Charges so que com a tipagem certa
df = df.dropna(subset=['TotalCharges_Pipeline']) #Filtro para eliminar valores inválidos



wr.s3.to_parquet(
    df=df,
    path=path_silver,
    dataset=True,
    mode="overwrite", # Modo Overwrite vai impedir que fique arquivo duplicado no S3
)
