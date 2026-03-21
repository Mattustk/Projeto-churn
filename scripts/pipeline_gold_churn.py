import boto3
import awswrangler as wr
import pandas as pd

session = boto3.Session(region_name="us-east-1") 

df_silver = pd.read_parquet('s3://projetochurnaws/Silver/churn_refinado.parquet')

df_gold = df_silver.groupby(['Contract', 'Churn']).agg({
    'gender': 'count',                     
    'MonthlyCharges': 'mean',              
    'TotalCharges_Pipeline': 'sum',          
    'tenure': 'mean'                       
}).rename(columns={
    'gender': 'Total_Clientes',            
    'MonthlyCharges': 'Media_Mensalidade',
    'TotalCharges_Pipeline': 'Faturamento_Total', # Ajustado aqui
    'tenure': 'Media_Meses_Fidelidade'
}).reset_index().round(2)


Internet = df_silver.groupby(['InternetService', 'Churn']).agg({
    'gender': 'count',                    
    'MonthlyCharges': 'mean',                                    
}).rename(columns={
    'gender': 'Total_Clientes',            
    'MonthlyCharges': 'Media_Mensalidade',
}).reset_index().round(2)


Pagamentos = df_silver.groupby(['PaymentMethod', 'Churn']).agg({
    'gender': 'count',
    'MonthlyCharges': 'mean',
    'TotalCharges_Pipeline': 'sum' 
}).rename(columns={
    'gender': 'Total_Clientes',
    'MonthlyCharges': 'Ticket_Medio',
    'TotalCharges_Pipeline': 'Faturamento_Total'
}).reset_index().round(2)


Vip = df_silver.groupby(['Partner','Dependents','Churn']).agg({
    'gender': 'count',
    'MonthlyCharges': 'mean',
    'tenure': 'mean' 
}).rename(columns={
    'gender': 'Total_Clientes',
    'MonthlyCharges': 'Ticket_Medio',
    'tenure': 'Tempo_Medio'
}).reset_index().round(2)


Streaming_Familia = df_silver.groupby(['Partner', 'Dependents', 'StreamingTV', 'StreamingMovies', 'Churn']).agg({
    'gender': 'count',
    'tenure': 'mean'
}).rename(columns={
    'gender': 'Total_Clientes',
    'tenure': 'Tempo_Medio'  
}).reset_index().round(2)



tabelas_gold = {
    "analise_internet": Internet,
    "analise_pagamento": Pagamentos,
    "analise_fidelidade": df_gold,
    "analise_perfil_vip": Vip,
    "analise_streaming": Streaming_Familia
}


for nome, df in tabelas_gold.items():
    path = f"s3://projetochurnaws/Gold/{nome}.parquet"
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        boto3_session=session
    )