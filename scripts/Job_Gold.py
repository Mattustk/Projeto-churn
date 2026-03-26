#Importação de Blibiotecas
import awswrangler as wr
import pandas as pd

# Pega os dados que você já limpou na pasta Silver
df_silver = pd.read_parquet('s3://projetochurnaws/Silver/churn_refinado.parquet')

# --- 1. TABELA DE CONTRATOS ---
# Aqui eu vejo se o tipo de contrato (mensal ou anual) faz o cliente sair mais
df_gold = df_silver.groupby(['Contract', 'Churn']).agg({
    'gender': 'count',                      
    'MonthlyCharges': 'mean',               
    'TotalCharges_Pipeline': 'sum',           
    'tenure': 'mean'                        
}).rename(columns={
    'gender': 'Total_Clientes',             
    'MonthlyCharges': 'Media_Mensalidade',
    'TotalCharges_Pipeline': 'Faturamento_Total',
    'tenure': 'Media_Meses_Fidelidade'
}).reset_index().round(2)

# --- 2. TABELA DE INTERNET ---
# Aqui eu vejo se a Fibra Óptica ou o DSL tá fazendo o pessoal cancelar
Internet = df_silver.groupby(['InternetService', 'Churn']).agg({
    'gender': 'count',                     
    'MonthlyCharges': 'mean',                                    
}).rename(columns={
    'gender': 'Total_Clientes',             
    'MonthlyCharges': 'Media_Mensalidade',
}).reset_index().round(2)

# --- 3. TABELA DE PAGAMENTOS ---
# Aqui eu vejo se quem paga no boleto sai mais do que quem paga no cartão
Pagamentos = df_silver.groupby(['PaymentMethod', 'Churn']).agg({
    'gender': 'count',
    'MonthlyCharges': 'mean',
    'TotalCharges_Pipeline': 'sum' 
}).rename(columns={
    'gender': 'Total_Clientes',
    'MonthlyCharges': 'Ticket_Medio',
    'TotalCharges_Pipeline': 'Faturamento_Total'
}).reset_index().round(2)

# --- 4. TABELA DE FAMÍLIAS (VIP) ---
# Aqui eu vejo se quem tem família (parceiro/filhos) gasta mais e fica mais tempo
Vip = df_silver.groupby(['Partner','Dependents','Churn']).agg({
    'gender': 'count',
    'MonthlyCharges': 'mean',
    'tenure': 'mean' 
}).rename(columns={
    'gender': 'Total_Clientes',
    'MonthlyCharges': 'Ticket_Medio', 
    'tenure': 'Tempo_Medio'
}).reset_index().round(2)

# --- 5. TABELA DE STREAMING ---
# Aqui eu vejo se quem assina Netflix/Filmes com a gente cancela menos
Streaming_Familia = df_silver.groupby(['Partner', 'Dependents', 'StreamingTV', 'StreamingMovies', 'Churn']).agg({
    'gender': 'count',
    'tenure': 'mean'
}).rename(columns={
    'gender': 'Total_Clientes',
    'tenure': 'Tempo_Medio'  
}).reset_index().round(2)

# --- SALVANDO TUDO NO S3 ---
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
   
