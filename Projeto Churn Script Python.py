import pandas as pd
import pyodbc
import os

conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-OKQHC3S;'
    'DATABASE=TelcoCustomerChurn;'
    'Trusted_Connection=yes;'
)

conn = pyodbc.connect(conn_str)
df = pd.read_sql("SELECT * FROM TabelaCustomer", conn)

df.dropna(subset=['customerID', 'Churn'], inplace=True)

servicos = ['PhoneService', 'MultipleLines', 'OnlineSecurity', 
            'OnlineBackup', 'DeviceProtection', 'TechSupport', 
            'StreamingTV', 'StreamingMovies']
df['TotalServices'] = (df[servicos] == 'Yes').sum(axis=1)
df['AvgMonthlySpend'] = df['MonthlyCharges'] / df['tenure'].replace(0, 1)

cancelaram = df[df['Churn'] == 'Yes']
ficaram = df[df['Churn'] == 'No']

output_dir = 'resultados'
os.makedirs(output_dir, exist_ok=True)

df.to_csv(f'{output_dir}/churn_completo.csv', index=False)
cancelaram.to_csv(f'{output_dir}/cancelaram.csv', index=False)
ficaram.to_csv(f'{output_dir}/ficaram.csv', index=False)