from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import os

def executar_pipeline():
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-OKQHC3S;'
        'DATABASE=TelcoCustomerChurn;'
        'Trusted_Connection=yes;'
    )
    conn = pyodbc.connect(conn_str)
    df = pd.read_sql("SELECT * FROM TabelaCustomer", conn)
    
    df.dropna(subset=['customerID', 'Churn'], inplace=True)
    
    servicos = ['CustomerID','PhoneService', 'MultipleLines', 'OnlineSecurity', 
                'OnlineBackup', 'DeviceProtection', 'TechSupport', 
                'StreamingTV', 'StreamingMovies','Churn']
    df['TotalServices'] = (df[servicos] == 'Yes').sum(axis=1)
    df['AvgMonthlySpend'] = df['MonthlyCharges'] / df['tenure'].replace(0, 1)
    
    cancelaram = df[df['Churn'] == 'Yes']
    ficaram = df[df['Churn'] == 'No']
    
    output_dir = '/opt/airflow/dags/output'
    os.makedirs(output_dir, exist_ok=True)
    
    df.to_csv(f'{output_dir}/churn_completo.csv', index=False)
    cancelaram.to_csv(f'{output_dir}/cancelaram.csv', index=False)
    ficaram.to_csv(f'{output_dir}/ficaram.csv', index=False)
    
    return "OK"

default_args = {
    'owner': 'guilherme',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pipeline_churn_final',
    default_args=default_args,
    description='Pipeline de análise de churn',
    schedule_interval='0 9 * * *',
    catchup=False,
) as dag:

    tarefa = PythonOperator(
        task_id='executar_pipeline',
        python_callable=executar_pipeline,
    )
