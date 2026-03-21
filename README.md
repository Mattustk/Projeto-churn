# 📊 AWS Data Pipeline: Churn Analytics
![AWS Glue](https://img.shields.io/badge/AWS-Glue-F59B17?style=flat&logo=amazonaws)
![S3](https://img.shields.io/badge/Amazon-S3-569A31?style=flat&logo=amazons3)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat&logo=python)
![Status](https://img.shields.io/badge/Status-Full%20Green-green)

## ⛓️ Orquestração do Workflow
![Pipeline Success](./images/workflow_success.png)
*Pipeline 100% automatizado: o Job Gold só dispara após o sucesso da camada Silver.*

## 🛠️ Transformações Principais (Silver Layer)
* **Tratamento de Tipagem:** Conversão de `TotalCharges` para numérico e remoção de valores nulos.
* **Feature Engineering:** Criação da coluna `Qtd_Servicos` (contagem de serviços ativos) e cálculo da `TotalCharges_Pipeline` (`MonthlyCharges` * `tenure`).
* **Otimização:** Dados salvos em formato **Parquet** para reduzir custos de armazenamento e consulta.
