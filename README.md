# 🏗️ Projeto de Engenharia de Dados: Pipeline Cloud de Análise de Churn

![Python](https://img.shields.io/badge/Python-3.11-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange)
![AWS Wrangler](https://img.shields.io/badge/AWS-Wrangler-blue)
![Parquet](https://img.shields.io/badge/Format-Parquet-green)
![Status](https://img.shields.io/badge/Status-Produção-success)


  ## 🎯 Sobre o Projeto 
Projeto focado na criação de Pipelines ETL para análise de cancelamento de clientes (Churn). O objetivo principal é demonstrar a transição de um ambiente local para uma **arquitetura moderna em Nuvem (Cloud)**.

- **Camadas de Dados:** Organização em camadas Bronze e Silver no **Amazon S3**.
- **Data Quality:** Limpeza, tratamento de nulos e remoção de duplicados com **Python/Pandas**.
- **Performance:** Armazenamento otimizado em **Apache Parquet**.

> 💡 **Nota:** Este projeto foi desenvolvido de forma modular. Embora tenha iniciado com estudos em Airflow/Docker, a solução final foi otimizada para execução Serverless na AWS, garantindo agilidade mesmo em períodos de alta demanda acadêmica na faculdade.

## 🛠️ Tecnologias Utilizadas
- **AWS S3** - Armazenamento escalável das camadas Bronze e Silver.
- **PYTHON** - Construção do Pipeline de processamento.
- **AWS Data Wrangler** - Integração de alta performance entre Python e Cloud.
- **PANDAS** - Engenharia de Atributos (Feature Engineering).
- **PARQUET** - Formato de arquivo colunar para redução de custos e ganho de velocidade.

## ⚙️ Requerimentos 
- `boto3`
- `awswrangler`
- `pandas`

## Resultados da análise

Após a execução dos pipelines, os 7.043 clientes analisados revelaram que:

- O tempo médio de relacionamento dos clientes é de **32.4 meses**
- O gasto mensal médio por contrato é de **R$ 64,76**
- A média de serviços contratados por cliente é de **3.4**
