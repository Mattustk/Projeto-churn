![AWS BUCKET S3](https://img.shields.io/badge/AWS%20S3-Dashboard-yellow)
![AWS GLUE](https://img.shields.io/badge/AWS%20Glue-2019-red)
![WORKFLOW](https://img.shields.io/badge/WORKFL0W%C3%ADdo-green)



# 📊 Pipeline de Dados End-to-End: Churn Analytics (AWS Glue)

Projeto de Engenharia de Dados focado na automação de um pipeline de ETL utilizando a **Arquitetura Medalhão** (Bronze, Silver e Gold) dentro do ecossistema AWS.

## 🏗️ Arquitetura do Projeto
O pipeline foi desenhado para ser **orientado a eventos** e totalmente orquestrado via **AWS Glue Workflows**.



### 📂 Camadas de Dados (S3):
* **Bronze:** Armazenamento dos dados brutos (Raw Data) em formato CSV.
* **Silver:** Dados limpos, tipados e tratados (Nulos removidos e normalização de colunas) salvos em formato **Parquet** para otimização de custo e performance.
* **Gold:** Camada de negócio com agregações e métricas prontas para consumo em Dashboards (Power BI/QuickSight).

## 🛠️ Tecnologias Utilizadas
* **Python 3.x**
* **AWS Glue** (Jobs Python Shell)
* **AWS SDK (Boto3)** & **AWS Wrangler**
* **Amazon S3** (Data Lake)
* **AWS Glue Workflows** (Orquestração)

## 🚀 Diferenciais Técnicos
* **Orquestração Automática:** Implementação de triggers de dependência (o Job Gold só inicia após o sucesso do Job Silver).
* **Otimização de Custos:** Conversão de CSV para Parquet, reduzindo o volume de dados lidos no S3 e acelerando consultas via Athena.
* **Resiliência:** Tratamento de erros de esquema (KeyErrors) e integridade de colunas durante o fluxo.

---
*Desenvolvido por um aspirante a Engenheiro de Dados focado em Cloud e Automação.*
