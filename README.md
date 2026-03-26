# 📊 AWS Data Pipeline: Churn Analytics
![AWS Glue](https://img.shields.io/badge/AWS-Glue-F59B17?style=flat&logo=amazonaws)
![S3](https://img.shields.io/badge/Amazon-S3-569A31?style=flat&logo=amazons3)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=flat&logo=python)
![Status](https://img.shields.io/badge/Status-Full%20Green-green)

## ⛓️ Orquestração e Fluxo de Dados (AWS Glue)
O projeto utiliza **AWS Glue Workflows** para garantir a integridade entre as camadas. O processamento da camada Gold é dependente do sucesso da Silver, automatizando todo o pipeline de ponta a ponta.

![Status do Workflow](./Images/WorkFlow_Sucess.png) 
*Status: Succeeded (Full Green)*

## 🛠️ Transformações Principais (Silver Layer)
* **Tratamento de Tipagem:** Conversão de `TotalCharges` para numérico e remoção de valores nulos.
* **Feature Engineering:** Criação da coluna `Qtd_Servicos` (contagem de serviços ativos) e cálculo da `TotalCharges_Pipeline` (`MonthlyCharges` * `tenure`).
* **Otimização:** Dados salvos em formato **Parquet** para reduzir custos de armazenamento e consulta.

## ⚙ Tecnologias Utilizaddas

* **Linguagem & Processamento:**  (ETL & Limpeza)

* **Armazenamento (Data Lake):**  Amazon S3 (Estrutura Bronze/Silver/Gold)

* **Governança & Catálogo:** AWS Glue Crawler (Mapeamento automático de Schema)

* **Orquestração:** ETL Jobs e lógica de Workflow para automação do pipeline.

## Os Insights e o Dashboards estão documentados nesse repositório 
* **https://github.com/Mattustk/Projeto-Churn-Insights**
