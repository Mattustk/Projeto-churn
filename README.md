# 🚀 CHURN ANALYTICS: AWS DATA LAKEHOUSE
> **Status:** 🟢 SUCCEEDED (Operacional no AWS Glue)

![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)

## 🏗️ ARQUITETURA MEDALLION
O pipeline foi construído para ser 100% **cloud-native** (Serverless).

* **🟠 BRONZE:** Dados brutos no S3.
* **⚪ SILVER:** Limpeza e conversão para **Parquet** via Pandas.
* **🟡 GOLD:** Tabelas analíticas agregadas prontas para o business.

## 🛠️ TECNOLOGIAS UTILIZADAS
- **AWS Glue Jobs:** Processamento via Python Shell.
- **Amazon S3:** Armazenamento distribuído.
- **AWS Glue Crawler:** Catalogação automática dos metadados.
- **Python Libraries:** `awswrangler`, `pandas`, `boto3`.

## 📂 ENTREGAS (DATA CATALOG)
As seguintes tabelas são geradas e atualizadas pelo Job:
- `analise_internet` | `analise_pagamento` | `analise_fidelidade` | `analise_perfil_vip` | `analise_streaming`

---
**Projeto desenvolvido para demonstrar automação de ETL na nuvem AWS.**
