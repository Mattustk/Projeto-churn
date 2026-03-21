# 🚀 CHURN ANALYTICS: AWS DATA LAKEHOUSE
> **Status do Projeto:** 🟢 SUCCEEDED (Operacional no AWS Glue)

![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![S3](https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)

---

## 🏗️ ARQUITETURA MEDALLION (SERVERLESS)
O pipeline foi construído para ser 100% **cloud-native**, eliminando a necessidade de servidores fixos e otimizando custos.

* **🟠 BRONZE:** Dados brutos estacionados no S3.
* **⚪ SILVER:** Processamento via Pandas e conversão para **Parquet** (Performance Máxima).
* **🟡 GOLD:** Tabelas analíticas agregadas prontas para o business.

## 🛠️ O CORAÇÃO DO PROJETO: AWS GLUE JOBS
Diferente de scripts locais, este projeto roda no **AWS Glue (Python Shell)**. 
- **Orquestração:** Automatizada via Triggers.
- **Data Catalog:** Schemas mapeados automaticamente por Crawlers.
- **Consultas:** Disponíveis via **Amazon Athena** usando SQL puro.

## 📈 INSIGHTS GERADOS
O pipeline entrega tabelas prontas para responder:
- Taxa de Churn por tipo de contrato.
- Faturamento total por método de pagamento.
- Ticket médio de clientes com perfil VIP.

---
### 🚀 COMO REPRODUZIR
1. Suba o arquivo bruto para o bucket S3.
2. Configure a IAM Role com permissões de `S3FullAccess` e `GlueServiceRole`.
3. Rode o Job no Glue Studio.
4. Consulte os resultados no Athena.

**Desenvolvido por um Engenheiro de Dados que não aceita menos que o Succeeded verde.** 🍻
