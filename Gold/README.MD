# 🟡 Camada Gold - Tabelas Analíticas

Nesta camada, os dados foram agregados e transformados em tabelas de negócio prontas para visualização ou modelos de Machine Learning.

### 📊 Tabelas Geradas:
* **analise_fidelidade:** Kpis de tempo de contrato (Tenure) vs Churn.
* **analise_internet:** Impacto do tipo de serviço (Fibra/DSL) no cancelamento.
* **analise_pagamento:** Relação entre método de pagamento e faturamento.
* **analise_perfil_vip:** Visão de clientes com dependentes e parceiros.
* **analise_streaming:** Engajamento em serviços de entretenimento.

### 🛠️ Especificações Técnicas:
* **Formato:** Apache Parquet (Armazenamento colunar).
* **Processamento:** Agregações (`groupby`) e arredondamentos realizados via AWS Glue.
