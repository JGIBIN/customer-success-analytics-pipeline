![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-orange)
![Tecnologias](https://img.shields.io/badge/Tecnologias-Python%20%7C%20GCP%20%7C%20dbt%20%7C%20SQL-blue)
![Output](https://img.shields.io/badge/Output-BigQuery%20%7C%20Streamlit%20%7C%20Power%20BI-green)

Customer Success Analytics Pipeline

Descrição

Pipeline de dados End-to-End para Customer Success (CS Ops) focado em predição de churn. Stack: Python (ingestão), GCP BigQuery (DWH), dbt + SQL (transformação) e Streamlit/Power BI (análise).

Objetivo do Projeto

Em empresas de SaaS (como a TOTVS), o maior desafio é a retenção de clientes. Equipes de Customer Success (CSM) precisam identificar proativamente quais clientes estão em risco de churn (cancelamento) para poderem atuar antes que seja tarde demais.

Este projeto tem como objetivo construir o pipeline de dados completo para processar, limpar e analisar dados de clientes, permitindo a criação de Health Scores e modelos de predição de churn.

Arquitetura Proposta

O projeto seguirá uma arquitetura ELT (Extract, Load, Transform) moderna, que é escalável e robusta.
Snippet de código

graph TD
    subgraph "Ingestão (Python)"
        A[Script Python<br>Geração de Lotes] --> B(GCP BigQuery<br>Dataset: cs_ops_raw_data);
    end

    subgraph "Transformação (dbt)"
        B --> C{dbt Core<br>models/staging/*.sql};
        C --> D(GCP BigQuery<br>Dataset: cs_ops_analytics);
    end

    subgraph "Análise & Consumo"
        D --> E[Power BI / Looker<br>Dashboard Executivo];
        D --> F[Streamlit App<br>CS Ops Command Center];
        D --> G[Python / Scikit-learn<br>Modelo de Churn];
    end

Tecnologias Planejadas

    Ingestão: Python (Pandas, Faker, pandas-gbq)

    Data Warehouse: Google Cloud Platform (GCP) (BigQuery)

    Transformação: dbt Core & SQL

    Análise & Modelagem: Python (Scikit-learn)

    Visualização: Streamlit & Power BI (ou Looker Studio)

    Ambiente: VSCode, Git / GitHub

Roadmap do Projeto

Este é um projeto em desenvolvimento. As etapas abaixo representam o plano de construção.

    [ ] Fase 0: Configuração e Infraestrutura

        [ ] Criar repositório no GitHub (customer-success-analytics-pipeline).

        [ ] Configurar projeto no Google Cloud (GCP).

        [ ] Ativar APIs do BigQuery e Cloud Storage.

        [ ] Criar datasets no BigQuery: cs_ops_raw_data (para dados brutos) e cs_ops_analytics (para dados limpos).

        [ ] Configurar ambiente local (VSCode, Git, Python venv).

        [ ] Configurar autenticação (gcloud auth).

    [ ] Fase 1: Ingestão de Dados (ELT - Extract & Load)

        [ ] Desenvolver script Python (ingestion/run_ingestion.py).

        [ ] Simular dados "sujos" (nulos, duplicatas, formatos errados).

        [ ] Implementar geração e carga de dados em lotes (batches).

        [ ] Carregar milhões de registros no dataset cs_ops_raw_data do BigQuery.

    [ ] Fase 2: Transformação de Dados (ELT - Transform)

        [ ] Iniciar o projeto dbt (cs_ops_dbt/).

        [ ] Configurar sources (fontes) para ler do cs_ops_raw_data.

        [ ] Criar modelos de staging (models/staging/) para limpar, padronizar e deduplicar os dados.

        [ ] Criar modelos de mart (models/marts/) com a tabela analítica final (fct_clientes_kpis).

        [ ] Executar dbt run e validar os dados limpos no dataset cs_ops_analytics.

    [ ] Fase 3: Análise & Modelagem (Machine Learning)

        [ ] Criar script/notebook (analysis/train_model.py).

        [ ] Ler dados limpos da tabela fct_clientes_kpis (do BigQuery).

        [ ] Treinar modelo de classificação (Scikit-learn) para prever churn_status.

        [ ] Salvar o modelo treinado (.pkl).

    [ ] Fase 4: Visualização & Aplicação (BI e Web App)

        [ ] Conectar o Power BI (ou Looker Studio) ao BigQuery (cs_ops_analytics).

        [ ] Criar dashboard executivo (KPIs, MRR, Churn Rate).

        [ ] Desenvolver a aplicação "CS Ops Command Center" (app.py) com Streamlit.

        [ ] Implementar no Streamlit um simulador que consome o modelo treinado.

    [ ] Fase 5: Documentação e Finalização

        [ ] Adicionar screenshots do projeto (BQ, dbt DAG, Dashboards, App).

        [ ] Atualizar o README.md com instruções finais de "Como Executar".

        [ ] (Opcional) Fazer deploy do app Streamlit no Community Cloud.

Autor

[Seu Nome Aqui]

    LinkedIn: [seu-linkedin-url]

    GitHub: [seu-github-url]
