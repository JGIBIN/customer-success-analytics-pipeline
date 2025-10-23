<p align="center"> <img alt="Status" src="https://img.shields.io/badge/Status-Em%20Desenvolvimento-orange"> <img alt="Tecnologias" src="https://img.shields.io/badge/Tecnologias-Python%20%7C%20GCP%20%7C%20dbt%20%7C%20SQL-blue"> <img alt="Output" src="https://img.shields.io/badge/Output-BigQuery%20%7C%20Streamlit%20%7C%20Power%20BI-green"> </p>

# Customer Success Analytics Pipeline

**🎯 O Desafio de Negócio**

Em qualquer empresa de SaaS (como a TOTVS), a retenção de clientes é o principal pilar de crescimento. O grande desafio das equipes de Customer Success (CSM) é identificar proativamente quais clientes estão em risco de churn (cancelamento) para poderem atuar antes que seja tarde demais.

Este projeto constrói o pipeline de dados completo para processar dados de uso, calcular Health Scores e treinar um modelo de predição de churn, transformando dados brutos em ações estratégicas de retenção.

---

**🏗️ Arquitetura da Solução**

O projeto seguirá uma arquitetura ELT (Extract, Load, Transform) moderna, que é escalável e robusta. Os dados são extraídos em Python, carregados em estado bruto no BigQuery, e só então transformados usando dbt e SQL.
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

---

💻 **Stack de Tecnologias**

    Ingestão: Python (Pandas, Faker, pandas-gbq)

    Data Warehouse: Google Cloud Platform (GCP) (BigQuery)

    Transformação: dbt Core & SQL

    Análise & Modelagem: Python (Scikit-learn)

    Visualização: Streamlit & Power BI (ou Looker Studio)

    Ambiente & DevOps: VSCode, Git / GitHub

---

🗺️ **Roadmap do Projeto**

Este é um projeto em desenvolvimento. As etapas abaixo representam o plano de construção.

    [ ] Fase 0: Configuração e Infraestrutura

        [X] Criar repositório no GitHub (customer-success-analytics-pipeline).

        [ ] Configurar projeto no Google Cloud (GCP).

        [ ] Ativar APIs do BigQuery e Cloud Storage.

        [ ] Criar datasets no BigQuery: cs_ops_raw_data e cs_ops_analytics.

        [ ] Configurar ambiente local (VSCode, Git, Python venv, .gitignore).

        [ ] Configurar autenticação (gcloud auth).

    [ ] Fase 1: Ingestão de Dados (ELT - Extract & Load)

        [ ] Desenvolver script Python (ingestion/run_ingestion.py).

        [ ] Simular dados "sujos" (nulos, duplicatas, formatos errados).

        [ ] Implementar geração e carga de dados em lotes (batches) para escalar (+1M linhas).

        [ ] Carregar dados no dataset cs_ops_raw_data do BigQuery.

    [ ] Fase 2: Transformação de Dados (ELT - Transform)

        [ ] Iniciar o projeto dbt (cs_ops_dbt/).

        [ ] Configurar sources (fontes) para ler do cs_ops_raw_data.

        [ ] Criar modelos de staging (models/staging/) para limpar, padronizar e deduplicar.

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

        [ ] Adicionar screenshots do projeto na seção "Vitrine".

        [ ] Preencher a seção "Como Executar" com o passo a passo final.

        [ ] (Opcional) Fazer deploy do app Streamlit no Community Cloud.

---

📸 **Vitrine do Projeto (Em Breve)**

(Esta seção será preenchida com screenshots à medida que as fases forem concluídas)

    Print 1: Amostra dos dados sujos no BigQuery (raw_data).

    Print 2: Amostra dos dados limpos após o dbt run (analytics).

    Print 3: Gráfico de Linhagem de Dados (DAG) do dbt docs.

    Print 4: Dashboard final no Power BI / Looker Studio.

    Print 5: Aplicação "CS Ops Command Center" rodando no Streamlit.

---

▶️ **Como Executar (Em Breve)**

(Instruções detalhadas de instalação e execução serão adicionadas na Fase 5.)

    LinkedIn: linkedin.com/in/[seu-linkedin-url]

    GitHub: github.com/[seu-github-url]
