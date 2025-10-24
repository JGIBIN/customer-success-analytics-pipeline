# app.py (Versão Refinada com Tooltips)

import streamlit as st
import pandas as pd
import joblib
import warnings

# Ignorar warnings específicos
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")
warnings.filterwarnings("ignore", category=UserWarning, module="google.auth._default")

print("--- Loading Streamlit App ---")

# --- 1. Carregar o Modelo Treinado ---
try:
    model = joblib.load('analysis/churn_model.pkl')
    print("Model loaded successfully.")
except FileNotFoundError:
    st.error("Erro: churn_model.pkl não encontrado. Certifique-se de que rodou o script de treinamento.")
    st.stop() # Para a execução se o modelo não for encontrado
except Exception as e:
    st.error(f"Ocorreu um erro ao carregar o modelo: {e}")
    st.stop()

# --- 2. Definir a Lista de Features (DEVE ser idêntica à do treino!) ---
features = [
    'mrr',
    'customer_tenure_days',
    'total_tickets',
    'total_critical_tickets',
    'tickets_last_30d',
    'total_logins',
    'total_minutes',
    'avg_session_minutes',
    'distinct_features_used',
    'days_since_last_login'
]
print(f"Model expects features: {features}")

# --- 3. Interface Streamlit (Layout Aprimorado) ---

st.set_page_config(layout="wide", page_title="Central de Comando Client Ops: Previsão de Churn")

st.image("https://img.icons8.com/plasticine/100/000000/line-chart.png", width=80) # Adiciona um ícone
st.title("Central de Comando Client Ops: Previsão de Churn")
st.markdown("Insira os dados do cliente abaixo para obter a probabilidade de churn em tempo real.")

st.divider() # Adiciona uma linha divisória

# Cria colunas para organizar os inputs
col1, col2, col3 = st.columns(3)

# Dicionário para guardar os inputs do usuário
input_data = {}

# === Coluna 1: Informações do Cliente ===
with col1:
    st.subheader("Dados do Cliente") # Usar subheader fica mais elegante
    input_data['mrr'] = st.number_input(
        label="Receita Mensal (MRR)",
        min_value=0.0, value=500.0, step=50.0,
        help="Valor mensal pago pelo cliente. Feature: `mrr`" # Explicação + Nome técnico no help
    )
    input_data['customer_tenure_days'] = st.number_input(
        label="Tempo de vida do Cliente - LTV (dias)",
        min_value=0, value=365, step=30,
        help="Há quantos dias o cliente assinou o contrato. Feature: `customer_tenure_days`"
    )

# === Coluna 2: Tickets de Suporte ===
with col2:
    st.subheader("Engajamento com Suporte")
    input_data['total_tickets'] = st.number_input(
        label="Total de Tickets",
        min_value=0, value=10, step=1,
        help="Número total de tickets abertos pelo cliente. Feature: `total_tickets`"
    )
    input_data['total_critical_tickets'] = st.number_input(
        label="Tickets Críticos (Bugs)",
        min_value=0, value=1, step=1,
        help="Número de tickets classificados como bug crítico. Feature: `total_critical_tickets`"
    )
    input_data['tickets_last_30d'] = st.number_input(
        label="Tickets abertos nos Últimos 30 dias",
        min_value=0, value=2, step=1,
        help="Número de tickets abertos nos últimos 30 dias. Feature: `tickets_last_30d`"
    )

# === Coluna 3: Uso da Plataforma ===
with col3:
    st.subheader("Atividade na Plataforma")
    input_data['total_logins'] = st.number_input(
        label="Total de Logins",
        min_value=0, value=150, step=10,
        help="Número total de vezes que o cliente logou. Feature: `total_logins`"
    )
    input_data['total_minutes'] = st.number_input(
        label="Total de Minutos de Uso",
        min_value=0, value=5000, step=100,
        help="Tempo total (em minutos) que o cliente passou logado. Feature: `total_minutes`"
    )
    input_data['avg_session_minutes'] = st.number_input(
        label="Duração Média da Sessão (min)",
        min_value=0.0, value=30.0, step=5.0,
        help="Tempo médio (em minutos) de cada login. Feature: `avg_session_minutes`"
    )
    input_data['distinct_features_used'] = st.number_input(
        label="Nº de Features Distintas Usadas",
        min_value=0, value=5, step=1,
        help="Número de funcionalidades diferentes que o cliente usou. Feature: `distinct_features_used`"
    )
    input_data['days_since_last_login'] = st.number_input(
        label="Dias Desde Último Login",
        min_value=0, value=7, step=1,
        help="Há quantos dias foi o último login. Feature: `days_since_last_login`"
    )

st.divider() # Outra linha divisória

# --- 4. Lógica da Previsão ---

# Botão para disparar a previsão, centralizado
col_btn1, col_btn2, col_btn3 = st.columns([1,1,1]) # Divide em 3, usa a do meio
with col_btn2:
    predict_button = st.button("📊 Prever Risco de Churn", use_container_width=True)

if predict_button:
    # Prepara os dados de input na ordem correta para o modelo
    try:
        input_df = pd.DataFrame([input_data])[features]
        print("Input DataFrame preparado:")
        print(input_df)

        # Faz a previsão de probabilidade
        prediction_proba = model.predict_proba(input_df)
        churn_probability = prediction_proba[0][1] # Pega a probabilidade de Churn (Classe 1)

        print(f"Probabilidades da previsão: {prediction_proba}")
        print(f"Probabilidade de Churn: {churn_probability:.4f}")

        # Mostra o resultado de forma mais visual
        st.subheader("Resultado da Previsão:")
        
        # Usar st.metric para um visual mais de KPI
        st.metric(label="Probabilidade de Churn", value=f"{churn_probability:.1%}")

        # Define limites e mostra recomendação
        if churn_probability >= 0.5:
            st.error("🔴 ALTO RISCO")
            st.warning("Recomendação: Engajamento proativo e imediato do CSM necessário.")
        elif churn_probability >= 0.3:
             st.warning("🟡 RISCO MÉDIO")
             st.info("Recomendação: Monitorar uso de perto, verificar NPS e agendar check-in.")
        else:
            st.success("🟢 BAIXO RISCO")
            st.info("Recomendação: Manter engajamento padrão e focar em upsell/cross-sell.")

    except Exception as e:
        st.error(f"Ocorreu um erro durante a previsão: {e}")
        print(f"Erro na previsão: {e}")

# --- 5. (Opcional) Display some data from BigQuery ---
# st.divider()
# st.header("Indicadores Gerais da Carteira (do BigQuery)")
# ... (código para conectar ao BQ e mostrar gráficos pode vir aqui) ...