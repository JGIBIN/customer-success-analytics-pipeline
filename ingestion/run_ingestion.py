# ingestion/run_ingestion.py
# VERSÃO 2: Com Lógica de Churn Inteligente

import pandas as pd
import numpy as np
from faker import Faker
import datetime
import random
import pandas_gbq
import warnings

# Ignore pandas-gbq FutureWarnings (optional, cleans up the output)
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas_gbq")

print("--- Starting Batch Ingestion Script (v2 - Smart Churn) ---")

# --- 1. Initial Configurations ---
fake = Faker('pt_BR')
GLOBAL_START_DATE = datetime.date.today() - datetime.timedelta(days=365)
TODAY = datetime.date.today()

# ----- (IMPORTANTE!) CONFIGURE SEU PROJETO AQUI -----
PROJECT_ID = "cs-ops-analytics-pipeline" # TROQUE PELO SEU ID DO PROJETO GCP
DATASET_NAME_RAW = "cs_ops_raw_data"
# -----------------------------------------------------

# ----- CONFIGURAÇÕES DE VOLUME -----
TOTAL_CUSTOMERS = 10000 
CHUNK_SIZE = 500 
# -----------------------------------------------------

# ----- Target Table Names -----
tbl_customers = f'{DATASET_NAME_RAW}.raw_clientes'
tbl_usage = f'{DATASET_NAME_RAW}.raw_uso_produto'
tbl_tickets = f'{DATASET_NAME_RAW}.raw_chamados_suporte'
tbl_churn = f'{DATASET_NAME_RAW}.raw_churn_target'

# --- Constants for data generation
PRODUCT_FEATURES = ['feature_A_financeiro', 'feature_B_relatorios', 'feature_C_nfe', 'feature_D_usuarios']
TICKET_TYPES = ['Dúvida', 'Bug Crítico', 'Bug Baixo', 'Solicitação Feature']

print(f"GCP Project: {PROJECT_ID}")
print(f"Raw Dataset: {DATASET_NAME_RAW}")
print(f"Generating {TOTAL_CUSTOMERS} customers in batches of {CHUNK_SIZE}...")

# --- 2. "Dirty" Functions (As mesmas de antes) ---

def dirty_customer_data(df):
    """Applies 'dirt' to the customer DataFrame for the batch."""
    print("    Applying 'dirt' to customers (nulls, formats, typos)...")
    df.loc[df.sample(frac=0.05).index, 'plano'] = np.nan
    dirty_plans = [' basic', 'pro ', 'ENTERPRISE', 'Básico', 'Proo']
    dirty_indices = df.sample(frac=0.1).index
    df.loc[dirty_indices, 'plano'] = [random.choice(dirty_plans) for _ in range(len(dirty_indices))]
    df['mrr_raw'] = df['mrr_raw'].astype(str)
    for idx in df.sample(frac=0.3).index:
        value = df.at[idx, 'mrr_raw']
        df.at[idx, 'mrr_raw'] = f"R$ {float(value):.2f}".replace('.', ',')
    df.loc[df.sample(frac=0.02).index, 'mrr_raw'] = 'Inválido'
    return df

def dirty_ticket_data(df):
    """Applies 'dirt' to the tickets DataFrame for the batch."""
    if df.empty: return df
    print("    Applying 'dirt' to tickets (nulls, typos, duplicates)...")
    df.loc[df.sample(frac=0.1).index, 'prioridade'] = np.nan
    dirty_types = ['bug critico', ' Dúvida', 'solicitação', 'Bug']
    dirty_indices = df.sample(frac=0.1).index
    df.loc[dirty_indices, 'tipo_chamado'] = [random.choice(dirty_types) for _ in range(len(dirty_indices))]
    duplicated_tickets = df.sample(n=max(1, int(len(df) * 0.01))) 
    df = pd.concat([df, duplicated_tickets], ignore_index=True)
    return df

def generate_orphan_logs(n_logs):
    """Generates 'orphan' usage logs (from customers that don't exist)."""
    print(f"    Generating {n_logs} orphan logs...")
    new_orphan_logs = []
    orphan_id = 'cli_99999'
    for i in range(n_logs):
        new_orphan_logs.append({
            'id_log': f'log_orfao_{i}', 'id_cliente': orphan_id,
            'data_log': TODAY - datetime.timedelta(days=random.randint(1, 30)),
            'feature_usada': random.choice(PRODUCT_FEATURES), 
            'minutos_na_feature': random.randint(5, 15)
        })
    return pd.DataFrame(new_orphan_logs)

# --- 3. Batch Generation and Loading ---
total_usage_logs = 0
total_tickets = 0
table_write_mode = 'replace' 

for i in range(0, TOTAL_CUSTOMERS, CHUNK_SIZE):
    print(f"\n--- Processing Batch {i // CHUNK_SIZE + 1} (Customers {i} to {i + CHUNK_SIZE - 1}) ---")
    
    customer_data_list = []
    usage_data_list = []
    ticket_data_list = []
    churn_data_list = []
    
    for j in range(CHUNK_SIZE):
        customer_id_num = i + j
        customer_id = f'cli_{1000 + customer_id_num}'
        
        # --- Passo A: Gerar Atributos do Cliente ---
        contract_date = GLOBAL_START_DATE + datetime.timedelta(days=random.randint(0, 365 - 60))
        plan = random.choices(['Basic', 'Pro', 'Enterprise'], weights=[0.5, 0.4, 0.1], k=1)[0]
        mrr = 0
        if plan == 'Basic': mrr = round(random.uniform(50, 150), 2)
        elif plan == 'Pro': mrr = round(random.uniform(200, 500), 2)
        else: mrr = round(random.uniform(1000, 5000), 2)
        
        customer_data_list.append({
            'id_cliente': customer_id, 'nome_empresa': fake.company(),
            'plano': plan, 'mrr_raw': mrr, 'data_contrato': contract_date
        })
        
        # --- Passo B: Calcular Fatores de Comportamento ---
        engagement_factor = np.random.normal(0.6, 0.3)
        if plan == 'Pro': engagement_factor = np.random.normal(0.9, 0.2)
        elif plan == 'Enterprise': engagement_factor = np.random.normal(1.2, 0.2)
        
        ticket_factor = 0.01
        if plan == 'Basic': ticket_factor = 0.03
        type_weights = [0.5, 0.1, 0.1, 0.3]
        if mrr < 200: type_weights = [0.4, 0.3, 0.2, 0.1]
            
        # --- Passo C: GERAR CHURN LÓGICO (A GRANDE MUDANÇA) ---
        # Agora o churn depende dos fatores que acabamos de calcular
        churn_chance = 0.05 # 5% de chance base
        if plan == 'Basic': churn_chance += 0.1
        if mrr < 200: churn_chance += 0.15 # Clientes com mais bugs
        if engagement_factor < 0.5: churn_chance += 0.3 # Baixo uso
        
        churn_status = 1 if random.random() < churn_chance else 0
        
        churn_data_list.append({
            'id_cliente': customer_id,
            'churn_status': churn_status
        })
        
        # --- Passo D: Gerar Logs de Uso e Tickets (como antes) ---
        for days_since_contract in range((TODAY - contract_date).days):
            # Gera Logs de Uso
            log_date = contract_date + datetime.timedelta(days=days_since_contract)
            if random.random() < (0.7 * engagement_factor): 
                num_features_used = max(1, int(np.random.normal(5, 2) * engagement_factor))
                for _ in range(num_features_used):
                    usage_data_list.append({
                        'id_log': f'log_{total_usage_logs + len(usage_data_list) + 1}',
                        'id_cliente': customer_id, 'data_log': log_date,
                        'feature_usada': random.choice(PRODUCT_FEATURES),
                        'minutos_na_feature': max(5, int(np.random.normal(15, 5)))
                    })
            # Gera Tickets
            if random.random() < ticket_factor:
                ticket_date = contract_date + datetime.timedelta(days=days_since_contract)
                ticket_data_list.append({
                    'id_chamado': f'tic_{total_tickets + len(ticket_data_list) + 1}',
                    'id_cliente': customer_id, 'data_abertura': ticket_date,
                    'tipo_chamado': random.choices(TICKET_TYPES, weights=type_weights, k=1)[0],
                    'prioridade': random.choices(['Alta', 'Média', 'Baixa'], weights=[0.3, 0.5, 0.2], k=1)[0]
                })

    # --- (O restante é igual) ---
    print("  Converting batch to DataFrames...")
    df_customers_chunk = pd.DataFrame(customer_data_list)
    df_usage_chunk = pd.DataFrame(usage_data_list)
    df_tickets_chunk = pd.DataFrame(ticket_data_list)
    df_churn_chunk = pd.DataFrame(churn_data_list)

    df_customers_chunk = dirty_customer_data(df_customers_chunk)
    df_tickets_chunk = dirty_ticket_data(df_tickets_chunk)
    
    print(f"  Loading batch into BigQuery (Mode: {table_write_mode})...")
    pandas_gbq.to_gbq(df_customers_chunk, tbl_customers, PROJECT_ID, if_exists=table_write_mode)
    pandas_gbq.to_gbq(df_usage_chunk, tbl_usage, PROJECT_ID, if_exists=table_write_mode)
    pandas_gbq.to_gbq(df_tickets_chunk, tbl_tickets, PROJECT_ID, if_exists=table_write_mode)
    pandas_gbq.to_gbq(df_churn_chunk, tbl_churn, PROJECT_ID, if_exists=table_write_mode)
    
    total_usage_logs += len(df_usage_chunk)
    total_tickets += len(df_tickets_chunk)
    table_write_mode = 'append' 

print(f"\n--- Batch Generation COMPLETE ---")

print("Adding orphan/invalid data...")
df_orphans = generate_orphan_logs(n_logs=200)
pandas_gbq.to_gbq(df_orphans, tbl_usage, PROJECT_ID, if_exists='append')

print("\n--- Process Finished ---")
print(f"Total Customers Generated: {TOTAL_CUSTOMERS}")
print(f"Total Usage Logs Generated: {total_usage_logs + len(df_orphans)}")
print(f"Total Tickets Generated: {total_tickets}")
print(f"Check your dataset '{DATASET_NAME_RAW}' in project '{PROJECT_ID}' on Google BigQuery.")