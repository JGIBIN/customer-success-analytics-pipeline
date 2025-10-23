import pandas as pd
import numpy as np
from faker import Faker
import datetime
import random
import pandas_gbq
import warnings

# Ignore pandas-gbq FutureWarnings (optional, cleans up the output)
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas_gbq")

print("--- Starting Batch Ingestion Script ---")

# --- 1. Initial Configurations ---
fake = Faker('pt_BR') # Generates data in Portuguese (e.g., Brazilian company names)
GLOBAL_START_DATE = datetime.date.today() - datetime.timedelta(days=365)
TODAY = datetime.date.today()

# ----- (IMPORTANT!) CONFIGURE YOUR PROJECT HERE -----
PROJECT_ID = "cs-ops-analytics-pipeline" # SWAP WITH YOUR GCP PROJECT ID
DATASET_NAME_RAW = "cs_ops_raw_data"
# -----------------------------------------------------

# ----- VOLUME CONFIGURATIONS -----
# How many customers to generate in total?
TOTAL_CUSTOMERS = 10000 
# How many customers per batch to save to BQ? (Batch Size)
CHUNK_SIZE = 500 
# -----------------------------------------------------

# ----- Target Table Names -----
tbl_customers = f'{DATASET_NAME_RAW}.raw_clientes'
tbl_usage = f'{DATASET_NAME_RAW}.raw_uso_produto'
tbl_tickets = f'{DATASET_NAME_RAW}.raw_chamados_suporte'
tbl_churn = f'{DATASET_NAME_RAW}.raw_churn_target'

# --- Constants for data generation
PRODUCT_FEATURES = ['feature_A_financeiro', 'feature_B_relatorios', 'feature_C_nfe', 'feature_D_usuarios']
TICKET_TYPES = ['Dúvida', 'Bug Crítico', 'Bug Baixo', 'Solicitação Feature'] # Data is still "real"

print(f"GCP Project: {PROJECT_ID}")
print(f"Raw Dataset: {DATASET_NAME_RAW}")
print(f"Generating {TOTAL_CUSTOMERS} customers in batches of {CHUNK_SIZE}...")

# --- 2. "Dirty" Functions (To make data realistic) ---

def dirty_customer_data(df):
    """Applies 'dirt' to the customer DataFrame for the batch."""
    print("    Applying 'dirt' to customers (nulls, formats, typos)...")
    
    # P1: Nulls (5% of plans will be null)
    df.loc[df.sample(frac=0.05).index, 'plano'] = np.nan
    
    # P2: Inconsistency (10% of plans will have typos or spaces)
    dirty_plans = [' basic', 'pro ', 'ENTERPRISE', 'Básico', 'Proo'] # Data is still "real"
    dirty_indices = df.sample(frac=0.1).index
    df.loc[dirty_indices, 'plano'] = [random.choice(dirty_plans) for _ in range(len(dirty_indices))]
    
    # P3: Wrong Data Type (MRR as string "R$ 1.234,50")
    df['mrr_R$'] = df['mrr_R$'].astype(str) # Convert all to string
    # Format 30% as currency
    for idx in df.sample(frac=0.3).index:
        value = df.at[idx, 'mrr_R$']
        df.at[idx, 'mrr_R$'] = f"R$ {float(value):.2f}".replace('.', ',')
    # Insert 2% of completely bad values
    df.loc[df.sample(frac=0.02).index, 'mrr_R$'] = 'Inválido'
    return df

def dirty_ticket_data(df):
    """Applies 'dirt' to the tickets DataFrame for the batch."""
    if df.empty:
        return df
        
    print("    Applying 'dirt' to tickets (nulls, typos, duplicates)...")
    # P1: Nulls (10% of priorities will be null)
    df.loc[df.sample(frac=0.1).index, 'prioridade'] = np.nan
    
    # P2: Inconsistency (10% of types will have typos)
    dirty_types = ['bug critico', ' Dúvida', 'solicitação', 'Bug'] # Data is still "real"
    dirty_indices = df.sample(frac=0.1).index
    df.loc[dirty_indices, 'tipo_chamado'] = [random.choice(dirty_types) for _ in range(len(dirty_indices))]
    
    # P5: Duplicates (1% of batch tickets will be duplicated)
    duplicated_tickets = df.sample(n=max(1, int(len(df) * 0.01))) 
    df = pd.concat([df, duplicated_tickets], ignore_index=True)
    return df

def generate_orphan_logs(n_logs):
    """Generates 'orphan' usage logs (from customers that don't exist)."""
    print(f"    Generating {n_logs} orphan logs...")
    new_orphan_logs = []
    orphan_id = 'cli_99999' # This ID won't exist in our customer base
    
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
# 'if_exists' controls what to do if the table already exists:
# 'replace' = Deletes old table and creates a new one (for 1st batch only)
# 'append' = Appends new data to the end of the table (for all other batches)
table_write_mode = 'replace' 

for i in range(0, TOTAL_CUSTOMERS, CHUNK_SIZE):
    print(f"\n--- Processing Batch {i // CHUNK_SIZE + 1} (Customers {i} to {i + CHUNK_SIZE - 1}) ---")
    
    # Data lists for this BATCH
    customer_data_list = []
    usage_data_list = []
    ticket_data_list = []
    churn_data_list = []
    
    # --- Generate data for the BATCH ---
    for j in range(CHUNK_SIZE):
        customer_id_num = i + j
        customer_id = f'cli_{1000 + customer_id_num}'
        
        # --- CUSTOMERS Table ---
        contract_date = GLOBAL_START_DATE + datetime.timedelta(days=random.randint(0, 365 - 60))
        plan = random.choices(['Basic', 'Pro', 'Enterprise'], weights=[0.5, 0.4, 0.1], k=1)[0]
        mrr = 0
        if plan == 'Basic': mrr = round(random.uniform(50, 150), 2)
        elif plan == 'Pro': mrr = round(random.uniform(200, 500), 2)
        else: mrr = round(random.uniform(1000, 5000), 2)
        
        customer_data_list.append({
            'id_cliente': customer_id, 'nome_empresa': fake.company(),
            'plano': plan, 'mrr_R$': mrr, 'data_contrato': contract_date
        })
        
        # --- CHURN Table (Target) ---
        churn_data_list.append({
            'id_cliente': customer_id,
            'churn_status': 1 if random.random() < 0.1 else 0 # 10% churn chance
        })

        # --- USAGE & TICKETS Tables (Fact tables) ---
        
        # Engagement/ticket factors (for realism)
        engagement_factor = np.random.normal(0.6, 0.3)
        if plan == 'Pro': engagement_factor = np.random.normal(0.9, 0.2)
        elif plan == 'Enterprise': engagement_factor = np.random.normal(1.2, 0.2)
        
        ticket_factor = 0.01
        if plan == 'Basic': ticket_factor = 0.03
        type_weights = [0.5, 0.1, 0.1, 0.3] # Good customer: more doubts/features
        if mrr < 200: type_weights = [0.4, 0.3, 0.2, 0.1] # Bad customer: more bugs
            
        for days_since_contract in range((TODAY - contract_date).days):
            # Generate Usage Logs
            log_date = contract_date + datetime.timedelta(days=days_since_contract)
            if random.random() < (0.7 * engagement_factor): # Chance of logging in
                num_features_used = max(1, int(np.random.normal(5, 2) * engagement_factor))
                for _ in range(num_features_used):
                    usage_data_list.append({
                        'id_log': f'log_{total_usage_logs + len(usage_data_list) + 1}',
                        'id_cliente': customer_id, 'data_log': log_date,
                        'feature_usada': random.choice(PRODUCT_FEATURES),
                        'minutos_na_feature': max(5, int(np.random.normal(15, 5)))
                    })
            # Generate Tickets
            if random.random() < ticket_factor: # Chance of opening a ticket
                ticket_date = contract_date + datetime.timedelta(days=days_since_contract)
                ticket_data_list.append({
                    'id_chamado': f'tic_{total_tickets + len(ticket_data_list) + 1}',
                    'id_cliente': customer_id, 'data_abertura': ticket_date,
                    'tipo_chamado': random.choices(TICKET_TYPES, weights=type_weights, k=1)[0],
                    'prioridade': random.choices(['Alta', 'Média', 'Baixa'], weights=[0.3, 0.5, 0.2], k=1)[0]
                })

    # --- Convert Lists to DataFrames for the BATCH ---
    print("  Converting batch to DataFrames...")
    df_customers_chunk = pd.DataFrame(customer_data_list)
    df_usage_chunk = pd.DataFrame(usage_data_list)
    df_tickets_chunk = pd.DataFrame(ticket_data_list)
    df_churn_chunk = pd.DataFrame(churn_data_list)

    # --- Apply "Dirt" to the BATCH ---
    df_customers_chunk = dirty_customer_data(df_customers_chunk)
    df_tickets_chunk = dirty_ticket_data(df_tickets_chunk)
    
    # --- Load BATCH into BigQuery ---
    print(f"  Loading batch into BigQuery (Mode: {table_write_mode})...")
    
    pandas_gbq.to_gbq(df_customers_chunk, tbl_customers, PROJECT_ID, if_exists=table_write_mode)
    pandas_gbq.to_gbq(df_usage_chunk, tbl_usage, PROJECT_ID, if_exists=table_write_mode)
    pandas_gbq.to_gbq(df_tickets_chunk, tbl_tickets, PROJECT_ID, if_exists=table_write_mode)
    pandas_gbq.to_gbq(df_churn_chunk, tbl_churn, PROJECT_ID, if_exists=table_write_mode)
    
    # Update counters
    total_usage_logs += len(df_usage_chunk)
    total_tickets += len(df_tickets_chunk)
    
    # IMPORTANT: Change write mode to 'append' for the next loop
    # Only the first batch 'replaces' (creates) the table. All others 'append'.
    table_write_mode = 'append' 

print(f"\n--- Batch Generation COMPLETE ---")

# --- 4. Add Orphan Records (at the end) ---
print("Adding orphan/invalid data...")
df_orphans = generate_orphan_logs(n_logs=200)
pandas_gbq.to_gbq(df_orphans, tbl_usage, PROJECT_ID, if_exists='append')

print("\n--- Process Finished ---")
print(f"Total Customers Generated: {TOTAL_CUSTOMERS}")
print(f"Total Usage Logs Generated: {total_usage_logs + len(df_orphans)}")
print(f"Total Tickets Generated: {total_tickets}")
print(f"Check your dataset '{DATASET_NAME_RAW}' in project '{PROJECT_ID}' on Google BigQuery.")