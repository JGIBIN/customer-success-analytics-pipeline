# analysis/train_model.py
# VERSÃO COM LIGHTGBM

import pandas as pd
import pandas_gbq
import joblib
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier                 
from sklearn.metrics import accuracy_score, classification_report
import warnings

# Ignore the GCP UserWarning
warnings.filterwarnings("ignore", category=UserWarning, module="google.auth._default")

print("--- Starting Model Training Script (LightGBM) ---") 

# ----- Configure yor project ID -----
PROJECT_ID = "cs-ops-analytics-pipeline" 
# -----------------------------------------------------

# 1. Load Clean Data from BigQuery (dbt's output)
print(f"Loading clean data from BigQuery: {PROJECT_ID}.cs_ops_analytics.fct_customer_kpis...")

sql = """
SELECT
    *
FROM
    `cs_ops_analytics.fct_customer_kpis`
"""
df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)

print(f"Loaded {len(df)} rows of data.")

# 2. Prepare Data for Machine Learning
print("Preparing data for ML...")

features = [
    # Customer Info
    'mrr',
    'customer_tenure_days',
    # Ticket KPIs
    'total_tickets',
    'total_critical_tickets',
    'tickets_last_30d',
    # Usage KPIs
    'total_logins',
    'total_minutes',
    'avg_session_minutes',
    'distinct_features_used',
    'days_since_last_login'
]
target = 'churn_status'

df_model = df.dropna(subset=[target])
X = df_model[features].fillna(0)
y = df_model[target].astype(int)

# 3. Split Data into Training and Testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"Training set size: {len(X_train)}")
print(f"Test set size: {len(X_test)}")

# 4. Train a LightGBM Model
print("Training LightGBM model...")

model = LGBMClassifier(
    random_state=90,
    class_weight='balanced', # Forma do LightGBM lidar com desbalanceamento
    objective='binary',      # Objetivo binário
    metric='binary_logloss',
    learning_rate=0.04
)

model.fit(X_train, y_train)

# 5. Evaluate the Model
print("Evaluating model...")
y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
print(f"\n--- Model Evaluation ---")
print(f"Accuracy: {accuracy:.2f}")
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# 6. Save the Model (Localmente)
print("Saving model to 'analysis/churn_model.pkl'...")
joblib.dump(model, 'analysis/churn_model.pkl')

print("--- Model training and saving complete. ---")