# analysis/train_model.py

import pandas as pd
import pandas_gbq
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
import warnings

# Ignore the GCP UserWarning
warnings.filterwarnings("ignore", category=UserWarning, module="google.auth._default")

print("--- Starting Model Training Script ---")

# ----- (IMPORTANTE!) CONFIGURE SEU PROJETO AQUI -----
PROJECT_ID = "cs-ops-analytics-pipeline" # TROQUE PELO SEU ID DO PROJETO GCP
# -----------------------------------------------------

# 1. Load Clean Data from BigQuery (dbt's output)
print(f"Loading clean data from BigQuery: {PROJECT_ID}.cs_ops_analytics.fct_customer_kpis...")

sql = """
SELECT
    *
FROM
    `cs_ops_analytics.fct_customer_kpis`
"""
# We must specify the project_id for pandas_gbq
df = pandas_gbq.read_gbq(sql, project_id=PROJECT_ID)

print(f"Loaded {len(df)} rows of data.")

# 2. Prepare Data for Machine Learning
print("Preparing data for ML...")

# Define our features (X) and our target (y)
# These are the KPIs we engineered in dbt
features = [
    'mrr',
    'total_tickets',
    'total_logins',
    'total_minutes',
    'avg_session_minutes'
]

target = 'churn_status'

# Handle potential missing data (e.g., if a customer had zero joins)
df_model = df.dropna(subset=[target]) # Ensure target is not null
X = df_model[features].fillna(0)     # Fill missing features with 0
y = df_model[target].astype(int)     # Ensure target is an integer

# 3. Split Data into Training and Testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
print(f"Training set size: {len(X_train)}")
print(f"Test set size: {len(X_test)}")

# 4. Train a Model
print("Training RandomForestClassifier model...")
model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
model.fit(X_train, y_train)

# 5. Evaluate the Model
print("Evaluating model...")
y_pred = model.predict(X_test)

accuracy = accuracy_score(y_test, y_pred)
print(f"\n--- Model Evaluation ---")
print(f"Accuracy: {accuracy:.2f}")
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

# 6. Save the Model
print("Saving model to 'analysis/churn_model.pkl'...")
joblib.dump(model, 'analysis/churn_model.pkl')

print("--- Model training and saving complete. ---")