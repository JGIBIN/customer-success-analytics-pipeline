# app.py

import streamlit as st
import pandas as pd
import joblib
import warnings

# Ignore specific warnings
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")
warnings.filterwarnings("ignore", category=UserWarning, module="google.auth._default")

print("--- Loading Streamlit App ---")

# --- 1. Load the Trained Model ---
# This assumes 'churn_model.pkl' is in the 'analysis/' directory
# relative to where you run streamlit
try:
    model = joblib.load('analysis/churn_model.pkl')
    print("Model loaded successfully.")
except FileNotFoundError:
    st.error("Error: churn_model.pkl not found. Make sure you've run the training script.")
    st.stop() # Stop the app if model isn't found
except Exception as e:
    st.error(f"An error occurred loading the model: {e}")
    st.stop()


# --- 2. Define the Feature List (MUST match the training script!) ---
# These are the features our model expects, in the correct order
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

# --- 3. Streamlit Interface ---

st.set_page_config(layout="wide", page_title="CS Ops Command Center")

st.title("🚀 CS Ops Command Center: Churn Prediction")
st.write("Enter customer data below to predict churn probability.")

# Create columns for input fields for better layout
col1, col2, col3 = st.columns(3)

# Dictionary to store user inputs
input_data = {}

with col1:
    st.header("Customer Info")
    input_data['mrr'] = st.number_input("MRR (Monthly Recurring Revenue)", min_value=0.0, value=500.0, step=50.0)
    input_data['customer_tenure_days'] = st.number_input("Customer Tenure (Days)", min_value=0, value=365, step=30)

with col2:
    st.header("Support Tickets")
    input_data['total_tickets'] = st.number_input("Total Tickets (All Time)", min_value=0, value=10, step=1)
    input_data['total_critical_tickets'] = st.number_input("Total Critical Bug Tickets", min_value=0, value=1, step=1)
    input_data['tickets_last_30d'] = st.number_input("Tickets Opened (Last 30 Days)", min_value=0, value=2, step=1)

with col3:
    st.header("Platform Usage")
    input_data['total_logins'] = st.number_input("Total Logins (All Time)", min_value=0, value=150, step=10)
    input_data['total_minutes'] = st.number_input("Total Minutes on Platform", min_value=0, value=5000, step=100)
    input_data['avg_session_minutes'] = st.number_input("Average Minutes per Session", min_value=0.0, value=30.0, step=5.0)
    input_data['distinct_features_used'] = st.number_input("Distinct Features Used", min_value=0, value=5, step=1)
    input_data['days_since_last_login'] = st.number_input("Days Since Last Login", min_value=0, value=7, step=1)

# --- 4. Prediction Logic ---

# Button to trigger prediction
if st.button("Predict Churn Risk"):
    
    # Prepare input data in the correct order for the model
    try:
        input_df = pd.DataFrame([input_data])[features] # Ensure correct order and columns
        print("Input DataFrame prepared:")
        print(input_df)

        # Make prediction (predict_proba gives [prob_class_0, prob_class_1])
        prediction_proba = model.predict_proba(input_df)
        churn_probability = prediction_proba[0][1] # Probability of Churn (Class 1)

        print(f"Prediction probabilities: {prediction_proba}")
        print(f"Churn probability: {churn_probability:.4f}")

        # Display the result
        st.subheader("Prediction Result:")
        
        if churn_probability >= 0.5: # Simple threshold, could be adjusted
            st.error(f"HIGH RISK of Churn (Probability: {churn_probability:.1%})")
            st.warning("Recommendation: Proactive CSM engagement required.")
        elif churn_probability >= 0.3: # Example of a 'Medium Risk'
             st.warning(f"MEDIUM RISK of Churn (Probability: {churn_probability:.1%})")
             st.info("Recommendation: Monitor usage and check NPS.")
        else:
            st.success(f"LOW RISK of Churn (Probability: {churn_probability:.1%})")
            st.info("Recommendation: Maintain standard engagement.")
            
    except Exception as e:
        st.error(f"An error occurred during prediction: {e}")
        print(f"Prediction error: {e}")


# --- 5. (Optional) Display some data from BigQuery ---
# st.divider()
# st.header("Overall Customer Health (from BigQuery)")
# add code here later to query fct_customer_kpis and show charts