from kafka import KafkaConsumer
import json
import pandas as pd
import pickle
import streamlit as st
from tensorflow.keras.models import load_model
from tensorflow.keras.models import save_model
import numpy as np
from twilio.rest import Client

# Twilio credentials (replace with your own credentials)
account_sid = 'AC977440168c5462b9f7378f01b83e66f0'
auth_token = '28202d345258e880679b8370ccf8b24b'
twilio_number = '+12058916851'
recipient_number = '+918299863066'

client = Client(account_sid, auth_token)

with open('credit_card_fraud_preprocessor.pkl', 'rb') as f:
    preprocessor = pickle.load(f)

model = load_model('credit_card_fraud_model_lstm.h5')


st.title("Real-Time Fraud Detection Monitoring")

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

placeholder = st.empty()

transaction_data = []
feedback_data = []

st.subheader("Incoming Transactions")

for message in consumer:
    raw_data = message.value
    df = pd.DataFrame([raw_data])  
    
    transaction_id = df.get("Transaction ID")
    if transaction_id is None or len(transaction_id) == 0:
        st.warning("Transaction ID is missing. Skipping this transaction.")
        continue

    transaction_data.append(transaction_id[0])  

    df.drop(["Transaction ID"], axis=1, inplace=True)
    df.dropna(axis=0, inplace=True)

    if df.empty:
        st.warning("DataFrame is empty after preprocessing. Skipping this transaction.")
        continue

    df["Amount"] = df["Amount"].str.replace("£", "").astype(float)
    df["Date"] = pd.to_datetime(df["Date"])
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day
    df.drop(["Date", "Month", "Year"], axis=1, inplace=True)

    data_processed = preprocessor.transform(df)

    if hasattr(data_processed, "toarray"):
        data_processed = data_processed.toarray()

    data_reshaped = data_processed.reshape((1, 1, data_processed.shape[1]))

    try:
        prediction = model.predict(data_reshaped)[0][0]
    except Exception as e:
        st.error(f"Prediction failed: {e}")
        continue

    if prediction >= 0.5:
        fraud_message = f"🚨 Fraud detected in Transaction ID: {transaction_id[0]}. Transaction UNDER INVESTIGATION."
        st.error(fraud_message)
        #SMS alert for fraud detection
        message = client.messages.create(
            body=fraud_message,
            from_=twilio_number,
            to=recipient_number
        )
    else:
        normal_message = f"✅ Normal transaction - Transaction ID: {transaction_id[0]}"
        st.success(normal_message)

    feedback = st.selectbox(f"Was this prediction correct for Transaction ID: {transaction_id[0]}?", ("Select", "Yes", "No"))

    if feedback != "Select":
        feedback_data.append({
            "transaction_id": transaction_id[0],
            "prediction": "Fraud" if prediction >= 0.5 else "Normal",
            "correct": feedback == "Yes"
        })

    

    if len(feedback_data) >= 50: 
        st.warning("Retraining model with new feedback data...")

        feedback_df = pd.DataFrame(feedback_data)

        
        retrain_data = []

        for _, row in feedback_df.iterrows():
            
            if not row["correct"]:  
                retrain_data.append({"Transaction ID": row["transaction_id"]})
                

        if retrain_data:
            retrain_df = pd.DataFrame(retrain_data)

            
            retrain_data_processed = preprocessor.transform(retrain_df)

            
            model.fit(retrain_data_processed, ... ) 

            
            model.save('credit_card_fraud_model_lstm_updated.h5')
            
            
            feedback_data.clear()
            st.success("Model retrained successfully!")
        else:
            st.info("")

   
    with placeholder.container():
        st.write(f"Total Transactions Processed: {len(transaction_data)}")

    
    if len(transaction_data) > 50:
        transaction_data.pop(48)

