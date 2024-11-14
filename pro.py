import pandas as pd
from kafka import KafkaProducer
import json
import time


data = pd.read_csv('CreditCardData.csv')

# Configure the Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce messages to Kafka
for index, row in data.iterrows():
    transaction = {
    'Transaction ID': row['Transaction ID'],
    'Date': row['Date'],
    'Day of Week': row['Day of Week'],
    'Time': row['Time'],
    'Type of Card': row['Type of Card'],
    'Entry Mode': row['Entry Mode'],
    'Amount': row['Amount'],
    'Type of Transaction': row['Type of Transaction'],
    'Merchant Group': row['Merchant Group'],
    'Country of Transaction': row['Country of Transaction'],
    'Shipping Address': row['Shipping Address'],
    'Country of Residence': row['Country of Residence'],
    'Gender': row['Gender'],
    'Age': row['Age'],
    'Bank': row['Bank'],
    'Fraud': row['Fraud'] 
}
    producer.send('transactions', transaction)
    print(f"Produced: {transaction}")
    time.sleep(1)  