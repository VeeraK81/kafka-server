import requests
from requests.auth import HTTPBasicAuth
import json
from kafka import KafkaConsumer
import os 

def sendRequest(msg):
    
    transaction_data = json.loads(msg)
    
    payload = {
        "conf": {
        "transaction_data": transaction_data
        }
    }
    print(f"Received message: {payload}")
    url = "http://host.docker.internal:8080/api/v1/dags/api_triggered_dag/dagRuns"
    auth = HTTPBasicAuth("airflow", "airflow")
    
    # Send the POST request to trigger the DAG
    try:
        response = requests.post(url, json=payload, auth=auth)
        response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)
        print("DAG triggered successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Failed to trigger DAG: {e}")



bootstrap_servers = ['kafka:9092']

topicName = 'source.public.experiments'

# Initialize consumer variable
consumer = KafkaConsumer(
    topicName , 
    auto_offset_reset='earliest',
    bootstrap_servers = bootstrap_servers, 
    group_id='sales-transactions'
    )

consumer.topics()

# Read and print message from consumer
for msg in consumer:
    
    try:
        # Process each message and send to Airflow
        transaction_data = msg.value.decode('utf-8')
        sendRequest(transaction_data)
    except json.JSONDecodeError as e:
        print(f"Error decoding message: {e}")
    except Exception as e:
        print(f"Unexpected error processing message: {e}")       

    
    