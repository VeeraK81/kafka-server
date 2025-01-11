import requests
from requests.auth import HTTPBasicAuth
import json
from kafka import KafkaConsumer
from threading import Thread
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os


# Function to send a transaction request to Airflow for processing
def sendTransactionRequest(transaction_data):
    # Check if 'is_fraud' key is missing, meaning the transaction has not been processed yet
    if transaction_data.get('is_fraud') is None: 
        # Prepare payload for sending to Airflow
        payload = {
            "conf": {
                "transaction_id": transaction_data['transaction_id'],
                "trans_date": transaction_data['trans_date'],
                "trans_num": transaction_data['trans_num'],
                "is_fraud": transaction_data['is_fraud']
            }
        }
        print("Payload id :", transaction_data['transaction_id'])
        
        # Get Airflow URL and authentication credentials from environment variables
        url = os.getenv("AIRFLOW_URL")
        auth = HTTPBasicAuth(os.getenv("AIRFLOW_UN"), os.getenv("AIRFLOW_PWD"))
        
        # Make a POST request to trigger the Airflow DAG
        response = requests.post(url, json=payload, auth=auth)
        
        # Check if the DAG was triggered successfully
        if response.status_code == 200:
            print("DAG triggered successfully.")
        else:
            print("Failed to trigger DAG:", response.status_code)
    else:
        print("Transaction already processed :", transaction_data['transaction_id'])

# Function to handle fraudulent transactions and send an email alert
def handleFraudTransaction(transaction_data):
    try:
        # Check if the transaction is flagged as fraudulent (is_fraud == 1)
        if transaction_data.get('is_fraud') == 1:
            print("Detected Fraud Transaction: ", transaction_data["id"]) 
            
            # Email configuration using environment variables
            sender_email = os.getenv("SENDER_EMAIL")
            receiver_email = os.getenv("RECEIVER_EMAIL")  
            smtp_server = "smtp.gmail.com"  
            smtp_port = 587  
            email_password = os.getenv("EMAIL_PWD")  

            # Prepare email content
            subject = "Fraudulent Transaction Alert"
            body = f"""
            <h3>Fraudulent Transaction Detected</h3>
            <p><strong>Transaction Details:</strong></p>
            <ul>
                <li>ID: {transaction_data.get('id')}</li>
                <li>Transaction Number: {transaction_data.get('trans_num')}</li>
            </ul>
            <p>Please review this transaction immediately.</p>
            
            <h3>Fraud Alert Machine Learning</h3>
            """

            # Set up the email message
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = receiver_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))  

            # Send the email using SMTP
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  
                server.login(sender_email, email_password)  
                server.send_message(msg)  

            print("Fraud alert email sent successfully.")
            return True  # Return True indicating the email was sent successfully
        else:
            print("No fraud detected in transaction.")
            return False  # Return False if no fraud was detected
    except Exception as e:
        # Catch any exception during email sending or transaction handling
        print(f"Error while handling transaction or sending email: {e}")
        return False  # Return False if there was an error

# Function to create a Kafka consumer for transactions
def create_transaction_consumer():
    # Set the Kafka broker and topic name
    bootstrap_servers = ['kafka:9092']
    topic_name = 'source.public.transactions'

    # Initialize the KafkaConsumer to consume messages from the 'transactions' topic
    consumer_transaction = KafkaConsumer(
        topic_name, 
        auto_offset_reset='earliest',  # Start reading from the earliest message
        bootstrap_servers=bootstrap_servers, 
        group_id='sales-transactions'  # Consumer group identifier
    )
    
    # Loop to consume messages from the Kafka topic
    for msg in consumer_transaction:
        try:
            # Decode the message and parse it as a JSON object
            transaction_data = json.loads(msg.value.decode(encoding='UTF-8'))
            # Call the function to send transaction request to Airflow
            sendTransactionRequest(transaction_data)
        except Exception as e:
            # Handle any exception that occurs while processing the message
            print(f"Error processing transaction message: {e}")

# Function to create a Kafka consumer for fraud detection messages
def create_fraud_consumer():
    # Set the Kafka broker and fraud detection topic name
    bootstrap_servers = ['kafka:9092']
    topic_fraud_detection = 'source.public.transactions_fraud_detection'

    # Initialize the KafkaConsumer to consume messages from the 'transactions_fraud_detection' topic
    consumer_fraud = KafkaConsumer(
        topic_fraud_detection, 
        auto_offset_reset='earliest',  
        bootstrap_servers=bootstrap_servers, 
        group_id='sales-transactions' 
    )

    # Loop to consume fraud detection messages
    for msg in consumer_fraud:
        try:
            # Decode the message and parse it as a JSON object
            transaction_data = json.loads(msg.value.decode(encoding='UTF-8'))
            # Call the function to handle fraud detection for the transaction
            handleFraudTransaction(transaction_data)
        except Exception as e:
            # Handle any exception that occurs while processing the fraud message
            print(f"Error processing fraud message: {e}")

# Main entry point of the script
if __name__ == "__main__":
    try:
        # Use threading to run both the transaction and fraud consumers concurrently
        transaction_thread = Thread(target=create_transaction_consumer, daemon=True)
        fraud_thread = Thread(target=create_fraud_consumer, daemon=True)

        # Start the threads
        transaction_thread.start()
        fraud_thread.start()

        # Keep the main thread alive to allow background threads to process messages
        transaction_thread.join()
        fraud_thread.join()

    except Exception as e:
        # Handle any exception that occurs while starting the consumers
        print("An error occurred while connecting to Kafka broker:", e)
