import requests
from requests.auth import HTTPBasicAuth
import json
from kafka import KafkaConsumer
from threading import Thread
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os



def sendTransactionRequest(transaction_data):
    if transaction_data.get('is_fraud') is None: 
        payload = {
            "conf": {
                "transaction_id": transaction_data['transaction_id'],
                "trans_date": transaction_data['trans_date'],
                "trans_num": transaction_data['trans_num'],
                "is_fraud": transaction_data['is_fraud']
            }
        }
        print("Payload id :", transaction_data['transaction_id'])
        
        url = os.getenv("AIRFLOW_URL")
        auth = HTTPBasicAuth(os.getenv("AIRFLOW_UN"), os.getenv("AIRFLOW_PWD"))
        response = requests.post(url, json=payload, auth=auth)
        if response.status_code == 200:
            print("DAG triggered successfully.")
        else:
            print("Failed to trigger DAG:", response.status_code)
    else:
        print("Transaction already processed :", transaction_data['transaction_id'])
    


def handleFraudTransaction(transaction_data):
    try:
        if transaction_data.get('is_fraud') == 1:
            print("Detected Fraud Transaction: ", transaction_data["id"]) 
            # Email configuration
            sender_email = os.getenv("SENDER_EMAIL")
            receiver_email = os.getenv("RECEIVER_EMAIL")  # Replace with the actual recipient
            smtp_server = "smtp.gmail.com"  # Change to your SMTP server
            smtp_port = 587  # Port for TLS
            email_password = os.getenv("EMAIL_PWD")  # Use environment variables or secrets for security

            # Email content
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

            # Set up the email
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = receiver_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))

            # Send the email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()  # Secure the connection
                server.login(sender_email, email_password)
                server.send_message(msg)

            print("Fraud alert email sent successfully.")
            return True
        else:
            print("No fraud detected in transaction.")
            return False
    except Exception as e:
        print(f"Error while handling transaction or sending email: {e}")
        return False


def create_transaction_consumer():
    bootstrap_servers = ['kafka:9092']
    topic_name = 'source.public.transactions'

    consumer_transaction = KafkaConsumer(
        topic_name, 
        auto_offset_reset='earliest',
        bootstrap_servers=bootstrap_servers, 
        group_id='sales-transactions'
    )
    
    for msg in consumer_transaction:
        try:
            transaction_data = json.loads(msg.value.decode(encoding='UTF-8'))
            sendTransactionRequest(transaction_data)
        except Exception as e:
            print(f"Error processing transaction message: {e}")


def create_fraud_consumer():
    bootstrap_servers = ['kafka:9092']
    topic_fraud_detection = 'source.public.transactions_fraud_detection'

    consumer_fraud = KafkaConsumer(
        topic_fraud_detection, 
        auto_offset_reset='earliest',
        bootstrap_servers=bootstrap_servers, 
        group_id='sales-transactions'
    )

    for msg in consumer_fraud:
        try:
            transaction_data = json.loads(msg.value.decode(encoding='UTF-8'))
            handleFraudTransaction(transaction_data)
        except Exception as e:
            print(f"Error processing fraud message: {e}")


if __name__ == "__main__":
    try:
        # Use threading to run both consumers simultaneously
        transaction_thread = Thread(target=create_transaction_consumer, daemon=True)
        fraud_thread = Thread(target=create_fraud_consumer, daemon=True)

        transaction_thread.start()
        fraud_thread.start()

        # Keep the main thread alive to allow background threads to process
        transaction_thread.join()
        fraud_thread.join()

    except Exception as e:
        print("An error occurred while connecting to Kafka broker:", e)

        