This project uses Docker Compose to set up a Kafka-based fraud detection system with Airflow for managing workflows. 


Builds and runs the services in detached mode
$ docker-compose up --build -d  


Stops and removes containers, networks, and volumes
$ docker-compose down   


Need to add variables:
KAFKA_TOPICS=source.public.transactions
AIRFLOW_URL=<Airflow Url with dag id>
AIRFLOW_UN=Username
AIRFLOW_PWD=Password
SENDER_EMAIL = <Sender Mail Address>
RECEIVER_EMAIL = <Recipient Mail Address>

Need to add post deconnectors:
