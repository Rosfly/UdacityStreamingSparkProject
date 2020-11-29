"""
# go into that container - in case of our yaml it is spark_project_kafka0_1
docker exec -it spark_project_kafka0_1 bash

# check topic you look for
kafka-topics --list --bootstrap-server PLAINTEXT://kafka0:9092 | grep <your_topic_name_pattern>

# create a console kafka consumer
kafka-console-consumer --topic <some_topic_name> --bootstrap-server PLAINTEXT://kafka0:9092 --from-beginning
"""

import producer_server

TOPIC_NAME = "sf.police.calls-for-service.v8"
BROKER_URL = "PLAINTEXT://localhost:9092"
JSON_FILENAME = "police-department-calls-for-service.json"

def run_kafka_server():

    producer = producer_server.ProducerServer(
        input_file= JSON_FILENAME,
        topic_name = TOPIC_NAME,
        bootstrap_servers= BROKER_URL,
    )
    return producer

def feed():
    producer = run_kafka_server()
    try:
        producer.generate_data()
    except KeyboardInterrupt as e:
        print('shutting down kafka producer...')
        print(producer.delete_topics())

if __name__ == "__main__":
    feed()