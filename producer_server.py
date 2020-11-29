# this is a kafka producer class definition, usage in kafka_server file
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import json
import time

class ProducerServer:

    def __init__(self, bootstrap_servers, topic_name, input_file): #, **kwargs):
        self.bootstrap_servers = bootstrap_servers
        self.input_file = input_file
        self.topic_name = topic_name
        self.client = AdminClient({"bootstrap.servers":self.bootstrap_servers})
        self.topic = NewTopic(self.topic_name, num_partitions=1, replication_factor=1)
        self.client.create_topics([self.topic])

    def generate_data(self):
        p = Producer({"bootstrap.servers":self.bootstrap_servers})
        # https://realpython.com/python-json/
        with open(self.input_file) as f:
            data = json.loads(f.read())
            for i in data:
                message = self.dict_to_binary(i)
                p.produce(self.topic_name,message)
                time.sleep(1)

    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')

    def delete_topics(self):
        fs = self.client.delete_topics([self.topic], operation_timeout=30)
        # Wait for operation to finish not working - throws "Failed to delete topic"
        # https://stackoverflow.com/questions/47051351/how-can-i-delete-topics-using-confluent-kafka-python
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} deleted".format(topic))
            except Exception as e:
                print("Failed to delete topic {}: {}".format(topic, e))


