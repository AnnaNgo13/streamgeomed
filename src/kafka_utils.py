import json
import os
from kafka import KafkaProducer


def publish_message(producer, topic_name, value):
    try:
        producer.send(topic_name, value=value)
        producer.flush()
        print(f"Message to {topic_name} published successfully")
    except Exception as e:
        print("Exception in publishing parsed log line")
        print(e)


class KafkaConfiguration:

    def __init__(self):
        self.bootstrap_servers = os.getenv("KAFKA_BROKER","127.0.0.1:9092")

    def connect_kafka_producer(self):
        producer = None
        try:
            producer = KafkaProducer(
                security_protocol="PLAINTEXT",
                bootstrap_servers=self.bootstrap_servers,
                api_version=(0, 10),
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
            )
        except Exception as ex:
            print("Exception while connecting Kafka".format(str(ex)))
        finally:
            return producer


