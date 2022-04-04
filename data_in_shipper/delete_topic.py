from  kafka import KafkaAdminClient
import sys
import os

admin_client = KafkaAdminClient(
        bootstrap_servers=os.getenv("KAFKA_BROKER","127.0.0.1:9092"),
        security_protocol="PLAINTEXT"
    )

topics = admin_client.list_topics()

print('list of topics before deletion:', topics)

topic = sys.argv[1]

if topic in topics:
    admin_client.delete_topics([topic])
    print("Kafka topic deleted")
else:
    print("topic inexistant")

