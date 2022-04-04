import time
import sys
import os
from icecream import ic
from datetime import datetime

import sys
sys.path.append('.')
from src.kafka_utils import KafkaConfiguration, publish_message


conf = {
    "aydat": {"file": "data-land-aydat-1m.csv", "topic": "sensor_aydat"},
    "zatu": {"file": "data-land-zatu-1m.csv", "topic": "sensor_zatu"},
}


def line_to_dict(line, header):
    return dict([(x, y) for x, y in zip(header, line)])


kafkaConf = KafkaConfiguration()
producer = kafkaConf.connect_kafka_producer()

source = sys.argv[1]

file = conf[source]["file"]
topic = conf[source]["topic"]
ic(file)
ic(topic)

filereader = open(os.path.join("data", file))
header = filereader.readline().rstrip().split(",")  # read header

print("how to send to kafka")
print("1. by batch")
print("2. by delay")
print("3. all in once")

method = input("? ")

if method == "1":
    while True:
        batch_size = int(input("how many "))
        for i in range(batch_size):
            line = filereader.readline().rstrip().split(",")
            publish_message(producer, topic, line_to_dict(line, header))

if method == "2":
    delay = float(input("delay in second: "))
    while True:
        line = filereader.readline().rstrip().split(",")
        line_in_dict = line_to_dict(line, header)
        line_in_dict["data_node_timestampUTC"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]  # change dateTime to now for window-based queries
        line_in_dict["data_temperature"] = float(
            line_in_dict["data_temperature"]
        )
        line_in_dict["data_airHumidity"] = float(
            line_in_dict["data_airHumidity"]
        )
        ic(line_in_dict)
        publish_message(producer, topic, line_in_dict)
        time.sleep(delay)

if method == "3":
    for line in filereader.readline():
        publish_message(producer, topic, line.rstrip())

filereader.close
