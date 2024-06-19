from confluent_kafka import Producer
import socket
import json
import pandas as pd
import random

KAFKA_TOPIC = "sensor_topic"

def init_kafka_producer():
    conf = {
        'bootstrap.servers': 'localhost:9093',
        'client.id': socket.gethostname()
    }
    producer = Producer(**conf)
    return producer

producer = init_kafka_producer()
df = pd.read_csv("old_data/fake_dataset.csv")
err_types = ["ERROR", "VALIDATION_ERROR", "SENSOR_ERROR"]
for i in range(1000):
    print(i)
    value = df.iloc[i % 1000].to_dict()
    value = {
        "err_type": err_types[random.randint(0, 2)],
        "ts": value.get("ts", "default_ts"),
        "station_id": f"ST{random.randint(0, 4)}",
        "sensor_id": random.randint(0, 4)
    }
    print(value)
    producer.produce(KAFKA_TOPIC, value=json.dumps(value).encode("utf-8"))
producer.flush()
