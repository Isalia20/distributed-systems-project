from datetime import datetime
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from confluent_kafka import Producer
from confluent_kafka import Consumer, KafkaException, KafkaError
import socket


df = pd.read_csv("fake_dataset.csv")

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('sensor_data')


insert_statement = session.prepare("""
INSERT INTO temperature_readings (ts, station_id, temperature)
VALUES (?, ?, ?)
""")


batch = BatchStatement()
for i, row in df.iterrows():
    batch.add(insert_statement, (datetime.fromisoformat(row['ts']), row['station_id'], row['temperature']))
    if i % 100 == 0:  # Execute batch every 100 inserts
        session.execute(batch)
        batch.clear()

if batch:
    session.execute(batch)

print("Data inserted successfully")
cluster.shutdown()