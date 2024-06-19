from confluent_kafka import Consumer, KafkaException, KafkaError


# Dirty data
def init_kafka_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9093',
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    }
    # Create Consumer instance
    consumer = Consumer(**conf)
    return consumer

def consume_events(topic):
    consumer = init_kafka_consumer()
    consumer.subscribe([topic])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition for topic {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f"Received message: key={msg.key()} value={msg.value()}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
