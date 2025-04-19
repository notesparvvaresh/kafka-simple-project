import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))


def run_consumer(topic, group_id):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=json_deserializer
    )

    logging.info(f"Consumer group '{group_id}' started")
    for msg in consumer:
        logging.info(f"Received from partition {msg.partition}: {msg.value}")

if __name__ == '__main__':
    run_consumer('test-topic', group_id='my-group')