import json
import time
import logging
from kafka import KafkaProducer

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')


def run_producer(topic):
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    for i in range(10):
        message = {'count': i, 'payload': f'message {i}'}
        producer.send(topic, value=message)
        logging.info(f"Sent to {topic}: {message}")
        time.sleep(0.5)

    producer.flush()
    producer.close()

if __name__ == '__main__':
    run_producer('test-topic')