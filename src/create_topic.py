from kafka.admin import KafkaAdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)

def create_topic(topic_name, num_partitions=1, replication_factor=1):
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    topic = NewTopic(name=topic_name,
                     num_partitions=num_partitions,
                     replication_factor=replication_factor)
    try:
        admin.create_topics(new_topics=[topic], validate_only=False)
        logging.info(f"Topic '{topic_name}' created")
    except Exception as e:
        logging.warning(f"Could not create topic: {e}")
    finally:
        admin.close()

if __name__ == '__main__':
    create_topic('alireza', num_partitions=3, replication_factor=1)