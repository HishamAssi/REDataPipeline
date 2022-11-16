import confluent_kafka
# from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaError
from kafka_producer import kafka_producer
import json
import certifi

class kafka_consumer():

    def __init__(self, conf_path, topic):
        conf = kafka_producer.read_ccloud_config(conf_path)
        conf['group.id'] = 'he_consumer_1'
        conf['auto.offset.reset'] = 'earliest'
        self.consumer = Consumer(conf)
        self.topic = topic

    def subscribe_to_topic(self):
        # Subscribe to topic
        self.consumer.subscribe([self.topic])

        # Process messages
        total_count = 0
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    # No message available within timeout.
                    # Initial message consumption may take up to
                    # `session.timeout.ms` for the consumer group to
                    # rebalance and start consuming
                    print("Waiting for message or event/error in poll()")
                    continue
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                else:
                    # Check for Kafka message
                    record_key = msg.key()
                    record_value = msg.value()
                    total_count += 1
                    print("Consumed record with key {} and value {}, \
                             and updated total count to {}"
                          .format(record_key, record_value, total_count))
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()


if __name__ == "__main__":
    consumer = kafka_consumer("/Users/hisham/PycharmProjects/pythonProject/venv/proj/KafkaDevConfig.properties",
                              "housesigmascraper")
    consumer.subscribe_to_topic()

