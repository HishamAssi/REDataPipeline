import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, KafkaError
import requests
import json
import certifi

class kafka_producer():

    def __init__(self, conf):
        self.producer = Producer(conf)
        self.delivered_records = 0


    def read_ccloud_config(config_file):
        """Read Confluent Cloud configuration for librdkafka clients"""

        conf = {}
        with open(config_file) as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    conf[parameter] = value.strip()

        conf['ssl.ca.location'] = certifi.where()

        return conf

    def create_topic(conf, topic):
        """
            Create a topic if needed
            Examples of additional admin API functionality:
            https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/adminapi.py
        """

        a = AdminClient(conf)

        fs = a.create_topics([NewTopic(
            topic,
            num_partitions=1,
            replication_factor=3
        )])
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
            except Exception as e:
                # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
                # Otherwise fail fast
                if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    print("Failed to create topic {}: {}".format(topic, e))
                    sys.exit(1)

    def produce_data(self, json, msg_key, topic):

        self.producer.produce(topic, key=msg_key, value=json)



    def flush(self):

        self.producer.flush()



# cert location: '/Users/hisham/PycharmProjects/pythonProject/venv/lib/python3.10/site-packages/certifi/cacert.pem'