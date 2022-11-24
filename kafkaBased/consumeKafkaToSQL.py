import confluent_kafka
# from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, KafkaError
from kafka_producer import kafka_producer
import json
import certifi
import psycopg2
import sqlalchemy
import login_credentials
import re
from ddlgenerator.ddlgenerator import Table

class postgres_write():

    def __init__(self):
        """

        :param hostname:
        :param port:
        :param username:
        :param password:
        """
        self.psql_engine = psycopg2.connect( database="postgres",
                                             user=login_credentials.db_user,
                                             password=login_credentials.db_password,
                                             host=login_credentials.db_hostname,
                                             port=login_credentials.db_port
                                             )
        self.table_name = 'housesigmasold'
    def create_ddl(self, message):
        # transform json to dict
        # with open("/Users/hisham/PycharmProjects/pythonProject/venv/proj/data/extracted_sold.json", "r") as json_file:
        #     lines = json_file.readlines()[0]
        #     data = json.loads(json.loads(lines))

        table = Table([message], table_name=self.table_name)
        sql = table.sql('postgresql')

        return sql


    def write_message(self, message):
        table = Table([message], table_name=self.table_name)
        sql = table.sql('postgresql', creates=False, inserts=True)
        # sql = sql.replace(self.table_name, 'public.'+self.table_name)
        print(sql)
        cur = self.psql_engine.cursor()
        cur.execute(sql)
        self.psql_engine.commit()


    def create_table(self, ddl):
        #Must figure out an easy way to produce table ddl.
        print(ddl)
        cur = self.psql_engine.cursor()
        cur.execute(ddl)
        self.psql_engine.commit()
        self.psql_engine.close()

class kafka_consumer():

    def __init__(self, conf_path, topic):
        conf = kafka_producer.read_ccloud_config(conf_path)
        conf['group.id'] = 'he_consumer_1'
        # conf['auto.offset.reset'] = 'earliest'
        self.consumer = Consumer(conf)
        self.topic = topic
        self.db_writer = postgres_write()

    def subscribe_to_topic(self):
        # Subscribe to topic
        self.consumer.subscribe([self.topic])

        # Process messages
        total_count = 0
        table_exists = False
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
                    record_value_dict = json.loads(record_value.decode('utf-8'))
                    total_count += 1
                    print(record_value_dict)
                    # if not table_exists:
                    #     self.db_writer.create_table(self.db_writer.create_ddl(record_value_dict))
                    #     table_exists = True
                    self.db_writer.write_message(record_value_dict)
                    print("Consumed record with key {} and value {}, \
                             and updated total count to {}"
                          .format(record_key, record_value, total_count))
        except KeyboardInterrupt:
            pass
        finally:
            # Leave group and commit final offsets
            self.consumer.close()


if __name__ == "__main__":
    # postgres_con = postgres_write()
    # ddl = postgres_con.create_ddl()
    # print(ddl)
    # postgres_con.create_table(ddl)
    consumer = kafka_consumer("/Users/hisham/PycharmProjects/pythonProject/venv/proj/credentials/KafkaDevConfig.properties",
                              "housesigmascraper")
    consumer.subscribe_to_topic()


