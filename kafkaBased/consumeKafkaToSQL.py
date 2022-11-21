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

    def create_ddl(self):
        # transform json to dict
        with open("/Users/hisham/PycharmProjects/pythonProject/venv/proj/data/extracted_sold.json", "r") as json_file:
            lines = json_file.readlines()[0]
            data = json.loads(json.loads(lines))

        data_types = data.values()
        column_names = list(data.keys())
        assert (len(data_types) == len(column_names))
        ddl_types = self.create_datatypes(data_types)
        for column in range(len(column_names)):
            column_names[column] = re.sub(r'\W+', '', column_names[column])
        create_statement = "CREATE TABLE IF NOT EXISTS housesigma_sold (\n"
        for i in range(len(ddl_types) - 1):
            create_statement = create_statement + column_names[i] + " " + ddl_types[i] + ",\n"
        create_statement += column_names[-1] + " " + ddl_types[-1] + "\n"
        create_statement += ");"

        return create_statement


        # print(data.values())

    def create_datatypes(self, data_types):
        ddl_types = []
        for value in data_types:
            print(value)
            if value is not None:
                if re.search('[a-zA-Z\s]', value):
                    ddl_types.append("varchar(255)")
                elif '-' in value and re.search('[0-9]', value):
                    ddl_types.append("date")
                elif '.' in value:
                    ddl_types.append("float")
                elif re.search('[0-9]', value):
                    ddl_types.append("int")
                else:
                    ddl_types.append("varchar(255)")
            else:
                ddl_types.append("varchar(255)")
            print(len(ddl_types))
        return ddl_types

    def write_message(self, message, tablename="housesigma_sold"):

        print("nothing")

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
    # consumer = kafka_consumer("/Users/hisham/PycharmProjects/pythonProject/venv/proj/credentials/KafkaDevConfig.properties",
    #                           "housesigmascraper")
    # consumer.subscribe_to_topic()
    postgres_con = postgres_write()
    ddl = postgres_con.create_ddl()
    postgres_con.create_table(ddl)
