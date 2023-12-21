import sys
sys.path.append(".")

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from dotenv import load_dotenv
load_dotenv()

import json
from spark_data_processing.utils import get_spark_session

import os
KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

from logger.logger import get_logger
logger = get_logger("consumer")

def value_deserializer_func(data):
    return json.loads(data.decode('utf-8'))

class Consumer():
    def __init__(self):

        # # spark session
        # self._spark = get_spark_session(jobname="SparkConsumerJob")

        # kafka producer
        self.consumer = KafkaConsumer(group_id="consumer_group",
                                      auto_offset_reset='earliest',
                                      bootstrap_servers=[KAFKA_URL],
                                      value_deserializer=value_deserializer_func,
                                      consumer_timeout_ms=2000)
        
        self.consumer.subscribe([KAFKA_TOPIC])

    def consume(self):
        raise NotImplementedError

if __name__ == "__main__":
    consumer = Consumer()
    consumer.consume()
