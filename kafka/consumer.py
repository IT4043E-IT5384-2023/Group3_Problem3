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
logger = get_logger("producer")

def value_deserializer_func(data):
    return json.loads(data.decode('utf-8'))

class Consumer():
    def __init__(self):

        # spark session
        self._spark = get_spark_session(jobname="ProducerJob")

        # kafka producer
        self.consumer = KafkaConsumer(bootstrap_servers=[KAFKA_URL],
                                      value_deserializer=value_deserializer_func)

    def consume():
        raise NotImplementedError

if __name__ == "__main__":
    consumer = Consumer()
    consumer.consume()
