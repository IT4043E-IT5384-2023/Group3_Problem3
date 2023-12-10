import sys
sys.path.append(".")

from kafka import KafkaProducer
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

def on_send_success(record_metadata):
    logger.info((record_metadata.topic, record_metadata.partition, record_metadata.offset))

def on_send_error(excp):
    logger.error('Error in producer', exc_info=excp)
    # handle exception

def value_serializer_func(data):
    return json.dumps(data).encode('utf-8')

class Producer():
    def __init__(self):

        # spark session
        self._spark = get_spark_session(jobname="ProducerJob")

        # kafka producer
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_URL],
                                      value_serializer=value_serializer_func)

    def produce():
        raise NotImplementedError

if __name__ == "__main__":
    producer = Producer()
    producer.produce()
