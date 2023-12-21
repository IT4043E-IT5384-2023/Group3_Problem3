import sys
sys.path.append(".")

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

from dotenv import load_dotenv
load_dotenv()

import json
import time
import argparse
from twitter_crawler.twitter_daily import get_twitter_session,crawl_tweet_kol_last_day, KEYWORDS_FOR_ACCOUNT

import os
KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

from logger.logger import get_logger

def on_send_success(record_metadata):
    logger.info((record_metadata.topic, record_metadata.partition, record_metadata.offset))

def on_send_error(excp):
    logger.error('Error in producer', exc_info=excp)
    # handle exception

def value_serializer_func(data):
    return json.dumps(data).encode('utf-8')


class Producer():
    def __init__(self, producer_id: int):

        self.producer_id = producer_id

        # create kafka topic
        self.create_topic(topic_name=KAFKA_TOPIC)

        # kafka producer
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_URL],
                                      api_version=(3, 6, 1),
                                      value_serializer=value_serializer_func)
        
    def create_topic(self, topic_name: str, partition: int = 3, replication_factor: int = 1):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_URL)

            admin_client.create_topics(new_topics=[NewTopic(name=topic_name,
                                                            num_partitions=partition,
                                                            replication_factor=replication_factor)],
                                       validate_only=False)

            logger.info(f"Trying to create topic: {topic_name}")
        except TopicAlreadyExistsError as e:
            logger.error(e)
            pass
    
    def produce(self):
        app = get_twitter_session(account_id=self.producer_id)

        crawled_data = crawl_tweet_kol_last_day(
            app=app,
            keywords=KEYWORDS_FOR_ACCOUNT[self.producer_id-1]
        )

        for tweet in crawled_data:
            self.producer.send(KAFKA_TOPIC, value=tweet).add_callback(on_send_success) \
                                                        .add_errback(on_send_error)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--producer-id", type=int, help="Producer id used to crawl tweets")
    args = parser.parse_args()

    logger = get_logger(f"producer_{args.producer_id}")
    time.sleep(5)

    producer = Producer(producer_id=args.producer_id)
    producer.produce()
