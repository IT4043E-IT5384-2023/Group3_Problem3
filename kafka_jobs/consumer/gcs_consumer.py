import sys
sys.path.append(".")

import time
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, when
from pyspark.sql.types import StructType, StringType, IntegerType

from dotenv import load_dotenv
load_dotenv()

import os
KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

from logger.logger import get_logger
logger = get_logger("gcs_consumer")

def value_deserializer_func(data):
    return json.loads(data.decode('utf-8'))

class Consumer():
    def __init__(self):

        # save paths
        self.local_save_path = "data"
        self.gsc_save_path = "gs://it4043e/it4043e_group3_problem3/data"
        self.checkpoint_path = "data/checkpoints/gcs"

        # spark session
        self._spark = SparkSession \
                    .builder \
                    .master('local') \
                    .appName("GCSConsumer") \
                    .config("spark.jars",
                            "jars/gcs-connector-hadoop3-latest.jar") \
                    .config("spark.jars.packages",
                            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                    .getOrCreate()

        self._spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS)
        self._spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        self._spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

        self._spark._jsc.hadoopConfiguration().set("fs.gs.outputstream.upload.buffer.size", "262144");
        self._spark._jsc.hadoopConfiguration().set("fs.gs.outputstream.upload.chunk.size", "1048576");
        self._spark._jsc.hadoopConfiguration().set("fs.gs.outputstream.upload.max.active.requests", "4");

        self._spark.sparkContext.setLogLevel("ERROR")

    def get_data_from_kafka(self):

        # wait for some seconds
        time.sleep(5)

        # Read messages from Kafka
        df = self._spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_URL) \
            .option("kafka.group.id", "gcs_consumer_group_test") \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Convert value column from Kafka to string
        df = df.selectExpr("CAST(value AS STRING)")
        logger.info(f"Consume topic: {KAFKA_TOPIC}")
        
        return df
    
    def save_data(self, batch_df, batch_id):

        records = batch_df.count()

        # Define the schema to extract specific fields
        schema = StructType() \
                .add("id", StringType()) \
                .add("name", StringType()) \
                .add("username", StringType()) \
                .add("bio", StringType()) \
                .add("location", StringType()) \
                .add("profile_url", StringType()) \
                .add("join_date", StringType()) \
                .add("statuses_count", IntegerType()) \
                .add("friends_count", IntegerType()) \
                .add("followers_count", IntegerType()) \
                .add("favourites_count", IntegerType()) \
                .add("media_count", IntegerType()) \
                .add("protected", StringType()) \
                .add("verified", StringType()) \
                .add("profile_image_url_https", StringType()) \
                .add("profile_banner_url", StringType())
        
        # Parse JSON messages using the adjusted schema
        parsed_df = batch_df.select(from_json(batch_df.value, schema).alias("data")) \
                            .select("data.*")

        # Convert "protected" and "verified" fields to integers (0 or 1)
        parsed_df = parsed_df.withColumn("protected", when(parsed_df["protected"] == "True", 1).otherwise(0)) \
                             .withColumn("verified", when(parsed_df["verified"] == "True", 1).otherwise(0))
        
        # Rename specific fields
        parsed_df = parsed_df \
            .withColumnRenamed("profile_url", "url") \
            .withColumnRenamed("statuses_count", "tweets") \
            .withColumnRenamed("friends_count", "following") \
            .withColumnRenamed("followers_count", "followers") \
            .withColumnRenamed("favourites_count", "likes") \
            .withColumnRenamed("media_count", "media") \
            .withColumnRenamed("profile_image_url_https", "profile_image_url") \
            .withColumnRenamed("profile_banner_url", "background_image")
        
        # Coalesce to a single partition before writing to Parquet
        parsed_df = parsed_df.coalesce(1)

        # Write the parsed messages to Parquet format
        parsed_df \
            .write \
            .format("parquet") \
            .option("path", self.local_save_path) \
            .mode("append") \
            .save()
        
        # parsed_df \
        #     .write \
        #     .format("parquet") \
        #     .option("path", self.gsc_save_path) \
        #     .mode("append") \
        #     .save()
        
        logger.info(f"Save data to GCS: ({records} records)")

    def consume(self):
        try: 
            df = self.get_data_from_kafka()

            stream = df \
                .writeStream \
                .trigger(processingTime='30 seconds') \
                .foreachBatch(self.save_data) \
                .outputMode("append") \
                .option("checkpointLocation", self.checkpoint_path) \
                .start()
            
            stream.awaitTermination(30)

        except Exception as e:
            logger.error(e)

        finally:
            self._spark.stop()

if __name__ == "__main__":
    consumer = Consumer()
    consumer.consume()
