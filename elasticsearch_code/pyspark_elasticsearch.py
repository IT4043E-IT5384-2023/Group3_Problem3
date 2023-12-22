from pyspark.sql import SparkSession
import os 

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ~/Documents/IT4043E_Group3_Problem3/jars/elasticsearch-spark-30_2.12-8.11.3.jar pyspark-shell'

spark = SparkSession.builder \
    .master("local") \
    .appName("ParquetToElasticsearch") \
    .getOrCreate()

# Read Parquet file from local folder
parquet_df = spark.read.parquet("data/part-00000-bec73947-9342-48e1-9b1d-b71e0c5e37b0-c000.snappy.parquet")

# Write the Parquet data to Elasticsearch
parquet_df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "34.143.255.36:9200") \
    .option("es.nodes.discovery", "false")\
    .option("es.nodes.wan.only", "true")\
    .option("es.resource", "my_great_test_index") \
    .option("es.net.http.auth.user", "elastic") \
    .option("es.net.http.auth.pass", "elastic2023") \
    .option("es.mapping.id", "id") \
    .option("es.write.operation", "upsert")\
    .mode("append") \
    .save()

spark.stop()
