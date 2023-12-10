from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from dotenv import load_dotenv
load_dotenv()
import os 
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

##Configuration

#config the connector jar file
spark = (SparkSession.builder.appName("SimpleSparkJob").master("spark://34.142.194.212:7077")
        .config("spark.jars", "jars/gcs-connector-hadoop2-latest.jar")
        .config("spark.executor.memory", "2G")  #excutor excute only 2G
        .config("spark.driver.memory","4G") 
        .config("spark.executor.cores","3") #Cluster use only 3 cores to excute
        .config("spark.python.worker.memory","1G") # each worker use 1G to excute
        .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
        .config("spark.kryoserializer.buffer.max","1024M")
        .getOrCreate())
#config the credential to identify the google cloud hadoop file 
spark.conf.set("google.cloud.auth.service.account.json.keyfile",GOOGLE_APPLICATION_CREDENTIALS)
spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')

## Connect to the file in Google Bucket with Spark

path=f"gs://it4043e-it5384/addresses.csv"
df = spark.read.csv(path)
df.show()

spark.stop() # Ending spark job
