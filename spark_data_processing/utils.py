from pyspark.sql import SparkSession

def get_spark_session(jobname: str="SimpleSparkJob"):
    spark = (SparkSession.builder.appName(jobname).master("spark://34.142.194.212:7077") 
        .config("spark.jars", "/opt/spark/jars/gcs-connector-latest-hadoop2.jar")
        .config("spark.executor.memory", "2G")  #excutor excute only 2G
        .config("spark.driver.memory","4G") 
        .config("spark.executor.cores","3") #Cluster use only 3 cores to excute
        .config("spark.python.worker.memory","1G") # each worker use 1G to excute
        .config("spark.driver.maxResultSize","3G") #Maximum size of result is 3G
        .config("spark.kryoserializer.buffer.max","1024M")
        .getOrCreate())
    
    spark.conf.set("google.cloud.auth.service.account.json.keyfile","/opt/spark/lucky-wall-393304-2a6a3df38253.json")
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    
    return spark
