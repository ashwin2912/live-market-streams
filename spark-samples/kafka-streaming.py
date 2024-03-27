from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import pyspark.sql.types as T

spark = SparkSession \
    .builder \
    .appName("KafkaReadExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "socket-messages") \
  .load()

from pyspark.sql.functions import col, expr

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
