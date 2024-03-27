from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, DoubleType, LongType

# Define the schema of the JSON data to help Spark parse it
schema = StructType([
    StructField("channel", StringType()),
    StructField("client_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("sequence_num", LongType()),
    StructField("events", ArrayType(
        StructType([
            StructField("type", StringType()),
            StructField("tickers", ArrayType(
                StructType([
                    StructField("type", StringType()),
                    StructField("product_id", StringType()),
                    StructField("price", StringType()),
                    StructField("volume_24_h", StringType()),
                    StructField("low_24_h", StringType()),
                    StructField("high_24_h", StringType()),
                    StructField("low_52_w", StringType()),
                    StructField("high_52_w", StringType()),
                    StructField("price_percent_chg_24_h", StringType()),
                ])
            )),
        ])
    )),
])

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("KafkaReadExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Read from Kafka (replace with your Kafka source details)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "socket-messages") \
    .load()

# Select the 'value' field and cast it to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse the JSON
df_parsed = df.select(from_json(col("value"), schema).alias("parsed"))

# Flatten the data
df_flattened = df_parsed.select(
    col("parsed.channel"),
    col("parsed.client_id"),
    col("parsed.timestamp"),
    col("parsed.sequence_num"),
    col("parsed.events")[0]["type"].alias("event_type"),
    col("parsed.events")[0]["tickers"][0]["type"].alias("ticker_type"),
    col("parsed.events")[0]["tickers"][0]["product_id"],
    col("parsed.events")[0]["tickers"][0]["price"],
    col("parsed.events")[0]["tickers"][0]["volume_24_h"],
    col("parsed.events")[0]["tickers"][0]["low_24_h"],
    col("parsed.events")[0]["tickers"][0]["high_24_h"],
    col("parsed.events")[0]["tickers"][0]["low_52_w"],
    col("parsed.events")[0]["tickers"][0]["high_52_w"],
    col("parsed.events")[0]["tickers"][0]["price_percent_chg_24_h"]
)

# Write the stream to a console or your storage system of choice (for demonstration purposes, using console)
query = df_flattened.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
