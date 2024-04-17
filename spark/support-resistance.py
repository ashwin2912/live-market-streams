from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, FloatType, LongType, TimestampType

schema = StructType([
    StructField("channel",StringType()),
    StructField("client_id",StringType()),
    StructField("timestamp",StringType()),
    StructField("sequence_num", LongType()),
    StructField("events", ArrayType(
        StructType([
            StructField("type",StringType()),
            StructField("product_id",StringType()),
            StructField("updates",ArrayType(
                StructType([
                    StructField("side", StringType()),
                    StructField("event_time", TimestampType()),
                    StructField("price_level", StringType()),
                    StructField("new_quantity",StringType())
                ])
            ))
        ])
    ))
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



df_exploded = df_parsed.withColumn("event", explode("parsed.events")) \
                .withColumn("update", explode("event.updates")) \
                .select(
                    "parsed.channel",
                    "parsed.client_id",
                    "parsed.timestamp",
                    "parsed.sequence_num",
                    "event.type",
                    "event.product_id",
                    "update.side",
                    "update.event_time",
                    "update.price_level",
                    "update.new_quantity"
                )

print(df_exploded.columns)

df_sides = df_exploded.select(
    col("timestamp").cast(TimestampType()),
    col("side"),
    col("price_level").cast("float").alias("price_level"),
    col("new_quantity").cast("float").alias("new_quantity")
)

df_agg = df_sides.groupby("timestamp","side","price_level").sum("new_quantity")



# Write the stream to a console or your storage system of choice (for demonstration purposes, using console)
query = df_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False)\
    .start()

query.awaitTermination()

df_agg.write.format("csv").mode("overwrite").save("/Users/ashdhan/Documents/Cave/Experiments/live-var/live-var-estimation/spark/support-and-resistance.csv")


'''

df_updates = df_parsed.select(
    explode(col('parsed.events.updates').alias('updates'))
)

df_flat = df_parsed.select(
    col('parsed.channel').alias('channel'),
    col('parsed.timestamp').alias('timestamp'),
    col('parsed.sequence_num').alias('seq_num'),
    col('parsed.events.type').alias('event_type'),
    col('parsed.events.product_id').alias('product_id'),
    explode('parsed.events.updates').alias('updates')
)

df_final = df_flat.select(
    'channel',
    'timestamp',
    'seq_num',
    'event_type',
    'product_id',
    'updates.side',
    'updates.event_time',
    'updates.price_level',
    'updates.new_quantity'
)

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
df_flattened = df_parsed.selectExpr(
    "parsed.channel",
    "parsed.timestamp",
    "parsed.sequence_num",
    "explode(parsed.events.updates) as updates"
)

'''
