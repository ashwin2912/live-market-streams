from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, expr, sum, max, window
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, LongType,DoubleType, TimestampType

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
df_parsed = df.select(from_json(col("value"), schema).alias("parsed")) \
              .selectExpr("parsed.*")

df_exploded = df_parsed.withColumn("event", explode("events")) \
                .withColumn("update", explode("event.updates")) \
                .select(
                    col("timestamp").cast(TimestampType()),
                    col("event.product_id"),
                    "update.side",
                    "update.event_time",
                    col("update.price_level").cast(DoubleType()).alias('price_level'),
                    col("update.new_quantity").cast(DoubleType()).alias('new_quantity')
                )

# Add a watermark
df_with_watermark = df_exploded \
    .withWatermark("event_time", "1 second")

df_with_watermark.printSchema()

# Windowed aggregation
df_windowed = df_with_watermark.groupBy(
    window(col("event_time"), "1 second"), "side"
).agg(
    sum("new_quantity").alias("total_quantity")
)

df_bid = df_windowed.filter(col("side") == "bid").groupBy("window").agg(max("total_quantity").alias("max_bid"))
df_ask = df_windowed.filter(col("side") == "ask").groupBy("window").agg(max("total_quantity").alias("max_ask"))

# Join bids and asks to calculate spread
spread_df = df_bid.join(df_ask, "window") \
    .withColumn("spread", expr("max_bid - max_ask"))

# Write the output
query = spread_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
