from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, min, max, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.functions import unix_timestamp
import pyspark.sql.functions as F



# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaDebeziumAggregation") \
    .config("spark.jars.packages", 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2') \
    .getOrCreate()

# Step 2: Define Kafka Read Stream
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.bank.holding") \
    .option("startingOffsets", "earliest") \
    .load()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

# Define the schema for the Debezium CDC output
debezium_schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("price", IntegerType(), True),
            StructField("volume", IntegerType(), True),
            StructField("datetime_created", LongType(), False)  # Microtimestamp represented as LongType
        ]), True),
        StructField("after", StructType([
            StructField("price", IntegerType(), True),
            StructField("volume", IntegerType(), True),
            StructField("datetime_created", LongType(), False)  # Microtimestamp represented as LongType
        ]), True),
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("ts_us", LongType(), True),
            StructField("ts_ns", LongType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True),
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True)
    ]), False),
    StructField("schema", StructType([]), False)  # Empty schema for structural alignment
])

# # Parse the Kafka value field as JSON
# parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
#     .select(from_json(col("json"), debezium_schema).alias("data")) \
#     .select("data.payload.before", "data.payload.after")

# # Output the DataFrame to the console
# query = parsed_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()




# parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
#     .select(from_json(col("json"), debezium_schema).alias("data")) 

# after_df = parsed_df.select("data.payload.after.*")

# # Convert microseconds to Timestamp for 'datetime_created'
# after_df = after_df.withColumn("timestamp", (col("datetime_created") / 1000000).cast(TimestampType()))

# # Windowed Aggregations for the last hour
# windowed_df = after_df \
#     .withWatermark("timestamp", "1 hour") \
#     .groupBy(window(col("timestamp"), "1 hour")) \
#     .agg(
#         first("price").alias("open_price"),
#         last("price").alias("close_price"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price")
#     )

# # Function to calculate Maximum Drawdown (MDD)
# def calculate_mdd(df, epoch_id):
#     # Find the running maximum price up to each point in time
#     running_max = df.select(max("price")).first()[0]
#     # Calculate drawdown for each price as the difference from the running maximum
#     drawdowns = df.withColumn("drawdown", (running_max - col("price")) / running_max)
#     # Calculate maximum drawdown
#     mdd = drawdowns.agg(max("drawdown")).first()[0]
#     print(f"Maximum Drawdown (MDD): {mdd}")

# # Apply stateful computation to compute MDD across all time
# mdd_query = after_df.writeStream \
#     .foreachBatch(calculate_mdd) \
#     .outputMode("update") \
#     .start()

# # Output the windowed aggregates to the console
# query = windowed_df.writeStream \
#     .format("console") \
#     .outputMode("update") \
#     .start()

# query.awaitTermination()
# mdd_query.awaitTermination()












value_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), debezium_schema).alias("data"))

# Select the relevant fields from the parsed JSON
price_df = value_df.select(
    col("data.payload.after.price").alias("price"),
    (col("data.payload.after.datetime_created") / 1000000).cast(TimestampType()).alias("timestamp")
)

# Define a time window
time_window = window(col("timestamp"), "1 hour")

# Aggregate min and max prices
agg_df = price_df.groupBy(time_window.alias("hour_window")).agg(
    min("price").alias("min_price"),
    max("price").alias("max_price")
)

# Define schema for state
state_schema = StructType([
    StructField("hour_window", StringType(), True),
    StructField("open_price", IntegerType(), True),
    StructField("close_price", IntegerType(), True)
])

# Define state update function
def update_state(hour_window, values):
    prices = sorted(list(values), key=lambda x: x.timestamp)
    
    if len(prices) == 0:
        return [(hour_window, None, None)]
    
    open_price = prices[0].price
    close_price = prices[-1].price
    
    return [(hour_window, open_price, close_price)]

# Define the function to process each group
def process_group(hour_window, values_iter):
    values = list(values_iter)
    return update_state(hour_window, values)

# Apply stateful processing
stateful_df = price_df.groupBy(time_window.alias("hour_window")).applyInPandas(process_group, schema=state_schema)

# Join the aggregated min/max prices with the stateful open/close prices
result_df = agg_df.join(stateful_df, on="hour_window", how="inner") \
    .select(
        col("hour_window"),
        col("open_price"),
        col("close_price"),
        col("min_price"),
        col("max_price")
    )

# Output the computed metrics to console
query = result_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()