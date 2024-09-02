# import findspark
# findspark.init()
# import os

# # Set up Spark to use the specified Kafka dependency version
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars ./libs/spark-sql-kafka-0-10_2.12-3.5.2.jar,./libs/kafka-clients-3.5.2.jar pyspark-shell'

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, window, min, max, expr
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

# # Initialize Spark session
# spark = SparkSession.builder \
#     .appName("KafkaDebeziumAggregation") \
#     .config("spark.jars.packages", 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2') \
#     .getOrCreate()

# # Define the schema based on the Debezium message format
# schema = StructType([
#     StructField("before", StructType([
#         StructField("price", IntegerType()),
#         StructField("volume", IntegerType()),
#         StructField("datetime_created", TimestampType())  # Updated to TimestampType
#     ])),
#     StructField("after", StructType([
#         StructField("price", IntegerType()),
#         StructField("volume", IntegerType()),
#         StructField("datetime_created", TimestampType())  # Updated to TimestampType
#     ])),
#     StructField("source", StructType([
#         StructField("version", StringType()),
#         StructField("connector", StringType()),
#         StructField("name", StringType()),
#         StructField("ts_ms", LongType()),
#         StructField("snapshot", StringType()),
#         StructField("db", StringType()),
#         StructField("sequence", StringType()),
#         StructField("ts_us", LongType()),
#         StructField("ts_ns", LongType()),
#         StructField("schema", StringType()),
#         StructField("table", StringType()),
#         StructField("txId", LongType()),
#         StructField("lsn", LongType()),
#         StructField("xmin", LongType())
#     ])),
#     StructField("op", StringType()),
#     StructField("ts_ms", LongType()),
#     StructField("ts_us", LongType()),
#     StructField("ts_ns", LongType())
# ])

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaDebeziumAggregation") \
    .config("spark.jars.packages", 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2') \
    .getOrCreate()

print("*"*50)
# Define the schema based on the provided JSON format
schema = StructType([
    StructField("before", StructType([
        StructField("price", IntegerType(), True),
        StructField("volume", IntegerType(), True),
        StructField("datetime_created", LongType(), True)  # Timestamp in microseconds
    ]), True),
    StructField("after", StructType([
        StructField("price", IntegerType(), True),
        StructField("volume", IntegerType(), True),
        StructField("datetime_created", LongType(), True)  # Timestamp in microseconds
    ]), True),
    StructField("source", StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("sequence", StringType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True),
        StructField("txId", LongType(), True),
        StructField("lsn", LongType(), True),
        StructField("xmin", LongType(), True)
    ]), True),
    StructField("transaction", StructType([
        StructField("id", StringType(), True),
        StructField("total_order", LongType(), True),
        StructField("data_collection_order", LongType(), True)
    ]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True)
])

# Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.bank.holding") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("topic").cast("string"),schema).alias("json_data"))\
    .select("json_data.*")

print("#"*50)
# Extract and parse the JSON value from the Kafka message
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .select(from_json(col("json_value"), schema).alias("data")) \
    .select("data.*")

# Convert microseconds to timestamp for easier readability
def convert_microseconds_to_timestamp(col_name):
    return (col(col_name) / 1000000).cast(TimestampType())

# Select and transform the necessary fields
transformed_df = parsed_df.select(
    col("before.price").alias("before_price"),
    col("before.volume").alias("before_volume"),
    convert_microseconds_to_timestamp("before.datetime_created").alias("before_datetime_created"),
    col("after.price").alias("after_price"),
    col("after.volume").alias("after_volume"),
    convert_microseconds_to_timestamp("after.datetime_created").alias("after_datetime_created"),
    col("source.version"),
    col("source.connector"),
    col("source.name"),
    col("source.ts_ms"),
    col("source.snapshot"),
    col("source.db"),
    col("source.sequence"),
    col("source.ts_us"),
    col("source.ts_ns"),
    col("source.schema"),
    col("source.table"),
    col("source.txId"),
    col("source.lsn"),
    col("source.xmin"),
    col("transaction.id"),
    col("transaction.total_order"),
    col("transaction.data_collection_order"),
    col("op"),
    col("ts_ms").alias("operation_ts_ms"),
    col("ts_us").alias("operation_ts_us"),
    col("ts_ns").alias("operation_ts_ns")
)

# Print the DataFrame to the console
query = transformed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("@"*50)
query.awaitTermination()



# # Read streaming data from Kafka
# kafka_df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "dbserver1.bank.holding") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Parse the JSON data and extract the 'after' field
# json_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
#     .select(from_json("json", schema).alias("data")) \
#     .select("data.after.*")


# # Perform aggregations
# aggregated_df = json_df.withWatermark("datetime_created", "1 hour") \
#     .groupBy(
#         window("datetime_created", "1 hour")
#     ) \
#     .agg(
#         # {"price": 'sum'}
#         expr("first(price) as open_price"),
#         expr("last(price) as close_price"),
#         min("price").alias("min_price"),
#         max("price").alias("max_price")
#     )

# # Write the results to the console for demonstration purposes
# query = aggregated_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# # Wait for the query to terminate
# query.awaitTermination()
