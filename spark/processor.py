import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, struct, to_json, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092") 
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "elasticsearch")
TOPIC_INPUT = "input-pins"
TOPIC_OUTPUT = "enriched-pins"

print(f"🚀 SPARK 3.4.2 PARTITO. Broker: {KAFKA_BROKER}")

spark = SparkSession.builder \
    .appName("PinSecurityProcessor") \
    .config("spark.es.nodes", ELASTIC_HOST) \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema Input (con Location)
schema = StructType([
    StructField("chat_id", StringType()),
    StructField("pin", StringType()),
    StructField("language", StringType()),
    StructField("location", StructType([
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType())
    ]))
])

def calculate_safety(pin):
    if not pin: return (0, "ERROR")
    score = 100
    length = len(pin)
    if length < 4: return (0, "INVALID")
    if length == 4: score -= 20
    common_patterns = ["1234", "0000", "1111", "1212", "7777", "2580", "1122"]
    if any(p in pin for p in common_patterns): score -= 50
    repeat_count = sum(1 for i in range(len(pin)-1) if pin[i] == pin[i+1])
    score -= (repeat_count * 15)
    score = max(0, min(100, score))
    
    if score < 30: status = "VERY_WEAK"
    elif score < 60: status = "WEAK"
    elif score < 80: status = "MEDIUM"
    else: status = "STRONG"
    return (score, status)

safety_udf = udf(calculate_safety, StructType([
    StructField("score", IntegerType()),
    StructField("status", StringType())
]))

# Streaming Pipeline
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_INPUT) \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Elaborazione + Timestamp
df_processed = df_parsed.withColumn("safety", safety_udf(col("pin"))) \
    .withColumn("timestamp", current_timestamp()) \
    .select(
        col("chat_id"),
        col("pin"),
        col("safety.score").alias("score"),
        col("safety.status").alias("status"),
        col("location"),
        col("timestamp") 
    )

# Output Elasticsearch
query_es = df_processed.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/es") \
    .option("es.resource", "pin-security-events") \
    .start()

# Output Kafka (Feedback)
query_kafka = df_processed.select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", TOPIC_OUTPUT) \
    .option("checkpointLocation", "/tmp/spark-checkpoints/kafka") \
    .start()

spark.streams.awaitAnyTermination()
