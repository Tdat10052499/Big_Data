"""Lab 3: the pieces every streaming script in this lab needs.

Import it rather than copying the schema into five files.
    from s0_common import spark_session, trip_stream, SCHEMA
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (StructType, StringType, DoubleType, LongType, BooleanType)

BOOTSTRAP = "localhost:9092"
TOPIC = "taxi-trips"
HDFS = "hdfs://localhost:9000"

# The shape of one trip event, exactly as trip_producer.py sends it.
SCHEMA = (StructType()
          .add("trip_id", StringType())
          .add("event_time", StringType())
          .add("pickup_zone", LongType())
          .add("dropoff_zone", LongType())
          .add("passenger_count", LongType())
          .add("trip_distance", DoubleType())
          .add("fare_amount", DoubleType())
          .add("total_amount", DoubleType())
          .add("sent_at", StringType())
          .add("is_late", BooleanType()))


def spark_session(name):
    spark = (SparkSession.builder
             .appName(name)
             .master("local[4]")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def trip_stream(spark, starting="earliest"):
    """Read the topic and turn the raw Kafka bytes into trip columns.

    Kafka hands Spark a fixed set of columns: key, value, topic, partition,
    offset, timestamp. The payload is in value, as bytes, so it has to be cast
    to a string and parsed. Nothing about the JSON is known to Kafka itself.
    """
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", starting)
           .load())

    return (raw
            .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"),
                    F.col("partition"), F.col("offset"))
            .select("t.*", "partition", "offset")
            .withColumn("event_time", F.to_timestamp("event_time"))
            .withColumn("sent_at", F.to_timestamp("sent_at")))
