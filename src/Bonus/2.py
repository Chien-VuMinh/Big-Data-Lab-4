from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, when, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_INPUT = "btc-price"
KAFKA_TOPIC_HIGHER = "btc-price-higher"
KAFKA_TOPIC_LOWER = "btc-price-lower"

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("BTCPriceTransform") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
            .getOrCreate()
                
        logger.info("Spark session created successfully")

        # Define schema for input data from btc-price topic
        schema = StructType([
            StructField("symbol", StringType(), False),
            StructField("price", DoubleType(), False),
            StructField("timestamp", StringType(), False)
        ])

        # Read from Kafka topic btc-price
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC_INPUT) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON data
        df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Convert timestamp to Spark timestamp type
        df = df.withColumn("event_time", col("timestamp").cast("timestamp"))

        # Define a 20-second window for processing
        windowed_df = df.withWatermark("event_time", "20 seconds") \
            .groupBy(
                col("symbol"),
                col("price").alias("base_price"),
                col("event_time").alias("base_time")
            )

        # Use Spark SQL to find higher and lower price records within 20 seconds
        df.createOrReplaceTempView("price_data")

        # Query to find the first higher price within 20 seconds
        higher_query = spark.sql("""
            SELECT 
                p1.symbol,
                p1.base_price,
                p1.base_time,
                MIN(p2.event_time) as higher_time
            FROM price_data p1
            LEFT JOIN price_data p2
            ON p2.event_time > p1.base_time
            AND p2.event_time <= p1.base_time + INTERVAL 20 SECONDS
            AND p2.price > p1.base_price
            GROUP BY p1.symbol, p1.base_price, p1.base_time
        """)

        # Query to find the first lower price within 20 seconds
        lower_query = spark.sql("""
            SELECT 
                p1.symbol,
                p1.base_price,
                p1.base_time,
                MIN(p2.event_time) as lower_time
            FROM price_data p1
            LEFT JOIN price_data p2
            ON p2.event_time > p1.base_time
            AND p2.event_time <= p1.base_time + INTERVAL 20 SECONDS
            AND p2.price < p1.base_price
            GROUP BY p1.symbol, p1.base_price, p1.base_time
        """)

        # Calculate time differences in seconds
        higher_df = higher_query.withColumn(
            "higher_window",
            when(col("higher_time").isNotNull(),
                 (unix_timestamp(col("higher_time")) - unix_timestamp(col("base_time"))).cast("double")
            ).otherwise(20.0)
        ).select(
            col("base_time").cast("string").alias("timestamp"),
            col("higher_window")
        )

        lower_df = lower_query.withColumn(
            "lower_window",
            when(col("lower_time").isNotNull(),
                 (unix_timestamp(col("lower_time")) - unix_timestamp(col("base_time"))).cast("double")
            ).otherwise(20.0)
        ).select(
            col("base_time").cast("string").alias("timestamp"),
            col("lower_window")
        )

        # Convert to JSON for Kafka output
        higher_output = higher_df.select(
            to_json(struct("timestamp", "higher_window")).alias("value")
        )

        lower_output = lower_df.select(
            to_json(struct("timestamp", "lower_window")).alias("value")
        )

        # Write to Kafka topic btc-price-higher
        higher_query = higher_output.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", KAFKA_TOPIC_HIGHER) \
            .option("checkpointLocation", "/tmp/spark/checkpoint/higher") \
            .outputMode("append") \
            .start()

        # Write to Kafka topic btc-price-lower
        lower_query = lower_output.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("topic", KAFKA_TOPIC_LOWER) \
            .option("checkpointLocation", "/tmp/spark/checkpoint/lower") \
            .outputMode("append") \
            .start()

        logger.info("Started streaming to Kafka topics btc-price-higher and btc-price-lower")

        # Await termination
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise
    finally:
        logger.info("Closing Spark session")
        spark.stop()



if __name__ == "__main__":
    main()