from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo Spark Session với các gói phụ thuộc Kafka
spark = SparkSession.builder \
    .appName("BTC Price Z-Score") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=D:/minhanh/env") \
    .getOrCreate()

# Thiết lập mức log
spark.sparkContext.setLogLevel("WARN")

# Định nghĩa schema cho dữ liệu giá
price_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Định nghĩa schema cho dữ liệu thống kê động
stats_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("stats", ArrayType(
        StructType([
            StructField("window", StringType(), True),
            StructField("avg_price", DoubleType(), True),
            StructField("std_price", DoubleType(), True)
        ])
    ), True)
])

# Đọc dữ liệu giá từ Kafka
price_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), price_schema).alias("data")) \
    .select("data.*")

# Chuyển đổi cột timestamp từ String sang Timestamp
price_df = price_df.withColumn("timestamp", to_timestamp("timestamp"))

# Đọc dữ liệu thống kê động từ Kafka
stats_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "btc-price-moving") \
    .option("startingOffsets", "latest") \
    .load() \
    .select(from_json(col("value").cast("string"), stats_schema).alias("data")) \
    .select("data.*")

# Chuyển đổi cột timestamp từ String sang Timestamp
stats_df = stats_df.withColumn("timestamp", to_timestamp("timestamp"))

# Tạo watermark để xử lý dữ liệu trễ
price_df = price_df.withWatermark("timestamp", "10 seconds")
stats_df = stats_df.withWatermark("timestamp", "10 seconds")

# Join hai DataFrame dựa trên timestamp và symbol
joined_df = price_df.join(
    stats_df,
    (price_df.timestamp == stats_df.timestamp) & (price_df.symbol == stats_df.symbol),
    "inner"
)

# Tạo UDF để tính điểm Z
def calculate_zscores(price, stats_array):
    result = []
    for stat in stats_array:
        window = stat["window"]
        avg_price = stat["avg_price"]
        std_price = stat["std_price"]
        
        # Tránh chia cho 0
        if std_price > 0:
            zscore = (price - avg_price) / std_price
        else:
            zscore = 0.0
            
        result.append({"window": window, "zscore_price": zscore})
    return result

calculate_zscores_udf = udf(calculate_zscores, ArrayType(
    StructType([
        StructField("window", StringType(), True),
        StructField("zscore_price", DoubleType(), True)
    ])
))

# Áp dụng UDF để tính điểm Z
output_df = joined_df.select(
    price_df.timestamp,
    price_df.symbol,
    calculate_zscores_udf(price_df.price, stats_df.stats).alias("zscores")
)

# Ghi kết quả vào Kafka
query = output_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "btc-price-zscore") \
    .option("checkpointLocation", "memory") \
    .outputMode("append") \
    .start()

logger.info("Z-Score Streaming started")
query.awaitTermination()