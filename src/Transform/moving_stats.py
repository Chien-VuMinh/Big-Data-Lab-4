from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("BTC Price Moving Stats") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=D:/hadoop") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Thiết lập mức log
spark.sparkContext.setLogLevel("WARN")

# Định nghĩa schema cho dữ liệu đầu vào
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Đọc dữ liệu từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON từ Kafka
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Chuyển đổi timestamp thành timestamp Spark - đơn giản hóa
parsed_df = parsed_df.withColumn(
    "event_time", 
    to_timestamp(col("timestamp"))
)

# Danh sách các cửa sổ thời gian cần tính toán
windows = [
    ("30 seconds", "10 seconds"),  # 30 giây, trượt 10 giây
    ("1 minute", "10 seconds"),    # 1 phút, trượt 10 giây
    ("5 minutes", "10 seconds"),   # 5 phút, trượt 10 giây
    ("15 minutes", "10 seconds"),  # 15 phút, trượt 10 giây
    ("30 minutes", "10 seconds"),  # 30 phút, trượt 10 giây
    ("1 hour", "10 seconds")       # 1 giờ, trượt 10 giây
]

# Tạo DataFrame rỗng để lưu kết quả
result_df = None

# Tính toán cho từng cửa sổ thời gian
for window_size, slide_size in windows:
    window_df = parsed_df \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window("event_time", window_size, slide_size),
            col("symbol")
        ) \
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        ) \
        .select(
            col("window.end").alias("window_end"),
            col("symbol"),
            lit(window_size.split(" ")[0] + window_size.split(" ")[1][0]).alias("window"),
            col("avg_price"),
            col("std_price")
        )
    
    if result_df is None:
        result_df = window_df
    else:
        result_df = result_df.union(window_df)

# Chuyển đổi kết quả thành định dạng JSON theo yêu cầu
output_df = result_df \
    .withColumn("timestamp", date_format(col("window_end"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
    .groupBy("timestamp", "symbol") \
    .agg(
        collect_list(
            struct(
                col("window"),
                col("avg_price"),
                col("std_price")
            )
        ).alias("stats")
    ) \
    .select(
        col("timestamp"),
        col("symbol"),
        col("stats")
    )

# Ghi kết quả vào Kafka
query = output_df \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "btc-price-moving") \
    .option("checkpointLocation", "memory") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

logger.info("Moving Stats Streaming started")
query.awaitTermination()