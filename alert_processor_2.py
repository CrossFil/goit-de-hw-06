from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, to_json, struct, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
)

# 1. SparkSession
spark = SparkSession.builder \
    .appName("KafkaAlertProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Схема вхідного повідомлення
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType())
])

# 3. Читання з Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("subscribe", "oleg_building_sensors") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .load()

# 4. Парсинг JSON
parsed = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))

# 5. Агрегація
agg = parsed \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(window(col("event_time"), "1 minute", "30 seconds")) \
    .agg({"temperature": "avg", "humidity": "avg"}) \
    .withColumnRenamed("avg(temperature)", "t_avg") \
    .withColumnRenamed("avg(humidity)", "h_avg")

# 6. Умови алертів
alert_schema = StructType([
    StructField("id", IntegerType()),
    StructField("humidity_min", DoubleType()),
    StructField("humidity_max", DoubleType()),
    StructField("temperature_min", DoubleType()),
    StructField("temperature_max", DoubleType()),
    StructField("code", StringType()),
    StructField("message", StringType())
])
alerts_df = spark.read \
    .option("header", True) \
    .schema(alert_schema) \
    .csv("/Users/admin/PycharmProjects/kafka-python/alerts_conditions.csv")

# 7. Cross join + фільтр
joined = agg.crossJoin(alerts_df)
cond = (
    ((col("humidity_min") == -999) | (col("h_avg") >= col("humidity_min"))) &
    ((col("humidity_max") == -999) | (col("h_avg") <= col("humidity_max"))) &
    ((col("temperature_min") == -999) | (col("t_avg") >= col("temperature_min"))) &
    ((col("temperature_max") == -999) | (col("t_avg") <= col("temperature_max")))
)
matched = joined.filter(cond)

# 8. Підготовка полів
prepared = matched \
    .withColumn("window_start", col("window.start").cast("string")) \
    .withColumn("window_end", col("window.end").cast("string")) \
    .withColumn("timestamp", current_timestamp().cast("string"))

# 9. Створення JSON
json_df = prepared.select(
    # Додаємо key = null
    lit(None).cast(StringType()).alias("key"),
    to_json(struct(
        col("window_start"),
        col("window_end"),
        col("t_avg"),
        col("h_avg"),
        col("code"),
        col("message"),
        col("timestamp")
    )).alias("value")
)

#10. (Діагностика) — раскоментуйте для перевірки в консоль
json_df.printSchema()
debug = json_df.writeStream.format("console").option("truncate", False).outputMode("append").start()
debug.awaitTermination(20)
debug.stop()

# 11. Запис до Kafka
query = json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "alerts_output") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required '
            'username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("checkpointLocation", "/tmp/checkpoints/alerts_final") \
    .outputMode("append") \
    .start()

query.awaitTermination()
