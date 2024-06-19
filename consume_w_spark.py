import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, date_trunc
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
KAFKA_ADDRESS = "localhost:9093"
KAFKA_TOPIC_NAME = "sensor_topic"
# KAFKA_TOPIC_NAME = "monitoring"


spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .getOrCreate()

schema = StructType([
    StructField("err_type", StringType(), True),
    StructField("ts", TimestampType(), True),
    StructField("station_id", StringType(), True),
    StructField("sensor_id", StringType(), True),
])


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_ADDRESS) \
    .option("subscribe", KAFKA_TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING)")
df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

def write_csv(df, name):
    df.write.format("csv").mode("append").option("header", "true").save(name)

def count_station_id(df):
    new_df = df.groupBy("station_id").agg(F.count(F.lit(1)).alias("count"))
    write_csv(new_df, "results/error_count_by_station")

def count_station_id_by_day(df):
    new_df = df.groupBy("station_id", "day").agg(F.count(F.lit(1)).alias("count"))
    write_csv(new_df, "results/error_count_by_day_and_station")

def count_station_id_by_sensors(df):
    new_df = df.filter(F.col("sensor_id").isNotNull()).groupBy("station_id", "sensor_id").agg(F.count(F.lit(1)).alias("count"))
    write_csv(new_df, "results/error_count_by_station_sensor_id")

def count_station_id_by_errors(df):
    new_df = df.groupBy("station_id", "err_type").agg(F.count(F.lit(1)).alias("count"))
    write_csv(new_df, "results/error_count_by_station_id_err_type")

def count_all_errs(df):
    print("Number of errors ", df.count())

def count_errors_by_day(df):
    new_df = df.groupBy("day").agg(F.count(F.lit(1)).alias("count"))
    write_csv(new_df, "results/error_count_by_days")

def count_errors_by_day_err_type(df):
    new_df = df.groupBy("day", "err_type").agg(F.count(F.lit(1)).alias("count"))
    write_csv(new_df, "results/error_count_by_days_and_err_type")

def process_batch(batch_df, batch_id):
    df = batch_df.filter(F.col("station_id").isNotNull())
    df = df.withColumn("day", date_trunc("day", col("ts"))).cache()
    df = df.repartition(1)
    count_station_id(df)
    count_station_id_by_day(df)
    count_station_id_by_sensors(df)
    count_station_id_by_errors(df)
    count_all_errs(df)
    count_errors_by_day(df)
    count_errors_by_day_err_type(df)

while True:
    try:
        query = df.writeStream \
            .foreachBatch(process_batch) \
            .trigger(processingTime="5 seconds") \
            .start()

        query.awaitTermination()
    except KeyboardInterrupt:
        break
    finally:
        print("error occurred")
