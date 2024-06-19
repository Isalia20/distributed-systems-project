from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType


CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_PORT = "9042"
#NOTE on dummy data
# KEYSPACE = "sensor_data"
# TABLE = "temperature_readings"
KEYSPACE = "weather"
TABLE = "clean_data"


spark = SparkSession.builder \
    .appName("ReadFromCassandra") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
    .getOrCreate()

df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=TABLE, keyspace=KEYSPACE) \
    .load()
print(df.columns)

df = df.withColumn("ts", F.col("ts").cast(TimestampType())).cache()

# Step 1
# Filter the last 24 hours
current_time = spark.sql("SELECT current_timestamp()").collect()[0][0]
last_24_hours_df = df.filter(F.col("ts") > (current_time - F.expr('INTERVAL 1 DAY')))

# Calculate the hourly average for each station
hourly_avg_df = last_24_hours_df.withColumn("hour", F.date_trunc("hour", F.col("ts")))
hourly_avg_df = hourly_avg_df.groupBy("station_id", "hour").agg(
    F.avg((F.col("temperature"))).alias("hourly_avg_temp")
)

hourly_avg_df.show(100)


# Step 2
last_7_days_df = df.filter(F.col("ts") > (current_time - F.expr('INTERVAL 7 DAY')))

# Calculate the daily metrics for each station
daily_metrics_df = last_7_days_df.withColumn("day", F.date_trunc("day", F.col("ts")))
daily_metrics_df = daily_metrics_df.groupBy("station_id", "day").agg(
    F.max((F.col("temperature"))).alias("daily_max_temp"),
    F.mean((F.col("temperature"))).alias("daily_avg_temp"),
    F.min((F.col("temperature"))).alias("daily_min_temp")
)

daily_metrics_df.show(100)
