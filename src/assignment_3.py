from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import col, to_date, current_date, date_sub

spark = SparkSession.builder.appName("data").getOrCreate()

# Create DataFrame with custom schema
schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_activity", StringType(), True),
    StructField("time_stamp", StringType(), True)
])

data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]

df = spark.createDataFrame(data, schema)

df.show()

# Rename columns using dynamic function
df = df.toDF(*['log_id', 'user_id', 'user_activity', 'time_stamp'])
df.show()

# Write a query to calculate the number of actions performed by each user in the last 7 days
seven_days_ago = date_sub(current_date(), 7)
user_activity_last_7_days = df.filter(col("time_stamp") >= seven_days_ago).groupBy("user_id").count()
user_activity_last_7_days.show()

# Convert the time stamp column to the login_date column with YYYY-MM-DD format
df = df.withColumn("login_date", to_date("time_stamp", 'yyyy-MM-dd HH:mm:ss'))
df.show()

# Show results
print("DataFrame with custom schema:")
df.show()

print("Number of actions performed by each user in the last 7 days:")
user_activity_last_7_days.show()

