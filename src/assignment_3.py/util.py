from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import col, to_date, current_date, date_sub

def create_custom_schema():
    """
    Create a custom schema for the DataFrame.
    """
    return StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_activity", StringType(), True),
        StructField("time_stamp", StringType(), True)
    ])

def calculate_user_activity_last_7_days(df):
    """
    Calculate the number of actions performed by each user in the last 7 days.
    """
    seven_days_ago = date_sub(current_date(), 7)
    return df.filter(col("time_stamp") >= seven_days_ago).groupBy("user_id").count()

def convert_time_stamp_to_login_date(df):
    """
    Convert the time stamp column to the login_date column with YYYY-MM-DD format.
    """
    return df.withColumn("login_date", to_date("time_stamp", 'yyyy-MM-dd HH:mm:ss'))
