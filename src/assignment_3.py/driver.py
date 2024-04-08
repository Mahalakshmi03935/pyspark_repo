from pyspark.sql import SparkSession
from util import create_custom_schema, calculate_user_activity_last_7_days, convert_time_stamp_to_login_date

def create_spark_session():
    """
    Create a SparkSession.
    """
    return SparkSession.builder.appName("data").getOrCreate()

def create_data_frame(spark, schema):
    """
    Create DataFrame using custom schema.
    """
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
    return spark.createDataFrame(data, schema)

if __name__ == "__main__":
    # Driver code
    spark = create_spark_session()
    schema = create_custom_schema()
    df = create_data_frame(spark, schema)

    print("DataFrame with custom schema:")
    df.show()

    df = df.toDF(*['log_id', 'user_id', 'user_activity', 'time_stamp'])

    print("Renamed DataFrame:")
    df.show()

    user_activity_last_7_days = calculate_user_activity_last_7_days(df)
    print("Number of actions performed by each user in the last 7 days:")
    user_activity_last_7_days.show()

    df = convert_time_stamp_to_login_date(df)
    print("DataFrame with login date:")
    df.show()

