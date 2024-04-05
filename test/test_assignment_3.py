import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, to_date, current_date, date_sub

class TestUserActivityAnalysis(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test").getOrCreate()

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

        cls.df = cls.spark.createDataFrame(data, schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_dataframe_creation(self):
        # Test DataFrame creation
        self.assertEqual(self.df.count(), 8)

    def test_column_renaming(self):
        # Test column renaming
        renamed_df = self.df.toDF(*['log_id', 'user_id', 'user_activity', 'time_stamp'])
        self.assertEqual(renamed_df.columns, ['log_id', 'user_id', 'user_activity', 'time_stamp'])

    def test_user_activity_last_7_days(self):
        # Test calculation of user activity in the last 7 days
        seven_days_ago = date_sub(current_date(), 7)
        user_activity_last_7_days = self.df.filter(col("time_stamp") >= seven_days_ago).groupBy("user_id").count()
        self.assertEqual(user_activity_last_7_days.count(), 3)

    def test_timestamp_conversion(self):
        # Test timestamp conversion
        converted_df = self.df.withColumn("login_date", to_date("time_stamp", 'yyyy-MM-dd HH:mm:ss'))
        self.assertEqual(converted_df.columns, ['log_id', 'user_id', 'user_activity', 'time_stamp', 'login_date'])

if __name__ == '__main__':
    unittest.main()

