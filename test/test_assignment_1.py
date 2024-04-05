import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

class TestSparkCode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_basic(self):
        # Test the basic functionality
        purchase_data_schema = StructType([
            StructField("customer", IntegerType(), nullable=False),
            StructField("product_model", StringType(), nullable=False)])
        purchase_data = [
            (1, "iphone13"),
            (1, "dell i5 core"),
            (2, "iphone13"),
            (2, "dell i5 core"),
            (3, "iphone13"),
            (3, "dell i5 core"),
            (1, "dell i3 core"),
            (1, "hp i5 core"),
            (1, "iphone14"),
            (3, "iphone14"),
            (4, "iphone13")
        ]
        purchase_data_df = self.spark.createDataFrame(data=purchase_data, schema=purchase_data_schema)
        self.assertEqual(purchase_data_df.count(), 11)

    def test_only_iphone13(self):
        # Test for customers who have bought only iphone13
        purchase_data_schema = StructType([
            StructField("customer", IntegerType(), nullable=False),
            StructField("product_model", StringType(), nullable=False)])
        purchase_data = [
            (1, "iphone13"),
            (1, "dell i5 core"),
            (2, "iphone13"),
            (2, "dell i5 core"),
            (3, "iphone13"),
            (3, "dell i5 core"),
            (1, "dell i3 core"),
            (1, "hp i5 core"),
            (1, "iphone14"),
            (3, "iphone14"),
            (4, "iphone13")
        ]
        purchase_data_df = self.spark.createDataFrame(data=purchase_data, schema=purchase_data_schema)
        only_iphone13_df = purchase_data_df.filter(col("product_model") == "iphone13")
        self.assertEqual(only_iphone13_df.count(), 6)

    # Add more test cases as described in the previous response

if __name__ == '__main__':
    unittest.main()

