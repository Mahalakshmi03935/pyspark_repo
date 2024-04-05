import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql.utils import AnalysisException

class TestCreditCardDataFrame(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('test').getOrCreate()
        cls.dataset = [
            ("1234567891234567",),
            ("5678912345671234",),
            ("9123456712345678",),
            ("1234567812341122",),
            ("1234567812341342",)
        ]
        cls.credit_card_df_schema = StructType([
            StructField("card_number", StringType())
        ])
        cls.credit_card_df = cls.spark.createDataFrame(data=cls.dataset, schema=cls.credit_card_df_schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_dataframe_creation(self):
        # Test DataFrame creation
        self.assertEqual(self.credit_card_df.count(), len(self.dataset))

    def test_dataframe_schema(self):
        # Test DataFrame schema
        expected_schema = ["card_number"]
        self.assertEqual(self.credit_card_df.columns, expected_schema)

    def test_udf_application(self):
        # Test UDF application
        def masked_card_number(card_number):
            masked_character = len(card_number) - 4
            masked_number = ('*' * masked_character) + card_number[-4:]
            return masked_number

        credit_card_udf = udf(masked_card_number, StringType())
        masked_card_df = self.credit_card_df.withColumn("masked_card_number", credit_card_udf(self.credit_card_df['card_number']))

        # Assert masked_card_df has a new column 'masked_card_number'
        self.assertIn('masked_card_number', masked_card_df.columns)

    def test_partition_manipulation(self):
        # Test partition manipulation
        initial_partitions = self.credit_card_df.rdd.getNumPartitions()
        increased_partitions = self.credit_card_df.repartition(initial_partitions + 5)
        self.assertEqual(increased_partitions.rdd.getNumPartitions(), initial_partitions + 5)

        decreased_partitions = increased_partitions.coalesce(initial_partitions)
        self.assertEqual(decreased_partitions.rdd.getNumPartitions(), initial_partitions)

    def test_invalid_csv_schema(self):
        # Test for reading CSV with invalid schema
        with self.assertRaises(AnalysisException):
            self.spark.read.csv("invalid_path", header=True, inferSchema=True)

if __name__ == '__main__':
    unittest.main()

