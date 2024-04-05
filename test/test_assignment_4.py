import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, posexplode, explode_outer, posexplode_outer, current_date, year, month, day
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType

class TestNestedJSONProcessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('test').getOrCreate()

        # Define the schema for the main JSON structure
        cls.json_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True),
            StructField("employees", ArrayType(
                StructType([
                    StructField("empId", LongType(), True),
                    StructField("empName", StringType(), True)
                ])
            ), True)
        ])

        # Define the function to read JSON
        def read_json(path, schema):
            return cls.spark.read.json(path, multiLine=True, schema=schema)

        json_path = "C:/Users/RajaMahalakshmiB/Desktop/pyspark_folder/pyspark_repo/resources/nested_json_file.json"

        # Read JSON file provided in the attachment using the dynamic function
        cls.json_df = read_json(json_path, cls.json_schema)

        # Flatten the DataFrame which is a custom schema
        cls.flatten_df = cls.json_df.select("*", "properties.name", "properties.storeSize").drop("properties") \
            .select("*", explode("employees").alias("new_employees")).drop("employees") \
            .select("*", "new_employees.empId", "new_employees.empName").drop("new_employees")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_json_dataframe_creation(self):
        # Test DataFrame creation from JSON
        self.assertEqual(self.json_df.count(), 3)

    def test_flattened_dataframe_creation(self):
        # Test DataFrame flattening
        self.assertEqual(self.flatten_df.count(), 6)

    def test_record_count_difference(self):
        # Test record count difference before and after flattening
        self.assertNotEqual(self.json_df.count(), self.flatten_df.count())

    # Add more test cases as needed...

if __name__ == '__main__':
    unittest.main()

