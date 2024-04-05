import unittest
from datetime import date
from pyspark.sql import SparkSession
from main_script import *

class TestSparkFunctions(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName('test_data').getOrCreate()
        
        cls.employee_data = [(11, "james", "D101", "ny", 9000, 34), (12, "michel", "D101", "ny", 8900, 32)]
        cls.department_data = [("D101", "sales"), ("D102", "finance")]
        cls.country_data = [("ny", "newyork"), ("ca", "California"), ("uk", "Russia")]
        
    def test_avg_salary(self):
        employee_df = self.spark.createDataFrame(self.employee_data, schema=employee_schema)
        avg_salary = employee_df.groupBy("department").avg("salary").collect()
        self.assertAlmostEqual(avg_salary[0]['avg(salary)'], 8950.0, delta=0.001)
        
    def test_employee_starts_with_m(self):
        employee_df = self.spark.createDataFrame(self.employee_data, schema=employee_schema)
        department_df = self.spark.createDataFrame(self.department_data, schema=department_schema)
        result = employee_df.filter(col("employee_name").startswith('m')).join(
            department_df, employee_df["department"] == department_df["dept_id"], "inner").collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['employee_name'], 'michel')
        self.assertEqual(result[0]['dept_name'], 'sales')
        
    def test_employee_bonus_df(self):
        employee_df = self.spark.createDataFrame(self.employee_data, schema=employee_schema)
        employee_bonus_df = employee_df.withColumn("bonus", col("salary") * 2).collect()
        self.assertEqual(employee_bonus_df[0]['bonus'], 18000)
        
    def test_employee_df_column_order(self):
        employee_df = self.spark.createDataFrame(self.employee_data, schema=employee_schema)
        employee_df_reordered = employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department").collect()
        self.assertEqual(employee_df_reordered.columns, ['employee_id', 'employee_name', 'salary', 'State', 'Age', 'department'])
        
    def test_dynamic_join(self):
        employee_df = self.spark.createDataFrame(self.employee_data, schema=employee_schema)
        department_df = self.spark.createDataFrame(self.department_data, schema=department_schema)
        
        inner_join_df = dynamic_join(employee_df, department_df, "inner").collect()
        self.assertEqual(len(inner_join_df), 2)
        
        left_join_df = dynamic_join(employee_df, department_df, "left").collect()
        self.assertEqual(len(left_join_df), 2)
        
        right_join_df = dynamic_join(employee_df, department_df, "right").collect()
        self.assertEqual(len(right_join_df), 2)
        
    def test_update_country_name(self):
        employee_df = self.spark.createDataFrame(self.employee_data, schema=employee_schema)
        country_name_df = update_country_name(employee_df).collect()
        self.assertEqual(country_name_df[0]['country_name'], 'New York')
        
    def test_column_to_lower(self):
        employee_df = self.spark.createDataFrame(self.employee_data, schema=employee_schema)
        lower_case_column_df = column_to_lower(employee_df).collect()
        self.assertTrue(all(column.lower() in lower_case_column_df.columns for column in employee_df.columns))
        self.assertEqual(lower_case_column_df[0]['load_date'], date.today())

if __name__ == '__main__':
    unittest.main()

