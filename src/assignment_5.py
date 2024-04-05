from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName('data').getOrCreate()

# Define schema for employee_df
employee_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("State", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])

# Define schema for department_df
department_schema = StructType([
    StructField("dept_id", StringType(), False),
    StructField("dept_name", StringType(), True)
])

# Define schema for country_df
country_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])

# Create employee_df with custom schema
employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]
employee_df = spark.createDataFrame(employee_data, schema=employee_schema)

# Create department_df with custom schema
department_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]
department_df = spark.createDataFrame(department_data, schema=department_schema)

# Create country_df with custom schema
country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]
country_df = spark.createDataFrame(country_data, schema=country_schema)

#2.Find avg salary of each department
avg_salary = employee_df.groupBy("department").avg("salary")
avg_salary.show()

#3.Find the employee’s name and department name whose name starts with ‘m’
employee_starts_with_m = employee_df.filter(employee_df["employee_name"].startswith('m'))
starts_with_m = employee_starts_with_m.join(department_df,
                                            employee_starts_with_m["department"] == department_df["dept_id"], "inner") \
    .select(employee_starts_with_m["employee_name"], department_df["dept_name"])
starts_with_m.show()

#4.Create another new column in employee_df as a bonus by multiplying employee salary * 2
employee_bonus_df = employee_df.withColumn("bonus", col("salary") * 2)
employee_bonus_df.show()

#5.Reorder the column names of employee_df columns
employee_df = employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
employee_df.show()

#6.Joining DataFrames dynamically
def dynamic_join(df1, df2, how):
    return df1.join(df2, df1["department"] == df2["dept_id"], how)

inner_join_df = dynamic_join(employee_df, department_df, "inner")
inner_join_df.show()

left_join_df = dynamic_join(employee_df, department_df, "left")
left_join_df.show()

right_join_df = dynamic_join(employee_df, department_df, "right")
right_join_df.show()

#7.Derive a new data frame with country_name instead of State in employee_df
def update_country_name(dataframe):
    country_dataframe = dataframe.withColumn("State", when(dataframe["State"] == "uk", "United Kingdom")
                                             .when(dataframe["State"] == "ny", "New York")
                                             .when(dataframe["State"] == "ca", "Canada"))
    new_df = country_dataframe.withColumnRenamed("State", "country_name")
    return new_df

country_name_df = update_country_name(employee_df)
country_name_df.show()

#8.Convert all column names into lowercase and add the load_date column with the current date
def column_to_lower(dataframe):
    new_columns = [col(column).alias(column.lower()) for column in dataframe.columns]
    return dataframe.select(*new_columns).withColumn("load_date", current_date())

lower_case_column_df = column_to_lower(country_name_df)
lower_case_column_df.show()

