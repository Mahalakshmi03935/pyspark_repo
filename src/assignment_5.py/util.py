from pyspark.sql.functions import col, expr, when, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_employee_df(spark):
    # Define schema for employee_df
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), False),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("Age", IntegerType(), True)
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
    return spark.createDataFrame(employee_data, schema=employee_schema)

def create_department_df(spark):
    # Define schema for department_df
    department_schema = StructType([
        StructField("dept_id", StringType(), False),
        StructField("dept_name", StringType(), True)
    ])

    # Create department_df with custom schema
    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]
    return spark.createDataFrame(department_data, schema=department_schema)

def create_country_df(spark):
    # Define schema for country_df
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    # Create country_df with custom schema
    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]
    return spark.createDataFrame(country_data, schema=country_schema)

def dynamic_join(df1, df2, how):
    return df1.join(df2, df1["department"] == df2["dept_id"], how)

def update_country_name(dataframe):
    country_dataframe = dataframe.withColumn("State", when(dataframe["State"] == "uk", "United Kingdom")
                                             .when(dataframe["State"] == "ny", "New York")
                                             .when(dataframe["State"] == "ca", "Canada"))
    new_df = country_dataframe.withColumnRenamed("State", "country_name")
    return new_df

def column_to_lower(dataframe):
    new_columns = [col(column).alias(column.lower()) for column in dataframe.columns]
    return dataframe.select(*new_columns).withColumn("load_date", current_date())
