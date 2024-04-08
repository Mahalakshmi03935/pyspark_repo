from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, posexplode, explode_outer, posexplode_outer, current_date, year, month, day
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType

def create_spark_session():
    """
    Create a SparkSession.
    """
    return SparkSession.builder.appName('nested_json_file').getOrCreate()

def read_json(path, schema):
    """
    Read JSON file provided in the attachment using the dynamic function.
    """
    return spark.read.json(path, multiLine=True, schema=schema)

def flatten_dataframe(df):
    """
    Flatten the DataFrame which has a custom schema.
    """
    return df.select("*", "properties.name", "properties.storeSize").drop("properties") \
        .select("*", explode("employees").alias("new_employees")).drop("employees") \
        .select("*", "new_employees.empId", "new_employees.empName").drop("new_employees")

def differentiate_record_count(json_df, flatten_df):
    """
    Find out the record count when flattened and when it's not flattened.
    """
    return json_df.count(), flatten_df.count()

def apply_explode_functions(json_df):
    """
    Differentiate the difference using explode, explode outer, posexplode functions.
    """
    exploded_df = json_df.select("id", "properties", explode("employees").alias("employee"))
    posexploded_df = json_df.select("id", "properties", posexplode("employees").alias("pos", "employee"))
    exploded_outer_df = json_df.select("id", "properties", explode_outer("employees").alias("employee_outer"))
    pos_explode_outer = json_df.select("id", "properties", posexplode_outer("employees").alias("posexplode_outer", "employee"))
    return exploded_df, posexploded_df, exploded_outer_df, pos_explode_outer

def filter_by_id(flatten_df, id_value):
    """
    Filter the DataFrame by id.
    """
    return flatten_df.filter(flatten_df["empId"] == id_value)

def convert_to_snake_case(dataframe):
    """
    Convert column names to snake_case.
    """
    for column in dataframe.columns:
        snake_case_col = ''.join(['_' + c.lower() if c.isupper() else c for c in column]).lstrip('_')
        dataframe = dataframe.withColumnRenamed(column, snake_case_col)
    return dataframe

def add_load_date_column(dataframe):
    """
    Add a new column named load_date with the current date.
    """
    return dataframe.withColumn("load_date", current_date())

def create_year_month_day_columns(dataframe):
    """
    Create 3 new columns as year, month, and day from the load_date column.
    """
    return dataframe.withColumn("year", year(dataframe.load_date)) \
        .withColumn("month", month(dataframe.load_date)) \
        .withColumn("day", day(dataframe.load_date))
