from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, posexplode, explode_outer, posexplode_outer, current_date, year, month, day
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType

spark = SparkSession.builder.appName('nested_json_file').getOrCreate()

# Define the schema for the main JSON structure
json_schema = StructType([
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
    return spark.read.json(path, multiLine=True, schema=schema)

json_path = "C:/Users/RajaMahalakshmiB/Desktop/pyspark_folder/pyspark_repo/resources/nested_json_file.json"

#1.Read JSON file provided in the attachment using the dynamic function
json_df = read_json(json_path, json_schema)
json_df.printSchema()
json_df.show(truncate=False)





#2.Flatten the data frame which is a custom schema
flatten_df = json_df.select("*", "properties.name", "properties.storeSize").drop("properties") \
    .select("*", explode("employees").alias("new_employees")).drop("employees") \
    .select("*", "new_employees.empId", "new_employees.empName").drop("new_employees")
flatten_df.show()

#3.Find out the record count when flattened and when it's not flattened (find out the difference why you are "
#getting more count)
print("Record Count before flatten:", json_df.count())
print("Record Count after flatten:", flatten_df.count())

#4. Differentiate the difference using explode, explode outer, posexplode functions
# Explode
exploded_df = json_df.select("id", "properties", explode("employees").alias("employee"))
print("Explode over the employees array")
exploded_df.show(truncate=False)

# Posexplode
posexploded_df = json_df.select("id", "properties", posexplode("employees").alias("pos", "employee"))
print("posexplode over the employees array")
posexploded_df.show(truncate=False)

# Explode_outer on the employees array
exploded_outer_df = json_df.select("id", "properties", explode_outer("employees").alias("employee_outer"))
print("explode_outer over the employees array")
exploded_outer_df.show(truncate=False)

# Posexplode_outer on the employees array
pos_explode_outer = json_df.select("id", "properties", posexplode_outer("employees").alias("posexplode_outer", "employee"))
print("Applied posexplode_outer over the employees array")
pos_explode_outer.show(truncate=False)

#5. Filter the id which is equal to 1001
filter_df = flatten_df.filter(flatten_df["empId"] == 1001)
filter_df.show()

def to_snake_case(dataframe):
    for column in dataframe.columns:
        snake_case_col = ''.join(['_' + c.lower() if c.isupper() else c for c in column]).lstrip('_')
        dataframe = dataframe.withColumnRenamed(column, snake_case_col)
    return dataframe

# Convert column names to snake_case
snake_case_df = to_snake_case(flatten_df)
snake_case_df.show()

# 7. Add a new column named load_date with the current date
load_date_df = snake_case_df.withColumn("load_date", current_date())
load_date_df.show()

# 8. Create 3 new columns as year, month, and day from the load_date column

year_month_day_df = load_date_df.withColumn("year", year(load_date_df.load_date)) \
    .withColumn("month", month(load_date_df.load_date)) \
    .withColumn("day", day(load_date_df.load_date))
year_month_day_df.show()
