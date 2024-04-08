from util import *

spark = create_spark_session()

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

json_path = "C:/Users/RajaMahalakshmiB/Desktop/pyspark_folder/pyspark_repo/resources/nested_json_file.json"

# Read JSON file provided in the attachment using the dynamic function
json_df = read_json(json_path, json_schema)
json_df.printSchema()
json_df.show(truncate=False)

flatten_df = flatten_dataframe(json_df)
flatten_df.show()

record_count_before, record_count_after = differentiate_record_count(json_df, flatten_df)
print("Record Count before flatten:", record_count_before)
print("Record Count after flatten:", record_count_after)

exploded_df, posexploded_df, exploded_outer_df, pos_explode_outer = apply_explode_functions(json_df)

exploded_df.show(truncate=False)
posexploded_df.show(truncate=False)
exploded_outer_df.show(truncate=False)
pos_explode_outer.show(truncate=False)

id_filtered_df = filter_by_id(flatten_df, 1001)
id_filtered_df.show()

snake_case_df = convert_to_snake_case(flatten_df)
snake_case_df.show()

load_date_df = add_load_date_column(snake_case_df)
load_date_df.show()

year_month_day_df = create_year_month_day_columns(load_date_df)
year_month_day_df.show()
