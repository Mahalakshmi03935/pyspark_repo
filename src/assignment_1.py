from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("data").getOrCreate()


purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

product_schema = StructType([
    StructField("product_model", StringType(), True)
])

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

product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]


purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)
product_data_df = spark.createDataFrame(product_data, schema=product_schema)


purchase_data_df.show()
product_data_df.show()

# 2. Find customers who have bought only iphone13
iphone13_customers = purchase_data_df.groupBy("customer").agg({"product_model": "collect_set"}).filter("array_contains(collect_set(product_model), 'iphone13') and not array_contains(collect_set(product_model), 'iphone14')").select("customer")
print("Customers who have bought only iphone13:")
iphone13_customers.show()

# 3. Find customers who upgraded from product iphone13 to product iphone14
iphone13_to_iphone14_customers = purchase_data_df.alias("p1").join(purchase_data_df.alias("p2"),
    (purchase_data_df['p1.customer'] == purchase_data_df['p2.customer']) &
    (purchase_data_df['p1.product_model'] == "iphone13") &
    (purchase_data_df['p2.product_model'] == "iphone14"), how='inner').select("p1.customer").distinct()
print("Customers who upgraded from iphone13 to iphone14:")
iphone13_to_iphone14_customers.show()

# 4. Find customers who have bought all models in the new Product Data
all_models_customers = purchase_data_df.groupBy("customer").agg({"product_model": "count"}).filter("count(product_model) = (select count(*) from product_data_df)").select("customer")
print("Customers who have bought all models in the new Product Data:")
all_models_customers.show()


spark.stop()

