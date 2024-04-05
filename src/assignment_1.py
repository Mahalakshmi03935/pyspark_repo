from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql.functions import when

spark = SparkSession.builder.appName("data").getOrCreate()

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

Product_data_schema = StructType([
    StructField("product_model", StringType(), nullable=False)
])

Product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]


#dataframes
purchase_data_df = spark.createDataFrame(data=purchase_data, schema=purchase_data_schema)
print("Purchase data:")
purchase_data_df.show()
product_data_df = spark.createDataFrame(data=Product_data, schema=Product_data_schema)
print("Product data:")
product_data_df.show()

#2.Customers who have bought only iphone13
only_iphone13_df = purchase_data_df.filter(col("product_model") == "iphone13")
only_iphone13_df.show()

#3.Customers who have bought only iphone14
only_iphone14_df = purchase_data_df.filter(col("product_model") == "iphone14")
only_iphone14_df.show()

#4. Customers who upgraded from iphone13 to iphone14
iphone13_to_iphone14_df = only_iphone13_df.select("customer").intersect(only_iphone14_df.select("customer"))
if not iphone13_to_iphone14_df.isEmpty():
    iphone13_to_iphone14_df.show()
else:
    print("No customers upgraded from iphone13 to iphone14")
#5. Customers who have bought all models in the new Product Data
distinct_models = product_data_df.distinct().count()
product_bought = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("product_by_user"))
result = product_bought.filter(product_bought.product_by_user == distinct_models).select("customer")
result.show() if not result.isEmpty() else print("No customers have bought all models in the new Product Data")

