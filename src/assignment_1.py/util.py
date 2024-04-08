from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_purchase_data(spark):
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
    
    return spark.createDataFrame(data=purchase_data, schema=purchase_data_schema)


def create_product_data(spark):
    product_data_schema = StructType([
        StructField("product_model", StringType(), nullable=False)
    ])

    product_data = [
        ("iphone13",),
        ("dell i5 core",),
        ("dell i3 core",),
        ("hp i5 core",),
        ("iphone14",)
    ]
    
    return spark.createDataFrame(data=product_data, schema=product_data_schema)

