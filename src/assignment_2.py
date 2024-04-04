# Importing libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Created a SparkSession
spark = SparkSession.builder.appName("Data") .getOrCreate()

#Creating a DataFrame
data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]

# Create DataFrame
credit_card_df = spark.createDataFrame(data, ["card_number"])
print("displaying credit card dataframe")
credit_card_df.show()

# Print number of partitions
print("No. of partitions :", credit_card_df.rdd.getNumPartitions())

# Increase the partition size to 5
credit_card_df = credit_card_df.repartition(5)
print("After increasing the partition size to 5:",credit_card_df.show())

# Decrease the partition size back to its original partition size
credit_card_df = credit_card_df.coalesce(credit_card_df.rdd.getNumPartitions())
print("After decreasing the partition size back to its original partition size:",credit_card_df.show())

# Creating a UDF to mask the card numbers
def mask_card_number(card_number):
    return '*' * 12 + card_number[-4:]

mask_card_number_udf = udf(mask_card_number, StringType())
print("Mask the card numbers:",mask_card_number_udf)

# Apply UDF to create masked_card_number column
credit_card_df = credit_card_df.withColumn("masked_card_number", mask_card_number_udf(col("card_number")))
print("applying udf to create masked_card_number col:",credit_card_df.show())
# Show the final DataFrame with masked card numbers
credit_card_df.show(truncate=False)

# Stop the SparkSession
spark.stop()

