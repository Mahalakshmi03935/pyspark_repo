from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

# Define your dataset and schema
Dataset = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

credit_card_df_schema = (
    StructType([
        StructField("card_number", StringType())
    ])
)

# Create SparkSession
spark = SparkSession.builder.appName('data').getOrCreate()

# Create DataFrame from dataset with defined schema
credit_card_df = spark.createDataFrame(data=Dataset, schema=credit_card_df_schema)

# Show DataFrame
credit_card_df.show()

# Reading CSV file with inferSchema
credit_card_df = spark.read.csv("C:/Users/RajaMahalakshmiB/Desktop/pyspark_folder/pyspark_repo/resources/credit_card_number.csv", header=True, inferSchema=True)
credit_card_df.show()
credit_card_df.printSchema()



# Print number of partitions
number_of_partition = credit_card_df_schema.rdd.getNumPartitions()
print("Number of partitions before repartitioning:", number_of_partition)

# Increase the partition by 5
increase_partition = credit_card_df_schema.repartition(number_of_partition + 5)
increase_partition1 = increase_partition.getNumPartitions()
print("Print increased partition: ", increase_partition1)

# Decrease the partition size back to its original partition size
decrease_partition = increase_partition.coalesce(number_of_partition)
decrease_partition1 = decrease_partition.getNumPartitions()
print("Back to its original partition size: ", decrease_partition1)

# Define UDF function to mask card numbers
def masked_card_number(card_number):
    masked_character = len(card_number) - 4
    masked_number = ('*' * masked_character) + card_number[-4:]
    return masked_number

# Register UDF
credit_card_udf = udf(masked_card_number, StringType())

# Add masked_card_number column using UDF
credit_card_df_udf = credit_card_df_schema.withColumn("masked_card_number", credit_card_udf(credit_card_df_schema['card_number']))
credit_card_df_udf.show()

