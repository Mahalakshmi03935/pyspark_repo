from util import *

# Define your dataset and schema
Dataset = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("1234567812341122",),
    ("1234567812341342",)
]

# Create SparkSession
spark = create_spark_session('data')

# Define schema
credit_card_df_schema = define_schema()

# Create DataFrame from dataset with defined schema
credit_card_df = spark.createDataFrame(data=Dataset, schema=credit_card_df_schema)

# Show DataFrame
credit_card_df.show()

# Reading CSV file with inferSchema
credit_card_df = read_dataset(spark, "C:/Users/RajaMahalakshmiB/Desktop/pyspark_folder/pyspark_repo/resources/credit_card_number.csv")

credit_card_df.show()
credit_card_df.printSchema()

# Print number of partitions
number_of_partition = credit_card_df.rdd.getNumPartitions()
print("Number of partitions before repartitioning:", number_of_partition)

# Increase the partition by 5
increase_partition = increase_partition_size(credit_card_df, number_of_partition + 5)
increase_partition1 = increase_partition.getNumPartitions()
print("Print increased partition: ", increase_partition1)

# Decrease the partition size back to its original partition size
decrease_partition = decrease_partition_size(increase_partition, number_of_partition)
decrease_partition1 = decrease_partition.getNumPartitions()
print("Back to its original partition size: ", decrease_partition1)

# Register UDF
credit_card_udf = register_udf(spark)

# Add masked_card_number column using UDF
credit_card_df_udf = credit_card_df.withColumn("masked_card_number", credit_card_udf(credit_card_df['card_number']))
credit_card_df_udf.show()

