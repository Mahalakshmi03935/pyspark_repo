from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

def create_spark_session(app_name):
    """
    Create SparkSession
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def define_schema():
    """
    Define schema for credit card DataFrame
    """
    credit_card_df_schema = StructType([
        StructField("card_number", StringType())
    ])
    return credit_card_df_schema

def read_dataset(spark, path, header=True, infer_schema=True):
    """
    Read dataset into a DataFrame
    """
    return spark.read.csv(path, header=header, inferSchema=infer_schema)

def increase_partition_size(df, num_partitions):
    """
    Increase the partition size of DataFrame
    """
    return df.repartition(num_partitions)

def decrease_partition_size(df, num_partitions):
    """
    Decrease the partition size of DataFrame
    """
    return df.coalesce(num_partitions)

def mask_card_number(card_number):
    """
    Mask card numbers
    """
    masked_character = len(card_number) - 4
    masked_number = ('*' * masked_character) + card_number[-4:]
    return masked_number

def register_udf(spark):
    """
    Register UDF for masking card numbers
    """
    return udf(mask_card_number, StringType())

