import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

raw_path = "s3://priyam-tibil-test/ecommerce-raw-data/"
curated_path = "s3://priyam-tibil-test/ecommerce-curated/"

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(raw_path)

df_clean = df.dropna(subset=["transaction_id","store_id","product_id","quantity","amount","payment_type","timestamp"])

df_clean = df_clean.withColumn("transaction_id", col("transaction_id").cast(StringType())) \
                   .withColumn("store_id", col("store_id").cast(StringType())) \
                   .withColumn("product_id", col("product_id").cast(StringType())) \
                   .withColumn("quantity", col("quantity").cast(IntegerType())) \
                   .withColumn("amount", col("amount").cast(DoubleType())) \
                   .withColumn("payment_type", col("payment_type").cast(StringType())) \
                   .withColumn("timestamp", col("timestamp").cast(TimestampType()))

df_clean = df_clean.withColumn("date", to_date(col("timestamp")))

df_clean.write.mode("overwrite") \
    .partitionBy("date") \
    .format("parquet") \
    .save(curated_path)

print("Glue ETL job completed successfully!")
