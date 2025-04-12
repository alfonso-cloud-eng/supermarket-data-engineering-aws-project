import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")


# Input path for raw historical transactions in S3 (CSV format)
input_path = "s3://supermarket-data-bucket/dynamic-data/raw/past_transactions.csv"

# Read the CSV data into a Spark DataFrame with header
raw_df = spark.read \
    .option("header", "true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .csv(input_path)

# Define a schema for the "items" JSON column
items_schema = ArrayType(StructType([
    StructField("sku", StringType(), True),
    StructField("quantity", IntegerType(), True)
]))

# Parse the "items" column from a JSON string into an array of structs
raw_df = raw_df.withColumn("items", from_json(col("items"), items_schema))

# Optional: Print schema for debugging
raw_df.printSchema()
raw_df.show(5, truncate=False)
# Expected schema should show "items" as an array of structs

# ---------------------------
# 1. Create Transaction Header Table
# ---------------------------
header_df = raw_df.withColumn("transaction_date", col("transaction_date").cast("timestamp")) \
                  .select("transaction_id", "supermarket_id", "transaction_date")

# ---------------------------
# 2. Create Transaction Detail Table
# ---------------------------
# Explode the "items" array so that each item becomes a separate row
detail_df = raw_df.select("transaction_id", explode("items").alias("item"))
detail_df = detail_df.select("transaction_id", 
                             col("item.sku").alias("sku"), 
                             col("item.quantity").alias("quantity"))

# ---------------------------
# 3. Write the DataFrames Back to S3 as Parquet files
# ---------------------------
output_header_path = "s3://supermarket-data-bucket/dynamic-data/headers/"
output_detail_path = "s3://supermarket-data-bucket/dynamic-data/details/"

header_df.write.mode("overwrite").parquet(output_header_path)
detail_df.write.mode("overwrite").parquet(output_detail_path)

job.commit()
