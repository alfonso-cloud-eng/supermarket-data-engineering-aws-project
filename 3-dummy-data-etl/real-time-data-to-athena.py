import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, from_json, when
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, TimestampType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------------------------
# 1. Paths: where the real-time logs are landing, and where the Parquet outputs go
# ------------------------------------------------------------------------------
real_time_logs_path = "s3://supermarket-data-bucket/dynamic-data/real-time-raw/"
output_header_path  = "s3://supermarket-data-bucket/dynamic-data/headers/"
output_detail_path  = "s3://supermarket-data-bucket/dynamic-data/details/"

# ------------------------------------------------------------------------------
# 2. Read the new line-based JSON logs from S3
# ------------------------------------------------------------------------------
# We assume each line is a JSON record with fields like:
# { date, log, container_id, container_name, source, ecs_cluster, ecs_task_arn, ecs_task_definition, ... }
raw_df = spark.read.json(real_time_logs_path)

# ------------------------------------------------------------------------------
# 3. Parse out the actual transaction JSON from the "log" column
# ------------------------------------------------------------------------------
# We’ll define a schema for the transaction structure we expect to find inside "log"
transaction_schema = StructType([
    StructField("transaction_id",   StringType(), True),
    StructField("supermarket_id",   StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("items", ArrayType(
        StructType([
            StructField("sku",      StringType(),  True),
            StructField("quantity", IntegerType(), True)
        ])
    ), True)
])

# We'll create a new column "tx" by parsing the "log" field as JSON
parsed_df = raw_df.withColumn("tx", from_json(col("log"), transaction_schema))

# ------------------------------------------------------------------------------
# 4. Filter out rows that did NOT parse as a valid transaction
# ------------------------------------------------------------------------------
# If the line doesn't contain a valid transaction JSON, tx will be null
parsed_df = parsed_df.filter(col("tx").isNotNull())

# ------------------------------------------------------------------------------
# 5. Build the 'headers' DataFrame
# ------------------------------------------------------------------------------
# Convert transaction_date into a proper timestamp
headers_df = parsed_df.select(
    col("tx.transaction_id").alias("transaction_id"),
    col("tx.supermarket_id").alias("supermarket_id"),
    col("tx.transaction_date").cast(TimestampType()).alias("transaction_date")
)

# ------------------------------------------------------------------------------
# 6. Build the 'details' DataFrame
# ------------------------------------------------------------------------------
# Explode the items array so each SKU line becomes a separate record
details_df = parsed_df.select(
    col("tx.transaction_id").alias("transaction_id"),
    explode(col("tx.items")).alias("item")
)

# Map the item fields
details_df = details_df.select(
    "transaction_id",
    col("item.sku").alias("sku"),
    col("item.quantity").alias("quantity")
)

# ------------------------------------------------------------------------------
# 7. Append the new data to your existing Parquet tables
# ------------------------------------------------------------------------------
# You already have data in "headers/" and "details/". We'll append so we don't overwrite the historical data.
headers_df.write.mode("append").parquet(output_header_path)
details_df.write.mode("append").parquet(output_detail_path)

job.commit()
