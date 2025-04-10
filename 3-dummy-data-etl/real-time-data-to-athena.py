import sys
import boto3
import datetime
from datetime import datetime, timedelta, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, TimestampType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ------------------------------------------------------------------------------
# 1. Paths and Setup
# ------------------------------------------------------------------------------
bucket_name         = "supermarket-data-bucket"
source_prefix       = "dynamic-data/real-time-raw/"
dest_prefix         = "dynamic-data/archive-real-time-raw/"

output_header_path  = "s3://supermarket-data-bucket/dynamic-data/headers/"
output_detail_path  = "s3://supermarket-data-bucket/dynamic-data/details/"

# S3 resource for listing/copying/deleting objects
s3 = boto3.resource('s3')
bucket = s3.Bucket(bucket_name)

# ------------------------------------------------------------------------------
# 2. Identify Files Older Than 1 Minute
# ------------------------------------------------------------------------------
cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=1)
files_to_process = []

for obj in bucket.objects.filter(Prefix=source_prefix):
    # Only pick files last modified over 1 minute ago
    if obj.last_modified < cutoff_time:
        file_url = f"s3://{bucket_name}/{obj.key}"
        files_to_process.append(file_url)

# If no eligible files, we can just exit gracefully
if not files_to_process:
    print("No files older than 1 minute found. Exiting job.")
    job.commit()
    sys.exit(0)

# ------------------------------------------------------------------------------
# 3. Read the Eligible Files into a Spark DataFrame
# ------------------------------------------------------------------------------
raw_df = spark.read.json(files_to_process)

# ------------------------------------------------------------------------------
# 4. Parse Out the Transaction JSON from the "log" Column
# ------------------------------------------------------------------------------
transaction_schema = StructType([
    StructField("transaction_id",   StringType(), True),
    StructField("supermarket_id",   StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("items", ArrayType(
        StructType([
            StructField("sku",      StringType(), True),
            StructField("quantity", IntegerType(), True)
        ])
    ), True)
])

# Create a new column "tx" by parsing the "log" field as JSON
parsed_df = raw_df.withColumn("tx", from_json(col("log"), transaction_schema))

# ------------------------------------------------------------------------------
# 5. Filter Out Rows That Did NOT Parse as a Valid Transaction
# ------------------------------------------------------------------------------
parsed_df = parsed_df.filter(col("tx").isNotNull())

# ------------------------------------------------------------------------------
# 6. Build the 'headers' DataFrame
# ------------------------------------------------------------------------------
headers_df = parsed_df.select(
    col("tx.transaction_id").alias("transaction_id"),
    col("tx.supermarket_id").alias("supermarket_id"),
    col("tx.transaction_date").cast(TimestampType()).alias("transaction_date")
)

# ------------------------------------------------------------------------------
# 7. Build the 'details' DataFrame
# ------------------------------------------------------------------------------
details_df = parsed_df.select(
    col("tx.transaction_id").alias("transaction_id"),
    explode(col("tx.items")).alias("item")
)
details_df = details_df.select(
    "transaction_id",
    col("item.sku").alias("sku"),
    col("item.quantity").alias("quantity")
)

# ------------------------------------------------------------------------------
# 8. Append the New Data to Historical Parquet Tables
# ------------------------------------------------------------------------------
headers_df.write.mode("append").parquet(output_header_path)
details_df.write.mode("append").parquet(output_detail_path)

job.commit()

# ------------------------------------------------------------------------------
# 9. Archive the Processed Files
# ------------------------------------------------------------------------------
for file_url in files_to_process:
    # Extract the object key by removing 's3://<bucket_name>/' from the start
    source_key = file_url.replace(f"s3://{bucket_name}/", "", 1)
    destination_key = source_key.replace(source_prefix, dest_prefix, 1)

    copy_source = {
        "Bucket": bucket_name,
        "Key": source_key
    }
    # Copy the object to the archive folder
    bucket.copy(copy_source, destination_key)
    # Delete the original object
    s3.Object(bucket_name, source_key).delete()
