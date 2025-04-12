import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    logger.info("Event received: %s", json.dumps(event))
    try:
        for record in event.get('Records', []):
            bucket = record["s3"]["bucket"]["name"]
            key = record["s3"]["object"]["key"]
            logger.info("Processing file: s3://%s/%s", bucket, key)
            
            # Get the file content from S3
            s3_response = s3_client.get_object(Bucket=bucket, Key=key)
            content = s3_response["Body"].read().decode("utf-8")

            headers_list = []
            details_list = []

            # Process each line in the file
            for line in content.splitlines():
                try:
                    record_data = json.loads(line)
                except Exception as e:
                    logger.error("Error parsing line: %s, error: %s", line, e)
                    continue

                log_field = record_data.get("log")
                if not log_field:
                    continue

                try:
                    tx_data = json.loads(log_field)
                except Exception as e:
                    logger.error("Error parsing log field: %s, error: %s", log_field, e)
                    continue

                if "transaction_id" not in tx_data:
                    continue

                # Prepare header and detail records
                headers_list.append({
                    "transaction_id": tx_data.get("transaction_id"),
                    "supermarket_id": tx_data.get("supermarket_id"),
                    "transaction_date": tx_data.get("transaction_date")
                })

                for item in tx_data.get("items", []):
                    details_list.append({
                        "transaction_id": tx_data.get("transaction_id"),
                        "sku": item.get("sku"),
                        "quantity": item.get("quantity")
                    })

            timestamp_suffix = int(time.time())
            if headers_list:
                headers_df = pd.DataFrame(headers_list)
                headers_buffer = io.BytesIO()
                table = pa.Table.from_pandas(headers_df)
                pq.write_table(table, headers_buffer)
                headers_buffer.seek(0)
                headers_key = f"dynamic-data/headers/headers_{timestamp_suffix}.parquet"
                s3_client.put_object(Bucket=bucket, Key=headers_key, Body=headers_buffer.getvalue())
                logger.info("Wrote headers parquet to %s", headers_key)
            else:
                logger.info("No header records found in file.")

            if details_list:
                details_df = pd.DataFrame(details_list)
                details_buffer = io.BytesIO()
                table = pa.Table.from_pandas(details_df)
                pq.write_table(table, details_buffer)
                details_buffer.seek(0)
                details_key = f"dynamic-data/details/details_{timestamp_suffix}.parquet"
                s3_client.put_object(Bucket=bucket, Key=details_key, Body=details_buffer.getvalue())
                logger.info("Wrote details parquet to %s", details_key)
            else:
                logger.info("No detail records found in file.")
    except Exception as e:
        logger.error("Unhandled exception: %s", e, exc_info=True)
        raise
    return {"statusCode": 200, "body": "Successfully processed records"}
