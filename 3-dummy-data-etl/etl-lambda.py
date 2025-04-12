import json
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Process each S3 event record (usually each event corresponds to one file)
    for record in event.get('Records', []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        
        # Get the file object from S3
        try:
            s3_response = s3_client.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            print(f"Error retrieving object {key} from {bucket}: {e}")
            continue
        
        # Read file content (assumes the file is plain text with one JSON record per line)
        content = s3_response["Body"].read().decode("utf-8")
        
        headers_list = []
        details_list = []
        
        # Process each line in the file
        for line in content.splitlines():
            try:
                record_data = json.loads(line)
            except Exception as e:
                # Skip lines that don't parse correctly
                continue
            
            # Check that the record contains a "log" field
            log_field = record_data.get("log")
            if not log_field:
                continue

            # Try to parse the "log" field as JSON. If it fails, it might be a non-transaction log.
            try:
                tx_data = json.loads(log_field)
            except Exception as e:
                # Skip non-transaction log lines
                continue
            
            # If the transaction identifier is not present, skip this record
            if "transaction_id" not in tx_data:
                continue
            
            # Build the header record (for the headers table)
            header = {
                "transaction_id":   tx_data.get("transaction_id"),
                "supermarket_id":   tx_data.get("supermarket_id"),
                "transaction_date": tx_data.get("transaction_date")
            }
            headers_list.append(header)
            
            # Build detail records for each item
            items = tx_data.get("items", [])
            for item in items:
                detail = {
                    "transaction_id": tx_data.get("transaction_id"),
                    "sku":            item.get("sku"),
                    "quantity":       item.get("quantity")
                }
                details_list.append(detail)
        
        # If records were extracted, convert them to DataFrames and then Parquet
        timestamp_suffix = int(time.time())
        
        if headers_list:
            headers_df = pd.DataFrame(headers_list)
            headers_buffer = io.BytesIO()
            table = pa.Table.from_pandas(headers_df)
            pq.write_table(table, headers_buffer)
            headers_buffer.seek(0)
            headers_key = f"dynamic-data/headers/headers_{timestamp_suffix}.parquet"
            s3_client.put_object(Bucket=bucket, Key=headers_key, Body=headers_buffer.getvalue())
            print(f"Wrote headers parquet to {headers_key}")
        
        if details_list:
            details_df = pd.DataFrame(details_list)
            details_buffer = io.BytesIO()
            table = pa.Table.from_pandas(details_df)
            pq.write_table(table, details_buffer)
            details_buffer.seek(0)
            details_key = f"dynamic-data/details/details_{timestamp_suffix}.parquet"
            s3_client.put_object(Bucket=bucket, Key=details_key, Body=details_buffer.getvalue())
            print(f"Wrote details parquet to {details_key}")
        
        # Optionally, you can archive or delete the original file after processing
        # For example:
        # s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': key}, Key=f"archive/{key}")
        # s3_client.delete_object(Bucket=bucket, Key=key)

    return {"statusCode": 200, "body": "Successfully processed records"}
