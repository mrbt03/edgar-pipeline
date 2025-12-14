# utils/s3_io.py

# this module provides utility functions for interacting with AWS S3.
# it includes:
# - s3_client(): creates an S3 client with the default region
# - put_bytes(): uploads bytes to an S3 bucket
# - object_exists(): checks if an object exists in an S3 bucket

# import python os module and boto3 for S3 operations
import os
import boto3
from botocore.exceptions import ClientError
# create an S3 client with the default region (or us-east-1 if not set)
def s3_client():
    # get default region from env var, or use us-east-1 if not set
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    # return an S3 client with the default region (or us-east-1 if not set)
    return boto3.client("s3", region_name=region)

# upload bytes to an S3 bucket
def put_bytes(bucket: str, key: str, data: bytes):
    # get an S3 client and upload the bytes to the bucket

    try:
        s3_client().put_object(Bucket=bucket, Key=key, Body=data)
    except ClientError as e:
        raise RuntimeError(f"Failed to upload bytes to S3 bucket {bucket} with key {key}: {e}") from e

# check if an object exists in an S3 bucket
def object_exists(bucket: str, key: str) -> bool:
    # get an S3 client and check if the object exists
    s3 = s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    # if the object does not exist, return False
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise
