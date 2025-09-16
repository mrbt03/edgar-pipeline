import os
import boto3

def s3_client():
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    return boto3.client("s3", region_name=region)

def put_bytes(bucket: str, key: str, data: bytes):
    s3_client().put_object(Bucket=bucket, Key=key, Body=data)

def object_exists(bucket: str, key: str) -> bool:
    s3 = s3_client()
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False
