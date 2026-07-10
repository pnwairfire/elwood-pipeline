"""S3 client helpers for the Elwood pipeline.
"""
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

EPA_ACCESS_KEY = (
    os.getenv("EPA_AWS_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY")
)
EPA_SECRET_ACCESS_KEY = (
    os.getenv("EPA_AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY")
)
EPA_BUCKET = os.getenv("EPA_BUCKET")

AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL") or None
AWS_REGION = os.getenv("AWS_REGION") or None
EPA_ENDPOINT_URL = (
    os.getenv("EPA_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL") or None
)
EPA_REGION = os.getenv("EPA_REGION") or os.getenv("AWS_REGION") or None


def init_epa_s3():
    # If EPA credentials are not set, fall back to default AWS client/credentials
    kwargs = {}
    if EPA_ACCESS_KEY and EPA_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"] = EPA_ACCESS_KEY
        kwargs["aws_secret_access_key"] = EPA_SECRET_ACCESS_KEY
    if EPA_ENDPOINT_URL:
        kwargs["endpoint_url"] = EPA_ENDPOINT_URL
    if EPA_REGION:
        kwargs["region_name"] = EPA_REGION

    return boto3.client("s3", **kwargs)


def fasm_layers_bucket():
    return EPA_BUCKET
