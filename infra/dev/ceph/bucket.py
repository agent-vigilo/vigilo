"""Create or check the development bucket through the standard S3 API."""

import argparse
from contextlib import closing
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def client():
    """Use explicit local credentials and bound each readiness attempt."""
    return boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:3900",
        region_name="us-east-1",
        aws_access_key_id=os.environ["DEV_S3_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["DEV_S3_SECRET_ACCESS_KEY"],
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=2,
            read_timeout=5,
            retries={"total_max_attempts": 1},
        ),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["create", "check"])
    args = parser.parse_args()
    with closing(client()) as s3:
        bucket = os.environ["DEV_S3_BUCKET"]
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError as error:
            if args.operation != "create" or error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            s3.create_bucket(Bucket=bucket)
            s3.head_bucket(Bucket=bucket)
