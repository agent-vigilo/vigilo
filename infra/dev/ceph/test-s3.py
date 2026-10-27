"""Opt-in S3 integration tests; run inside the development Ceph container.

Uses real signed AWS SDK requests. Every fixture owns exact random object keys;
cleanup never lists or deletes an unrelated bucket prefix. Payloads are bounded.
"""

from contextlib import closing
import hashlib
import os
import unittest
import uuid

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

from bucket import client


class S3ContractSmoke(unittest.TestCase):
    def setUp(self):
        self.s3 = client()
        self.bucket = os.environ["DEV_S3_BUCKET"]
        self.key = f"dev-smoke/{uuid.uuid4().hex}/payload.json"
        self.keys = [self.key]
        self.uploads = []
        self.payload = b'{"input":"Ceph S3 fixture","expected_output":"verified"}\n'
        self.addCleanup(self.cleanup)
        self.s3.put_object(
            Bucket=self.bucket, Key=self.key, Body=self.payload,
            Metadata={"sha256": hashlib.sha256(self.payload).hexdigest()},
        )

    def cleanup(self):
        try:
            errors = []
            for key, upload in self.uploads:
                try:
                    self.s3.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload)
                except ClientError as error:
                    if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                        errors.append(str(error))
            result = self.s3.delete_objects(
                Bucket=self.bucket, Delete={"Objects": [{"Key": key} for key in self.keys]},
            )
            self.assertFalse(errors or result.get("Errors"), (errors, result))
        finally:
            self.s3.close()

    def read(self, key, **kwargs):
        result = self.s3.get_object(Bucket=self.bucket, Key=key, **kwargs)
        with result["Body"] as body:
            return body.read(), result

    def test_round_trip_and_integrity_metadata(self):
        body, result = self.read(self.key)
        self.assertEqual(body, self.payload)
        self.assertEqual(result["Metadata"]["sha256"], hashlib.sha256(body).hexdigest())
        head = self.s3.head_object(Bucket=self.bucket, Key=self.key)
        self.assertEqual(head["ContentLength"], len(self.payload))

    def test_range_and_bounded_listing(self):
        body, result = self.read(self.key, Range="bytes=0-9")
        self.assertEqual(result["ResponseMetadata"]["HTTPStatusCode"], 206)
        self.assertEqual(body, self.payload[:10])
        listing = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.key, MaxKeys=1)
        self.assertEqual([item["Key"] for item in listing["Contents"]], [self.key])

    def test_missing_object(self):
        with self.assertRaises(ClientError) as caught:
            self.s3.get_object(Bucket=self.bucket, Key=self.key + ".missing")
        self.assertEqual(caught.exception.response["Error"]["Code"], "NoSuchKey")

    def test_unsigned_and_invalid_credentials_are_denied(self):
        for signature, secret in [(UNSIGNED, "unused"), ("s3v4", "incorrect-secret")]:
            with self.subTest(signature=signature), closing(boto3.client(
                "s3", endpoint_url="http://127.0.0.1:3900", region_name="us-east-1",
                aws_access_key_id=os.environ["DEV_S3_ACCESS_KEY_ID"],
                aws_secret_access_key=secret,
                config=Config(signature_version=signature, s3={"addressing_style": "path"},
                              connect_timeout=2, read_timeout=5,
                              retries={"total_max_attempts": 1}),
            )) as invalid:
                with self.assertRaises(ClientError) as caught:
                    invalid.get_object(Bucket=self.bucket, Key=self.key)
                self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 403)

    def test_conditional_write_preserves_existing_object(self):
        with self.assertRaises(ClientError) as caught:
            self.s3.put_object(Bucket=self.bucket, Key=self.key, Body=b"replacement", IfNoneMatch="*")
        self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 412)
        body, _ = self.read(self.key)
        self.assertEqual(body, self.payload)

    def test_multipart_upload_and_abort(self):
        key = self.key + ".multipart"
        self.keys.append(key)
        part_one = b"a" * (5 * 1024 * 1024)
        part_two = b"last part"
        upload = self.s3.create_multipart_upload(Bucket=self.bucket, Key=key)["UploadId"]
        self.uploads.append((key, upload))
        completed = False
        try:
            parts = []
            for number, payload in enumerate([part_one, part_two], start=1):
                result = self.s3.upload_part(Bucket=self.bucket, Key=key, UploadId=upload,
                                             PartNumber=number, Body=payload)
                parts.append({"PartNumber": number, "ETag": result["ETag"]})
            self.s3.complete_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload,
                                              MultipartUpload={"Parts": parts})
            completed = True
            self.uploads.remove((key, upload))
            body, _ = self.read(key)
            self.assertEqual(body, part_one + part_two)
        finally:
            if not completed:
                self.s3.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload)

        aborted = self.s3.create_multipart_upload(Bucket=self.bucket, Key=key)["UploadId"]
        self.uploads.append((key, aborted))
        self.s3.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=aborted)
        self.uploads.remove((key, aborted))
        with self.assertRaises(ClientError) as caught:
            self.s3.list_parts(Bucket=self.bucket, Key=key, UploadId=aborted)
        self.assertEqual(caught.exception.response["ResponseMetadata"]["HTTPStatusCode"], 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
