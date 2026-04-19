import json

import boto3

from .base import ObjectStorage


class MinIOStorage(ObjectStorage):

    def __init__(self, endpoint, access_key, secret_key, bucket):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def put_object(self, key: str, data: bytes) -> int:
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType="application/x-ndjson",
        )
        return len(data)

    def put_json_lines(self, key: str, events: list[dict]) -> int:
        body = "\n".join(json.dumps(e) for e in events).encode("utf-8")
        return self.put_object(key, body)