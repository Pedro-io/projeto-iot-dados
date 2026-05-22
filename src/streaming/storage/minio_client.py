"""Cliente MinIO/S3 para gravação de Bronze."""
import json
import logging
from typing import Any, Dict, List

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError as exc:
    raise ImportError("boto3 não instalado. Execute: pip install boto3") from exc

log = logging.getLogger("bronze_consumer")


class MinIOClient:
    """Wrapper sobre boto3 para gravar arquivos JSON Lines no MinIO/S3."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(signature_version="s3v4"),
        )
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            self.client.head_bucket(Bucket=self.bucket)
            log.info("Bucket verificado", extra={"bucket": self.bucket, "status": "exists"})
        except ClientError:
            self.client.create_bucket(Bucket=self.bucket)
            log.info("Bucket criado", extra={"bucket": self.bucket, "status": "created"})

    def put_json(self, key: str, events: List[Dict[str, Any]]) -> int:
        """Grava eventos como JSON Lines. Retorna bytes gravados."""
        body = "\n".join(json.dumps(e, ensure_ascii=False) for e in events)
        body_bytes = body.encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body_bytes,
            ContentType="application/x-ndjson",
        )
        return len(body_bytes)