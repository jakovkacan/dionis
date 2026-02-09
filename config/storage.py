"""MinIO storage configuration and connection management."""

import os
from minio import Minio
from minio.error import S3Error
import yaml


def load_config():
    """Load configuration from config.yaml."""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


def get_minio_client() -> Minio:
    """
    Create and return MinIO client.

    Returns:
        Minio: MinIO client instance
    """
    config = load_config()
    minio_config = config['minio']

    endpoint = os.getenv('MINIO_ENDPOINT', minio_config['endpoint'])
    access_key = os.getenv('MINIO_ACCESS_KEY', minio_config['access_key'])
    secret_key = os.getenv('MINIO_SECRET_KEY', minio_config['secret_key'])
    secure = minio_config.get('secure', False)

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

    print(f"Connected to MinIO at {endpoint}")

    return client


def ensure_buckets(client: Minio):
    """
    Ensure all required buckets exist.

    Args:
        client: MinIO client instance
    """
    config = load_config()
    buckets = config['minio']['buckets']

    for bucket_name in buckets.values():
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"Created bucket: {bucket_name}")
            else:
                print(f"Bucket exists: {bucket_name}")
        except S3Error as e:
            print(f"Error with bucket {bucket_name}: {e}")
            raise


def upload_file(client: Minio, bucket_name: str, object_name: str,
                file_path: str, content_type: str = 'application/octet-stream') -> str:
    """
    Upload a file to MinIO.

    Args:
        client: MinIO client instance
        bucket_name: Name of the bucket
        object_name: Name for the object in storage
        file_path: Path to the file to upload
        content_type: MIME type of the file

    Returns:
        str: Object path in MinIO
    """
    try:
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=file_path,
            content_type=content_type
        )
        return f"{bucket_name}/{object_name}"
    except S3Error as e:
        print(f"Error uploading file: {e}")
        raise


def upload_bytes(client: Minio, bucket_name: str, object_name: str,
                 data: bytes, content_type: str = 'application/octet-stream') -> str:
    """
    Upload bytes data to MinIO.

    Args:
        client: MinIO client instance
        bucket_name: Name of the bucket
        object_name: Name for the object in storage
        data: Bytes data to upload
        content_type: MIME type of the data

    Returns:
        str: Object path in MinIO
    """
    from io import BytesIO

    try:
        data_stream = BytesIO(data)
        client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_stream,
            length=len(data),
            content_type=content_type
        )
        return f"{bucket_name}/{object_name}"
    except S3Error as e:
        print(f"Error uploading data: {e}")
        raise
