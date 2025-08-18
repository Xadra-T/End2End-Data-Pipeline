import os

from minio import Minio
import pytest


@pytest.fixture(scope='module')
def minio_client() -> Minio:
    """Establish a connection to MinIO.
    
    Returns:
        MinIO client.
    """
    try:
        minio_client = Minio(
            endpoint='minio:9000',
            access_key=os.environ['MINIO_ROOT_USER'],
            secret_key=os.environ['MINIO_ROOT_PASSWORD'],
            secure=False
        )
        minio_client.list_buckets()  # Ping the server
        return minio_client
    except Exception as e:
        pytest.fail(f'An unexpected error occurred while connecting to MinIO: {type(e).__name__} - {e}')


def test_minio_bucket_exists(minio_client: Minio):
    """Test that the bucket was created by the minio-init service."""
    bucket_name = os.environ['MINIO_BUCKET_NAME']
    assert minio_client.bucket_exists(bucket_name=bucket_name), f"Bucket '{bucket_name}' should exist, but it doesn't."
