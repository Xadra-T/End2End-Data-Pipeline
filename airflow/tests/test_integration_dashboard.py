from __future__ import annotations

import io
import json

import pytest
import requests
from minio.error import S3Error


from common import MINIO_BUCKET_NAME
DASHBOARD_API_URL_TEST = 'http://dashboard-api:8080/report'


@pytest.fixture(autouse=True)
def test_setup_teardown(minio_client):
    """Clean the api storage before and after each test."""
    requests.delete(f'{DASHBOARD_API_URL_TEST}/report')
    
    yield
    
    requests.delete(f'{DASHBOARD_API_URL_TEST}/report')
    
    try:
        objects = minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True)
        for obj in objects:
            minio_client.remove_object(MINIO_BUCKET_NAME, obj.object_name)
    except Exception as e:
        print(f'Could not clean up MinIO bucket: {e}')
        raise


def test_integration_dashboard_success(send_func, minio_client):
    """Test that a valid JSON report is read from MinIO and sent to the dashboard API."""
    report_data = {'report': {'total_events': 100, 'total_errors': 5}}
    report_json = json.dumps(report_data)
    object_name = '2025-08-10_12-00.json'
    
    minio_client.put_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(report_json.encode('utf-8')),
        length=len(report_json)
    )
    
    file_path = f's3a://{MINIO_BUCKET_NAME}/{object_name.replace("json", "parquet")}'
    send_func(file_path=file_path)
    
    response = requests.get(f'{DASHBOARD_API_URL_TEST}')
    response.raise_for_status()
    
    received_report = response.json()
    assert received_report == report_data


def test_integration_dashboard_invalid_filename_failure(send_func):
    """Test that an invalid file path causes S3Error."""
    object_name = 'invalid_filename.json'
    
    file_path = f's3a://{MINIO_BUCKET_NAME}/{object_name.replace("json", "parquet")}'
    
    with pytest.raises(S3Error) as exc_info:
        send_func(file_path=file_path)
    
    assert exc_info.value.code == 'NoSuchKey'


def test_integration_dashboard_invalid_json_failure(send_func, minio_client):
    """Test that an invalid json file causes JSONDecodeError."""
    report_json = "{'bad dict': {'total_events': }}"
    object_name = '2025-08-10_12-00.json'
    
    minio_client.put_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(report_json.encode('utf-8')),
        length=len(report_json)
    )
    
    file_path = f's3a://{MINIO_BUCKET_NAME}/{object_name.replace("json", "parquet")}'
    
    with pytest.raises(json.JSONDecodeError):
        send_func(file_path=file_path)
