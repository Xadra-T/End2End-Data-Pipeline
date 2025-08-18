from __future__ import annotations

import json
from unittest.mock import Mock
from urllib3.response import HTTPResponse

import pytest
from minio import Minio

from common import MINIO_BUCKET_NAME


@pytest.fixture
def mock_minio_response(mocker):
    """Mock minio client and its `get_object` response."""
    mock_response = Mock(spec=HTTPResponse)
    mock_response.read.return_value = json.dumps({'analysis': 'result'})
    
    mock_minio = Mock(spec=Minio)
    mock_minio.get_object.return_value = mock_response
    
    mocker.patch('pipeline.get_minio_client', return_value=mock_minio, autospec=True)
    return mock_minio, mock_response


@pytest.fixture
def mock_request_post(mocker):
    """Mock `requests.post`."""
    mock_post = mocker.patch('requests.post', autospec=True)
    return mock_post


def test_send_to_dashboard_with_parquet_path(send_func, mock_minio_response, mock_request_post):
    """Test send_to_dashboard handles path with '.parquet', fetches JSON, and sends to dashboard API."""
    mock_minio, mock_response = mock_minio_response
    filename = '2025-08-10_12-00'
    send_func(f's3a://{MINIO_BUCKET_NAME}/{filename}.parquet')
    
    mock_minio.get_object.assert_called_once_with(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=f'{filename}.json'
    )
    
    mock_request_post.assert_called_once_with(
        url='http://dashboard-api:8080/report',
        json={'analysis': 'result'}
    )
    
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_send_to_dashboard_without_parquet_path(send_func, mock_minio_response, mock_request_post):
    """Test send_to_dashboard handles path without '.parquet' by appending '.json'."""
    mock_minio, mock_response = mock_minio_response
    
    filename = '2025-08-10_12-00'
    send_func(f's3a://{MINIO_BUCKET_NAME}/{filename}')
    
    mock_minio.get_object.assert_called_once_with(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=f'{filename}.json'
    )
    
    mock_request_post.assert_called_once_with(
        url='http://dashboard-api:8080/report',
        json={'analysis': 'result'}
    )
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_send_to_dashboard_exception(mocker, send_func):
    """Test send_to_dashboard raises exception on failure and cleans up."""
    mock_response = Mock()
    err_msg = 'Read failed'
    mock_response.read.side_effect = ValueError(err_msg)
    mock_response.close = Mock()
    mock_response.release_conn = Mock()
    mock_minio = Mock()
    mock_minio.get_object.return_value = mock_response
    
    mocker.patch('pipeline.get_minio_client', return_value=mock_minio, autospec=True)
    
    with pytest.raises(ValueError, match=err_msg):
        send_func(f's3a://{MINIO_BUCKET_NAME}/2025-08-10_12-00.parquet')
    
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_send_to_dashboard_s3_error(mocker, send_func):
    """Test send_to_dashboard handles S3Error correctly."""
    from minio.error import S3Error
    
    mock_minio = Mock(spec=Minio)
    filename = '2025-08-10_12-00'
    code = 'NoSuchKey'
    resource = f'{filename}.json'
    mock_minio.get_object.side_effect = S3Error(
        code=code,
        message='The specified key does not exist.',
        resource=resource,
        request_id='test-request-id',
        host_id='test-host-id',
        response='test-response'
    )
    
    mocker.patch('pipeline.get_minio_client', return_value=mock_minio)
    
    with pytest.raises(S3Error) as exc_info:
        send_func(f's3a://{MINIO_BUCKET_NAME}/{filename}.parquet')
    
    assert exc_info.value.code == 'NoSuchKey'
    assert exc_info.value._resource == resource


def test_send_to_dashboard_json_decode_error(send_func, mock_minio_response):
    """Test send_to_dashboard handles JSONDecodeError correctly."""
    mock_minio, mock_response = mock_minio_response
    mock_response.read.return_value = 'invalid json'
    
    with pytest.raises(json.JSONDecodeError):  # exc_info was not much informative
        send_func(f's3a://{MINIO_BUCKET_NAME}/2025-08-10_12-00.parquet')
    
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()


def test_send_to_dashboard_request_exception(send_func, mock_minio_response, mock_request_post):
    """Test send_to_dashboard handles RequestException correctly."""
    import requests
    
    mock_minio, mock_response = mock_minio_response
    mock_request_post.side_effect = requests.RequestException('Connection failed')
    
    with pytest.raises(requests.RequestException):
        send_func(f's3a://{MINIO_BUCKET_NAME}/2025-08-10_12-00.parquet')
    
    mock_response.close.assert_called_once()
    mock_response.release_conn.assert_called_once()
