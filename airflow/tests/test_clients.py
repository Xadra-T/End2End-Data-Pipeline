from __future__ import annotations

import os
from unittest.mock import MagicMock

from airflow.hooks.base import BaseHook
from airflow.models import Connection

from pipeline import get_minio_client


MINIO_ENDPOINT = 'minio:9000'
MINIO_ROOT_USER = os.environ['MINIO_ROOT_USER']
MINIO_ROOT_PASSWORD = os.environ['MINIO_ROOT_PASSWORD']


def test_get_minio_client(mocker):
    """Test get_minio_client retrieves connection and creates MinIO client correctly."""
    mock_minio_conn = MagicMock(spec=Connection)
    mock_minio_conn.extra_dejson = {'host': MINIO_ENDPOINT}
    mock_minio_conn.login = MINIO_ROOT_USER
    mock_minio_conn.password = MINIO_ROOT_PASSWORD
    mocker.patch.object(BaseHook, 'get_connection', return_value=mock_minio_conn, autospec=True)
    mock_minio_class = mocker.patch('pipeline.Minio', autospec=True)
    
    client = get_minio_client()
    
    mock_minio_class.assert_called_once_with(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ROOT_USER,
        secret_key=MINIO_ROOT_PASSWORD,
        secure=False
    )
    assert client == mock_minio_class.return_value
