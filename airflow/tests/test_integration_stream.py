from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq
import pytest
from minio.error import S3Error

from common import insert_test_data, CLICKHOUSE_TABLE, MINIO_BUCKET_NAME
from pipeline import schema


def test_integration_stream_with_data(stream_func, clickhouse_client, minio_client, test_timestamp, delete_all_data):
    """Test streaming data from ClickHouse to MinIO with real services."""
    num_rows = 5
    insert_test_data(clickhouse_client, test_timestamp, num_rows=num_rows)
    
    data_interval_start = test_timestamp + timedelta(minutes=1)
    result = stream_func(data_interval_start=data_interval_start)
    
    timestamp_str = test_timestamp.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    expected_path = f's3a://{MINIO_BUCKET_NAME}/{timestamp_str}.parquet'
    assert result == expected_path
    
    object_name = f'{timestamp_str}.parquet'
    try:
        stat = minio_client.stat_object(MINIO_BUCKET_NAME, object_name)
        assert stat.size > 0
    except S3Error:
        pytest.fail(f"Expected object {object_name} not found in MinIO")
    
    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
        minio_client.fget_object(MINIO_BUCKET_NAME, object_name, tmp.name)
        
        table = pq.read_table(tmp.name)
        assert table.num_rows == num_rows
        assert table.schema == schema


def test_integration_stream_no_data(stream_func, minio_client, test_timestamp, delete_all_data):
    """Test streaming when no data exists in ClickHouse."""
    data_interval_start = test_timestamp + timedelta(minutes=1)
    result = stream_func(data_interval_start=data_interval_start)
    
    timestamp_str = test_timestamp.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    expected_path = f's3a://{MINIO_BUCKET_NAME}/{timestamp_str}'
    assert result == expected_path
    
    object_name = f'{timestamp_str}.parquet'
    with pytest.raises(S3Error) as exc_info:
        minio_client.stat_object(MINIO_BUCKET_NAME, object_name)
    
    assert exc_info.value.code == 'NoSuchKey'


def test_integration_stream_large_dataset(stream_func, clickhouse_client, minio_client, test_timestamp, delete_all_data):
    """Test streaming with a larger dataset to verify chunking works correctly."""
    insert_test_data(clickhouse_client, test_timestamp, num_rows=1000)
    
    data_interval_start = test_timestamp + timedelta(minutes=1)
    stream_func(data_interval_start=data_interval_start)
    
    timestamp_str = test_timestamp.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    object_name = f'{timestamp_str}.parquet'
    
    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
        minio_client.fget_object(MINIO_BUCKET_NAME, object_name, tmp.name)
        
        table = pq.read_table(tmp.name)
        assert table.num_rows == 1000
        
        df_result = table.to_pandas()
        assert df_result['event_type'].dtype == 'object'
        assert df_result['status'].dtype == 'object'


def test_integration_stream_data_transformation(stream_func, clickhouse_client, minio_client, test_timestamp, delete_all_data):
    """Test that data transformations are applied correctly in the real pipeline."""
    timestamp_with_microseconds = test_timestamp.replace(microsecond=123456)
    
    latency = 100
    product_id = 9900
    event_type = 'VIEW_PRODUCT'
    status = 'SUCCESS'
    test_data = pd.DataFrame([{
        'event_id': str(uuid4()),
        'user_id': str(uuid4()),
        'session_id': str(uuid4()),
        'event_type': event_type,
        'event_timestamp': timestamp_with_microseconds,
        'request_latency_ms': latency,
        'status': status,
        'error_code': None,
        'product_id': product_id,
    }])
    
    clickhouse_client.insert_df(CLICKHOUSE_TABLE, test_data)
    
    data_interval_start = test_timestamp + timedelta(minutes=1)
    stream_func(data_interval_start=data_interval_start)
    
    timestamp_str = test_timestamp.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    object_name = f'{timestamp_str}.parquet'
    
    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
        minio_client.fget_object(MINIO_BUCKET_NAME, object_name, tmp.name)
        
        table = pq.read_table(tmp.name)
        df_result = table.to_pandas()
        
        assert isinstance(df_result['event_type'].iloc[0], str)
        assert isinstance(df_result['status'].iloc[0], str)
        
        assert df_result['event_type'].iloc[0] == event_type
        assert df_result['status'].iloc[0] == status


def test_integration_timezone_handling(stream_func, clickhouse_client, minio_client, delete_all_data):
    """Test that timezone conversions are handled correctly."""
    utc_timestamp = datetime(2025, 1, 2, 6, 30, 0, tzinfo=ZoneInfo('UTC'))
    tehran_timestamp = utc_timestamp.astimezone(ZoneInfo('Asia/Tehran'))
    
    insert_test_data(clickhouse_client, tehran_timestamp, num_rows=1)
    
    data_interval_start = utc_timestamp + timedelta(minutes=1)
    result = stream_func(data_interval_start=data_interval_start)
    
    expected_filename = tehran_timestamp.strftime('%Y-%m-%d_%H-%M')
    assert expected_filename in result
    
    object_name = f'{expected_filename}.parquet'
    try:
        stat = minio_client.stat_object(MINIO_BUCKET_NAME, object_name)
        assert stat.size > 0
    except S3Error:
        pytest.fail(f"Expected object {object_name} not found in MinIO")
