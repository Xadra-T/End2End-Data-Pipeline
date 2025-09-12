from __future__ import annotations

import os
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest
from airflow.sdk import Connection
from clickhouse_connect.driver.client import Client
from pyarrow.fs import S3FileSystem

from common import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB, MINIO_BUCKET_NAME
from pipeline import schema


df_chunk = pd.DataFrame({
    'event_type': ['VIEW_PRODUCT'],
    'status': ['SUCCESS'],
})


@pytest.fixture
def mock_ch_client(mocker) -> Mock:
    """Mock ClickHouse client."""
    mock_client = Mock(spec=Client)
    mocker.patch('pipeline.clickhouse_connect.get_client', return_value=mock_client)
    return mock_client


@pytest.fixture
def mock_connections(mocker):
    """Mock ClickHouse and MinIO connections and patch `get_connection`."""
    mock_ch_conn = Mock(spec=Connection)
    mock_ch_conn.host = CLICKHOUSE_HOST
    mock_ch_conn.port = CLICKHOUSE_PORT
    mock_ch_conn.login = CLICKHOUSE_USER
    mock_ch_conn.password = CLICKHOUSE_PASSWORD
    mock_ch_conn.schema = CLICKHOUSE_DB
    
    mock_minio_conn = Mock(spec=Connection)
    mock_minio_conn.login = os.environ['MINIO_ROOT_USER']
    mock_minio_conn.password = os.environ['MINIO_ROOT_PASSWORD']
    mock_minio_conn.extra_dejson = {'host': 'http://minio:9000'}
    
    def get_connection_side_effect(conn_name: str):
        if conn_name == os.environ['CLICKHOUSE_CONN_NAME']:
            return mock_ch_conn
        
        if conn_name == os.environ['MINIO_CONN_NAME']:
            return mock_minio_conn
        
        msg = f'Unknown connection name: {conn_name}'
        raise ValueError(msg)
    
    mocker.patch('pipeline.BaseHook.get_connection', side_effect=get_connection_side_effect)
    return mock_ch_conn, mock_minio_conn


@pytest.fixture
def mock_s3_fs(mocker):
    """Mock S3 Filesystem and its stream operation."""
    mock_fs = MagicMock(spec=S3FileSystem)
    mock_s3_stream = Mock()
    mock_fs.open_output_stream.return_value.__enter__.return_value = mock_s3_stream
    mocker.patch('pipeline.fs.S3FileSystem', return_value=mock_fs)
    return mock_fs, mock_s3_stream


@pytest.fixture
def mock_parquet_writer(mocker):
    """Mock Parquet Writer."""
    mock_writer_instance = Mock()
    mock_context_manager = MagicMock()
    mock_context_manager.__enter__.return_value = mock_writer_instance
    mock_writer_class = mocker.patch('pipeline.pq.ParquetWriter', return_value=mock_context_manager)
    return mock_writer_class, mock_writer_instance


def create_mock_df_stream(dataframes):
    """Helper to create a mock stream that yields dataframes."""
    mock_stream = MagicMock()
    mock_stream.__enter__.return_value = iter(dataframes)
    return mock_stream


def test_stream_from_clickhouse_to_minio_with_data(stream_func, mock_ch_client, mock_s3_fs, mock_parquet_writer, mock_connections):
    """Test stream_from_clickhouse_to_minio handles data streaming and MinIO upload."""
    date_time = datetime(2025, 8, 10, 13, 5, tzinfo=ZoneInfo('UTC'))
    mock_ch_client.query_df_stream.return_value = create_mock_df_stream([df_chunk])
    data_interval_start = date_time + timedelta(minutes=1)
    date_time_str = date_time.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    parquet_path = f's3a://{MINIO_BUCKET_NAME}/{date_time_str}.parquet'
    mock_s3, mock_stream = mock_s3_fs
    mock_writer_class, mock_writer_instance = mock_parquet_writer
    
    result = stream_func(data_interval_start=data_interval_start)
    
    mock_ch_client.query_df_stream.assert_called_once_with(
        query='SELECT event_type, status FROM %(table)s WHERE event_minute = %(timestamp)s;',
        parameters={'table': os.environ['CLICKHOUSE_TABLE'], 'timestamp': date_time},
        settings={'max_block_size': 100000}
    )
    mock_writer_class.assert_called_once_with(where=mock_stream, schema=schema)
    mock_writer_instance.write_table.assert_called_once()
    written_table = mock_writer_instance.write_table.call_args[1]['table']
    assert written_table.schema == schema
    mock_s3.open_output_stream.assert_called_once_with(path=parquet_path.replace('s3a://', ''))
    assert result == parquet_path


def test_stream_from_clickhouse_to_minio_no_data(mocker, stream_func, mock_parquet_writer, mock_ch_client, mock_s3_fs, mock_connections):
    """Test stream_from_clickhouse_to_minio handles no data case without upload."""
    mock_ch_client.query_df_stream.return_value = create_mock_df_stream([])
    data_interval_start = datetime(2025, 8, 10, 8, 31, 0, tzinfo=ZoneInfo('UTC'))
    filename = (data_interval_start.astimezone(ZoneInfo('Asia/Tehran')) - timedelta(minutes=1)).strftime("%Y-%m-%d_%H-%M")
    mock_table = Mock()
    mocker.patch('pipeline.pa.Table', return_value=mock_table)
    mock_s3, mock_stream = mock_s3_fs
    mock_writer_class, mock_writer_instance = mock_parquet_writer
    
    result = stream_func(data_interval_start=data_interval_start)
    
    assert result == f's3a://{MINIO_BUCKET_NAME}/{filename}'
    mock_writer_class.assert_called_once_with(where=mock_stream, schema=schema)
    mock_table.from_pandas.assert_not_called()
    mock_writer_instance.write_table.assert_not_called()
    mock_s3.delete_file.assert_called_once_with(f'{MINIO_BUCKET_NAME}/{filename}.parquet')
    mock_ch_client.close.assert_called_once()


def test_stream_from_clickhouse_to_minio_exception(stream_func, mock_ch_client):
    """Test stream_from_clickhouse_to_minio raises exception on failure."""
    mock_ch_client.query_df_stream.side_effect = ValueError('Query failed')
    data_interval_start = datetime(2025, 8, 10, 8, 31, 0, tzinfo=ZoneInfo('UTC'))
    
    with pytest.raises(ValueError, match='Query failed'):
        stream_func(data_interval_start)


def test_error_propagation(mocker, stream_func) -> None:
    """Test that Exception error is propagated."""
    err_msg = 'Connection not found'
    mocker.patch('pipeline.BaseHook.get_connection', side_effect=Exception(err_msg))
    
    with pytest.raises(Exception) as exc_info:
        stream_func(data_interval_start=datetime.now(ZoneInfo('Asia/Tehran')))
    
    assert err_msg in str(exc_info.value)


def test_data_transformation_in_stream(stream_func, mock_ch_client, mock_parquet_writer, mock_s3_fs, mock_connections):
    """Test that data transformations are applied correctly."""
    date_time = datetime(2025, 8, 10, 12, 0, tzinfo=ZoneInfo('UTC'))
    chunk1 = df_chunk.copy()
    chunk2 = df_chunk.copy()
    chunk2['status'] = ['ERROR']
    mock_ch_client.query_df_stream.return_value = create_mock_df_stream([chunk1, chunk2])
    _, mock_writer_instance = mock_parquet_writer
    written_tables = []
    mock_writer_instance.write_table.side_effect = lambda table: written_tables.append(table)
    mock_s3, _ = mock_s3_fs
    expected_filename = date_time.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    
    result = stream_func(data_interval_start=date_time + timedelta(minutes=1))
    
    mock_s3.open_output_stream.assert_called_once_with(path=f'{MINIO_BUCKET_NAME}/{expected_filename}.parquet')
    assert result == f's3a://{MINIO_BUCKET_NAME}/{expected_filename}.parquet'
    assert len(written_tables) == 2
    assert mock_writer_instance.write_table.call_count == 2
    for written_table, chunk in zip(written_tables, [chunk1, chunk2]):
        assert written_table.column('status').type == pa.string()
        assert written_table.column('event_type').to_pylist()[0] == chunk['event_type'].iloc[0]
        assert written_table.column('status').to_pylist()[0] == chunk['status'].iloc[0]
        assert written_table.schema == schema


def test_stream_from_clickhouse_to_minio_empty_chunk(stream_func, mock_ch_client, mock_parquet_writer, mock_s3_fs, mock_connections):
    """Test stream_from_clickhouse_to_minio handles empty DataFrame correctly."""
    date_time = datetime(2025, 8, 10, 12, 1, tzinfo=ZoneInfo('UTC'))
    empty_df = pd.DataFrame(columns=df_chunk.columns)
    mock_ch_client.query_df_stream.return_value = create_mock_df_stream([empty_df])
    data_interval_start = date_time + timedelta(minutes=1)
    date_time_str = date_time.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    mock_s3, mock_stream = mock_s3_fs
    mock_writer_class, mock_writer_instance = mock_parquet_writer
    
    result = stream_func(data_interval_start=data_interval_start)
    
    mock_writer_class.assert_called_once_with(where=mock_stream, schema=schema)
    mock_writer_instance.write_table.assert_not_called()
    mock_s3.delete_file.assert_called_once_with(f'{MINIO_BUCKET_NAME}/{date_time_str}.parquet')
    assert result == f's3a://{MINIO_BUCKET_NAME}/{date_time_str}'
