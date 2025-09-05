from __future__ import annotations

import os
from datetime import datetime, timedelta
from unittest.mock import Mock
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pytest
import pyarrow.parquet as pq
from airflow.hooks.base import BaseHook
from clickhouse_connect.driver.client import Client
from minio import Minio
from pyarrow.parquet import ParquetWriter

from common import MINIO_BUCKET_NAME
from pipeline import schema


date_time = datetime(2025, 8, 10, 12, 0, tzinfo=ZoneInfo('UTC'))
df_chunk = pd.DataFrame({
    'event_type': ['VIEW_PRODUCT'],
    'status': ['SUCCESS'],
})


def test_stream_from_clickhouse_to_minio_with_data(mocker, stream_func, mock_df_stream, mock_ch_conn):
    """Test stream_from_clickhouse_to_minio handles data streaming, Parquet writing, and MinIO upload."""
    mock_ch_client = mock_df_stream([df_chunk])
    
    mock_namedtemp = mocker.patch('pipeline.tempfile.NamedTemporaryFile')
    tempfile_name = '/tmp/test.parquet'
    mock_namedtemp.return_value.__enter__.return_value.name = tempfile_name
    
    mock_writer = Mock(spec=ParquetWriter)
    mocker.patch('pipeline.pq.ParquetWriter', return_value=mock_writer)
    
    mock_minio_client = Mock(spec=Minio)
    mocker.patch('pipeline.get_minio_client', return_value=mock_minio_client)
    
    data_interval_start = date_time + timedelta(minutes=1)
    result = stream_func(data_interval_start=data_interval_start)
    
    mock_ch_client.query_df_stream.assert_called_once_with(
        query='SELECT event_type, status FROM %(table)s WHERE event_minute = %(timestamp)s;',
        parameters={'table': os.environ['CLICKHOUSE_TABLE'], 'timestamp': date_time},
        settings={'max_block_size': 100000}
    )
    
    mock_writer.write_table.assert_called_once()
    written_table = mock_writer.write_table.call_args[1]['table']
    assert written_table.schema == schema
    
    date_time_str = date_time.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    mock_minio_client.fput_object.assert_called_once_with(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=date_time_str + '.parquet',
        file_path=tempfile_name
    )
    
    assert result == f's3a://{MINIO_BUCKET_NAME}/{date_time_str}.parquet'


def test_stream_from_clickhouse_to_minio_no_data(mocker, stream_func, mock_df_stream, mock_ch_conn):
    """Test stream_from_clickhouse_to_minio handles no data case without upload."""
    mock_ch_client = mock_df_stream([])
    
    mock_minio = Mock(spec=Minio)
    mocker.patch('pipeline.get_minio_client', return_value=mock_minio)
    
    data_interval_start = datetime(2025, 8, 10, 8, 31, 0, tzinfo=ZoneInfo('UTC'))
    result = stream_func(data_interval_start=data_interval_start)
    
    mock_ch_client.query_df_stream.assert_called_once()
    mock_minio.fput_object.assert_not_called()
    filename = (data_interval_start.astimezone(ZoneInfo('Asia/Tehran')) - timedelta(minutes=1)).strftime("%Y-%m-%d_%H-%M")
    assert result == f's3a://{MINIO_BUCKET_NAME}/{filename}'


def test_stream_from_clickhouse_to_minio_exception(mocker, stream_func):
    """Test stream_from_clickhouse_to_minio raises exception on failure."""
    mock_ch_client = Mock(spec=Client)
    mock_ch_client.query_df_stream.side_effect = ValueError('Query failed')
    
    mocker.patch('clickhouse_connect.get_client', return_value=mock_ch_client)
    
    data_interval_start = datetime(2025, 8, 10, 8, 31, 0, tzinfo=ZoneInfo('UTC'))
    
    with pytest.raises(ValueError, match='Query failed'):
        stream_func(data_interval_start)


def test_error_propagation(mocker, stream_func) -> None:
    """Test that errors are properly propagated through the pipeline."""
    err_msg = 'Connection not found'
    mocker.patch.object(BaseHook, 'get_connection', side_effect=Exception(err_msg))
    
    with pytest.raises(Exception) as exc_info:
        stream_func(data_interval_start=datetime.now(ZoneInfo('Asia/Tehran')))
    
    assert err_msg in str(exc_info.value)


def test_data_transformation_in_stream(mocker, stream_func, mock_df_stream, mock_ch_conn):
    """Test that data transformations are applied correctly."""
    chunk1 = df_chunk.copy()
    chunk2 = df_chunk.copy()
    chunk2['status'] = ['ERROR']
    mock_df_stream([chunk1, chunk2])
    
    mock_minio_client = Mock(spec=Minio)
    mocker.patch('pipeline.get_minio_client', return_value=mock_minio_client)
    
    mock_namedtemp = mocker.patch('tempfile.NamedTemporaryFile')
    tempfile_name = '/tmp/test.parquet'
    mock_namedtemp.return_value.__enter__.return_value.name = tempfile_name
    
    written_tables = []
    mock_writer_instance = Mock(spec=pq.ParquetWriter)
    mock_writer_instance.write_table.side_effect = lambda table: written_tables.append(table)
    mock_writer_class = mocker.patch('pipeline.pq.ParquetWriter', return_value=mock_writer_instance)
    
    result = stream_func(data_interval_start=date_time + timedelta(minutes=1))
    
    mock_writer_class.assert_called_once_with(where=tempfile_name, schema=schema)
    
    assert len(written_tables) == 2
    assert mock_writer_instance.write_table.call_count == 2
    
    for written_table, chunk in zip(written_tables, [chunk1, chunk2]):
        assert written_table.column('status').type == pa.string()
        assert written_table.column('event_type').to_pylist()[0] == chunk['event_type'].iloc[0]
        assert written_table.column('status').to_pylist()[0] == chunk['status'].iloc[0]
        assert written_table.schema == schema
        
    expected_filename = date_time.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    mock_minio_client.fput_object.assert_called_once()
    mock_minio_client.fput_object.assert_called_once_with(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=f'{expected_filename}.parquet',
        file_path=tempfile_name
    )
    
    assert result == f's3a://{MINIO_BUCKET_NAME}/{expected_filename}.parquet'


def test_stream_from_clickhouse_to_minio_empty_chunk(mocker, stream_func, mock_df_stream, mock_ch_conn):
    """Test stream_from_clickhouse_to_minio handles empty DataFrame correctly."""
    empty_df = pd.DataFrame(columns=df_chunk.columns)
    mock_df_stream([empty_df])
    
    mock_writer = Mock(spec=pq.ParquetWriter)
    mocker.patch('pipeline.pq.ParquetWriter', return_value=mock_writer)
    
    mock_minio_client = Mock(spec=Minio)
    mocker.patch('pipeline.get_minio_client', return_value=mock_minio_client)
    
    data_interval_start = date_time + timedelta(minutes=1)
    result = stream_func(data_interval_start=data_interval_start)
    
    mock_writer.write_table.assert_not_called()
    mock_minio_client.fput_object.assert_not_called()
    
    date_time_str = date_time.astimezone(ZoneInfo('Asia/Tehran')).strftime('%Y-%m-%d_%H-%M')
    assert result == f's3a://{MINIO_BUCKET_NAME}/{date_time_str}'
