from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from uuid import uuid4

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from minio import Minio
from pyspark.sql import SparkSession

from spark import analyze_events, main


MINIO_BUCKET_NAME = os.environ['MINIO_BUCKET_NAME']
NUM_ERROR = 3
NUM_SUCCESS = 17
EVENTS = {'VIEW_PRODUCT', 'ADD_TO_CART', 'CHECKOUT', 'PAYMENT', 'SEARCH'}
SCHEMA = pa.schema([
    pa.field('event_id', pa.string()),
    pa.field('user_id', pa.string()),
    pa.field('session_id', pa.string()),
    pa.field('event_type', pa.string()),
    pa.field('event_timestamp', pa.timestamp('ms', tz='Asia/Tehran')),
    pa.field('request_latency_ms', pa.int32()),
    pa.field('status', pa.string()),
    pa.field('error_code', pa.int32(), nullable=True),
    pa.field('product_id', pa.int32(), nullable=True),
])


@pytest.fixture(scope='module')
def spark():
    """Create a SparkSession for integration testing."""
    spark_session = SparkSession.builder \
        .appName('TestEventAnalysis') \
        .master('spark://spark-master:7077') \
        .config('spark.driver.host', 'spark-test-runner') \
        .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000') \
        .config('spark.hadoop.fs.s3a.access.key', os.environ['MINIO_ROOT_USER']) \
        .config('spark.hadoop.fs.s3a.secret.key', os.environ['MINIO_ROOT_PASSWORD']) \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
        .getOrCreate()
    
    yield spark_session
    spark_session.stop()


@pytest.fixture
def minio_client() -> Minio:
    """Create a real MinIO client for integration testing."""
    minio_client = Minio(
        endpoint='minio:9000',
        access_key=os.environ['MINIO_ROOT_USER'],
        secret_key=os.environ['MINIO_ROOT_PASSWORD'],
        secure=False
    )
    return minio_client


@pytest.fixture
def parquet_file(minio_client):
    """Create a test parquet file in MinIO and yield its S3 path."""
    timestamp = datetime(2025, 1, 15, 10, 0)
    timestamp_str = timestamp.strftime('%Y-%m-%d_%H-%M')
    object_name = f'{timestamp_str}.parquet'
    
    test_data = []
    
    for event_type in EVENTS:
        test_data.extend(
            {
                'event_id': str(uuid4()),
                'user_id': str(uuid4()),
                'session_id': str(uuid4()),
                'event_type': event_type,
                'event_timestamp': timestamp,
                'request_latency_ms': 50,
                'status': 'ERROR',
                'error_code': 500,
                'product_id': 1000 if event_type in {'VIEW_PRODUCT', 'ADD_TO_CART'} else None,
            }
            for _ in range(NUM_ERROR)
        )
        test_data.extend(
            {
                'event_id': str(uuid4()),
                'user_id': str(uuid4()),
                'session_id': str(uuid4()),
                'event_type': event_type,
                'event_timestamp': timestamp,
                'request_latency_ms': 50,
                'status': 'SUCCESS',
                'error_code': None,
                'product_id': 1000 if event_type in {'VIEW_PRODUCT', 'ADD_TO_CART'} else None,
            }
            for _ in range(NUM_SUCCESS)
        )
    
    df = pd.DataFrame(test_data)
    
    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
        table = pa.Table.from_pandas(df, schema=SCHEMA)
        pq.write_table(table, tmp.name)
        
        minio_client.fput_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            file_path=tmp.name
        )
    
    s3_path = f's3a://{MINIO_BUCKET_NAME}/{object_name}'
    
    yield s3_path
    
    minio_client.remove_object(MINIO_BUCKET_NAME, object_name)


def test_spark_analyze_events_with_data(spark: SparkSession, parquet_file: str) -> None:
    """Test `analyze_events` with real data."""
    result = analyze_events(spark=spark, file_path=parquet_file)
    
    assert result['total_events'] == len(EVENTS) * (NUM_ERROR + NUM_SUCCESS)
    assert result['total_errors'] == len(EVENTS) * NUM_ERROR
    
    for event_type, stats in result['by_event_type'].items():
        assert event_type in EVENTS
        assert stats['SUCCESS'] == NUM_SUCCESS
        assert stats['ERROR'] == NUM_ERROR


def test_spark_analyze_events_empty_file(spark: SparkSession, minio_client: Minio) -> None:
    """Test `analyze_events` with an empty parquet file."""
    object_name = 'empty-test.parquet'
    
    empty_df = pd.DataFrame(
        columns=[
            'event_id', 'user_id', 'session_id', 'event_type',
            'event_timestamp', 'request_latency_ms', 'status',
            'error_code', 'product_id'
        ]
    )
    
    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
        table = pa.Table.from_pandas(empty_df, schema=SCHEMA)
        pq.write_table(table, tmp.name)
        
        minio_client.fput_object(
            bucket_name=MINIO_BUCKET_NAME,
            object_name=object_name,
            file_path=tmp.name
        )
    
    s3_path = f's3a://{MINIO_BUCKET_NAME}/{object_name}'
    
    try:
        result = analyze_events(spark=spark, file_path=s3_path)
        
        assert result['total_events'] == 0
        assert result['total_errors'] == 0
        assert result['by_event_type'] == {}
    finally:
        minio_client.remove_object(MINIO_BUCKET_NAME, object_name)


def test_spark_main_with_data(mocker, minio_client: Minio, parquet_file: str) -> None:
    """Test the main function of spark.py with real data."""
    mocker.patch('sys.argv', ['spark.py', parquet_file])
    
    with pytest.raises(SystemExit) as exc_info:
        main()
    
    assert exc_info.value.code == 0
    
    json_object_name = parquet_file.split(os.sep)[-1].replace('.parquet', '.json')
    try:
        response = minio_client.get_object(MINIO_BUCKET_NAME, json_object_name)
        result_data = json.loads(response.read())
        
        report = result_data['report']
        assert report['total_events'] == len(EVENTS) * (NUM_ERROR + NUM_SUCCESS)
        assert report['total_errors'] == len(EVENTS) * NUM_ERROR
    finally:
        response.close()
        response.release_conn()


def test_spark_main_no_data(mocker, minio_client: Minio) -> None:
    """Test spark `main` function with no parquet file."""
    timestamp_str = '2025-01-15_11-00'
    s3_path = f's3a://{MINIO_BUCKET_NAME}/{timestamp_str}'
    
    mocker.patch('sys.argv', ['spark.py', s3_path])
    
    with pytest.raises(SystemExit) as exc_info:
        main()
    
    assert exc_info.value.code == 0
    
    json_object_name = f'{timestamp_str}.json'
    
    try:
        response = minio_client.get_object(MINIO_BUCKET_NAME, json_object_name)
        result_data = json.loads(response.read())
        
        assert 'report' in result_data
        assert result_data['report'] == f'No data for {timestamp_str}.'
    finally:
        response.close()
        response.release_conn()
        minio_client.remove_object(MINIO_BUCKET_NAME, json_object_name)
