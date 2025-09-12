import logging
from datetime import datetime
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import clickhouse_connect
import pytest
from minio import Minio
from minio.deleteobjects import DeleteObject
from minio.error import S3Error

from pipeline import etar_pipeline, get_minio_client
from common import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_TABLE, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB, MINIO_BUCKET_NAME


logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def dag():
    """Load the DAG instance."""
    return etar_pipeline()


@pytest.fixture(scope='session')
def minio_client() -> Minio:
    """Provide a real Minio client."""
    return get_minio_client()


@pytest.fixture(scope='module')
def report_func(dag):
    """Extract the send_to_dashboard task callable."""
    return dag.get_task('send_to_dashboard').python_callable


@pytest.fixture(scope='module')
def stream_func(dag):
    """Extract the stream_from_clickhouse_to_minio task callable."""
    return dag.get_task('stream_from_clickhouse_to_minio').python_callable


@pytest.fixture
def mock_ch_client(mocker):
    """Mock ClickHouse client."""
    mock_client = Mock()
    mocker.patch('pipeline.clickhouse_connect.get_client', return_value=mock_client)
    return mock_client


@pytest.fixture
def clickhouse_client():
    """Create a real ClickHouse client for integration testing."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )
    yield client
    client.close()


@pytest.fixture
def test_timestamp() -> datetime:
    """Generate a test timestamp for consistent testing."""
    return datetime(2025, 1, 1, 10, 30, tzinfo=ZoneInfo('UTC'))


@pytest.fixture
def delete_all_data(clickhouse_client, minio_client):
    """Clean up all test data before and after each test.
    
    This fixture does not need to be called inside the test.
    """
    logger.info('Calling `delete_all_data` before test.')
    # Before test
    _delete_all_data_clickhouse(clickhouse_client)
    _delete_all_data_minio(minio_client)
    
    yield
    
    # After test
    logger.info('Calling `delete_all_data` after test.')
    _delete_all_data_clickhouse(clickhouse_client)
    _delete_all_data_minio(minio_client)


def _delete_all_data_clickhouse(client) -> None:
    """Remove all data from ClickHouse."""
    count_query = 'SELECT COUNT(*) FROM {table}'
    report = 'ClickHouse: db: {db}, table: {table}, number of rows {stage} truncating: {row_count}.'
    
    row_count = client.command(count_query.format(table=CLICKHOUSE_TABLE))
    logger.info(report.format(db=CLICKHOUSE_DB, table=CLICKHOUSE_TABLE, stage='before', row_count=row_count))
    
    client.command(f'TRUNCATE TABLE IF EXISTS {CLICKHOUSE_TABLE}')
    
    row_count = client.command(count_query.format(table=CLICKHOUSE_TABLE))
    logger.info(report.format(db=CLICKHOUSE_DB, table=CLICKHOUSE_TABLE, stage='after', row_count=row_count))


def _delete_all_data_minio(client) -> None:
    """Remove all files from MinIO.
    
    Args:
        client: MinIO client.
    
    Raises:
        S3Error: If fails to delete an object from MinIO.
    """
    report = 'Minio: bucket: {bucket}, objects {stage} delete: {object_names}'
    
    objects_to_delete = client.list_objects(bucket_name=MINIO_BUCKET_NAME, recursive=True)
    object_names = [obj.object_name for obj in objects_to_delete]
    logger.info(report.format(bucket=MINIO_BUCKET_NAME, stage='before', object_names=object_names))
    
    if object_names:
        delete_object_list = [DeleteObject(name) for name in object_names]
        errors = client.remove_objects(bucket_name=MINIO_BUCKET_NAME, delete_object_list=delete_object_list)
        
        has_errors = False
        for error in errors:
            has_errors = True
            logger.error('Error occurred when trying to delete object %s from MinIO bucket %s.', error, MINIO_BUCKET_NAME)
        
        if has_errors:
            raise S3Error(message='Failed to delete one or more objects from Minio. Check logs for details.')
        
        logger.info('Minio: bucket %s cleared.', MINIO_BUCKET_NAME)
    else:
        logger.info('Minio bucket %s was empty.', MINIO_BUCKET_NAME)
    
    objects_to_delete = client.list_objects(bucket_name=MINIO_BUCKET_NAME, recursive=True)
    object_names = [obj.object_name for obj in objects_to_delete]
    logger.info(report.format(bucket=MINIO_BUCKET_NAME, stage='after', object_names=object_names))
