import logging
import sys
from datetime import datetime
from unittest.mock import Mock, MagicMock
from zoneinfo import ZoneInfo

import clickhouse_connect
import pytest
from airflow.hooks.base import BaseHook
from airflow.sdk import Connection
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
def send_func(dag):
    """Extract the send_to_dashboard task callable."""
    return dag.get_task('send_to_dashboard').python_callable


@pytest.fixture(scope='module')
def stream_func(dag):
    """Extract the stream_from_clickhouse_to_minio task callable."""
    return dag.get_task('stream_from_clickhouse_to_minio').python_callable


@pytest.fixture
def mock_ch_conn(mocker) -> Mock:
    """Mock ClickHouse connection and patch `get_connection` with it.
    
    This fixture does not need to be called in the body of the test; just needs to be initialized (done by fdefault by pytest) to patch the object.
    (This only effects tests that explicitly request it as a parameter.)
    """
    mock_conn = Mock(spec=Connection)
    mock_conn.host = CLICKHOUSE_HOST
    mock_conn.port = CLICKHOUSE_PORT
    mock_conn.login = CLICKHOUSE_USER
    mock_conn.password = CLICKHOUSE_PASSWORD
    mock_conn.schema = CLICKHOUSE_DB
    mocker.patch.object(BaseHook, 'get_connection', return_value=mock_conn)
    return mock_conn


@pytest.fixture
def mock_df_stream(mocker):
    def inner(data):
        mock_yielded_data = MagicMock()
        mock_yielded_data.__iter__.return_value = iter(data)
        
        mock_ctx_manager = MagicMock()
        mock_ctx_manager.__enter__.return_value = mock_yielded_data
        mock_ch_client = MagicMock()
        mock_ch_client.query_df_stream.return_value = mock_ctx_manager
        mocker.patch('clickhouse_connect.get_client', return_value=mock_ch_client, autospec=True)
        return mock_ch_client
    
    return inner


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
    logger.info(f'Calling `delete_all_data` before test.')
    # Before test
    _delete_all_data_clickhouse(clickhouse_client)
    _delete_all_data_minio(minio_client)
    
    yield
    
    # After test
    logger.info(f'Calling `delete_all_data` after test.')
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
    """Remove all files from MinIO."""
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
            logger.error(f'Error occurred when trying to delete object {error} from MinIO bucket {MINIO_BUCKET_NAME}.')
        
        if has_errors:
            raise S3Error('Failed to delete one or more objects from Minio. Check logs for details.')
        
        logger.info(f'Minio: bucket {MINIO_BUCKET_NAME} cleared.')
    else:
        logger.info(f'Minio bucket {MINIO_BUCKET_NAME} was empty.')
    
    objects_to_delete = client.list_objects(bucket_name=MINIO_BUCKET_NAME, recursive=True)
    object_names = [obj.object_name for obj in objects_to_delete]
    logger.info(report.format(bucket=MINIO_BUCKET_NAME, stage='after', object_names=object_names))
