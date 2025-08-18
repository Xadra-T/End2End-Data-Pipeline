import os
import uuid
from datetime import datetime, timezone
from socket import gaierror

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError


CLICKHOUSE_TABLE = os.environ['CLICKHOUSE_TABLE']


@pytest.fixture(scope='module')
def clickhouse_client() -> Client:
    """Establish a connection to ClickHouse.
    
    Yields:
        ClickHouse client.
    """
    try:
        client = clickhouse_connect.get_client(
            host=os.environ['CLICKHOUSE_HOST'],
            port=int(os.environ['CLICKHOUSE_PORT']),
            user=os.environ['CLICKHOUSE_USER'],
            password=os.environ['CLICKHOUSE_PASSWORD'],
            database=os.environ['CLICKHOUSE_DB']
        )
        client.ping()
        yield client
        client.command(f'TRUNCATE TABLE IF EXISTS {CLICKHOUSE_TABLE}')
    except (ConnectionRefusedError, gaierror) as e:
        pytest.fail(f'Could not connect to ClickHouse due to a network error: {e}')
    except ClickHouseError as e:
        pytest.fail(f'A ClickHouse server error occurred during connection: {e}')
    except Exception as e:
        pytest.fail(f'An unexpected error occurred while connecting to ClickHouse: {type(e).__name__} - {e}')


def test_clickhouse_insert_and_select_valid_data(clickhouse_client: Client):
    """Test that a valid row can be inserted and retrieved correctly, verifying data types and materialized column."""
    event_ts = datetime.now()
    
    test_row = (
        uuid.uuid4(),   # event_id
        uuid.uuid4(),   # user_id
        uuid.uuid4(),   # session_id
        'ADD_TO_CART',  # event_type
        event_ts,       # event_timestamp
        250,            # request_latency_ms
        'SUCCESS',      # status
        None,           # error_code
        12345,          # product_id
    )
    clickhouse_client.insert(table=CLICKHOUSE_TABLE, data=[test_row])
    
    result = clickhouse_client.query(
        f'SELECT *, event_minute FROM {CLICKHOUSE_TABLE} WHERE event_id = %(event_id)s',
        parameters={'event_id': test_row[0]}
    )
    
    retrieved_row = result.result_rows[0]
    assert result.row_count == 1
    assert retrieved_row[0] == test_row[0]
    assert retrieved_row[3] == 'ADD_TO_CART'
    assert retrieved_row[4].strftime('%Y-%m-%d %H:%M:%S') == event_ts.strftime('%Y-%m-%d %H:%M:%S')
    assert retrieved_row[5] == 250
    assert retrieved_row[7] is None
    
    expected_minute = event_ts.replace(second=0, microsecond=0)
    
    assert retrieved_row[9] == expected_minute, 'Materialized event_minute column is incorrect.'


def test_clickhouse_handles_nullable_fields(clickhouse_client: Client):
    """Test inserting a row where nullable fields are explicitly None."""
    event_ts = datetime.now(tz=timezone.utc)
    column_names = [
        'event_id', 'user_id', 'session_id', 'event_type', 'event_timestamp',
        'request_latency_ms', 'status', 'error_code', 'product_id'
    ]
    test_data = [(
        uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), 'PAYMENT', event_ts,
        250, 'ERROR', 503, None
    )]
    
    clickhouse_client.insert(table=CLICKHOUSE_TABLE, data=test_data, column_names=column_names)
    result = clickhouse_client.query(
        query="SELECT product_id, error_code FROM %(CLICKHOUSE_TABLE)s WHERE event_type = 'PAYMENT';",
        parameters={'CLICKHOUSE_TABLE': CLICKHOUSE_TABLE}
    )
    assert result.row_count > 0
    retrieved_row = result.result_rows[0]
    assert retrieved_row[0] is None
    assert retrieved_row[1] == 503
