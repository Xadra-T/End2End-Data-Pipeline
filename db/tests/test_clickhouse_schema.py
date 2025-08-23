from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from socket import gaierror

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError


CLICKHOUSE_TABLE = os.environ['CLICKHOUSE_TABLE']


@pytest.fixture(scope='module')
def clickhouse_client() -> Iterator[Client]:
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
        'SELECT *, event_minute FROM %(table)s WHERE event_id = %(event_id)s',
        parameters={'table': CLICKHOUSE_TABLE, 'event_id': test_row[0]}
    )
    
    retrieved_row = result.result_rows[0]
    assert result.row_count == 1
    assert retrieved_row[0] == test_row[0]
    assert retrieved_row[3] == test_row[3]
    assert retrieved_row[4].replace(microsecond=0) == event_ts.replace(microsecond=0)
    assert retrieved_row[5] == test_row[5]
    assert retrieved_row[7] == test_row[7]
    assert retrieved_row[9] == event_ts.replace(second=0, microsecond=0), 'Materialized event_minute column is incorrect.'


def test_clickhouse_handles_nullable_fields(clickhouse_client: Client):
    """Test inserting a row where nullable fields are explicitly None."""
    column_names = [
        'event_id', 'user_id', 'session_id', 'event_type', 'event_timestamp',
        'request_latency_ms', 'status', 'error_code', 'product_id'
    ]
    test_data = [(
        uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), 'PAYMENT', datetime.now(tz=timezone.utc),
        250, 'ERROR', 503, None
    )]
    
    clickhouse_client.insert(table=CLICKHOUSE_TABLE, data=test_data, column_names=column_names)
    result = clickhouse_client.query(
        query="SELECT product_id, error_code FROM %(table)s WHERE event_type = 'PAYMENT';",
        parameters={'table': CLICKHOUSE_TABLE}
    )
    assert result.row_count > 0
    retrieved_row = result.result_rows[0]
    assert retrieved_row[0] == test_data[0][8]
    assert retrieved_row[1] == test_data[0][7]
