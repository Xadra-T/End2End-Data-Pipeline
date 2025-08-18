import logging
import os
import sys
from socket import gaierror

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)


@pytest.fixture
def clickhouse_client() -> Client:
    """Establish a connection to ClickHouse.
    
    Returns:
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
        return client
    except (ConnectionRefusedError, gaierror) as e:
        pytest.fail(f'Could not connect to ClickHouse due to a network error: {e}')
    except ClickHouseError as e:
        pytest.fail(f'A ClickHouse server error occurred during connection: {e}')
    except Exception as e:
        pytest.fail(f'An unexpected error occurred while connecting to ClickHouse: {type(e).__name__} - {e}')
