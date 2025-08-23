import logging
import os
import pandas as pd
import random
import sys
from datetime import datetime
from uuid import uuid4


CLICKHOUSE_HOST = os.environ['CLICKHOUSE_HOST']
CLICKHOUSE_PORT = int(os.environ['CLICKHOUSE_PORT'])
CLICKHOUSE_TABLE = os.environ['CLICKHOUSE_TABLE']
CLICKHOUSE_USER = os.environ['CLICKHOUSE_USER']
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']
CLICKHOUSE_DB = os.environ['CLICKHOUSE_DB']

DASHBOARD_API_URL = os.environ['DASHBOARD_API_URL']

EVENTS = ['ADD_TO_CART', 'CHECKOUT', 'PAYMENT', 'SEARCH', 'VIEW_PRODUCT']

MINIO_BUCKET_NAME = os.environ['MINIO_BUCKET_NAME']

REPORT_SAMPLE = {
    'report': {
        'total_events': 5805,
        'total_errors': 1398,
        'by_event_type': {
            'ADD_TO_CART': {'SUCCESS': 876, 'ERROR': 292},
            'CHECKOUT': {'SUCCESS': 846, 'ERROR': 289},
            'PAYMENT': {'SUCCESS': 884, 'ERROR': 281},
            'SEARCH': {'SUCCESS': 933, 'ERROR': 261},
            'VIEW_PRODUCT': {'SUCCESS': 868, 'ERROR': 275}
        },
        'process_time': 22.15983009338379,
        'file_name': '2025-08-04_19-04.json'
    }
}

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)


def insert_test_data(clickhouse_client, timestamp: datetime, num_rows: int = 10) -> pd.DataFrame:
    """Create test data, insert it into ClickHouse and return the DataFrame.
    
    Returns:
        Test data.
    """
    row_count = clickhouse_client.command(f'SELECT COUNT(*) FROM {CLICKHOUSE_TABLE}')
    logger.info('ClickHouse table: %s number, of rows before data insertion: %d.', CLICKHOUSE_TABLE, row_count)
    logger.info('Inserting data to ClickHouse...')
    
    rows = []
    for _ in range(num_rows):
        error_probability = random.uniform(0, 0.5)
        has_error = random.random() < error_probability
        event_type = random.choice(EVENTS)
        row = {
            'event_id': str(uuid4()),
            'user_id': str(uuid4()),
            'session_id': str(uuid4()),
            'event_type': event_type,
            'event_timestamp': timestamp,
            'request_latency_ms': random.randint(50, 1500),
            'status': 'ERROR' if has_error else 'SUCCESS',
            'error_code': random.randint(400, 599) if has_error else None,
            'product_id': random.randint(1, 10000) if event_type in {'VIEW_PRODUCT', 'ADD_TO_CART'} else None
        }
        rows.append(row)
    
    df = pd.DataFrame(rows)
    clickhouse_client.insert_df(CLICKHOUSE_TABLE, df)
    
    row_count = clickhouse_client.command(f'SELECT COUNT(*) FROM {CLICKHOUSE_TABLE}')
    logger.info('ClickHouse table: %s number, of rows after data insertion: %d.', CLICKHOUSE_TABLE, row_count)
    return df
