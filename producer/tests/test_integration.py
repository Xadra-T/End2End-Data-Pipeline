import logging
import os
import time
from datetime import datetime
import pytest
from confluent_kafka import DeserializingConsumer

from config import Events, Status
from producer import worker
from schema_registry import avro_deserializer


logger = logging.getLogger(__name__)
TABLE = os.environ['CLICKHOUSE_TABLE']


def test_produces_to_clickhouse_pipeline(clickhouse_client):
    """Test the pipeline: Producer -> Kafka -> Connect -> ClickHouse."""
    num_test_messages = 2
    
    clickhouse_client.command(f'TRUNCATE TABLE {TABLE}')
    
    worker(worker_id=101, max_messages=num_test_messages)
    
    poll_interval_seconds = 2  # Takes time for messages to reach ClickHouse
    start = time.time()
    end_time = poll_interval_seconds * num_test_messages + start
    final_count = 0
    while time.time() < end_time:
        try:
            final_count = clickhouse_client.query(f'SELECT count() FROM {TABLE};').result_rows[0][0]
            if final_count >= num_test_messages:
                break
            logger.info(f'Found {final_count}/{num_test_messages} rows. Waiting...')
            time.sleep(poll_interval_seconds)
        except Exception as e:
            logger.info(f'An error occurred while polling ClickHouse: {e}. Retrying...')
            time.sleep(poll_interval_seconds)
    
    assert final_count == num_test_messages, f'Expected {num_test_messages} rows, but found {final_count}.'
    
    row = clickhouse_client.query('SELECT event_type, status, event_timestamp FROM %(table)s LIMIT 1', parameters={'table': TABLE}).result_rows[0]
    assert isinstance(row[0], str)
    assert row[0] in Events.__members__
    assert isinstance(row[1], str)
    assert row[1] in Status.__members__
    assert isinstance(row[2], datetime), f'Timestamp should be a datetime object, but got {type(row[2])}'


def test_producer_worker_sends_valid_avro_messages(clickhouse_client):
    """Verify the worker function produces valid Avro messages to the Kafka topic."""
    kafka_consumer_conf = {
        'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        'group.id': 'test-integration-consumer-group',
        'auto.offset.reset': 'earliest',
        'value.deserializer': avro_deserializer,
    }
    
    max_messages = 3
    worker(worker_id=99, max_messages=max_messages)
    
    consumer = DeserializingConsumer(conf=kafka_consumer_conf)
    consumer.subscribe([os.environ['KAFKA_TOPIC']])
    consumed_messages = []
    try:
        while len(consumed_messages) < max_messages:
            msg = consumer.poll(timeout=1)
            
            if msg is None:
                pytest.fail(reason=f'Timed out waiting for messages. Received {len(consumed_messages)} out of {max_messages}.')
            
            if msg.error():
                pytest.fail(reason=f'Consumer error: {msg.error()}')
            
            consumed_messages.append(msg.value())
    finally:
        consumer.close()
    
    assert len(consumed_messages) == max_messages
    
    first_event = consumed_messages[0]
    assert isinstance(first_event, dict)
    assert 'event_id' in first_event
    assert isinstance(first_event['event_type'], str)
    assert first_event['status'] in {'SUCCESS', 'ERROR'}
    
    clickhouse_client.command(f'TRUNCATE TABLE {TABLE}')
