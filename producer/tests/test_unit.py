import random
import uuid
from unittest.mock import call
from uuid import UUID

import pytest
from confluent_kafka.error import KafkaException, ValueSerializationError

from config import EVENT_INTERVAL_SECONDS, KAFKA_TOPIC
from producer import delivery_report, generate_event, worker
from schema_registry import uuid_serializer


COLUMNS = [
    'event_id', 'user_id', 'session_id', 'event_type', 'event_timestamp',
    'request_latency_ms', 'status', 'error_code', 'product_id'
]


def test_generate_event_with_productid_relevant_eventtype_no_error(mocker):
    """Test that generate_event produces a dictionary that has product_id with relevant event_type and no error."""
    epoch = 1000000
    radnint = 123
    event_type = 'VIEW_PRODUCT'
    mocker.patch('random.choice', return_value=event_type)
    mocker.patch('random.random', return_value=1)
    mocker.patch('random.randint', return_value=radnint)
    mocker.patch('time.time', return_value=epoch)
    user_id = uuid.uuid4()
    session_id = uuid.uuid4()
    
    event = generate_event(user_id, session_id)
    
    assert isinstance(event, dict)
    assert list(event.keys()) == COLUMNS
    assert event['user_id'] == str(user_id)
    assert event['session_id'] == str(session_id)
    assert event['event_type'] == event_type
    assert event['event_timestamp'] == epoch * 1000
    assert event['request_latency_ms'] == radnint
    assert event['status'] == 'SUCCESS'
    assert event['error_code'] is None
    assert event['product_id'] == radnint


def test_generate_event_without_productid_nonrelevant_eventtype_no_error(mocker):
    """Test that generate_event produces a dictionary that doesn't have product_id with non-relevant event_type and no error."""
    event_type = 'SEARCH'
    mocker.patch('random.choice', return_value=event_type)
    mocker.patch('random.random', return_value=1)
    user_id = uuid.uuid4()
    session_id = uuid.uuid4()
    
    event = generate_event(user_id, session_id)
    
    assert isinstance(event, dict)
    assert list(event.keys()) == COLUMNS
    assert event['event_type'] == event_type
    assert event['status'] == 'SUCCESS'
    assert event['error_code'] is None
    assert event['product_id'] is None


def test_generate_event_without_productid_relevant_eventtype_error(mocker):
    """Test that generate_event produces a dictionary that has product_id with relevant event_type and has error."""
    error_code = 503
    event_type = 'VIEW_PRODUCT'
    latency = 100
    product_id = 1000
    mocker.patch('random.choice', return_value=event_type)
    mocker.patch('random.random', return_value=-1)
    mocker.patch('random.randint', side_effect=[latency, error_code, product_id])
    
    event = generate_event(uuid.uuid4(), uuid.uuid4())
    
    assert event['event_type'] == 'VIEW_PRODUCT'
    assert event['status'] == 'ERROR'
    assert event['request_latency_ms'] == latency
    assert event['error_code'] == error_code
    assert event['product_id'] == product_id


def test_uuid_serializer_success():
    """Test that the UUID serializer correctly converts a UUID to bytes."""
    test_uuid = uuid.uuid4()
    assert uuid_serializer(uuid_obj=test_uuid, _=None) == test_uuid.bytes


def test_uuid_serializer_invalid_uuid_failure():
    """Test that the UUID serializer raises a TypeError for invalid uuid input."""
    with pytest.raises(TypeError):
        uuid_serializer(uuid_obj='not-a-uuid', _=None)


def test_uuid_serializer_None_type_failure():
    """Test that the UUID serializer returns None for None input."""
    assert uuid_serializer(None, None) is None


def test_worker_produces_messages(mocker):
    """Test the worker function's core logic of producing a set number of messages."""
    user_id = UUID('123e4567-e89b-12d3-a456-426614174000')
    user_event = {'event_id': 'test-event'}
    max_messages = 2
    mocker.patch('producer.generate_event', return_value=user_event)
    mocker.patch('random.random', side_effect=[random.random(), 1] * max_messages)
    mocker.patch('uuid.uuid4', return_value=user_id)
    
    mock_producer = mocker.MagicMock()
    mocker.patch('producer.SerializingProducer', return_value=mock_producer, autospec=True)
    mock_producer.flush.return_value = 0
    
    worker(worker_id=102, max_messages=max_messages)
    
    assert mock_producer.produce.call_count == max_messages
    
    calls_list = mock_producer.produce.call_args_list
    assert all(call == mocker.call(topic=KAFKA_TOPIC, key=user_id, value=user_event, on_delivery=delivery_report) for call in calls_list)
    
    first_call_args = calls_list[0]
    assert first_call_args.kwargs['topic'] == KAFKA_TOPIC
    assert first_call_args.kwargs['key'] == user_id
    assert first_call_args.kwargs['value'] == {'event_id': 'test-event'}
    assert first_call_args.kwargs['on_delivery'] is not None


def test_worker_polls_and_handles_buffer_error(mocker):
    """Test that the worker polls correctly and handles BufferError."""
    mock_producer = mocker.MagicMock()
    mock_producer.produce.side_effect = [None, BufferError]
    mocker.patch('producer.SerializingProducer', return_value=mock_producer)
    mock_producer.flush.return_value = 0
    
    worker(worker_id=101, max_messages=2)
    
    assert mock_producer.produce.call_count == 2
    
    assert mock_producer.poll.call_args_list == [
        mocker.call(0),
        mocker.call(EVENT_INTERVAL_SECONDS),
        mocker.call(1),
        mocker.call(EVENT_INTERVAL_SECONDS),
    ]


def test_worker_survives_serialization_error_and_logs_exception(mocker):
    """Verify that, given a ValueSerializationError, the worker logs the exception and gracefully finishes its execution loop without crashing."""
    mock_producer = mocker.MagicMock()
    mocker.patch('producer.SerializingProducer', return_value=mock_producer)
    mocker.patch('producer.generate_event', return_value={'event_id': 'bad-data'})
    mock_producer.produce.side_effect = ValueSerializationError("Invalid Avro schema")
    mock_producer.flush.return_value = 0
    logger_exception_mock = mocker.patch('producer.logger.exception')
    
    worker_id = 103
    worker(worker_id=worker_id, max_messages=1)
    
    mock_producer.produce.assert_called_once()
    assert mocker.call(0) not in mock_producer.poll.call_args_list
    
    logger_exception_mock.assert_called_once()
    log_call_args = logger_exception_mock.call_args[0]
    assert 'Message serialization failed:' in log_call_args[0]
    assert log_call_args[1] == worker_id


def test_worker_survives_kafka_exception_and_logs_it(mocker):
    """Verify that, given a KafkaException during produce, the worker logs the exception and continues without crashing."""
    mock_producer = mocker.MagicMock()
    mocker.patch('producer.SerializingProducer', return_value=mock_producer)
    mocker.patch('producer.generate_event', return_value={'event_id': 'event'})
    mock_producer.produce.side_effect = KafkaException("Broker is down")
    mock_producer.flush.return_value = 0
    logger_exception_mock = mocker.patch('producer.logger.exception')
    
    worker(worker_id=104, max_messages=1)
    
    mock_producer.produce.assert_called_once()
    logger_exception_mock.assert_called_once()
    log_call_args = logger_exception_mock.call_args[0]
    assert 'Kafka error:' in log_call_args[0]
    assert mocker.call(0) not in mock_producer.poll.call_args_list


def test_worker_pauses_on_unexpected_exception(mocker):
    """Verify that, given an unexpected RuntimeError, the worker logs the exception and pauses for 5 seconds, regardless of other pauses in the loop."""
    mock_producer = mocker.MagicMock()
    mocker.patch('producer.SerializingProducer', return_value=mock_producer)
    mocker.patch('producer.generate_event', return_value={'event_id': 'event'})
    mock_producer.produce.side_effect = RuntimeError("Something completely unexpected happened")
    mock_producer.flush.return_value = 0
    logger_exception_mock = mocker.patch('producer.logger.exception')
    
    worker_id = 105
    worker(worker_id=worker_id, max_messages=1)
    
    mock_producer.produce.assert_called_once()
    
    logger_exception_mock.assert_called_once()
    call_args, _ = logger_exception_mock.call_args
    assert call_args[0] == 'Worker %d: Unexpected error occurred.'
    assert call_args[1] == worker_id
    
    mock_producer.poll.assert_any_call(5)
    
    assert len(mock_producer.poll.call_args_list) == 2
    assert mock_producer.poll.call_args_list[0] == call(5)
