from __future__ import annotations

import logging
import os
import random
import sys
import time
import uuid
from multiprocessing import Process
from typing import TYPE_CHECKING
from uuid import UUID

from confluent_kafka.error import KafkaError, KafkaException, ValueSerializationError
from confluent_kafka.serializing_producer import SerializingProducer

from config import EVENT_INTERVAL_SECONDS, Events, Status, NUM_WORKERS, NEW_USER_SESSION_PROBABILITY, PRODUCER_CONF, KAFKA_TOPIC

if TYPE_CHECKING:
    from confluent_kafka import Message
    
    from custom_types import Event


logger = logging.getLogger(__name__)


def generate_event(user_id: UUID, session_id: UUID) -> Event:
    """Generate a user event dictionary.
    
    Args:
        user_id: The UUID of the user.
        session_id: The UUID of the session.
    
    Returns:
        A dictionary representing the event log.
    """
    error_probability = random.uniform(0, 0.5)
    has_error = random.random() < error_probability
    event_type = random.choice(list(Events))
    
    return {
        'event_id': str(uuid.uuid4()),
        'user_id': str(user_id),
        'session_id': str(session_id),
        'event_type': event_type,
        
        'event_timestamp': int(time.time() * 1000),
        # time.time() unit is second (in UTC). Avro timestamp-millis expects milliseconds. Multiplying by 1000 is the unit conversion.
        # ClickHouse column is set DateTime64(3, 'UTC') to store and display the value in UTC with millisecond precision.
        
        'request_latency_ms': random.randint(50, 1500),
        'status': Status.ERROR if has_error else Status.SUCCESS,
        'error_code': random.randint(400, 599) if has_error else None,
        'product_id': random.randint(1, 10000) if event_type in {Events.VIEW_PRODUCT, Events.ADD_TO_CART} else None
    }


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Report delivery failures.
    
    Args:
        err: KafkaError on failure; None on success.
        msg: The Message containing topic/partition/offset metadata (on success), and the original key/value.
    """
    if err is not None:
        try:
            code = err.code()
            reason = err.str()
        except Exception:
            code = 'unknown'
            reason = str(err)
        logger.error(
            'Delivery failed: topic=%s, partition=%s, key=%s, error_code=%s, reason=%s',
            msg.topic(),
            msg.partition(),
            msg.key(),
            code,
            reason,
        )


def worker(worker_id: int, max_messages: int | None = None) -> None:
    """Continuously generate data and send it to Kafka.
    
    Args:
        worker_id: A unique identifier for the worker process.
        max_messages: If provided, the worker will stop after producing this many messages. Used for testing.
    """
    logger.info('Starting worker %d (PID: %d)', worker_id, os.getpid())
    producer = SerializingProducer(PRODUCER_CONF)
    
    user_id = uuid.uuid4()
    session_id = uuid.uuid4()
    count = 0
    time_start = time.time()
    
    while True if max_messages is None else count < max_messages:
        count += 1
        user_event = generate_event(user_id, session_id)
        if count % 1000 == 0:
            time_sofar = time.time() - time_start
            logger.info('Worker %d produced %d messages in %f seconds with an average speed of %.2f MPS.', worker_id, count, time_sofar, count / time_sofar)
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=user_id,
                value=user_event,
                on_delivery=delivery_report
            )
            producer.poll(0)
        except BufferError:
            logger.info('Worker %d: Producer buffer full. Polling for 1s before retrying...', worker_id)
            producer.poll(1)
        except ValueSerializationError:
            logger.exception('Worker %d: Message serialization failed:', worker_id)
        except KafkaException:
            logger.exception('Worker %d: Kafka error:', worker_id)
        except Exception:
            logger.exception('Worker %d: Unexpected error occurred.', worker_id)
            producer.poll(5)
        
        if random.random() < NEW_USER_SESSION_PROBABILITY:
            user_id = uuid.uuid4()
            session_id = uuid.uuid4()
        
        producer.poll(EVENT_INTERVAL_SECONDS)
    
    if max_messages:
        logger.info('Worker %d: Loop done. Flushing producer...', worker_id)
        while remaining_messages := producer.flush(timeout=1):
            logger.info('Worker %d: %d messages still in queue after flush.', worker_id, remaining_messages)
    logger.info('Worker %d: All messages flushed successfully.', worker_id)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout,
    )
    
    processes = []
    logger.info('Spawning %d worker processes...', NUM_WORKERS)
    for i in range(NUM_WORKERS):
        p = Process(target=worker, args=(i + 1,))
        processes.append(p)
        p.start()
    
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info('Shutdown signal received. Terminating workers.')
        for p in processes:
            p.terminate()
