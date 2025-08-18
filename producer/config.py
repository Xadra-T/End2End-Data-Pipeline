import os
from enum import Enum

from dotenv import load_dotenv
from schema_registry import avro_serializer, uuid_serializer


load_dotenv()


class Events(str, Enum):
    ADD_TO_CART = 'ADD_TO_CART'
    CHECKOUT = 'CHECKOUT'
    PAYMENT = 'PAYMENT'
    SEARCH = 'SEARCH'
    VIEW_PRODUCT = 'VIEW_PRODUCT'


class Status(str, Enum):
    SUCCESS = 'SUCCESS'
    ERROR = 'ERROR'


NUM_WORKERS = 1
KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
EVENT_INTERVAL_SECONDS = 0.01
NEW_USER_SESSION_PROBABILITY = 0.01

PRODUCER_CONF = {
    'acks': 'all',
    'batch.size': 32768,  # 32 KB
    'linger.ms': 20,
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'compression.type': 'snappy',
    'key.serializer': uuid_serializer,
    'value.serializer': avro_serializer,
}
