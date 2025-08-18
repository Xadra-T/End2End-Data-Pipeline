from __future__ import annotations

import os
from typing import TYPE_CHECKING
from uuid import UUID

from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from dotenv import load_dotenv

if TYPE_CHECKING:
    from confluent_kafka.serialization import SerializationContext


def uuid_serializer(uuid_obj: UUID, _: SerializationContext | None) -> bytes | None:
    """Serialize a uuid object to bytes."""
    if not uuid_obj:
        return None
    if not isinstance(uuid_obj, UUID):
        msg = f'Excpected a UUID object, got {type(uuid_obj)}'
        raise TypeError(msg)
    return uuid_obj.bytes


load_dotenv()

schema = str(avro.load('user_event_schema.avsc'))
sr_config = {'url': f'{os.environ["SCHEMA_REGISTRY_URL"]}'}
serializer_config = {'auto.register.schemas': True}
sr_client = SchemaRegistryClient(sr_config)
avro_serializer = AvroSerializer(schema_registry_client=sr_client, schema_str=schema, conf=serializer_config)
avro_deserializer = AvroDeserializer(schema_registry_client=sr_client, schema_str=schema)
