from __future__ import annotations

import os
from uuid import UUID

from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext
from dotenv import load_dotenv


def uuid_serializer(uuid_obj: UUID, _: SerializationContext | None) -> bytes | None:
    """Serialize a uuid object to bytes."""
    # FIX 5: Original used falsy check `if not uuid_obj` which incorrectly treats
    # a valid but all-zero UUID as None. Use explicit None check instead.
    if uuid_obj is None:
        return None
    if not isinstance(uuid_obj, UUID):
        msg = f'Expected a UUID object, got {type(uuid_obj)}'  # FIX 6: Fixed typo 'Excpected'
        raise TypeError(msg)
    return uuid_obj.bytes


load_dotenv()

schema = str(avro.load('user_event_schema.avsc'))
sr_config = {'url': os.environ['SCHEMA_REGISTRY_URL']}  # FIX 7: Removed redundant f-string wrapping
serializer_config = {'auto.register.schemas': True}
sr_client = SchemaRegistryClient(sr_config)
avro_serializer = AvroSerializer(schema_registry_client=sr_client, schema_str=schema, conf=serializer_config)
avro_deserializer = AvroDeserializer(schema_registry_client=sr_client, schema_str=schema)
