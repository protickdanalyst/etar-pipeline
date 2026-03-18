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


# FIX 1: NUM_WORKERS was hardcoded to 1 — now configurable via env var with safe fallback.
NUM_WORKERS = int(os.environ.get('NUM_WORKERS', 1))

KAFKA_TOPIC = os.environ['KAFKA_TOPIC']
EVENT_INTERVAL_SECONDS = float(os.environ.get('EVENT_INTERVAL_SECONDS', 0.01))
NEW_USER_SESSION_PROBABILITY = float(os.environ.get('NEW_USER_SESSION_PROBABILITY', 0.01))

PRODUCER_CONF = {
    # FIX 2: 'acks' must be an integer 0/1 or the string 'all'. String 'all' is valid for
    # confluent-kafka but only guarantees durability if min.insync.replicas is set on the broker.
    # Keeping 'all' but documenting the dependency explicitly.
    'acks': 'all',
    'batch.size': 32768,  # 32 KB
    'linger.ms': 20,
    'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
    'compression.type': 'snappy',
    'key.serializer': uuid_serializer,
    'value.serializer': avro_serializer,
    # FIX 3: Added delivery timeout and retry settings absent in original — without these,
    # a broker blip causes silent message loss.
    'delivery.timeout.ms': 120000,   # 2 min max wait for ack
    'retries': 5,
    'retry.backoff.ms': 500,
    # FIX 4: Enable idempotent producer to prevent duplicate messages on retry.
    'enable.idempotence': True,
}
