from __future__ import annotations

import logging
import logging.handlers
import os
import random
import signal
import sys
import time
import uuid
from multiprocessing import Process
from uuid import UUID

from confluent_kafka import Message
from confluent_kafka.error import KafkaError, KafkaException, ValueSerializationError
from confluent_kafka.serializing_producer import SerializingProducer

from config import EVENT_INTERVAL_SECONDS, Events, Status, NUM_WORKERS, NEW_USER_SESSION_PROBABILITY, PRODUCER_CONF, KAFKA_TOPIC
from custom_types import Event


logger = logging.getLogger(__name__)

_shutdown = False


def _handle_sigterm(signum: int, frame: object) -> None:  # noqa: ARG001
    global _shutdown
    _shutdown = True
    logger.info('SIGTERM received — initiating graceful shutdown.')


signal.signal(signal.SIGTERM, _handle_sigterm)


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
        # time.time() unit is second (in UTC). Avro timestamp-millis expects milliseconds.

        'request_latency_ms': random.randint(50, 1500),
        'status': Status.ERROR if has_error else Status.SUCCESS,
        'error_code': random.randint(400, 599) if has_error else None,
        'product_id': random.randint(1, 10000) if event_type in {Events.VIEW_PRODUCT, Events.ADD_TO_CART} else None,
    }


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    """Report delivery failures.

    Args:
        err: KafkaError on failure; None on success.
        msg: The Message containing topic/partition/offset metadata on success.
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


def _configure_worker_logging() -> None:
    """Configure logging for a forked worker process.

    After fork(), file descriptors are inherited but handlers are not
    re-opened. Re-configuring logging in every child avoids interleaved
    or silently dropped log lines when the parent uses a rotating handler.
    """
    root = logging.getLogger()
    # Remove any handlers inherited from the parent to avoid duplicate output.
    for h in root.handlers[:]:
        root.removeHandler(h)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    root.addHandler(handler)
    root.setLevel(logging.INFO)


def worker(worker_id: int, max_messages: int | None = None) -> None:
    """Continuously generate data and send it to Kafka.

    Args:
        worker_id: A unique identifier for the worker process.
        max_messages: If provided, the worker will stop after producing this many messages (used in tests).
    """
    # Re-register signal handler inside child process — signal handlers are not
    # inherited reliably across fork() on all platforms.
    signal.signal(signal.SIGTERM, _handle_sigterm)

    # Re-configure logging in each child process to avoid interleaved or
    # silently dropped log lines from inherited parent file handles.
    _configure_worker_logging()

    logger.info('Starting worker %d (PID: %d)', worker_id, os.getpid())
    producer = SerializingProducer(PRODUCER_CONF)

    user_id = uuid.uuid4()
    session_id = uuid.uuid4()
    count = 0
    time_start = time.time()

    def _should_continue() -> bool:
        if _shutdown:
            return False
        if max_messages is not None:
            return count < max_messages
        return True

    while _should_continue():
        count += 1
        user_event = generate_event(user_id, session_id)
        if count % 1000 == 0:
            time_sofar = time.time() - time_start
            logger.info(
                'Worker %d produced %d messages in %.2f seconds (%.2f MPS).',
                worker_id, count, time_sofar, count / time_sofar,
            )
        try:
            producer.produce(
                topic=KAFKA_TOPIC,
                key=user_id,
                value=user_event,
                on_delivery=delivery_report,
            )
            producer.poll(0)
        except BufferError:
            logger.warning('Worker %d: Producer buffer full. Polling for 1s before retrying...', worker_id)
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

        # Use time.sleep() for rate-limiting — it is a true pause.
        # producer.poll() drains delivery-report callbacks; using it as a
        # rate limiter was a semantic misuse: it blocks only until the next
        # delivery event fires, not for a fixed wall-clock interval.
        time.sleep(EVENT_INTERVAL_SECONDS)
        producer.poll(0)

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

    processes: list[Process] = []
    logger.info('Spawning %d worker processes...', NUM_WORKERS)
    for i in range(NUM_WORKERS):
        p = Process(target=worker, args=(i + 1,))
        processes.append(p)
        p.start()

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info('Shutdown signal received. Sending SIGTERM to workers.')
        for p in processes:
            # p.terminate() sends SIGTERM (not SIGKILL) on Unix, giving workers
            # a chance to flush their Kafka buffers via the _shutdown flag.
            # p.kill() would send SIGKILL and bypass the graceful flush.
            p.terminate()
        for p in processes:
            p.join(timeout=30)
            if p.is_alive():
                logger.warning('Worker PID %d did not exit in time; force-killing.', p.pid)
                p.kill()
