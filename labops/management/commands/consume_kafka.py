import base64
import json
import logging
import time

from confluent_kafka import Consumer, KafkaException
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from labops.events import deliver
from labops.event_schema import canonical_json_bytes, EventValidationError
from labops.kafka_config import consumer_config, source_identity, source_key
from labops.worker_metrics import (StopController, start_worker_metrics, stop_worker_metrics,
                                   heartbeat, database_processing_budget, EVENTS,
                                   OFFSET_COMMITS, REBALANCES, PROCESSING, operation_deadline,
                                   OperationDeadlineExceeded)

logger = logging.getLogger('labops')


def reject_constant(value):
    raise ValueError('Non-finite JSON constant')


def reject_deep_json(value):
    # Python JSON implementations differ in recursion limits. Keep a fixed
    # nesting bound before canonical hashing and JSONB persistence.
    stack = [(value, 0)]
    while stack:
        node, depth = stack.pop()
        if depth > 32:
            raise ValueError('JSON nesting exceeds the event contract limit')
        if isinstance(node, dict):
            if any('\x00' in key for key in node):
                raise ValueError('JSON object key contains a PostgreSQL-incompatible NUL')
            stack.extend((child, depth + 1) for child in node.values())
        elif isinstance(node, list):
            stack.extend((child, depth + 1) for child in node)
        elif isinstance(node, str) and '\x00' in node:
            raise ValueError('JSON string contains a PostgreSQL-incompatible NUL')


class Command(BaseCommand):
    help = 'Run one independent consumer, committing only durable database outcomes.'

    def add_arguments(self, parser):
        parser.add_argument('consumer', choices=['notification', 'analytics'])
        parser.add_argument('--max-messages', type=int, default=0)
        parser.add_argument('--idle-timeout', type=int, default=0)
        parser.add_argument('--metrics-port', type=int, default=None)

    def handle(self, *args, **options):
        if options['max_messages'] < 0 or options['idle_timeout'] < 0:
            raise CommandError('Message and idle limits cannot be negative')
        name = options['consumer']
        client = Consumer(consumer_config(name))
        metrics = None
        assigned = set()
        lost = set()
        count = 0
        last = time.monotonic()
        stop = StopController()

        def on_assign(_client, partitions):
            REBALANCES.labels(name, 'assign').inc()
            for partition in partitions:
                key = (partition.topic, partition.partition)
                assigned.add(key)
                lost.discard(key)

        def on_revoke(_client, partitions):
            REBALANCES.labels(name, 'revoke').inc()
            # Every completed operation is committed immediately. No speculative
            # stored offset is committed on revoke or close.
            for partition in partitions:
                key = (partition.topic, partition.partition)
                assigned.discard(key)
                lost.add(key)

        def on_lost(_client, partitions):
            REBALANCES.labels(name, 'lost').inc()
            for partition in partitions:
                key = (partition.topic, partition.partition)
                assigned.discard(key)
                lost.add(key)

        try:
            metrics = start_worker_metrics(name, port=options['metrics_port'])
            client.subscribe([settings.KAFKA_TOPIC], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
            cluster, generation = source_identity()
            with stop:
                while not stop.stopped:
                    heartbeat(name)
                    message = client.poll(1)
                    if stop.stopped:
                        break
                    if message is None:
                        if options['idle_timeout'] and time.monotonic() - last >= options['idle_timeout']:
                            break
                        continue
                    if message.error():
                        EVENTS.labels(name, 'failure').inc()
                        raise KafkaException(message.error())
                    raw = message.value() or b''
                    try:
                        event = json.loads(raw, parse_constant=reject_constant)
                        reject_deep_json(event)
                        canonical_json_bytes(event)
                    except (ValueError, UnicodeError, EventValidationError, RecursionError):
                        event = {'invalid_payload_base64': base64.b64encode(raw).decode()}
                    if not isinstance(event, dict):
                        event = {'invalid_payload': event}
                    started = time.monotonic()
                    # A database failure in effects OR failure persistence exits
                    # here. No commit is attempted on that path.
                    with operation_deadline(settings.KAFKA_CONSUMER_PROCESS_TIMEOUT_SECONDS), database_processing_budget():
                        deliver(name, event, source_key(message.topic(), message.partition(), message.offset()),
                                source_cluster=cluster, source_generation=generation)
                    elapsed = time.monotonic() - started
                    PROCESSING.labels(name).observe(elapsed)
                    if elapsed > settings.KAFKA_CONSUMER_PROCESS_TIMEOUT_SECONDS:
                        raise CommandError('Consumer processing budget exceeded; offset retained for safe replay')
                    partition = (message.topic(), message.partition())
                    if partition in lost:
                        OFFSET_COMMITS.labels(name, 'lost').inc()
                        raise CommandError('Partition ownership lost; offset retained for safe replay')
                    try:
                        with operation_deadline(min(settings.KAFKA_SOCKET_TIMEOUT_MS / 1000 + 1, stop.remaining())):
                            committed = client.commit(message=message, asynchronous=False)
                        for offset in committed or []:
                            if offset.error is not None:
                                raise KafkaException(offset.error)
                    except (Exception, OperationDeadlineExceeded):
                        OFFSET_COMMITS.labels(name, 'failure').inc()
                        logger.error('consumer_offset_commit_failed consumer=%s', name)
                        raise
                    OFFSET_COMMITS.labels(name, 'success').inc()
                    count += 1
                    last = time.monotonic()
                    if options['max_messages'] and count >= options['max_messages']:
                        break
        finally:
            # Auto-commit is disabled, including on close.
            try:
                with operation_deadline(min(settings.KAFKA_SOCKET_TIMEOUT_MS / 1000 + 1, stop.remaining())):
                    client.close()
            finally:
                stop_worker_metrics(metrics)
        self.stdout.write(f'Handled {count} durable deliveries for {name}')
