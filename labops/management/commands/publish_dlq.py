import logging
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections
from labops.events import producer, publish_dlq
from labops.worker_metrics import (StopController, start_worker_metrics, stop_worker_metrics, heartbeat,
                                   operation_deadline, database_statement_budget)


class Command(BaseCommand):
    help = 'Mirror durable DEAD records using an independent DLQ Kafka identity.'

    def add_arguments(self, parser):
        parser.add_argument('--loop', action='store_true')
        parser.add_argument('--limit', type=int, default=100)
        parser.add_argument('--metrics-port', type=int, default=None)

    def handle(self, *args, **options):
        if options['limit'] < 1:
            raise CommandError('Limit must be positive')
        broker = producer(role='dlq')
        metrics = None
        try:
            metrics = start_worker_metrics('dlq', port=options['metrics_port'])
            with StopController() as stop:
                try:
                    while not stop.stopped:
                        heartbeat('dlq')
                        try:
                            for _ in range(options['limit']):
                                if stop.stopped:
                                    break
                                # A failed claim returns zero like an empty
                                # queue. Continue so another eligible row runs.
                                budget = settings.KAFKA_PRODUCER_QUEUE_WAIT_SECONDS + settings.KAFKA_PUBLISH_FLUSH_SECONDS + settings.EVENT_PUBLISH_DB_BUDGET_SECONDS
                                with operation_deadline(min(budget, stop.remaining())), database_statement_budget(settings.EVENT_PUBLISH_DB_BUDGET_SECONDS):
                                    publish_dlq(broker, limit=1)
                        except DatabaseError:
                            connections['default'].close()
                            logging.getLogger('labops').exception('dlq_database_failed')
                            if not options['loop']:
                                raise
                        except Exception:
                            logging.getLogger('labops').exception('dlq_worker_failed')
                            if not options['loop']:
                                raise
                        if not options['loop']:
                            break
                        stop.wait(1)
                finally:
                    remaining = broker.flush(min(settings.KAFKA_PUBLISH_FLUSH_SECONDS, stop.remaining()))
                    if remaining:
                        logging.getLogger('labops').error('dlq_shutdown_unacknowledged count=%s', remaining)
        finally:
            stop_worker_metrics(metrics)
