import logging
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from labops.events import producer, publish_one
from labops.worker_metrics import (StopController, start_worker_metrics, stop_worker_metrics, heartbeat,
                                   operation_deadline, database_statement_budget)


class Command(BaseCommand):
    help = 'Publish inventory outbox records; retry and DLQ are separate workers.'

    def add_arguments(self, parser):
        parser.add_argument('--loop', action='store_true')
        parser.add_argument('--limit', type=int, default=100)
        parser.add_argument('--metrics-port', type=int, default=None)
        parser.add_argument('--shard-index', type=int, default=settings.EVENT_PUBLISHER_SHARD_INDEX)
        parser.add_argument('--shard-count', type=int, default=settings.EVENT_PUBLISHER_SHARD_COUNT)

    def handle(self, *args, **options):
        if options['limit'] < 1 or options['shard_count'] < 1 or not 0 <= options['shard_index'] < options['shard_count']:
            raise CommandError('Limit/shard count must be positive and shard index within shard count')
        broker = producer()
        metrics = None
        try:
            metrics = start_worker_metrics('publisher', port=options['metrics_port'])
            with StopController() as stop:
                try:
                    while not stop.stopped:
                        heartbeat('publisher')
                        try:
                            for _ in range(options['limit']):
                                if stop.stopped:
                                    break
                                budget = settings.KAFKA_PRODUCER_QUEUE_WAIT_SECONDS + settings.KAFKA_PUBLISH_FLUSH_SECONDS + settings.EVENT_PUBLISH_DB_BUDGET_SECONDS
                                with operation_deadline(min(budget, stop.remaining())), database_statement_budget(settings.EVENT_PUBLISH_DB_BUDGET_SECONDS):
                                    published = publish_one(broker, shard_index=options['shard_index'], shard_count=options['shard_count'])
                                if not published:
                                    break
                        except Exception:
                            logging.getLogger('labops').exception('publisher_failed')
                            if not options['loop']:
                                raise
                        if not options['loop']:
                            break
                        stop.wait(1)
                finally:
                    remaining = broker.flush(min(settings.KAFKA_PUBLISH_FLUSH_SECONDS, stop.remaining()))
                    if remaining:
                        logging.getLogger('labops').error('publisher_shutdown_unacknowledged count=%s', remaining)
        finally:
            stop_worker_metrics(metrics)
