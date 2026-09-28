import logging
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from labops.events import retry_deliveries
from labops.kafka_config import validate_runtime
from labops.worker_metrics import (StopController, start_worker_metrics, stop_worker_metrics,
                                   heartbeat, database_processing_budget, operation_deadline)


class Command(BaseCommand):
    help = 'Retry durable consumer failures independently from publishers and DLQ.'

    def add_arguments(self, parser):
        parser.add_argument('--loop', action='store_true')
        parser.add_argument('--limit', type=int, default=100)
        parser.add_argument('--metrics-port', type=int, default=None)

    def handle(self, *args, **options):
        if options['limit'] < 1:
            raise CommandError('Limit must be positive')
        validate_runtime()
        metrics = start_worker_metrics('retry', port=options['metrics_port'])
        count = 0
        try:
            with StopController() as stop:
                while not stop.stopped:
                    heartbeat('retry')
                    try:
                        for _ in range(options['limit']):
                            if stop.stopped:
                                break
                            with operation_deadline(min(settings.KAFKA_CONSUMER_PROCESS_TIMEOUT_SECONDS, stop.remaining())), database_processing_budget():
                                processed = retry_deliveries(limit=1)
                            count += processed
                            if not processed:
                                break
                    except Exception:
                        logging.getLogger('labops').exception('retry_worker_failed')
                        if not options['loop']:
                            raise
                    if not options['loop']:
                        break
                    stop.wait(1)
        finally:
            stop_worker_metrics(metrics)
        self.stdout.write(f'Handled {count} durable retries')
