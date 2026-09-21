import logging
import time
from django.core.management.base import BaseCommand
from labops.events import producer, publish_one, publish_dlq, retry_deliveries

class Command(BaseCommand):
    help = 'Publish durable outbox and DLQ records; retry parked consumer deliveries.'
    def add_arguments(self, p):
        p.add_argument('--loop', action='store_true')
        p.add_argument('--limit', type=int, default=100)
    def handle(self, *args, **options):
        broker = producer()
        while True:
            try:
                retry_deliveries()
                for _ in range(options['limit']):
                    if not publish_one(broker): break
                publish_dlq(broker)
            except Exception:
                logging.getLogger('labops').exception('publisher_failed')
                if not options['loop']: raise
            if not options['loop']: break
            time.sleep(1)
