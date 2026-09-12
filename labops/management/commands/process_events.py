import time
from django.core.management.base import BaseCommand
from django.core.management import call_command
from django.conf import settings
import logging
from django.utils import timezone
from labops.operations.services import check_alerts,consume_events,process_imports
class Command(BaseCommand):
    help='Consume durable events; --loop checks daily alerts and polls every 10 seconds.'
    def add_arguments(self,p): p.add_argument('--loop',action='store_true')
    def handle(self,*a,**opts):
        last=None
        while True:
            try:
                if last!=timezone.localdate():
                    check_alerts()
                    if settings.DATABASES['default']['ENGINE']=='django.db.backends.sqlite3':call_command('backup_database')
                    last=timezone.localdate()
                process_imports()
                count=consume_events()
                if count:self.stdout.write(f'Processed {count} events')
            except Exception:
                logging.getLogger('labops').exception('operations_worker_failed')
            if not opts['loop']: break
            time.sleep(10)
