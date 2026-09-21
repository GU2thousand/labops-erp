import json
import base64
from django.conf import settings
from django.core.management.base import BaseCommand
from confluent_kafka import Consumer, KafkaException
from labops.events import deliver

class Command(BaseCommand):
    help = 'Run an independent notification or analytics consumer group.'
    def add_arguments(self, p):
        p.add_argument('consumer', choices=['notification', 'analytics'])
        p.add_argument('--max-messages', type=int, default=0)
        p.add_argument('--idle-timeout', type=int, default=0)
    def handle(self, *args, **options):
        import time
        name = options['consumer']
        client = Consumer({'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'labops.'+name+'.v1', 'enable.auto.commit': False,
            'enable.auto.offset.store': False, 'auto.offset.reset': 'earliest'})
        client.subscribe([settings.KAFKA_TOPIC]); count = 0; last = time.monotonic()
        try:
            while True:
                message = client.poll(1)
                if message is None:
                    if options['idle_timeout'] and time.monotonic()-last >= options['idle_timeout']: break
                    continue
                if message.error(): raise KafkaException(message.error())
                try: event = json.loads(message.value())
                except (ValueError, UnicodeDecodeError): event = {'invalid_payload_base64': base64.b64encode(message.value() or b'').decode()}
                if not isinstance(event, dict): event = {'invalid_payload': event}
                deliver(name, event, f'{message.topic()}:{message.partition()}:{message.offset()}')
                client.commit(message=message, asynchronous=False)
                count += 1; last = time.monotonic()
                if options['max_messages'] and count >= options['max_messages']: break
        finally: client.close()
        self.stdout.write(f'Handled {count} deliveries for {name}')
