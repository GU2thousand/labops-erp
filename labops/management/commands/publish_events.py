import logging
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections
from labops.events import producer, publish_one
from labops.publisher_shards import publisher_shard_owner, ShardOwnershipLost
from labops.worker_metrics import (StopController, start_worker_metrics, stop_worker_metrics, heartbeat,
                                   operation_deadline, database_statement_budget)


class Command(BaseCommand):
    help = 'Publish inventory outbox with one PostgreSQL owner per shard. Stop all publishers before changing shard count.'

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
            with StopController() as stop, publisher_shard_owner(options['shard_index'], options['shard_count']) as owner:
                owner_lost = False
                try:
                    while not stop.stopped:
                        heartbeat('publisher')
                        batch_complete = False
                        try:
                            for _ in range(options['limit']):
                                if stop.stopped:
                                    break
                                budget = settings.KAFKA_PRODUCER_QUEUE_WAIT_SECONDS + settings.KAFKA_PUBLISH_FLUSH_SECONDS + settings.EVENT_PUBLISH_DB_BUDGET_SECONDS
                                with operation_deadline(min(budget, stop.remaining())):
                                    # Verify the dedicated session before touching
                                    # a possibly stale application DB connection.
                                    owner.assert_owned()
                                    with database_statement_budget(settings.EVENT_PUBLISH_DB_BUDGET_SECONDS):
                                        published = publish_one(broker, shard_index=options['shard_index'], shard_count=options['shard_count'],
                                                                ownership_check=owner.assert_owned)
                                if not published:
                                    break
                            else:
                                batch_complete = True
                        except ShardOwnershipLost:
                            # A loop never silently reacquires after owner loss.
                            owner_lost = True
                            raise
                        except DatabaseError:
                            # Only the application connection is replaceable.
                            # The independently owned shard session is never reacquired.
                            connections['default'].close()
                            logging.getLogger('labops').exception('publisher_database_failed')
                            if not options['loop']:
                                raise
                        except Exception:
                            logging.getLogger('labops').exception('publisher_failed')
                            if not options['loop']:
                                raise
                        if not options['loop']:
                            break
                        # Backlog work can continue immediately after a full
                        # successful batch; idle, partial and failed batches
                        # retain their interruptible retry/idle wait.
                        if not batch_complete:
                            stop.wait(1)
                finally:
                    if owner_lost:
                        # Do not drain extra queued sends from a stale owner.
                        # Purging cannot retract broker records already in flight.
                        broker.purge(in_queue=True, in_flight=True, blocking=False)
                        broker.poll(0)
                    else:
                        remaining = broker.flush(min(settings.KAFKA_PUBLISH_FLUSH_SECONDS, stop.remaining()))
                        if remaining:
                            logging.getLogger('labops').error('publisher_shutdown_unacknowledged count=%s', remaining)
        finally:
            stop_worker_metrics(metrics)
