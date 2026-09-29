"""Independent worker metrics; no payloads or event IDs in labels."""
import hmac
import signal
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import GaugeMetricFamily

REGISTRY = CollectorRegistry()
EVENTS = Counter('labops_worker_events_total', 'Worker durable outcomes', ['worker', 'outcome'], registry=REGISTRY)
PUBLISH_ACK = Histogram('labops_worker_publish_ack_seconds', 'Broker acknowledgement latency', ['worker'], registry=REGISTRY)
EFFECT_LATENCY = Histogram('labops_worker_effect_latency_seconds', 'Outbox creation to committed database effect', ['consumer'],
                           buckets=(.1, .5, 1, 2, 5, 15, 30, 60, 300, 900, 3600), registry=REGISTRY)
LEASE_REJECTIONS = Counter('labops_worker_lease_rejections_total', 'Lost ownership writeback rejects', ['worker'], registry=REGISTRY)
SCHEMA_REJECTIONS = Counter('labops_worker_schema_rejections_total', 'Rejected event contracts', ['code'], registry=REGISTRY)
OFFSET_COMMITS = Counter('labops_worker_offset_commits_total', 'Synchronous commit outcomes', ['consumer', 'outcome'], registry=REGISTRY)
REBALANCES = Counter('labops_worker_rebalances_total', 'Partition lifecycle callbacks', ['consumer', 'action'], registry=REGISTRY)
HEARTBEAT = Gauge('labops_worker_heartbeat_timestamp_seconds', 'Last progress of worker main loop', ['worker'], registry=REGISTRY)
PROCESSING = Histogram('labops_worker_processing_seconds', 'Durable delivery duration', ['consumer'], registry=REGISTRY)
WORKERS = {'publisher', 'notification', 'analytics', 'retry', 'dlq'}
SCHEMA_CODES = {'json', 'size', 'schema', 'type', 'uuid', 'timestamp', 'aggregate', 'decimal', 'payload', 'lines',
                'string', 'hash_conflict', 'unknown', 'invalid_json', 'invalid_decimal', 'unsupported_schema', 'unknown_type',
                'schema_version', 'event_type', 'payload_conflict', 'missing_business_event'}


def schema_rejected(code):
    SCHEMA_REJECTIONS.labels(code if code in SCHEMA_CODES else 'unknown').inc()


def heartbeat(worker):
    HEARTBEAT.labels(worker).set(time.time())


class DurableCounts:
    """Counts/oldest queries only; no ledger reconciliation on scrape."""
    def collect(self):
        from django.db import close_old_connections
        from django.db.models import Count
        from django.utils import timezone
        from .models import FailedDelivery, OutboxEvent
        close_old_connections()
        database = GaugeMetricFamily('labops_worker_database_available', 'Durable state scrape available')
        try:
            states = dict(OutboxEvent.objects.filter(transport='kafka').values('status').annotate(n=Count('id')).values_list('status', 'n'))
            outbox = GaugeMetricFamily('labops_worker_outbox_events', 'Durable Kafka outbox counts', labels=['status'])
            for status in ('PENDING', 'PROCESSING', 'PUBLISHED', 'DEAD'):
                outbox.add_metric([status], states.get(status, 0))
            failures = GaugeMetricFamily('labops_worker_failed_deliveries', 'Durable failures distinct from committed offsets', labels=['consumer', 'status'])
            counts = {(name, status): n for name, status, n in FailedDelivery.objects.values('consumer_name', 'status').annotate(n=Count('id')).values_list('consumer_name', 'status', 'n')}
            age = GaugeMetricFamily('labops_worker_retry_oldest_seconds', 'Oldest unresolved retry age', labels=['consumer'])
            for name in ('notification', 'analytics'):
                for status in ('RETRY', 'DEAD', 'RESOLVED'):
                    failures.add_metric([name, status], counts.get((name, status), 0))
                oldest = FailedDelivery.objects.filter(consumer_name=name, status='RETRY').order_by('created_at').values_list('created_at', flat=True).first()
                age.add_metric([name], max(0, (timezone.now() - oldest).total_seconds()) if oldest else 0)
            database.add_metric([], 1)
            yield outbox
            yield failures
            yield age
        except Exception:
            database.add_metric([], 0)
        finally:
            close_old_connections()
        yield database


REGISTRY.register(DurableCounts())


def start_worker_metrics(worker, port=None):
    if worker not in WORKERS:
        raise ImproperlyConfigured('Unknown metrics worker identity')
    heartbeat(worker)
    if not settings.WORKER_METRICS_ENABLED:
        return None
    from .kafka_config import read_secret_file
    token = (read_secret_file(settings.WORKER_METRICS_TOKEN_FILE, 'WORKER_METRICS_TOKEN_FILE')
             if settings.WORKER_METRICS_TOKEN_FILE else settings.WORKER_METRICS_TOKEN)
    if not token:
        raise ImproperlyConfigured('Enabled worker metrics require WORKER_METRICS_TOKEN or its secret file')
    port = settings.WORKER_METRICS_PORT if port is None else port
    if not 1 <= port <= 65535:
        raise ImproperlyConfigured('WORKER_METRICS_PORT must be between 1 and 65535')

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != '/metrics':
                self.send_error(404)
                return
            if not hmac.compare_digest(self.headers.get('Authorization', '').encode('utf-8'), ('Bearer ' + token).encode('utf-8')):
                self.send_error(403)
                return
            body = generate_latest(REGISTRY)
            self.send_response(200)
            self.send_header('Content-Type', CONTENT_TYPE_LATEST)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer((settings.WORKER_METRICS_HOST, port), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name='worker-metrics', daemon=True).start()
    return server


def stop_worker_metrics(server):
    if server is not None:
        server.shutdown()
        server.server_close()


class OperationDeadlineExceeded(BaseException):
    """Abort and roll back; broad business failure catch must not swallow it."""


@contextmanager
def operation_deadline(seconds):
    if threading.current_thread() is not threading.main_thread() or not hasattr(signal, 'setitimer'):
        raise ImproperlyConfigured('Event workers require POSIX main-thread deadline support')
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    started = time.monotonic()

    def expire(_number, _frame):
        raise OperationDeadlineExceeded('Durable worker operation exceeded its deadline; offset remains unconfirmed')

    signal.signal(signal.SIGALRM, expire)
    signal.setitimer(signal.ITIMER_REAL, max(.001, seconds))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0]:
            signal.setitimer(signal.ITIMER_REAL, max(.001, previous_timer[0] - (time.monotonic() - started)), previous_timer[1])


@contextmanager
def database_statement_budget(seconds):
    """Bound each publisher query without holding a DB transaction across Kafka."""
    from django.db import connection, DatabaseError
    if connection.vendor != 'postgresql':
        yield
        return
    try:
        with connection.cursor() as cursor:
            # Materialize both old values before either session setter runs.
            # Target-list evaluation order alone is not a read-before-set fence.
            cursor.execute("""WITH previous AS MATERIALIZED (
                SELECT current_setting('statement_timeout') AS statement_value,
                       current_setting('lock_timeout') AS lock_value
            )
            SELECT previous.statement_value, previous.lock_value,
                   set_config('statement_timeout', %s, false),
                   set_config('lock_timeout', %s, false)
            FROM previous""", [str(int(seconds * 1000)), str(settings.EVENT_DB_LOCK_TIMEOUT_MS)])
            previous_statement, previous_lock, _, _ = cursor.fetchone()
    except BaseException:
        # A failed execute/fetch can follow successful server-side setters,
        # while their prior values are still unknown to Python. Discard this
        # application session rather than reuse an ambiguous timeout budget.
        try:
            connection.close()
        except BaseException:
            pass  # Cleanup must not replace the original setup/control error.
        raise
    try:
        yield
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)",
                               [previous_statement, previous_lock])
        except DatabaseError:
            connection.close()


class StopController:
    """Signal handlers set a flag; finish one bounded durable operation."""
    def __init__(self):
        self.event = threading.Event()
        self.requested_at = None
        self.previous = {}

    def request(self, *_args):
        if self.requested_at is None:
            self.requested_at = time.monotonic()
        self.event.set()

    @property
    def stopped(self):
        return self.event.is_set()

    def remaining(self):
        if self.requested_at is None:
            return settings.WORKER_SHUTDOWN_TIMEOUT_SECONDS
        return max(0, settings.WORKER_SHUTDOWN_TIMEOUT_SECONDS - (time.monotonic() - self.requested_at))

    def wait(self, seconds):
        self.event.wait(seconds)

    def __enter__(self):
        if threading.current_thread() is threading.main_thread():
            for number in (signal.SIGTERM, signal.SIGINT):
                self.previous[number] = signal.signal(number, self.request)
        return self

    def __exit__(self, *_args):
        for number, handler in self.previous.items():
            signal.signal(number, handler)


@contextmanager
def database_processing_budget():
    from django.db import connection, transaction
    with transaction.atomic():
        if connection.vendor == 'postgresql':
            with connection.cursor() as cursor:
                cursor.execute("SELECT set_config('statement_timeout', %s, true), set_config('lock_timeout', %s, true)",
                               [str(settings.EVENT_RETRY_STATEMENT_TIMEOUT_MS), str(settings.EVENT_RETRY_LOCK_TIMEOUT_MS)])
        yield
