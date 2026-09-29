"""Independent worker metrics; no payloads or event IDs in labels."""
import hmac
import inspect
import logging
import signal
import threading
import time
from contextlib import contextmanager
from types import FunctionType
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import connections
from django.db.backends.postgresql import psycopg_any as _budget_psycopg_any
from psycopg import Connection as _BudgetConnection
from psycopg import adapters as _budget_driver_adapters
from psycopg.pq import PGconn as _BudgetPGconn
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import GaugeMetricFamily

_BUDGET_SIGNAL_CALLS = (signal.getitimer, signal.getsignal)
_BUDGET_THREAD_CALLS = (threading.get_ident, threading.current_thread, threading.main_thread)
_BUDGET_ADAPTER_FACTORY = _budget_psycopg_any.get_adapters_template
_BUDGET_RAW_CLOSE = _BudgetConnection.close
_BUDGET_PHYSICAL_FINISH = _BudgetPGconn.finish
_BUDGET_ADAPTER_TYPES = frozenset(kind for mapping in (*_budget_driver_adapters._loaders,
    *_budget_driver_adapters._dumpers_by_oid, *_budget_driver_adapters._dumpers.values()) for kind in mapping.values())
_BUDGET_ADAPTER_TYPES = _BUDGET_ADAPTER_TYPES | {_budget_psycopg_any.DjangoRangeDumper}
_BUDGET_TZ_LOADER = _budget_psycopg_any.BaseTzLoader
_BUDGET_TZ_LOAD = _BUDGET_TZ_LOADER.load
_BUDGET_ADAPTER_META = type(_BUDGET_TZ_LOADER)
_BUDGET_TZ_HOOK = vars(_BUDGET_TZ_LOADER).get('__subclasshook__')
_BUDGET_ABC_STATE_TYPE = type(vars(_BUDGET_TZ_LOADER).get('_abc_impl'))


def _publisher_standard_tz_adapter(kind, timezone):
    """Django's generated loader uses the standard Protocol/ABC metaclass."""
    if type(kind) is not _BUDGET_ADAPTER_META:
        return False
    state = vars(kind)
    allowed = {'__module__', '__doc__', 'timezone', '__firstlineno__', '__static_attributes__',
        '__abstractmethods__', '__parameters__', '__subclasshook__', '_abc_impl', '_is_protocol'}
    hook = state.get('__subclasshook__')
    return (len(kind.__bases__) == 1 and kind.__bases__[0] is _BUDGET_TZ_LOADER
        and inspect.getattr_static(kind, 'load') is _BUDGET_TZ_LOAD
        and type(state.get('__module__')) is str
        and state['__module__'] == 'django.db.backends.postgresql.psycopg_any'
        and state.get('timezone') is timezone and not (state.keys() - allowed)
        and state.get('_is_protocol') is False
        and type(state.get('__parameters__')) is tuple and not state['__parameters__']
        and type(state.get('__abstractmethods__')) is frozenset and not state['__abstractmethods__']
        and type(state.get('_abc_impl')) is _BUDGET_ABC_STATE_TYPE
        and ('__firstlineno__' not in state or type(state['__firstlineno__']) is int)
        and ('__static_attributes__' not in state or (type(state['__static_attributes__']) is tuple and not state['__static_attributes__']))
        and type(hook) is classmethod and type(hook.__func__) is FunctionType
        and type(_BUDGET_TZ_HOOK) is classmethod
        and hook.__func__.__code__ is _BUDGET_TZ_HOOK.__func__.__code__)

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


class PublisherBatchBudget:
    """Private command policy; the public per-operation budget is unchanged.

    Retention changes session GUC visibility between consecutive records. It is
    admitted only for the plain command call graph, never arbitrary helpers.
    This object owns the captured physical session, not a connection alias.
    """
    def __init__(self, seconds, limit, admission=None):
        self.seconds = seconds
        self.lock_timeout = settings.EVENT_DB_LOCK_TIMEOUT_MS
        self.limit = limit
        self.admission = admission or (lambda: False)
        self.thread = threading.get_ident()
        self.database = self.raw = self.pgconn = self.pid = self.previous = self.finish_raw = None
        self.in_record = False
        self.gap_plain = False
        self.returned_true = self.returned_false = 0
        self.physical_finish_attempts = 0
        self.counts = dict.fromkeys(('setup_attempts', 'setup_completed', 'reused_records',
            'restore_attempts', 'restore_completed', 'discard_attempts', 'discard_completed',
            'fallback_records', 'admitted_records'), 0)

    def before_deadline(self):
        # An outer alarm/nested command cannot be silently suspended over gaps.
        if (signal.getitimer is not _BUDGET_SIGNAL_CALLS[0] or signal.getsignal is not _BUDGET_SIGNAL_CALLS[1]
                or any(value is not expected for value, expected in zip(
                    (threading.get_ident, threading.current_thread, threading.main_thread), _BUDGET_THREAD_CALLS))):
            self.gap_plain = False
            return
        self.gap_plain = (threading.get_ident() == self.thread
            and threading.current_thread() is threading.main_thread()
            and hasattr(signal, 'getitimer') and not signal.getitimer(signal.ITIMER_REAL)[0]
            and signal.getsignal(signal.SIGALRM) is signal.SIG_DFL)

    @staticmethod
    def _session(database):
        from django.db.backends.postgresql.base import DatabaseWrapper, Cursor, ServerBindingCursor
        from psycopg import Connection
        from psycopg.pq import TransactionStatus
        if type(database) is not DatabaseWrapper:
            return None
        state = vars(database)
        options = database.settings_dict['OPTIONS']
        if (database.alias != 'default' or database.settings_dict['ENGINE'] != 'django.db.backends.postgresql'
                or any(key not in {'server_side_binding', 'prepare_threshold', 'sslmode', 'sslrootcert',
                    'sslcert', 'sslkey', 'connect_timeout', 'application_name'} for key in options)
                or state.get('in_atomic_block') or state.get('needs_rollback')
                or state.get('closed_in_transaction') or state.get('autocommit') is not True
                or state.get('_thread_sharing_count', 0) or state.get('_thread_ident') != threading.get_ident()
                or state.get('health_check_enabled') or database.settings_dict.get('CONN_HEALTH_CHECKS')
                or state.get('execute_wrappers') or state.get('run_on_commit')
                or any(name in state for name in ('cursor', '_cursor', 'ensure_connection', 'close',
                    'create_cursor', 'get_autocommit', 'set_autocommit', 'execute_wrapper'))):
            return None
        raw = state.get('connection')
        if (Connection is not _BudgetConnection or Connection.close is not _BUDGET_RAW_CLOSE
                or type(raw) is not Connection or raw.closed or not raw.autocommit
                or any(name in vars(raw) for name in ('cursor', 'close', 'execute'))
                or vars(raw).get('_pool') is not None
                or type(raw.pgconn) is not _BudgetPGconn or _BudgetPGconn.finish is not _BUDGET_PHYSICAL_FINISH
                or raw._notice_handlers or raw._notify_handlers):
            return None
        from psycopg.rows import tuple_row
        if (raw.cursor_factory is not Cursor and raw.cursor_factory is not ServerBindingCursor) or raw.row_factory is not tuple_row:
            return None
        # Driver adapters are another synchronous callback boundary. Unknown
        # loaders/dumpers (including custom JSON loaders) use the original path.
        adapters = raw.adapters
        for mapping in (*adapters._loaders, *adapters._dumpers_by_oid, *adapters._dumpers.values()):
            if type(mapping) is not dict or len(mapping) > 1024:
                return None
            for kind in mapping.values():
                if type(kind) is not type and type(kind) is not _BUDGET_ADAPTER_META:
                    return None
                if kind not in _BUDGET_ADAPTER_TYPES and not _publisher_standard_tz_adapter(kind, database.timezone):
                    return None
        if adapters._register_loader_callback is not None:
            return None
        info = raw.info
        if info.transaction_status != TransactionStatus.IDLE:
            return None
        return raw, info.backend_pid

    def _same_session(self):
        if threading.get_ident() != self.thread or connections['default'] is not self.database:
            return False
        current = self._session(self.database)
        return current is not None and current[0] is self.raw and self.raw.pgconn is self.pgconn and current[1] == self.pid

    def _discard(self, primary=None):
        raw, pgconn, database, finish_raw = self.raw, self.pgconn, self.database, self.finish_raw
        self.raw = self.database = self.pgconn = self.pid = self.previous = self.finish_raw = None
        if raw is None:
            return True
        self.counts['discard_attempts'] += 1
        error = None
        finished = False
        try:
            # Only the captured libpq handle is finished. The same Python raw
            # object may now refer to a different physical replacement handle.
            try:
                # Idempotent native finish also covers an alarm interrupting
                # Python close before libpq actually destroys the old handle.
                self.physical_finish_attempts += 1
                finish_raw()
                finished = True
            except BaseException as caught:
                if error is None:
                    error = caught
                if not isinstance(caught, Exception):
                    # A one-shot control can land immediately before the C
                    # call. One bounded idempotent local finish retains the
                    # original error; it grants no new SQL/operation budget.
                    try:
                        self.physical_finish_attempts += 1
                        finish_raw()
                        finished = True
                    except BaseException:
                        pass
            finally:
                if raw.pgconn is pgconn:
                    object.__setattr__(raw, '_closed', True)
                if (threading.get_ident() == self.thread and vars(database).get('connection') is raw
                        and raw.pgconn is pgconn):
                    if vars(database).get('in_atomic_block'):
                        object.__setattr__(database, 'closed_in_transaction', True)
                        object.__setattr__(database, 'needs_rollback', True)
                    else:
                        object.__setattr__(database, 'connection', None)
                    object.__setattr__(database, 'run_on_commit', [])
            if finished:
                self.counts['discard_completed'] += 1
        except BaseException as caught:
            if error is None:
                error = caught
        if error is not None:
            # No restore/retry is safe after an ambiguous session transition.
            if primary is None:
                raise error
        return finished

    def finish(self, primary=None):
        if self.raw is None:
            return
        from django.db import DatabaseError
        try:
            safe = self._same_session() if primary is None or isinstance(primary, Exception) else False
        except BaseException as error:
            self._discard(primary if primary is not None else error)
            if primary is None:
                raise
            return
        if not safe:
            self._discard(primary)
            return
        try:
            database, previous = self.database, self.previous
            self.counts['restore_attempts'] += 1
            with database.cursor() as cursor:
                if database.connection is not self.raw:
                    raise RuntimeError('Publisher budget session changed before restoration')
                cursor.execute("SELECT set_config('statement_timeout', %s, false), set_config('lock_timeout', %s, false)", previous)
            self.counts['restore_completed'] += 1
            self.raw = self.database = self.pgconn = self.pid = self.previous = self.finish_raw = None
        except BaseException as error:
            closed = self._discard(primary if primary is not None else error)
            if primary is None and (not isinstance(error, DatabaseError) or not closed):
                raise

    @contextmanager
    def record(self, original_budget, seconds=None):
        retained = False
        admitted = completed = False
        seconds = self.seconds if seconds is None else seconds
        class Record:
            def retain(_self, value):
                nonlocal retained
                retained = value is True
            def complete(_self, value):
                nonlocal completed
                if admitted and not completed and threading.get_ident() == self.thread:
                    completed = True
                    if value is True:
                        self.returned_true += 1
                    elif value is False:
                        self.returned_false += 1
        token = Record()
        if self.in_record or threading.get_ident() != self.thread:
            self.counts['fallback_records'] += 1
            with original_budget(seconds):
                yield token
            return
        plain = False
        try:
            plain = (self.gap_plain and not self.in_record and type(self.limit) is int
                and 1 < self.limit <= 500 and type(seconds) in (int, float)
                and seconds == self.seconds and type(settings.EVENT_DB_LOCK_TIMEOUT_MS) is int
                and settings.EVENT_DB_LOCK_TIMEOUT_MS == self.lock_timeout and self.admission() is True)
        except Exception:
            pass
        if not plain:
            self.finish()
            self.counts['fallback_records'] += 1
            with original_budget(seconds):
                yield token
            return
        database = connections['default']
        try:
            current = self._session(database)
        except Exception:
            current = None
        if current is None:
            self.finish()
            self.counts['fallback_records'] += 1
            with original_budget(seconds):
                yield token
            return
        if self.raw is not None and (database is not self.database or current[0] is not self.raw
                or current[0].pgconn is not self.pgconn or current[1] != self.pid):
            self._discard()
        self.in_record = True
        primary = None
        try:
            if self.raw is None:
                self.database, (self.raw, self.pid) = database, current
                self.pgconn = self.raw.pgconn
                self.finish_raw = _BUDGET_PHYSICAL_FINISH.__get__(self.pgconn, _BudgetPGconn)
                self.counts['setup_attempts'] += 1
                try:
                    with database.cursor() as cursor:
                        if database.connection is not self.raw:
                            raise RuntimeError('Publisher budget session changed before setup')
                        cursor.execute("""WITH previous AS MATERIALIZED (
                            SELECT current_setting('statement_timeout') AS statement_value,
                                   current_setting('lock_timeout') AS lock_value
                        )
                        SELECT previous.statement_value, previous.lock_value,
                               set_config('statement_timeout', %s, false),
                               set_config('lock_timeout', %s, false)
                        FROM previous""", [str(int(self.seconds * 1000)), str(settings.EVENT_DB_LOCK_TIMEOUT_MS)])
                        statement, lock, _, _ = cursor.fetchone()
                        if type(statement) is not str or type(lock) is not str:
                            raise ValueError('Publisher budget setup returned invalid timeout values')
                        self.previous = [statement, lock]
                    self.counts['setup_completed'] += 1
                except BaseException as error:
                    self._discard(error)
                    raise
            else:
                self.counts['reused_records'] += 1
            self.counts['admitted_records'] += 1
            admitted = True
            yield token
        except BaseException as error:
            primary = error
            raise
        finally:
            self.in_record = False
            # Unknown result, stop, final record, error, or changed session ends
            # retention before this record's deadline exits.
            if primary is not None or not retained:
                self.finish(primary)

    def cleanup_gap(self, remaining, primary=None):
        if self.raw is None:
            return
        if primary is not None and not isinstance(primary, Exception):
            self._discard(primary)
            return
        if remaining <= 0:
            self._discard(primary)
            return
        try:
            with operation_deadline(min(self.seconds, remaining)):
                self.finish(primary)
        except BaseException as error:
            self._discard(primary if primary is not None else error)
            if primary is None:
                raise

    def receipt(self, outcome, primary=None):
        if self.counts['setup_attempts'] and (self.returned_true or self.counts['reused_records']
                or outcome in {'failed', 'stopped', 'partial'}):
            # One bounded, payload-free receipt per nonempty admitted batch.
            try:
                logging.getLogger('labops').info('publisher_budget_reuse schema=1 outcome=%s returned_true=%s returned_false=%s physical_finish_attempts=%s %s',
                    outcome, self.returned_true, self.returned_false, self.physical_finish_attempts,
                    ' '.join(f'{name}={value}' for name, value in self.counts.items()))
            except BaseException as error:
                if primary is None and not isinstance(error, Exception):
                    raise


class PublisherBudgetAdmission:
    """Static capabilities for the ordinary command, with no admission SQL.

    This stricter policy also covers read conversions and synchronous tracing
    callbacks, which the independent native claim optimization need not replace.
    Unknown capabilities choose the public helper before touching its SQL.
    """
    def __init__(self, aliases):
        from pathlib import Path
        from django.db import models
        from django.db.models.manager import Manager
        from django.db.models.query import QuerySet, RawQuerySet, RawModelIterable, ModelIterable
        from django.db.models.sql.compiler import SQLCompiler, SQLUpdateCompiler
        from django.db.backends.postgresql.base import DatabaseWrapper
        from django.db.backends.postgresql.operations import DatabaseOperations
        from django.db.backends.utils import CursorWrapper, CursorDebugWrapper
        from django.db.transaction import Atomic
        from opentelemetry import trace, propagate, context
        from opentelemetry.context.contextvars_context import ContextVarsRuntimeContext
        from opentelemetry.propagators.composite import CompositePropagator
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
        from opentelemetry.baggage.propagation import W3CBaggagePropagator
        from . import events, event_schema, publisher_shards
        self.events, self.aliases = events, tuple(aliases)
        self.model = events.OutboxEvent
        self.model_meta = self.model._meta
        self.fields = tuple(self.model_meta.concrete_fields)
        self.functions = []
        self.descriptors = []
        self.valid = True

        def function(value, allowed):
            # Only exact Python functions are inspected, never unknown getters.
            seen = set()
            while type(value) is FunctionType:
                if id(value) in seen or len(seen) >= 8:
                    self.valid = False
                    return
                seen.add(id(value))
                filename = str(Path(value.__code__.co_filename).resolve())
                if not any(filename == item or filename.startswith(item + '/') for item in allowed):
                    self.valid = False
                kwdefaults = dict(value.__kwdefaults__) if value.__kwdefaults__ is not None else None
                self.functions.append((value, value.__code__, value.__defaults__, kwdefaults, dict(value.__dict__)))
                wrapped = value.__dict__.get('__wrapped__')
                if wrapped is None:
                    return
                value = wrapped
            self.valid = False

        source = str(Path(events.__file__).resolve())
        schema_source = str(Path(event_schema.__file__).resolve())
        shard_source = str(Path(publisher_shards.__file__).resolve())
        django_source = str(Path(models.__file__).resolve().parents[2])
        import contextlib, json, uuid
        otel_source = str(Path(trace.__file__).resolve().parents[1])
        std_sources = [str(Path(module.__file__).resolve()) for module in (contextlib, json, uuid)]
        own_source = str(Path(__file__).resolve())
        if any(value is not expected for value, expected in zip(self.aliases,
                (events.producer, events.publish_one, operation_deadline, database_statement_budget,
                 publisher_shards.publisher_shard_owner))):
            self.valid = False
        self.global_refs = []
        self.global_refs.extend((module, name, vars(module).get(name)) for module, names in (
            (events, ('json', 'uuid', 'timezone', 'time', 'trace')),
            (event_schema, ('json',)),
        ) for name in names)
        for module, names in ((signal, ('getitimer', 'getsignal', 'signal', 'setitimer')),
                (threading, ('get_ident', 'current_thread', 'main_thread'))):
            self.global_refs.extend((module, name, vars(module)[name]) for name in names)
        for module, names, allowed in (
            (events, ('producer', 'publish_one', 'claim_event', '_plain_outbox_claim', '_claim_event_postgresql',
                'owned_event', 'send', 'envelope', 'raw_envelope', 'broker_error', 'classify_failure',
                'safe_error', 'retry_delay', 'validate_inventory_envelope', 'canonical_payload_hash', 'extract'),
                [source, schema_source, otel_source]),
            (event_schema, tuple(name for name, value in vars(event_schema).items() if type(value) is FunctionType
                and value.__module__ == event_schema.__name__), [schema_source]),
            (propagate, ('extract', 'get_global_textmap'), [otel_source]),
            (context, ('get_current', 'attach', 'detach'), [otel_source]),
            (json, ('dumps', 'loads'), std_sources),
            (uuid, ('uuid4',), std_sources),
            (events.timezone, ('now',), [django_source]),
        ):
            for name in names:
                value = vars(module).get(name)
                self.global_refs.append((module, name, value))
                function(value, allowed)
        for value in (operation_deadline, database_statement_budget):
            function(value, [own_source, *std_sources])
        from django.db.backends.postgresql import psycopg_any
        import psycopg
        driver_source = str(Path(psycopg.__file__).resolve().parent)
        self.global_refs.append((psycopg_any, 'get_adapters_template', psycopg_any.get_adapters_template))
        factory = psycopg_any.get_adapters_template
        from functools import _lru_cache_wrapper
        if type(factory) is not _lru_cache_wrapper:
            self.valid = False
        else:
            function(factory.__wrapped__, [django_source])
        self.adapter_types = _BUDGET_ADAPTER_TYPES
        for kind in self.adapter_types | {psycopg_any.BaseTzLoader}:
            for name in ('load', 'dump', '_loads', '_dumps', '__init__', '__getattribute__', '__setattr__'):
                method = inspect.getattr_static(kind, name, None)
                value = method.__func__ if type(method) is staticmethod else method
                if type(value) is FunctionType:
                    function(value, [django_source, driver_source, *std_sources])
                if method is not None:
                    self.descriptors.append((kind, name, method))
        self.tz_loader = psycopg_any.BaseTzLoader
        self.tz_load = inspect.getattr_static(self.tz_loader, 'load')
        function(self.tz_load, [django_source])
        self.descriptors.append((self.tz_loader, 'load', self.tz_load))
        from django.db.models.signals import pre_init, post_init, pre_save, post_save
        from django.db.backends.signals import connection_created
        self.signals = (pre_init, post_init, pre_save, post_save, connection_created)
        self.descriptors.extend((psycopg.Connection, name, inspect.getattr_static(psycopg.Connection, name))
            for name in ('close', 'cursor', 'closed', 'info', 'adapters', 'autocommit'))
        for kind, names in (
            (publisher_shards.PublisherShardOwner, ('assert_owned',)),
            (models.Model, events._OUTBOX_CLAIM_MODEL_METHODS),
            (Manager, ('get_queryset', 'filter', 'raw', 'get', 'select_for_update')),
            (QuerySet, ('__init__', '_clone', '_chain', '_fetch_all', '__iter__', 'filter', '_filter_or_exclude',
                '_filter_or_exclude_inplace', 'exclude', 'exists', 'get', 'update', '_update', 'select_for_update',
                'alias', 'annotate', 'order_by', 'first', 'iterator', 'raw')),
            (RawQuerySet, ('__init__', '__iter__', '_fetch_all', 'iterator', 'resolve_model_init_order')),
            (RawModelIterable, ('__iter__',)), (ModelIterable, ('__iter__',)),
            (SQLCompiler, ('execute_sql', 'get_converters', 'apply_converters', 'as_sql')),
            (SQLUpdateCompiler, ('execute_sql', 'as_sql')),
            (DatabaseWrapper, ('cursor', '_cursor', '_prepare_cursor', 'create_cursor', 'ensure_connection',
                'get_autocommit', 'set_autocommit', 'close', 'commit', 'rollback')),
            (DatabaseOperations, ('compiler', 'get_db_converters', 'quote_name', 'adapt_datetimefield_value',
                'adapt_json_value')),
            (CursorWrapper, ('execute', '_execute', '_execute_with_wrappers', '__enter__', '__exit__')),
            (CursorDebugWrapper, ('execute',)), (Atomic, ('__enter__', '__exit__')),
            (trace.ProxyTracer, ('start_as_current_span', '_tracer')),
            (trace.NoOpTracer, ('start_as_current_span', 'start_span')),
            (CompositePropagator, ('extract',)),
            (TraceContextTextMapPropagator, ('extract',)), (W3CBaggagePropagator, ('extract',)),
            (ContextVarsRuntimeContext, ('get_current', 'attach', 'detach')),
        ):
            for name in names:
                descriptor = inspect.getattr_static(kind, name)
                self.descriptors.append((kind, name, descriptor))
                value = descriptor.__func__ if type(descriptor) in (classmethod, staticmethod) else (
                    descriptor.fget if type(descriptor) is property else descriptor)
                function(value, [django_source, shard_source, otel_source, *std_sources])
        self.field_methods = ('from_db_value', 'get_db_converters', 'get_db_prep_value', 'get_db_prep_save',
            'get_prep_value', 'pre_save', 'to_python')
        self.field_descriptors = []
        for field in self.fields:
            for name in self.field_methods:
                descriptor = inspect.getattr_static(type(field), name, None)
                self.field_descriptors.append((field, name, descriptor))
                if descriptor is not None:
                    function(descriptor, [django_source])
        # Nested Client methods are the exact code objects in the pinned factory.
        client_code = next((item for item in events.producer.__code__.co_consts
            if type(item) is type(events.producer.__code__) and item.co_name == 'Client'), None
            ) if type(events.producer) is FunctionType else None
        self.client_codes = {item.co_name: item for item in client_code.co_consts
            if type(item) is type(client_code)} if client_code else {}

    def plain(self, broker, owner, aliases, stop=None):
        from django.db import connections, router
        from django.db.models.manager import Manager
        from django.db.models.query import QuerySet
        from django.db.models.signals import pre_init, post_init, pre_save, post_save
        from django.db.backends.signals import connection_created
        from opentelemetry import trace, propagate, context
        from opentelemetry.context.contextvars_context import ContextVarsRuntimeContext
        from opentelemetry.context.context import Context
        from opentelemetry.propagators.composite import CompositePropagator
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
        from opentelemetry.baggage.propagation import W3CBaggagePropagator
        from confluent_kafka.cimpl import Producer
        from .publisher_shards import PublisherShardOwner
        try:
            if (not self.valid or len(aliases) != len(self.aliases)
                    or any(value is not expected for value, expected in zip(aliases, self.aliases))
                    or type(owner) is not PublisherShardOwner
                    or type(settings.EVENT_PUBLISH_DB_BUDGET_SECONDS) not in (int, float)
                    or settings.DATABASE_ROUTERS or router.routers):
                return False
            if stop is not None and (type(stop) is not StopController
                    or any(name in vars(stop) for name in ('remaining', 'wait', 'stopped', 'request'))):
                return False
            if any(vars(module).get(name) is not value for module, name, value in self.global_refs):
                return False
            if any(value.__code__ is not code or value.__defaults__ is not defaults
                    or (value.__kwdefaults__ is None) != (kwdefaults is None)
                    or (kwdefaults is not None and (value.__kwdefaults__.keys() != kwdefaults.keys()
                        or any(value.__kwdefaults__[name] is not item for name, item in kwdefaults.items())))
                    or value.__dict__.keys() != attributes.keys()
                    or any(value.__dict__[name] is not item for name, item in attributes.items())
                    for value, code, defaults, kwdefaults, attributes in self.functions):
                return False
            if any(inspect.getattr_static(kind, name) is not descriptor for kind, name, descriptor in self.descriptors):
                return False
            kind = type(broker)
            if type(kind) is not type or vars(kind).get('__module__') != 'labops.events':
                return False
            for name in ('error', '__getattr__'):
                method = vars(kind).get(name)
                if type(method) is not FunctionType or method.__code__ is not self.client_codes.get(name):
                    return False
            if (inspect.getattr_static(kind, '__getattribute__') is not object.__getattribute__
                    or inspect.getattr_static(kind, '__setattr__') is not object.__setattr__
                    or set(vars(broker)) - {'client', 'last_security_error'}
                    or type(vars(broker).get('client')) is not Producer
                    or 'assert_owned' in vars(owner)):
                return False
            if any(inspect.getattr_static(kind, name, None) is not None for name in ('produce', 'flush', 'poll', 'purge')):
                return False
            # No SDK/provider, custom propagator, custom active context, or
            # instrumented backend is admitted by merely having an empty env.
            tracer = self.events.tracer
            if trace._TRACER_PROVIDER is not None or type(tracer) is not trace.ProxyTracer:
                return False
            if (vars(tracer).get('_real_tracer') is not None or type(vars(tracer).get('_noop_tracer')) is not trace.NoOpTracer
                    or vars(vars(tracer)['_noop_tracer'])
                    or any(name in vars(tracer) for name in ('start_span', 'start_as_current_span', '_tracer'))):
                return False
            propagator = propagate._HTTP_TEXT_FORMAT
            if type(propagator) is not CompositePropagator or 'extract' in vars(propagator):
                return False
            children = vars(propagator).get('_propagators')
            if type(children) is not list or tuple(type(item) for item in children) != (TraceContextTextMapPropagator, W3CBaggagePropagator):
                return False
            if any(vars(item) for item in children):
                return False
            runtime = context._RUNTIME_CONTEXT
            from contextvars import ContextVar
            if (type(runtime) is not ContextVarsRuntimeContext or type(vars(runtime).get('_current_context')) is not ContextVar
                    or any(name in vars(runtime) for name in ('get_current', 'attach', 'detach'))):
                return False
            current = runtime._current_context.get()
            if type(current) is not Context or current:
                return False
            if self.events.OutboxEvent is not self.model or self.model._meta is not self.model_meta:
                return False
            signals = (pre_init, post_init, pre_save, post_save, connection_created)
            if any(item is not expected for item, expected in zip(signals, self.signals)):
                return False
            if any(vars(item).get('receivers') for item in self.signals):
                return False
            database = connections['default']
            from django.db.backends.postgresql.operations import DatabaseOperations
            if type(database.ops) is not DatabaseOperations or any(name in vars(database.ops)
                    for name in ('compiler', 'get_db_converters', 'quote_name', 'adapt_datetimefield_value', 'adapt_json_value')):
                return False
            dedicated = vars(owner).get('_connection')
            if dedicated is not None:
                from django.db.backends.postgresql.base import DatabaseWrapper
                state = vars(dedicated)
                if (type(dedicated) is not DatabaseWrapper or state.get('execute_wrappers')
                        or any(name in state for name in ('cursor', '_cursor', 'create_cursor', 'ensure_connection'))):
                    return False
            raw = vars(database).get('connection')
            if raw is not None:
                from psycopg import Connection
                if type(raw) is not Connection:
                    return False
                for mapping in (*raw.adapters._loaders, *raw.adapters._dumpers_by_oid, *raw.adapters._dumpers.values()):
                    if type(mapping) is not dict:
                        return False
                    for adapter in mapping.values():
                        if type(adapter) is not type and type(adapter) is not _BUDGET_ADAPTER_META:
                            return False
                        if adapter not in self.adapter_types:
                            if not _publisher_standard_tz_adapter(adapter, database.timezone):
                                return False
            effective_fields = self.model_meta.concrete_fields
            if len(effective_fields) != len(self.fields) or any(item is not expected for item, expected in zip(effective_fields, self.fields)):
                return False
            for field, name, descriptor in self.field_descriptors:
                if inspect.getattr_static(field, name, None) is not descriptor:
                    return False
            for field in self.fields:
                name = vars(field).get('name')
                if type(field) is not self.events._OUTBOX_CLAIM_FIELDS.get(name):
                    return False
                if name == 'payload_json' and (inspect.getattr_static(field, 'encoder', None) is not None
                        or inspect.getattr_static(field, 'decoder', None) is not None):
                    return False
            for name in ('__new__', '__getattribute__', '__setattr__'):
                if inspect.getattr_static(self.model, name) is not inspect.getattr_static(object, name):
                    return False
            for name in self.events._OUTBOX_CLAIM_MODEL_METHODS:
                if inspect.getattr_static(self.model, name) is not inspect.getattr_static(self.events.models.Model, name):
                    return False
            for manager in (self.model.objects, self.model._base_manager, self.model._default_manager):
                if (type(manager) is not Manager or manager.model is not self.model or manager._db is not None
                        or manager._hints or manager._queryset_class is not QuerySet):
                    return False
                if any(name in vars(manager) for name in ('get_queryset', 'filter', 'raw', 'get', 'select_for_update')):
                    return False
            return True
        except Exception:
            return False


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
