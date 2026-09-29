import logging
from types import FunctionType as _ObservationFunctionType
from labops.publisher_observation import current as _observation_current
from labops.worker_metrics import OperationDeadlineExceeded as _ObservationDeadline
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections
from labops.events import producer, publish_one
from labops.publisher_shards import publisher_shard_owner, ShardOwnershipLost
from labops.worker_metrics import (StopController, start_worker_metrics, stop_worker_metrics, heartbeat,
                                   operation_deadline, database_statement_budget, PublisherBatchBudget, PublisherBudgetAdmission)


def _budget_aliases():
    return (producer, publish_one, operation_deadline, database_statement_budget, publisher_shard_owner)


try:
    _BUDGET_ADMISSION = PublisherBudgetAdmission(_budget_aliases())
except Exception:
    # Unknown framework capabilities cannot stop an otherwise valid command.
    _BUDGET_ADMISSION = None


class Command(BaseCommand):
    help = 'Publish inventory outbox with one PostgreSQL owner per shard. Stop all publishers before changing shard count.'

    def add_arguments(self, parser):
        parser.add_argument('--loop', action='store_true')
        parser.add_argument('--limit', type=int, default=100)
        parser.add_argument('--metrics-port', type=int, default=None)
        parser.add_argument('--shard-index', type=int, default=settings.EVENT_PUBLISHER_SHARD_INDEX)
        parser.add_argument('--shard-count', type=int, default=settings.EVENT_PUBLISHER_SHARD_COUNT)

    def handle(self, *args, _get_observer=_observation_current, _observer_identity=_observation_current,
            _observer_code=_observation_current.__code__, _observer_defaults=_observation_current.__defaults__,
            _observer_globals=_observation_current.__globals__, _observer_type=type,
            _observer_function=_ObservationFunctionType, _observer_dict=dict,
            _observer_deadline=_ObservationDeadline, **options):
        if options['limit'] < 1 or options['shard_count'] < 1 or not 0 <= options['shard_index'] < options['shard_count']:
            raise CommandError('Limit/shard count must be positive and shard index within shard count')
        broker = producer()
        metrics = None
        try:
            metrics = start_worker_metrics('publisher', port=options['metrics_port'])
            with StopController() as stop, publisher_shard_owner(options['shard_index'], options['shard_count']) as owner:
                owner_lost = False
                if (_BUDGET_ADMISSION is None or any(value is not expected for value, expected in
                        zip(_budget_aliases(), _BUDGET_ADMISSION.aliases))):
                    try:
                        logging.getLogger('labops').info('publisher_budget_reuse_unsupported schema=1 reason=command_extension')
                    except Exception:
                        pass
                try:
                    while not stop.stopped:
                        heartbeat('publisher')
                        batch_complete = False
                        def admission():
                            observer = None
                            try:
                                if (_observer_type(_get_observer) is _observer_function
                                        and _get_observer is _observer_identity
                                        and _get_observer.__code__ is _observer_code
                                        and _get_observer.__defaults__ is _observer_defaults
                                        and _get_observer.__globals__ is _observer_globals
                                        and _get_observer.__kwdefaults__ is None
                                        and _observer_type(_get_observer.__dict__) is _observer_dict
                                        and not _get_observer.__dict__):
                                    observer = _get_observer(_get_observer)
                            except _observer_deadline:
                                raise
                            except Exception:
                                observer = None
                            if observer is None:
                                return _BUDGET_ADMISSION is not None and _BUDGET_ADMISSION.plain(broker, owner, _budget_aliases(), stop)
                            with observer.measure('admission_call'):
                                result = _BUDGET_ADMISSION is not None and _BUDGET_ADMISSION.plain(broker, owner, _budget_aliases(), stop)
                            try:
                                observer.guard_result(_BUDGET_ADMISSION, result)
                            except _observer_deadline:
                                raise
                            except Exception:
                                observer.recording_failed = True
                            return result
                        batch = PublisherBatchBudget(settings.EVENT_PUBLISH_DB_BUDGET_SECONDS, options['limit'], admission=admission)
                        batch_error = None
                        batch_outcome = 'partial'
                        try:
                            try:
                                for index in range(options['limit']):
                                    if stop.stopped:
                                        break
                                    budget = settings.KAFKA_PRODUCER_QUEUE_WAIT_SECONDS + settings.KAFKA_PUBLISH_FLUSH_SECONDS + settings.EVENT_PUBLISH_DB_BUDGET_SECONDS
                                    observer = None
                                    try:
                                        if (_observer_type(_get_observer) is _observer_function
                                                and _get_observer is _observer_identity
                                                and _get_observer.__code__ is _observer_code
                                                and _get_observer.__defaults__ is _observer_defaults
                                                and _get_observer.__globals__ is _observer_globals
                                                and _get_observer.__kwdefaults__ is None
                                                and _observer_type(_get_observer.__dict__) is _observer_dict
                                                and not _get_observer.__dict__):
                                            observer = _get_observer(_get_observer)
                                    except _observer_deadline:
                                        raise
                                    except Exception:
                                        observer = None
                                    token = None
                                    if observer is not None:
                                        try:
                                            token = observer.begin_attempt()
                                        except _observer_deadline:
                                            raise
                                        except Exception:
                                            observer.recording_failed = True
                                    primary = None
                                    try:
                                        batch.before_deadline()
                                        with operation_deadline(min(budget, stop.remaining())):
                                            record_error = None
                                            try:
                                                # Every record keeps the dedicated owner
                                                # check before application session access.
                                                owner.assert_owned()
                                                with batch.record(database_statement_budget, settings.EVENT_PUBLISH_DB_BUDGET_SECONDS) as record:
                                                    published = publish_one(broker, shard_index=options['shard_index'], shard_count=options['shard_count'],
                                                                            ownership_check=owner.assert_owned)
                                                    if observer is not None:
                                                        try:
                                                            observer.publish_result(published)
                                                        except _observer_deadline:
                                                            raise
                                                        except Exception:
                                                            observer.recording_failed = True
                                                    record.complete(published)
                                                    record.retain(published is True and index + 1 < options['limit'] and not stop.stopped)
                                            except BaseException as error:
                                                record_error = error
                                                raise
                                            finally:
                                                if record_error is not None:
                                                    batch.finish(record_error)
                                    except BaseException as error:
                                        primary = error
                                        raise
                                    finally:
                                        # No temporary observation hook remains at record/session rechecks.
                                        try:
                                            observer = None
                                            try:
                                                if (_observer_type(_get_observer) is _observer_function
                                                        and _get_observer is _observer_identity
                                                        and _get_observer.__code__ is _observer_code
                                                        and _get_observer.__defaults__ is _observer_defaults
                                                        and _get_observer.__globals__ is _observer_globals
                                                        and _get_observer.__kwdefaults__ is None
                                                        and _observer_type(_get_observer.__dict__) is _observer_dict
                                                        and not _get_observer.__dict__):
                                                    observer = _get_observer(_get_observer)
                                            except _observer_deadline:
                                                raise
                                            except Exception:
                                                observer = None
                                        except BaseException as cleanup_error:
                                            observer = None
                                            if primary is None:
                                                raise
                                        if observer is not None:
                                            try:
                                                observer.end_attempt(token, primary)
                                            except _observer_deadline:
                                                if primary is None:
                                                    raise
                                                observer.recording_failed = True
                                            except Exception:
                                                observer.recording_failed = True
                                            except BaseException:
                                                if primary is None:
                                                    raise
                                    if not published:
                                        batch_outcome = 'empty'
                                        break
                                else:
                                    batch_complete = True
                                    batch_outcome = 'full'
                            except BaseException as error:
                                batch_error = error
                                batch_outcome = 'failed'
                                raise
                            finally:
                                # Covers a stop between record deadlines as well as
                                # an owner failure before the next record entered.
                                cleanup_error = batch_error
                                try:
                                    batch.cleanup_gap(stop.remaining(), batch_error)
                                except BaseException as error:
                                    cleanup_error = batch_error if batch_error is not None else error
                                    raise
                                finally:
                                    batch.receipt('failed' if cleanup_error is not None else (
                                        'stopped' if stop.stopped else batch_outcome), cleanup_error)
                        except ShardOwnershipLost:
                            # A loop never silently reacquires after owner loss.
                            owner_lost = True
                            raise
                        except DatabaseError:
                            batch_complete = False
                            # Only the application connection is replaceable.
                            # The independently owned shard session is never reacquired.
                            connections['default'].close()
                            logging.getLogger('labops').exception('publisher_database_failed')
                            if not options['loop']:
                                raise
                        except Exception:
                            batch_complete = False
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
