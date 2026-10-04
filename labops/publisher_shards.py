"""One publisher owner per shard, using a dedicated PostgreSQL session.

Use a direct PostgreSQL connection or session pooling, never transaction pooling.
Stop every publisher before changing shard count: count is part of the lock key
and UUID routing. This is database ownership, not broker-side producer fencing;
a session failure between the final check and Kafka send can still send stale
records. Event IDs and database deduplication remain the recovery boundary.

SQLite demo has a process-local guard for count=1 only. It does not support
multiple publisher processes or sharded publisher concurrency.
"""
import hashlib
import threading
from contextlib import contextmanager

from django.conf import settings
from django.core.management.base import CommandError
from django.db import connections, DatabaseError

_SQLITE_GUARD = threading.Lock()


class ShardAlreadyOwned(CommandError):
    pass


class ShardOwnershipLost(CommandError):
    pass


def shard_lock_key(index, count):
    if type(index) is not int or type(count) is not int or count < 1 or not 0 <= index < count:
        raise CommandError('Shard index must be within a positive shard count')
    value = f'labops.publisher.shard.v1:{count}:{index}'.encode('ascii')
    return int.from_bytes(hashlib.sha256(value).digest()[:8], 'big', signed=True)


class PublisherShardOwner:
    def __init__(self, index, count):
        self.index = index
        self.count = count
        self.key = shard_lock_key(index, count)
        self.backend_pid = None
        self._connection = None
        self._raw_connection = None
        self._sqlite_owned = False
        self._lost = False

    def acquire(self):
        base = connections['default']
        if base.vendor == 'sqlite':
            if self.count != 1:
                raise CommandError('SQLite demo supports one publisher process and shard count 1 only')
            if not _SQLITE_GUARD.acquire(blocking=False):
                raise ShardAlreadyOwned('SQLite demo publisher is already active in this process')
            self._sqlite_owned = True
            return self
        if base.vendor != 'postgresql':
            raise CommandError('Publisher shard ownership requires PostgreSQL')
        # A copied wrapper is deliberately not registered as an application
        # connection. Application reconnects/atomic blocks cannot release it.
        self._connection = base.copy(alias='publisher_shard_owner')
        self._connection.settings_dict['OPTIONS'] = dict(self._connection.settings_dict.get('OPTIONS', {}))
        # A pooled wrapper could return a backend still holding this session
        # lock. This ownership connection must always be physically closed.
        self._connection.settings_dict['OPTIONS'].pop('pool', None)
        self._connection.settings_dict['AUTOCOMMIT'] = True
        self._connection.settings_dict['CONN_HEALTH_CHECKS'] = False
        self._connection.settings_dict['CONN_MAX_AGE'] = None
        try:
            self._connection.ensure_connection()
            self._raw_connection = self._connection.connection
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT set_config('statement_timeout', %s, false)",
                               [str(int(settings.EVENT_PUBLISH_DB_BUDGET_SECONDS * 1000))])
                cursor.execute('SELECT pg_backend_pid(), pg_try_advisory_lock(%s)', [self.key])
                self.backend_pid, acquired = cursor.fetchone()
            if not acquired:
                raise ShardAlreadyOwned(f'Publisher shard {self.index}/{self.count} is already owned')
            self.assert_owned()
        except (DatabaseError, OSError):
            self.close()
            raise ShardOwnershipLost('Unable to establish publisher shard ownership') from None
        except BaseException:
            self.close()
            raise
        return self

    def assert_owned(self):
        if self._lost:
            raise ShardOwnershipLost('Publisher shard session was lost; restart is required')
        if self._sqlite_owned:
            return
        owner = self._connection
        if owner is None or owner.connection is not self._raw_connection or self._raw_connection is None or self._raw_connection.closed:
            self._lost = True
            raise ShardOwnershipLost('Publisher shard session was lost; restart is required')
        unsigned = self.key & ((1 << 64) - 1)
        try:
            # Backend identity and pg_locks are checked together, and never
            # acquire a lock or silently reconnect on this validation path.
            with owner.cursor() as cursor:
                cursor.execute('''SELECT pg_backend_pid(), EXISTS (
                    SELECT 1 FROM pg_locks WHERE locktype = 'advisory'
                    AND pid = pg_backend_pid() AND granted AND objsubid = 1 AND mode = 'ExclusiveLock'
                    AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                    AND classid::bigint = %s AND objid::bigint = %s
                )''', [unsigned >> 32, unsigned & ((1 << 32) - 1)])
                backend_pid, owned = cursor.fetchone()
        except (DatabaseError, OSError):
            self._lost = True
            raise ShardOwnershipLost('Publisher shard session was lost; restart is required') from None
        if backend_pid != self.backend_pid or not owned or owner.connection is not self._raw_connection:
            self._lost = True
            raise ShardOwnershipLost('Publisher shard ownership was lost; restart is required')

    def close(self):
        if self._sqlite_owned:
            self._sqlite_owned = False
            _SQLITE_GUARD.release()
        owner = self._connection
        if owner is not None:
            # Closing the dedicated backend releases its session advisory lock.
            # No reconnect/unlock query is attempted after a failed ownership check.
            try:
                owner.close()
            except (DatabaseError, OSError):
                pass
            finally:
                self._connection = None
                self._raw_connection = None
        self._lost = True


@contextmanager
def publisher_shard_owner(index, count):
    owner = PublisherShardOwner(index, count).acquire()
    try:
        yield owner
    finally:
        owner.close()
