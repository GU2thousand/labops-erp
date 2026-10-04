from contextlib import nullcontext
from unittest import skipUnless
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.db import connection
from django.test import SimpleTestCase, TransactionTestCase, override_settings

from labops.publisher_shards import (publisher_shard_owner, shard_lock_key,
                                     ShardAlreadyOwned, ShardOwnershipLost)


@skipUnless(connection.vendor == 'postgresql', 'Real PostgreSQL session advisory locks')
class PublisherShardPostgreSQLTests(TransactionTestCase):
    def test_same_shard_owner_is_refused_different_indices_can_progress(self):
        with publisher_shard_owner(0, 2) as first:
            first.assert_owned()
            with self.assertRaises(ShardAlreadyOwned):
                with publisher_shard_owner(0, 2):
                    self.fail('A second publisher claimed the active shard')
            with publisher_shard_owner(1, 2) as second:
                second.assert_owned()
                self.assertNotEqual(first.backend_pid, second.backend_pid)
            first.assert_owned()
        with publisher_shard_owner(0, 2) as replacement:
            replacement.assert_owned()

    def test_backend_termination_loses_owner_without_reacquiring(self):
        with publisher_shard_owner(0, 1) as owner:
            backend_pid = owner.backend_pid
            with connection.cursor() as cursor:
                cursor.execute('SELECT pg_terminate_backend(%s)', [backend_pid])
                self.assertTrue(cursor.fetchone()[0])
            with self.assertRaises(ShardOwnershipLost):
                owner.assert_owned()
            with publisher_shard_owner(0, 1) as replacement:
                replacement.assert_owned()
                self.assertNotEqual(replacement.backend_pid, backend_pid)
                with self.assertRaises(ShardOwnershipLost):
                    owner.assert_owned()
                self.assertEqual(owner.backend_pid, backend_pid)

    def test_manual_lock_release_is_detected_without_reacquiring(self):
        with publisher_shard_owner(0, 1) as owner:
            with owner._connection.cursor() as cursor:
                cursor.execute('SELECT pg_advisory_unlock(%s)', [owner.key])
                self.assertTrue(cursor.fetchone()[0])
            with self.assertRaises(ShardOwnershipLost):
                owner.assert_owned()
            with self.assertRaises(ShardOwnershipLost):
                owner.assert_owned()

    def test_application_connection_close_does_not_release_dedicated_owner(self):
        with publisher_shard_owner(0, 1) as owner:
            connection.close()
            owner.assert_owned()
            with self.assertRaises(ShardAlreadyOwned):
                with publisher_shard_owner(0, 1):
                    self.fail('Closing an application connection released shard ownership')


@override_settings(WORKER_METRICS_ENABLED=False, KAFKA_REQUIRE_SECURITY=False,
                   KAFKA_SECURITY_PROTOCOL='PLAINTEXT')
class PublisherShardCommandTests(SimpleTestCase):
    def test_stable_keys_include_index_and_count(self):
        self.assertEqual(shard_lock_key(0, 2), shard_lock_key(0, 2))
        self.assertNotEqual(shard_lock_key(0, 2), shard_lock_key(1, 2))
        self.assertNotEqual(shard_lock_key(0, 1), shard_lock_key(0, 2))

    def test_loop_exits_on_lost_owner_without_publishing(self):
        owner = MagicMock(spec=['assert_owned'])
        owner.assert_owned.side_effect = ShardOwnershipLost('Session lost')
        broker = MagicMock()
        broker.flush.return_value = 0
        with patch('labops.management.commands.publish_events.producer', return_value=broker), \
             patch('labops.management.commands.publish_events.publisher_shard_owner', return_value=nullcontext(owner)) as ownership, \
             patch('labops.management.commands.publish_events.database_statement_budget', return_value=nullcontext()), \
             patch('labops.management.commands.publish_events.publish_one') as publish:
            with self.assertRaises(ShardOwnershipLost):
                call_command('publish_events', loop=True, limit=2)
        ownership.assert_called_once_with(0, 1)
        owner.assert_owned.assert_called_once()
        publish.assert_not_called()
        broker.flush.assert_not_called()
        broker.purge.assert_called_once_with(in_queue=True, in_flight=True, blocking=False)


@skipUnless(connection.vendor == 'sqlite', 'SQLite single-process demo guard')
class PublisherShardSQLiteTests(SimpleTestCase):
    def test_single_process_guard_and_sharding_rejection(self):
        with publisher_shard_owner(0, 1) as owner:
            owner.assert_owned()
            with self.assertRaises(ShardAlreadyOwned):
                with publisher_shard_owner(0, 1):
                    self.fail('Second in-process SQLite publisher was allowed')
        with publisher_shard_owner(0, 1) as owner:
            owner.assert_owned()
        with self.assertRaisesMessage(Exception, 'shard count 1 only'):
            with publisher_shard_owner(0, 2):
                self.fail('SQLite publisher sharding was allowed')
