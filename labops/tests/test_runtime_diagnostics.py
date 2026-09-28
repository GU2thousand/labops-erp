"""Pure diagnostic contracts: actual clocks/SQL fixtures, no live databases."""
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from benchmarks.events.runtime_diagnostics import (
    CommandDiagnostics, RuntimeDiagnostics, PostgreSQLSampler,
    counter_delta, diagnostics_profile, sanitize_resources,
)
from labops.tests.test_container_diagnostics import LinuxFixture, CGROUP_PATH, SERVICE
from labops.tests.test_process_resources import ProcessFixture
from benchmarks.events.process_resources import GENERATOR_ROLES


class FakeConnection:
    def __init__(self):
        self.wrappers = []
        self.commit_error = None
        self.sql_error = None
        self.closed = False

    def commit(self):
        if self.commit_error is not None:
            raise self.commit_error
        return 'committed'

    @contextmanager
    def execute_wrapper(self, wrapper):
        self.wrappers.append(wrapper)
        try:
            yield
        finally:
            self.wrappers.remove(wrapper)

    def execute(self, sql, params=None):
        def execute(_sql, _params, _many, _context):
            if self.sql_error is not None:
                raise self.sql_error
            return 'executed'
        callback = execute
        for wrapper in reversed(self.wrappers):
            previous = callback
            callback = lambda query, values, many, context, wrap=wrapper, inner=previous: wrap(inner, query, values, many, context)
        return callback(sql, params, False, {})


class FakeDatabaseSampler:
    def __init__(self):
        self.calls = 0
        self.sampled = threading.Event()
        self.closed = False

    def snapshot(self):
        self.calls += 1
        self.sampled.set()
        return {'observed': True, 'settings': [], 'activities': [], 'locks': [],
            'database': {'xact_commit': self.calls}, 'wal': {'wal_sync': self.calls}, 'errors': []}

    def close(self):
        self.closed = True


def process_reader():
    return {'user_cpu_seconds': 2.0, 'system_cpu_seconds': 1.0, 'rss_bytes': 1000,
            'io': None, 'threads': None, 'errors': []}


class CommandDiagnosticTests(unittest.TestCase):
    def test_real_execute_and_physical_commit_are_counted_without_text_or_parameters(self):
        connection = FakeConnection()
        with CommandDiagnostics(connection) as observation:
            self.assertEqual(connection.execute('SELECT TOP_SECRET', ['password']), 'executed')
            connection.execute(' update PRIVATE_TABLE set secret=%s', ['credential'])
            connection.execute('ALTER role PRIVATE_PASSWORD', [])
            self.assertEqual(connection.commit(), 'committed')
        record = observation.summary()
        self.assertTrue(record['observed'])
        self.assertTrue(record['completed'])
        self.assertEqual(record['sql']['operation_counts'], {'SELECT': 1, 'UPDATE': 1, 'OTHER': 1})
        self.assertEqual(record['physical_commit']['attempts'], 1)
        self.assertGreaterEqual(record['physical_commit']['wall_seconds'], 0)
        self.assertGreaterEqual(record['thread_cpu_seconds'], 0)
        self.assertNotIn('commit', connection.__dict__)
        self.assertEqual(connection.wrappers, [])
        text = json.dumps(record)
        for secret in ('TOP_SECRET', 'PRIVATE_TABLE', 'PRIVATE_PASSWORD', 'password', 'credential'):
            self.assertNotIn(secret, text)

    def test_preexisting_instance_commit_override_is_restored_exactly(self):
        connection = FakeConnection()
        original = lambda: 'custom'
        connection.commit = original
        with CommandDiagnostics(connection) as observation:
            self.assertEqual(connection.commit(), 'custom')
        self.assertIs(connection.commit, original)
        self.assertEqual(observation.summary()['physical_commit']['attempts'], 1)

    def test_physical_commit_failure_preserves_the_original_exception_and_restores_instance(self):
        connection = FakeConnection()
        error = RuntimeError('PRIVATE commit failure')
        connection.commit_error = error
        with self.assertRaises(RuntimeError) as caught:
            with CommandDiagnostics(connection) as observation:
                connection.commit()
        self.assertIs(caught.exception, error)
        self.assertNotIn('commit', connection.__dict__)
        self.assertEqual(observation.summary()['physical_commit']['error_type_counts'], {'RuntimeError': 1})
        self.assertNotIn('PRIVATE', json.dumps(observation.summary()))

    def test_commit_clock_interruption_never_replaces_original_commit_failure(self):
        connection = FakeConnection()
        error = RuntimeError('PRIVATE primary')
        connection.commit_error = error
        calls = 0
        def clock():
            nonlocal calls
            calls += 1
            if calls >= 3:
                raise KeyboardInterrupt('PRIVATE secondary')
            return float(calls)
        with self.assertRaises(RuntimeError) as caught:
            with CommandDiagnostics(connection, monotonic=clock) as observation:
                connection.commit()
        self.assertIs(caught.exception, error)
        self.assertNotIn('commit', connection.__dict__)
        self.assertTrue(observation.summary()['diagnostic_errors'])
        self.assertNotIn('PRIVATE', json.dumps(observation.summary()))

    def test_sql_failure_is_retained_even_when_exit_cpu_clock_is_interrupted(self):
        connection = FakeConnection()
        original = ValueError('PRIVATE SQL parameters')
        connection.sql_error = original
        calls = 0
        def thread_clock():
            nonlocal calls
            calls += 1
            if calls >= 2:
                raise KeyboardInterrupt('PRIVATE timer')
            return 1.0
        with self.assertRaises(ValueError) as caught:
            with CommandDiagnostics(connection, thread_clock=thread_clock) as observation:
                connection.execute('SELECT private', ['password'])
        self.assertIs(caught.exception, original)
        self.assertEqual(observation.summary()['sql']['error_type_counts'], {'ValueError': 1})
        self.assertIsNone(observation.summary()['thread_cpu_seconds'])
        self.assertNotIn('PRIVATE', json.dumps(observation.summary()))

    def test_two_owning_threads_instrument_only_their_respective_connection(self):
        connections = [FakeConnection(), FakeConnection()]
        barrier = threading.Barrier(2)
        records = []
        def worker(connection):
            with CommandDiagnostics(connection) as observation:
                barrier.wait(timeout=2)
                connection.execute('SELECT 1')
                connection.commit()
            records.append(observation.summary())
        threads = [threading.Thread(target=worker, args=(connection,)) for connection in connections]
        for thread in threads: thread.start()
        for thread in threads: thread.join(timeout=2)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(len(records), 2)
        self.assertTrue(all(record['sql']['count'] == record['physical_commit']['attempts'] == 1 for record in records))
        self.assertTrue(all('commit' not in connection.__dict__ for connection in connections))

    def test_wrapper_factory_failure_restores_commit_and_retains_original_error(self):
        original = RuntimeError('PRIVATE factory')
        class FailedFactory(FakeConnection):
            def execute_wrapper(self, wrapper):
                raise original
        connection = FailedFactory()
        observation = CommandDiagnostics(connection)
        with self.assertRaises(RuntimeError) as caught:
            observation.__enter__()
        self.assertIs(caught.exception, original)
        self.assertNotIn('commit', connection.__dict__)
        self.assertFalse(observation.summary()['collection_complete'])

    def test_factory_error_survives_secondary_commit_restore_interruption(self):
        original = RuntimeError('PRIVATE factory')
        class FailedFactoryAndRestore(FakeConnection):
            def execute_wrapper(self, wrapper):
                raise original
            def __delattr__(self, name):
                if name == 'commit':
                    raise KeyboardInterrupt('PRIVATE cleanup')
                super().__delattr__(name)
        observation = CommandDiagnostics(FailedFactoryAndRestore())
        with self.assertRaises(RuntimeError) as caught:
            observation.__enter__()
        self.assertIs(caught.exception, original)
        self.assertEqual(observation.summary()['diagnostic_errors'], [
            {'stage': 'commit_restore', 'error_type': 'KeyboardInterrupt'}])

    def test_commit_setter_interruption_after_assignment_restores_instance(self):
        original = KeyboardInterrupt('PRIVATE setter')
        class InterruptedSetter(FakeConnection):
            def __setattr__(self, name, value):
                super().__setattr__(name, value)
                if name == 'commit':
                    raise original
        connection = InterruptedSetter()
        observation = CommandDiagnostics(connection)
        with self.assertRaises(KeyboardInterrupt) as caught:
            observation.__enter__()
        self.assertIs(caught.exception, original)
        self.assertNotIn('commit', connection.__dict__)
        self.assertFalse(observation.summary()['collection_complete'])

    def test_nonfinite_sql_and_commit_clocks_are_unknown_and_incomplete(self):
        clocks = iter([0., 1., float('nan'), 3., float('inf'), 5.])
        connection = FakeConnection()
        with CommandDiagnostics(connection, monotonic=lambda: next(clocks)) as observation:
            connection.execute('SELECT PRIVATE', ['secret'])
            connection.commit()
        record = observation.summary()
        self.assertIsNone(record['sql']['wall_seconds'])
        self.assertIsNone(record['sql']['operation_wall_seconds']['SELECT'])
        self.assertIsNone(record['physical_commit']['wall_seconds'])
        self.assertFalse(record['collection_complete'])
        self.assertEqual(record['wall_seconds'], 5.)
        self.assertNotIn('PRIVATE', json.dumps(record, allow_nan=False))

    def test_new_exit_clock_interruption_is_rethrown_after_instance_restore(self):
        original = KeyboardInterrupt('PRIVATE timer')
        calls = 0
        def clock():
            nonlocal calls
            calls += 1
            if calls == 2:
                raise original
            return 1.
        connection = FakeConnection()
        with self.assertRaises(KeyboardInterrupt) as caught:
            with CommandDiagnostics(connection, thread_clock=clock) as observation:
                pass
        self.assertIs(caught.exception, original)
        self.assertNotIn('commit', connection.__dict__)
        self.assertTrue(observation.summary()['completed'])
        self.assertFalse(observation.summary()['collection_complete'])


class DiagnosticContractTests(unittest.TestCase):
    def test_missing_reset_nonfinite_counters_are_unknown_instead_of_zero(self):
        result = counter_delta({'valid': 2, 'reset': 8, 'missing': 2},
            {'valid': 5, 'reset': 3, 'bad': float('inf')}, ('valid', 'reset', 'missing', 'bad'))
        self.assertEqual(result['values'], {'valid': 3, 'reset': None, 'missing': None, 'bad': None})
        self.assertEqual(result['reset_counters'], ['reset'])

    def test_resource_sanitizer_keeps_cgroup_units_and_drops_identity_and_credentials(self):
        fixture = LinuxFixture()
        data = fixture.discover().snapshot()
        data['containers'][SERVICE]['credential'] = 'TOP_SECRET'
        data['containers']['SECRET_PROJECT'] = {'cpu_total_seconds': 1, 'memory_usage_bytes': 1}
        record = sanitize_resources(data)
        row = record['containers'][0]
        self.assertEqual(row['cpu_total_seconds'], .0009)
        self.assertEqual(row['memory_usage_bytes'], 65536)
        self.assertEqual(row['io_read_bytes'], 4096)
        self.assertEqual(row['cpu_throttled_periods'], 2)
        self.assertNotIn('TOP_SECRET', json.dumps(record))
        self.assertNotIn('SECRET_PROJECT', json.dumps(record))
        self.assertNotIn('identity', row)
        self.assertNotIn('path', row)

    def test_invalid_resource_snapshot_and_nonfinite_values_stay_unknown(self):
        self.assertFalse(sanitize_resources({'password': 'TOP_SECRET'})['observed'])
        row = sanitize_resources({'containers': [{'role': 'postgres', 'status': 'available',
            'cpu_total_seconds': float('nan'), 'memory_usage_bytes': -1}]})['containers'][0]
        self.assertIsNone(row['cpu_total_seconds'])
        self.assertIsNone(row['memory_usage_bytes'])

    def test_profile_freezes_required_and_optional_measurements(self):
        profile = diagnostics_profile()
        self.assertEqual(profile['interval_seconds'], 1)
        self.assertEqual(profile['statement_timeout_ms'], 250)
        self.assertEqual(profile['max_samples'], 7200)
        self.assertIn('running_container_cpu_total_seconds', profile['required_counters'])
        self.assertIn('container_io', profile['optional_counters'])

    def test_postgresql_settings_and_activity_do_not_echo_arbitrary_names(self):
        sampler = PostgreSQLSampler('default')
        sampler.connection = object()
        def query(sql):
            if 'pg_settings' in sql:
                return [{'name': 'max_connections', 'setting': '100', 'unit': None},
                    {'name': 'PRIVATE_SETTING', 'setting': 'TOP_SECRET', 'unit': None}]
            if 'pg_stat_activity WHERE datname' in sql:
                return [{'pid': 12, 'backend_type': 'client backend', 'state': 'active',
                    'wait_event_type': 'Lock', 'wait_event': 'transactionid',
                    'user_role': 'TOP_SECRET', 'application_role': 'TOP_SECRET', 'blocking_pids': [99]}]
            return []
        sampler._query = query
        result = sampler.snapshot()
        self.assertEqual(result['activities'][0]['backend_pid'], 12)
        self.assertEqual(result['activities'][0]['blocking_pids'], [99])
        self.assertEqual(result['activities'][0]['application_role'], 'unknown')
        self.assertEqual(result['settings'], [{'name': 'max_connections', 'setting': 100, 'unit': None}])
        self.assertNotIn('TOP_SECRET', json.dumps(result))
        self.assertNotIn('PRIVATE_SETTING', json.dumps(result))


class RuntimeSamplerTests(unittest.TestCase):
    def test_dual_frozen_contract_passes_and_single_relabelling_is_incomplete(self):
        for selected in ('single', 'notification-dual'):
            fixture = ProcessFixture(required_workers=('notification', 'notification-1'), consumer_topology='notification-dual')
            fixture.start_all()
            with tempfile.TemporaryDirectory() as directory:
                diagnostics, database = self.start(directory, roles=(),
                    managed_process_sampler=fixture.catalog.sample, consumer_topology=selected)
                with diagnostics:
                    self.assertTrue(database.sampled.wait(timeout=2))
                    fixture.finish_generators()
                report = diagnostics.summary()
                self.assertEqual(report['collection_complete'], selected == 'notification-dual')
                self.assertEqual(report['managed_process_resources']['collection_complete'], selected == 'notification-dual')

    def start(self, directory, *, resources=None, database=None, roles=(SERVICE,), stopped=(), **kwargs):
        database = database or FakeDatabaseSampler()
        diagnostics = RuntimeDiagnostics(Path(directory)/'runtime.json', scenario='steady',
            resource_sampler=resources, expected_resource_roles=roles, known_stopped_roles=stopped,
            database_sampler=database, process_reader=process_reader, host_reader=lambda: {},
            facts_reader=lambda: {'logical_cpu_count': 4}, interval_seconds=.01, **kwargs)
        return diagnostics, database

    def test_scoped_process_callback_starts_before_children_and_requires_final_cleanup_receipts(self):
        fixture = ProcessFixture()
        planned, live = threading.Event(), threading.Event()
        def snapshot():
            value = fixture.catalog.sample()
            generators = [row for row in value['processes'] if row['role'] in GENERATOR_ROLES]
            if all(row['stage'] == 'planned' for row in generators): planned.set()
            if all(row['stage'] == 'running' for row in generators): live.set()
            return value
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, roles=(), managed_process_sampler=snapshot)
            with diagnostics:
                self.assertTrue(planned.wait(timeout=2))
                fixture.start_all()
                self.assertTrue(live.wait(timeout=2))
                fixture.finish_generators()
            report = json.loads((Path(directory)/'runtime.json').read_text())
            self.assertTrue(report['collection_complete'])
            self.assertTrue(report['lifecycle_complete'])
            self.assertTrue(report['managed_process_sampler_supplied'])
            self.assertEqual(report['managed_process_resources']['generator_complete_roles'], list(GENERATOR_ROLES))
            self.assertEqual(report['managed_process_resources']['worker_complete_roles'], ['publisher'])
            self.assertTrue(database.closed)
            final = report['final_managed_processes']['processes'][0]
            self.assertEqual(final['stage'], 'exited')
            self.assertIsNone(final['value'])
            self.assertEqual(final['final_snapshot']['source'], 'child_origin')

    def test_missing_live_process_counter_invalidates_good_prior_coverage(self):
        fixture = ProcessFixture()
        fixture.start_all()
        bad = threading.Event()
        calls = 0
        def snapshot():
            nonlocal calls
            calls += 1
            if calls == 2:
                fixture.files['/proc/1001/stat'] = PermissionError('PRIVATE missing live process')
            value = fixture.catalog.sample()
            if calls == 2:
                fixture.set('generator-0')
                bad.set()
            return value
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, _ = self.start(directory, roles=(), managed_process_sampler=snapshot)
            with diagnostics:
                self.assertTrue(bad.wait(timeout=2))
                fixture.finish_generators()
            record = diagnostics.summary()
            self.assertTrue(record['lifecycle_complete'])
            self.assertFalse(record['collection_complete'])
            self.assertFalse(record['managed_process_resources']['live_sample_coverage_complete'])
            self.assertIn('generator-0', record['managed_process_resources']['observed_live_roles'])
            self.assertNotIn('PRIVATE', json.dumps(record))

    def test_scoped_callback_failure_and_final_interruption_preserve_primary_business_exception(self):
        original = ValueError('PRIVATE business')
        fixture = ProcessFixture()
        fixture.start_all()
        database = FakeDatabaseSampler()
        def snapshot():
            if database.closed:
                raise KeyboardInterrupt('PRIVATE final callback')
            return fixture.catalog.sample()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, _ = self.start(directory, roles=(), database=database,
                managed_process_sampler=snapshot)
            with self.assertRaises(ValueError) as caught:
                with diagnostics:
                    self.assertTrue(database.sampled.wait(timeout=2))
                    raise original
            self.assertIs(caught.exception, original)
            self.assertTrue(database.closed)
            self.assertTrue(diagnostics.summary()['sampler_joined'])
            self.assertFalse(diagnostics.summary()['collection_complete'])
            report = json.loads((Path(directory)/'runtime.json').read_text())
            self.assertEqual(report['final_managed_processes']['errors'][0]['error_type'], 'KeyboardInterrupt')
            self.assertNotIn('PRIVATE', json.dumps(report))

    def test_optional_scoped_callback_none_preserves_existing_collection_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, roles=(), managed_process_sampler=None)
            with diagnostics:
                self.assertTrue(database.sampled.wait(timeout=2))
            report = diagnostics.summary()
            self.assertTrue(report['collection_complete'])
            self.assertFalse(report['managed_process_sampler_supplied'])
            self.assertIsNone(report['managed_process_resources'])
            self.assertIsNone(report['final_managed_processes'])

    def test_native_container_resources_to_runtime_complete_and_unknown_io_retained(self):
        fixture = LinuxFixture()
        fixture.files[CGROUP_PATH+'/io.stat'] = PermissionError('TOP_SECRET')
        resources = fixture.discover()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, resources=resources.snapshot)
            with diagnostics:
                self.assertTrue(database.sampled.wait(timeout=2))
            record = json.loads((Path(directory)/'runtime.json').read_text())
            self.assertTrue(record['collection_complete'])
            self.assertTrue(record['sampler_joined'])
            self.assertTrue(database.closed)
            self.assertEqual(record['observed_cpu_memory_roles'], [SERVICE])
            self.assertNotIn('io_read_bytes', record['samples'][0]['resources']['containers'][0])
            self.assertNotIn('TOP_SECRET', json.dumps(record))

    def test_inspected_main_pid_fallback_cannot_substitute_missing_container_cpu(self):
        fixture = LinuxFixture()
        fixture.files[CGROUP_PATH+'/cpu.stat'] = FileNotFoundError('TOP_SECRET')
        resources = fixture.discover()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, resources=resources.snapshot)
            with diagnostics:
                self.assertTrue(database.sampled.wait(timeout=2))
            self.assertFalse(diagnostics.summary()['collection_complete'])
            self.assertEqual(diagnostics.summary()['observed_cpu_memory_roles'], [])

    def test_later_required_cgroup_read_failure_invalidates_earlier_good_coverage(self):
        fixture = LinuxFixture()
        resources = fixture.discover()
        second = threading.Event()
        calls = 0
        def snapshot():
            nonlocal calls
            calls += 1
            if calls >= 2:
                fixture.files[CGROUP_PATH+'/cpu.stat'] = PermissionError('PRIVATE cpu')
                fixture.files[CGROUP_PATH+'/memory.current'] = PermissionError('PRIVATE memory')
            value = resources.snapshot()
            if calls >= 2:
                # An accidentally retained upstream scalar must not hide the
                # native per-file failures in this actual second sample.
                value['containers'][SERVICE]['cpu_total_seconds'] = 10.
                value['containers'][SERVICE]['memory_usage_bytes'] = 20
                second.set()
            return value
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, _ = self.start(directory, resources=snapshot)
            with diagnostics:
                self.assertTrue(second.wait(timeout=2))
            summary = diagnostics.summary()
            record = json.loads((Path(directory)/'runtime.json').read_text())
            self.assertTrue(summary['lifecycle_complete'])
            self.assertFalse(summary['collection_complete'])
            self.assertEqual(summary['observed_cpu_memory_roles'], [SERVICE])
            row = record['samples'][1]['resources']['containers'][0]
            self.assertIsNone(row['cpu_total_seconds'])
            self.assertIsNone(row['memory_usage_bytes'])
            self.assertEqual(row['required_counter_status']['cpu_stat_error_type'], 'PermissionError')
            self.assertEqual(row['required_counter_status']['memory_current_error_type'], 'PermissionError')
            self.assertNotIn('PRIVATE', json.dumps(record))

    def test_verified_stopped_container_has_explicit_unmeasured_coverage_without_zero(self):
        fixture = LinuxFixture()
        fixture.inspection['status'], fixture.inspection['pid'] = 'exited', 0
        resources = fixture.discover()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, resources=resources.snapshot, roles=(), stopped=(SERVICE,))
            with diagnostics:
                self.assertTrue(database.sampled.wait(timeout=2))
            record = json.loads((Path(directory)/'runtime.json').read_text())
            self.assertTrue(record['collection_complete'])
            self.assertEqual(record['observed_known_stopped_roles'], [SERVICE])
            self.assertNotIn('cpu_total_seconds', record['samples'][0]['resources']['containers'][0])

    def test_database_failure_records_unknown_and_does_not_copy_exception_message(self):
        class FailedDatabase(FakeDatabaseSampler):
            def snapshot(self):
                self.sampled.set()
                raise RuntimeError('postgres://private:password@PRIVATE_ENDPOINT')
        database = FailedDatabase()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, _ = self.start(directory, database=database)
            with diagnostics:
                self.assertTrue(database.sampled.wait(timeout=2))
            record = json.loads((Path(directory)/'runtime.json').read_text())
            self.assertFalse(record['collection_complete'])
            self.assertTrue(record['lifecycle_complete'])
            self.assertIsNone(record['samples'][0]['database']['value'])
            self.assertNotIn('PRIVATE_ENDPOINT', json.dumps(record))

    def test_thread_start_interruption_after_native_start_waits_for_own_close(self):
        native_thread = threading.Thread
        closing, release = threading.Event(), threading.Event()
        original = KeyboardInterrupt('PRIVATE start')
        class BlockedClose(FakeDatabaseSampler):
            def close(self):
                closing.set()
                self.closed = release.wait(timeout=2)
        class InterruptedStart(native_thread):
            def start(self):
                super().start()
                raise original
        database = BlockedClose()
        releaser = native_thread(target=lambda: (closing.wait(timeout=2), release.set()))
        releaser.start()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, _ = self.start(directory, database=database, roles=())
            with patch('benchmarks.events.runtime_diagnostics.threading.Thread', InterruptedStart):
                with self.assertRaises(KeyboardInterrupt) as caught:
                    diagnostics.__enter__()
            self.assertIs(caught.exception, original)
            self.assertTrue(database.closed)
            self.assertTrue(diagnostics._sampler_done.is_set())
            self.assertTrue(diagnostics.summary()['sampler_joined'])
            self.assertFalse(diagnostics.summary()['lifecycle_complete'])
            self.assertFalse((Path(directory)/'runtime.json').exists())
        releaser.join(timeout=2)

    def test_thread_start_failure_before_native_start_never_waits_for_done(self):
        original = RuntimeError('PRIVATE start')
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, roles=())
            with patch('benchmarks.events.runtime_diagnostics.threading.Thread.start', side_effect=original):
                with self.assertRaises(RuntimeError) as caught:
                    diagnostics.__enter__()
            self.assertIs(caught.exception, original)
            self.assertEqual(database.calls, 0)
            self.assertFalse(diagnostics._sampler_started.is_set())
            self.assertFalse(diagnostics.summary()['lifecycle_complete'])

    def test_interrupted_wait_cannot_finish_before_blocked_sample_and_close(self):
        sampling, release_sample = threading.Event(), threading.Event()
        closing, release_close = threading.Event(), threading.Event()
        interrupted_wait = threading.Event()
        original = KeyboardInterrupt('PRIVATE wait')
        class BlockedSampleAndClose(FakeDatabaseSampler):
            def snapshot(self):
                sampling.set()
                release_sample.wait(timeout=2)
                return super().snapshot()
            def close(self):
                closing.set()
                self.closed = release_close.wait(timeout=2)
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, database=BlockedSampleAndClose(), roles=())
            diagnostics.__enter__()
            self.assertTrue(sampling.wait(timeout=2))
            native_wait = diagnostics._sampler_done.wait
            def wait(*args, **kwargs):
                if not interrupted_wait.is_set():
                    interrupted_wait.set()
                    raise original
                return native_wait(*args, **kwargs)
            diagnostics._sampler_done.wait = wait
            def release():
                interrupted_wait.wait(timeout=2)
                release_sample.set()
                closing.wait(timeout=2)
                release_close.set()
            releaser = threading.Thread(target=release)
            releaser.start()
            with self.assertRaises(KeyboardInterrupt) as caught:
                diagnostics.__exit__(None, None, None)
            releaser.join(timeout=2)
            self.assertIs(caught.exception, original)
            self.assertTrue(database.closed)
            self.assertTrue(diagnostics._sampler_done.is_set())
            self.assertTrue(diagnostics.summary()['sampler_joined'])

    def test_persistence_failure_after_success_is_incomplete_and_rethrown(self):
        original = OSError('PRIVATE file')
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, roles=())
            diagnostics.__enter__()
            self.assertTrue(database.sampled.wait(timeout=2))
            with patch.object(Path, 'open', side_effect=original):
                with self.assertRaises(OSError) as caught:
                    diagnostics.__exit__(None, None, None)
            self.assertIs(caught.exception, original)
            self.assertTrue(database.closed)
            self.assertFalse(diagnostics.summary()['lifecycle_complete'])
            self.assertFalse(diagnostics.summary()['collection_complete'])

    def test_sampling_bound_is_finite_and_marks_incomplete_when_exhausted(self):
        database = FakeDatabaseSampler()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, _ = self.start(directory, database=database, roles=(), max_samples=1)
            with diagnostics:
                self.assertTrue(database.sampled.wait(timeout=2))
                self.assertTrue(diagnostics._sampler_done.wait(timeout=2))
            self.assertEqual(diagnostics.summary()['sample_count'], 1)
            self.assertTrue(diagnostics.summary()['samples_truncated'])
            self.assertFalse(diagnostics.summary()['collection_complete'])

    def test_cleanup_and_native_join_complete_before_interruption_rethrow(self):
        closing, release = threading.Event(), threading.Event()
        class BlockedClose(FakeDatabaseSampler):
            def close(self):
                closing.set()
                release.wait(timeout=2)
                self.closed = True
        database = BlockedClose()
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, _ = self.start(directory, database=database, roles=())
            diagnostics.__enter__()
            self.assertTrue(database.sampled.wait(timeout=2))
            original_join = diagnostics._thread.join
            interrupted = KeyboardInterrupt('PRIVATE interruption')
            def join(*args, **kwargs):
                if not getattr(join, 'called', False):
                    join.called = True
                    raise interrupted
                return original_join(*args, **kwargs)
            diagnostics._thread.join = join
            releaser = threading.Thread(target=lambda: (closing.wait(timeout=2), release.set()))
            releaser.start()
            with self.assertRaises(KeyboardInterrupt) as caught:
                diagnostics.__exit__(None, None, None)
            releaser.join(timeout=2)
            self.assertIs(caught.exception, interrupted)
            self.assertTrue(database.closed)
            self.assertTrue(diagnostics._sampler_done.is_set())
            self.assertTrue(diagnostics.summary()['sampler_joined'])

    def test_cleanup_read_and_persistence_interruptions_never_mask_business_exception(self):
        original = ValueError('PRIVATE business')
        class FailedClose(FakeDatabaseSampler):
            def close(self):
                raise KeyboardInterrupt('PRIVATE close')
        reads = 0
        def reader():
            nonlocal reads
            reads += 1
            if reads > 2:
                raise KeyboardInterrupt('PRIVATE final read')
            return process_reader()
        with tempfile.TemporaryDirectory() as directory:
            database = FailedClose()
            diagnostics, _ = self.start(directory, database=database, roles=())
            diagnostics.process_reader = reader
            with self.assertRaises(ValueError) as caught:
                with diagnostics:
                    self.assertTrue(database.sampled.wait(timeout=2))
                    raise original
            self.assertIs(caught.exception, original)
            self.assertFalse(diagnostics.summary()['collection_complete'])
            self.assertNotIn('PRIVATE', json.dumps(diagnostics.summary()))
        with tempfile.TemporaryDirectory() as directory:
            diagnostics, database = self.start(directory, roles=())
            with self.assertRaises(ValueError) as caught:
                with diagnostics:
                    self.assertTrue(database.sampled.wait(timeout=2))
                    with patch.object(Path, 'open', side_effect=KeyboardInterrupt('PRIVATE write')):
                        diagnostics.__exit__(ValueError, original, None)
                    raise original
            self.assertIs(caught.exception, original)
            self.assertFalse(diagnostics.summary()['collection_complete'])
            self.assertNotIn('PRIVATE', json.dumps(diagnostics.summary()))
