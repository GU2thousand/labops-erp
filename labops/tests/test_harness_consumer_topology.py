"""Owned topology protocol controls; no real processes, Kafka or database."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.test import SimpleTestCase
from benchmarks.events import acceptance
from benchmarks.events.acceptance import Harness, main
from benchmarks.events.consumer_topology import topology_profile, freeze_topology, consumer_roles
from benchmarks.events.recovery_matrix import _RecoveryMatrix
from benchmarks.events.workers import validation_worker_identity


class Process:
    def __init__(self, pid):
        self.pid, self.returncode, self.signals, self.kills = pid, None, [], 0

    def poll(self):
        return self.returncode

    def send_signal(self, value):
        self.signals.append(value)

    def wait(self, timeout):
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def kill(self):
        self.kills += 1
        self.returncode = -9


def process_snapshot(pid):
    return {'source': 'child_origin', 'status': 'available', 'pid': pid,
        'start_time_ticks': 42 + pid, 'user_cpu_seconds': 1., 'system_cpu_seconds': .5,
        'rss_bytes': 4096, 'captured_monotonic': 100.}


class ConsumerPoolTests(SimpleTestCase):
    def harness(self, preset='notification-dual'):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        h = object.__new__(Harness)
        h.evidence = Path(directory.name)
        (h.evidence / 'logs').mkdir()
        h.args = SimpleNamespace(run_id='topology-control', generated_dir=h.evidence, fault_repetitions=1)
        h.consumer_topology, h.topology = preset, topology_profile(preset)
        h.workers, h.children, h.logs, h.shutdowns, h.cases = {}, [], [], [], []
        h.child_groups, h.child_metrics, h.child_identities, h.worker_closures = {}, {}, {}, {}
        h.cleanup_errors, h.supervisor_restarts = [], []
        h.connections = SimpleNamespace(close_all=Mock())
        h.consumer_group = lambda name: 'control.' + name + '.v1'

        def spawn(label, argv, role, **kwargs):
            process = Process(100 + len(h.children))
            h.children.append(process)
            identity = {'role': label, 'logical_consumer': role, 'pid': process.pid,
                'generation': len(h.children) - 1, 'client_id': 'acceptance-' + str(process.pid),
                'start_time_ticks': 42 + process.pid, 'process_identity_status': 'available',
                'identity_receipt': label + '-' + str(process.pid), 'application_name': 'control-' + str(process.pid)}
            h.child_identities[process.pid] = identity
            h.child_groups[process.pid] = h.consumer_group(role)
            h.child_metrics[process.pid] = 21000 + len(h.children)
            base = {'role': label, 'pid': process.pid, 'expected_application_name': identity['application_name'],
                    'process_snapshot': process_snapshot(process.pid)}
            started = {**base, 'status': 'ready', 'autocommit': True, 'in_atomic_block': False,
                'backend_identity': {'backend_pid': 500 + process.pid, 'database_name': 'labops_events',
                                     'application_name': identity['application_name']}}
            closed = {**base, 'status': 'closed', 'cleanup_complete': True, 'connection_closed': True, 'errors': []}
            for suffix, value in (('.started', started), ('.closed', closed)):
                (h.evidence / 'logs' / (identity['identity_receipt'] + suffix)).write_text(json.dumps(value))
            return process
        h.spawn = Mock(side_effect=spawn)
        h.wait_group_assignment = Mock(side_effect=lambda name, **kwargs:
            {'group': h.consumer_group(name), 'client_ids': list(kwargs['client_ids']),
             'member_count': len(kwargs['client_ids']), 'members': []})
        return h

    @contextmanager
    def proc(self):
        def observe(pid):
            return {**process_snapshot(pid), 'source': 'kernel_proc_stat'}
        with patch('benchmarks.events.process_resources.registered_process_snapshot', side_effect=observe):
            yield

    def test_default_and_dual_start_exact_pools_and_owned_assignment_ids(self):
        for preset, count in (('single', 1), ('notification-dual', 2)):
            h = self.harness(preset)
            with self.proc():
                result = h.restore_consumer_pool('notification')
            self.assertEqual(set(h.workers), set(consumer_roles('notification', preset)))
            self.assertEqual(result['member_count'], count)
            self.assertEqual(result['client_ids'], ['acceptance-' + str(h.workers[label].pid)
                for label in h.pool_roles('notification')])
            self.assertEqual(len({h.child_metrics[p.pid] for p in h.workers.values()}), count)
            observations = [call.args[1][call.args[1].index('--observations') + 1] for call in h.spawn.call_args_list]
            self.assertEqual(len(set(observations)), count)

    def test_second_slot_start_failure_retains_first_owned_child_and_cleanup(self):
        h = self.harness()
        spawn = h.spawn.side_effect
        original = RuntimeError('second startup')
        def partial(label, *args, **kwargs):
            if label == 'notification-1':
                raise original
            return spawn(label, *args, **kwargs)
        h.spawn.side_effect = partial
        with self.assertRaises(RuntimeError) as raised:
            h.start_consumer_pool('notification')
        self.assertIs(raised.exception, original)
        self.assertEqual(set(h.workers), {'notification'})
        self.assertIsNone(h.cleanup_workers())
        self.assertTrue(all(p.poll() is not None for p in h.children))

    def test_historical_pid_reuse_reaps_new_child_without_inheriting_old_close(self):
        h = self.harness()
        h.start_consumer('notification')
        old = h.workers['notification']
        h.stop('notification')
        old_identity = copy.deepcopy(h.child_identities[old.pid])
        old_closure = copy.deepcopy(h.worker_closures[old.pid])
        old_metrics, old_groups = copy.deepcopy(h.child_metrics), copy.deepcopy(h.child_groups)
        new = Process(old.pid)
        h.env, h.secrets = {'KAFKA_GROUP_PREFIX': 'control'}, {'notification': 'fixture'}
        with patch('benchmarks.events.acceptance.subprocess.Popen', return_value=new), \
             patch('benchmarks.events.process_resources.registered_process_snapshot') as snapshot, \
             self.assertRaises(AssertionError):
            Harness.spawn(h, 'notification-1', [str(acceptance.HERE / 'workers.py'),
                '--consumer', 'notification'], 'notification')
        snapshot.assert_not_called()
        self.assertIs(h.children[-1], new)
        self.assertEqual(new.returncode, -9)
        self.assertEqual(h.child_identities[old.pid], old_identity)
        self.assertEqual(h.worker_closures[old.pid], old_closure)
        self.assertEqual(h.child_metrics, old_metrics)
        self.assertEqual(h.child_groups, old_groups)
        self.assertIsNone(h.cleanup_workers())
        rejection = h.rejected_child_generations[id(new)]
        self.assertNotEqual(rejection['identity_receipt'], old_identity['identity_receipt'])
        self.assertFalse(rejection['close_outcome']['owning_close_receipt_complete'])
        self.assertTrue(rejection['close_outcome']['historical_pid_reuse_rejected'])
        observed = []
        @contextmanager
        def cursor():
            yield SimpleNamespace(execute=lambda sql, params: observed.extend(params[0]), fetchall=lambda: [])
        h.connection = SimpleNamespace(cursor=cursor)
        self.assertTrue(h.settle_worker_sessions())
        self.assertEqual(set(observed), {old_identity['application_name'], rejection['application_name']})

    def test_group_stop_sweeps_second_member_and_temporary_group_child(self):
        h = self.harness()
        h.start_consumer_pool('notification')
        temporary = h.spawn('notification-fault', [], 'notification')
        h.start_consumer('analytics')
        h.stop_consumer_role('notification')
        self.assertEqual(set(h.workers), {'analytics'})
        self.assertTrue(all(p.poll() is not None for p in h.children if p is not h.workers['analytics']))
        self.assertEqual(temporary.returncode, 0)

    def test_first_stop_failure_attempts_later_members_and_preserves_error(self):
        h = self.harness()
        h.start_consumer_pool('notification')
        real_stop = h.stop
        original = RuntimeError('first stop')
        calls = []
        def failing(label, **kwargs):
            calls.append(label)
            if label == 'notification':
                raise original
            return real_stop(label, **kwargs)
        h.stop = failing
        with self.assertRaises(RuntimeError) as raised:
            h.stop_consumer_role('notification')
        self.assertIs(raised.exception, original)
        self.assertEqual(calls, ['notification', 'notification-1'])
        self.assertTrue(all(p.poll() is not None for p in h.children))
        self.assertEqual(h.cleanup_errors[0]['error_type'], 'RuntimeError')

    def test_unreaped_child_cannot_be_declared_clean_and_other_children_are_attempted(self):
        h = self.harness()
        h.start_consumer_pool('notification')
        first, second = h.children
        original = OSError('reap')
        first.send_signal = Mock(side_effect=original)
        first.kill = Mock(side_effect=original)
        self.assertIs(h.cleanup_workers(), original)
        self.assertIsNone(first.poll())
        self.assertIsNotNone(second.poll())
        self.assertTrue(any(row['stage'] == 'final_child_reap' for row in h.cleanup_errors))

    def test_dying_or_replaced_owner_during_admin_future_never_becomes_ready(self):
        for change in ('exit', 'replacement', 'start_ticks'):
            h = self.harness()
            h.start_consumer_pool('notification')
            def future(*args, **kwargs):
                if change == 'exit':
                    h.workers['notification-1'].returncode = 1
                elif change == 'replacement':
                    h.workers['notification-1'] = Process(900)
                else:
                    h.child_identities[h.workers['notification-1'].pid]['start_time_ticks'] += 1
                return {'member_count': 2, 'client_ids': kwargs['client_ids']}
            h.wait_group_assignment.side_effect = future
            with self.proc(), self.assertRaises(AssertionError):
                h.wait_consumer_pool('notification')
            self.assertFalse((h.evidence / 'consumer-pool-readiness.jsonl').exists())

    def test_changed_start_receipt_or_late_future_is_rejected(self):
        h = self.harness()
        h.start_consumer_pool('notification')
        clock = [100.]
        def late(*args, **kwargs):
            clock[0] += 2.001
            return {'member_count': 2}
        h.wait_group_assignment.side_effect = late
        with self.proc(), patch('benchmarks.events.acceptance.time.monotonic', lambda: clock[0]), self.assertRaises(AssertionError):
            h.wait_consumer_pool('notification', timeout=2)
        self.assertFalse((h.evidence / 'consumer-pool-readiness.jsonl').exists())
        h = self.harness()
        h.start_consumer_pool('notification')
        identity = h.child_identities[h.workers['notification-1'].pid]
        path = h.evidence / 'logs' / (identity['identity_receipt'] + '.started')
        value = json.loads(path.read_text())
        value['process_snapshot']['start_time_ticks'] += 1
        path.write_text(json.dumps(value))
        clock = [100.]
        def advance(seconds):
            clock[0] += seconds
        with self.proc(), patch('benchmarks.events.acceptance.time.monotonic', lambda: clock[0]), \
             patch('benchmarks.events.acceptance.time.sleep', side_effect=advance), self.assertRaises(AssertionError):
            h.wait_consumer_pool('notification', timeout=2)
        h.wait_group_assignment.assert_not_called()
        self.assertFalse((h.evidence / 'consumer-pool-readiness.jsonl').exists())

    def test_dead_fault_member_replaced_with_new_identity_without_restarting_live_member(self):
        h = self.harness()
        with self.proc():
            h.restore_consumer_pool('notification')
            first, old = h.workers['notification'], h.workers['notification-1']
            old.returncode = 1
            result = h.ensure_consumer_pool('notification')
        self.assertIs(h.workers['notification'], first)
        self.assertNotEqual(h.workers['notification-1'].pid, old.pid)
        self.assertNotIn('acceptance-' + str(old.pid), result['client_ids'])
        self.assertTrue(h.worker_closures[old.pid]['fault_context'])

    def test_graceful_close_receipt_failure_stays_failed_even_after_process_reap(self):
        h = self.harness()
        h.start_consumer('notification')
        child = h.workers['notification']
        identity = h.child_identities[child.pid]
        path = h.evidence / 'logs' / (identity['identity_receipt'] + '.closed')
        value = json.loads(path.read_text())
        value['cleanup_complete'] = False
        path.write_text(json.dumps(value))
        with self.assertRaises(AssertionError):
            h.stop('notification')
        self.assertIsNotNone(child.poll())
        self.assertFalse(h.worker_closures[child.pid]['owning_close_receipt_complete'])

    def test_required_one_three_one_drill_restores_selected_pool(self):
        h = self.harness()
        h.start_consumer_pool('notification')
        h.start_consumer_pool('analytics')
        h.generate = Mock(return_value=(['event'], {}))
        h.drained = Mock()
        with self.proc():
            h.rebalance()
        case = h.cases[0]
        self.assertEqual([case[key]['notification']['member_count'] for key in
            ('membership_before', 'membership_scaled', 'membership_after')], [1, 3, 1])
        self.assertEqual([case[key]['analytics']['member_count'] for key in
            ('membership_before', 'membership_scaled', 'membership_after')], [1, 3, 1])
        self.assertEqual(case['selected_preset_restoration']['notification']['member_count'], 2)
        self.assertEqual(case['selected_preset_restoration']['analytics']['member_count'], 1)
        self.assertEqual(set(h.workers), {'notification', 'notification-1', 'analytics'})
        self.assertEqual(len(case['shutdowns']), 4)
        self.assertEqual(sum(row['requested_signal'] == 'SIGKILL' for row in case['shutdowns']), 2)

    def test_group_readiness_uses_one_deadline_and_rejects_late_second_group(self):
        for restoring in (False, True):
            h = self.harness()
            clock, budgets = [100.], []
            def observe(name, *, timeout, labels=None):
                budgets.append((name, timeout))
                clock[0] += 110. if name == 'notification' else 10.001
                return {'member_count': len(h.pool_roles(name))}
            h.wait_consumer_pool = observe
            with patch('benchmarks.events.acceptance.time.monotonic', lambda: clock[0]), \
                 self.assertRaises(AssertionError):
                if restoring:
                    h.restore_consumer_pools(timeout=120)
                else:
                    h.wait_consumer_pools(timeout=120)
            self.assertEqual(budgets, [('notification', 120.), ('analytics', 10.)])
            self.assertFalse(h.cases)

    def test_outer_restore_and_replacement_deadlines_reject_late_nested_return(self):
        for method in ('restore_consumer_pool', 'restore_consumer_pools', 'ensure_consumer_pool'):
            h = self.harness()
            clock = [100.]
            def late(*args, **kwargs):
                clock[0] = 190.001
                return {'member_count': 2}
            h.wait_consumer_pool = Mock(side_effect=late)
            h.wait_consumer_pools = Mock(side_effect=late)
            with patch('benchmarks.events.acceptance.time.monotonic', lambda: clock[0]), \
                 self.assertRaises(AssertionError):
                if method == 'restore_consumer_pools':
                    h.restore_consumer_pools(timeout=90)
                else:
                    getattr(h, method)('notification', timeout=90)
            self.assertFalse(h.cases)

    def test_recovery_matrix_pause_and_resume_restore_full_selected_pool(self):
        h = self.harness()
        h.start_consumer_pool('notification')
        h.start_consumer_pool('analytics')
        h.start_publisher = Mock()
        matrix = object.__new__(_RecoveryMatrix)
        matrix.h = h
        matrix._pause_main()
        self.assertEqual(h.workers, {})
        with self.proc():
            matrix._resume_main()
        self.assertEqual(set(h.workers), {'notification', 'notification-1', 'analytics'})

    def test_metrics_targets_use_distinct_member_endpoints_and_logical_notification(self):
        h = self.harness()
        (h.evidence / 'metrics').mkdir()
        h.start_consumer_pool('notification')
        targets = json.loads((h.evidence / 'metrics' / 'targets.json').read_text())
        self.assertEqual({row['labels']['worker'] for row in targets}, {'notification'})
        self.assertEqual({row['labels']['owned_role'] for row in targets}, {'notification', 'notification-1'})
        self.assertEqual(len({row['targets'][0] for row in targets}), 2)


class FrozenTopologyTests(SimpleTestCase):
    def test_invalid_or_profiled_nondefault_topology_is_refused_before_any_setup_io(self):
        with TemporaryDirectory() as directory:
            argv = ['acceptance', '--run-id', 'refused', '--evidence-dir', directory,
                    '--consumer-topology', 'notification-dual', '--diagnostic-profile']
            with patch('sys.argv', argv), patch('benchmarks.events.acceptance.load_environment') as env, \
                 patch('benchmarks.events.acceptance.Harness') as harness, self.assertRaises(SystemExit):
                main()
            env.assert_not_called()
            harness.assert_not_called()
            self.assertEqual(list(Path(directory).iterdir()), [])
        with self.assertRaises(ValueError):
            topology_profile('foreign')

    def test_frozen_preset_cannot_change_and_default_numeric_topology_is_preserved(self):
        self.assertEqual([topology_profile()[key] for key in ('writer_lanes', 'publisher_members',
            'notification_members', 'analytics_members', 'inventory_partitions')], [4, 1, 1, 1, 3])
        with TemporaryDirectory() as directory:
            path = Path(directory) / 'consumer-topology.json'
            frozen = freeze_topology(path, 'fixed', 'notification-dual')
            self.assertEqual(frozen['notification_members'], 2)
            original = path.read_bytes()
            with self.assertRaises(ValueError):
                freeze_topology(path, 'fixed', 'single')
            self.assertEqual(path.read_bytes(), original)


class WorkerIdentityReceiptTests(SimpleTestCase):
    def fixture(self):
        db = SimpleNamespace(settings_dict={'ENGINE': 'django.db.backends.postgresql', 'NAME': 'labops_events',
            'OPTIONS': {'server_side_binding': True, 'prepare_threshold': None, 'connect_timeout': 5}},
            connection=None, in_atomic_block=False, get_autocommit=lambda: True)
        queries, receipts = [], {}
        def ensure():
            db.connection = SimpleNamespace(info=SimpleNamespace(backend_pid=77))
        @contextmanager
        def cursor():
            yield SimpleNamespace(execute=lambda sql: queries.append(sql),
                fetchone=lambda: (77, 'labops_events', 'owned-app'))
        db.ensure_connection, db.cursor = ensure, cursor
        close = Mock(side_effect=lambda: setattr(db, 'connection', None))
        group = SimpleNamespace(close_all=close)
        def writer(path, value):
            receipts[path] = copy.deepcopy(value)
        return db, group, queries, receipts, writer

    def test_startup_identity_and_owning_close_preserve_options_without_per_record_query(self):
        db, group, queries, receipts, writer = self.fixture()
        with patch('benchmarks.events.workers.os.getpid', return_value=123):
            with validation_worker_identity(db, group, receipt_path='receipt', role='notification-1',
                application_name='owned-app', snapshot=lambda: process_snapshot(123), writer=writer):
                self.assertEqual(len(queries), 1)
                self.assertEqual(db.settings_dict['OPTIONS']['connect_timeout'], 5)
                self.assertTrue(db.settings_dict['OPTIONS']['server_side_binding'])
        self.assertTrue(receipts['receipt.closed']['cleanup_complete'])
        self.assertIsNone(receipts['receipt.closed']['all_process_sessions_closed'])
        self.assertEqual(len(queries), 1)
        group.close_all.assert_called_once()

    def test_original_business_baseexception_survives_close_and_receipt_failure(self):
        class Business(BaseException):
            def __bool__(self):
                return False
        original = Business()
        db, group, queries, receipts, writer = self.fixture()
        group.close_all.side_effect = OSError('private close')
        def broken_writer(path, value):
            writer(path, value)
            if path.endswith('.closed'):
                raise RuntimeError('private receipt')
        with patch('benchmarks.events.workers.os.getpid', return_value=123), self.assertRaises(Business) as raised:
            with validation_worker_identity(db, group, receipt_path='receipt', role='notification',
                application_name='owned-app', snapshot=lambda: process_snapshot(123), writer=broken_writer):
                raise original
        self.assertIs(raised.exception, original)
        self.assertFalse(receipts['receipt.closed']['cleanup_complete'])
        self.assertNotIn('private', json.dumps(receipts))

    def test_invalid_native_identity_never_runs_business_and_still_attempts_close(self):
        db, group, queries, receipts, writer = self.fixture()
        db.cursor = Mock(side_effect=OSError('private identity'))
        body = Mock()
        with patch('benchmarks.events.workers.os.getpid', return_value=123), self.assertRaises(OSError):
            with validation_worker_identity(db, group, receipt_path='receipt', role='notification',
                application_name='owned-app', snapshot=lambda: process_snapshot(123), writer=writer):
                body()
        body.assert_not_called()
        group.close_all.assert_called_once()
        self.assertEqual(receipts['receipt.started']['status'], 'failed')
        self.assertNotIn('private', json.dumps(receipts))
