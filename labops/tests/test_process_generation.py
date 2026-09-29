"""Actual spawn-process contracts with importable, resource-free workers."""
import json
import math
import multiprocessing
import os
import queue
import signal
import socket
import threading
import time
import unittest
from unittest.mock import patch

from benchmarks.events.process_generation import ProcessGenerationError, run_paced_processes


class PureWorker:
    def __init__(self, lane, bootstrap):
        self.lane = lane
        self.bootstrap = bootstrap

    def ready_metadata(self):
        return {'lane': self.lane, 'pid': os.getpid(), 'resource_mode': 'pure-test'}

    def execute(self, index, target, emit_status):
        starts = self.bootstrap.get('starts')
        if starts is not None:
            with starts.get_lock():
                starts[index] += 1
        emit_status({'phase': 'before_work', 'position': index % 4})
        entered = self.bootstrap.get('entered')
        if entered is not None:
            entered[self.lane].set()
        release = self.bootstrap.get('release')
        if release is not None and not release.wait(5):
            raise RuntimeError('Test execution release was not signaled')
        emit_status({'phase': 'after_work', 'position': index % 4})
        return {'index': index, 'lane': self.lane, 'position': index % 4, 'pid': os.getpid()}

    def close(self):
        counts = self.bootstrap.get('cleanups')
        if counts is not None:
            with counts.get_lock():
                counts[self.lane] += 1
        return {'lane': self.lane, 'pid': os.getpid(), 'closed': True}


def pure_worker_factory(lane, bootstrap):
    return PureWorker(lane, bootstrap)


class KillWindowWorker(PureWorker):
    def execute(self, index, target, emit_status):
        starts = self.bootstrap['starts']
        with starts.get_lock():
            starts[index] += 1
        # The parent kills only after this frame, outside shared synchronization.
        emit_status({'phase': 'kill_window'})
        time.sleep(2)
        return {'index': index, 'lane': self.lane, 'pid': os.getpid()}


def kill_window_factory(lane, bootstrap):
    return KillWindowWorker(lane, bootstrap)


class LifecycleDelayWorker(PureWorker):
    def ready_metadata(self):
        time.sleep(.015)
        return super().ready_metadata()

    def close(self):
        time.sleep(.015)
        return super().close()


def lifecycle_delay_factory(lane, bootstrap):
    return LifecycleDelayWorker(lane, bootstrap)


class LargeCleanupWorker(PureWorker):
    def close(self):
        return {**super().close(), 'receipt_payload': self.bootstrap['cleanup_payload']}


def large_cleanup_factory(lane, bootstrap):
    return LargeCleanupWorker(lane, bootstrap)


class TermIgnoringWorker(PureWorker):
    def ready_metadata(self):
        if self.lane == 0:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        return super().ready_metadata()

    def execute(self, index, target, emit_status):
        emit_status({'phase': 'stuck_execute'})
        time.sleep(60)
        return {'index': index, 'lane': self.lane, 'pid': os.getpid()}


def term_ignoring_factory(lane, bootstrap):
    return TermIgnoringWorker(lane, bootstrap)


class FixtureStartupFailure(RuntimeError):
    stage = 'startup_factory'
    outcome = 'not_started'


def partial_startup_factory(lane, bootstrap):
    if lane == 2:
        for ready in bootstrap['parent_ready'][:2]:
            if not ready.wait(5):
                raise RuntimeError('Parent did not observe the first two ready workers')
        raise FixtureStartupFailure('TOP_SECRET raw startup message')
    return PureWorker(lane, bootstrap)


def sixth_startup_factory(lane, bootstrap):
    if lane == 5:
        for ready in bootstrap['parent_ready'][:5]:
            if not ready.wait(5):
                raise RuntimeError('Parent did not observe the first five ready workers')
        raise FixtureStartupFailure('TOP_SECRET raw sixth-writer startup message')
    return PureWorker(lane, bootstrap)


class AuthoredCommittedFailure(RuntimeError):
    stage = 'after_commit'
    outcome = 'committed'


class CommittedFailureWorker(PureWorker):
    def execute(self, index, target, emit_status):
        emit_status({'phase': 'after_commit', 'outcome': 'committed'})
        raise AuthoredCommittedFailure('TOP_SECRET raw after-commit message')


def committed_failure_factory(lane, bootstrap):
    return CommittedFailureWorker(lane, bootstrap)


class PostResultInterruptionWorker(PureWorker):
    def __init__(self, lane, bootstrap):
        super().__init__(lane, bootstrap)
        self.interrupted_after_result = False
        original_send = socket.socket.send

        def interrupt_after_result(channel, data, *args, **kwargs):
            sent = original_send(channel, data, *args, **kwargs)
            frame = json.loads(data)
            if (frame.get('kind') == 'completed' and frame.get('global_index') == 0
                    and not self.interrupted_after_result):
                self.interrupted_after_result = True
                raise AuthoredCommittedFailure('TOP_SECRET interruption after completed result send')
            return sent

        self.send_patch = patch.object(socket.socket, 'send', new=interrupt_after_result)
        self.send_patch.start()

    def close(self):
        try:
            return {**super().close(), 'interrupted_after_result': self.interrupted_after_result}
        finally:
            self.send_patch.stop()


def post_result_interruption_factory(lane, bootstrap):
    return PostResultInterruptionWorker(lane, bootstrap)


class GenericBusinessExecutionError(RuntimeError):
    def __init__(self, outcome):
        super().__init__('TOP_SECRET raw wrapped database exception')
        self.details = {'error_type': 'OriginalDatabaseFailure',
                        'stage': 'postgres_commit', 'outcome': outcome,
                        'message': 'TOP_SECRET raw exception detail',
                        'credentials': 'TOP_SECRET private detail'}


class GenericBusinessFailureWorker(PureWorker):
    def execute(self, index, target, emit_status):
        raise GenericBusinessExecutionError(self.bootstrap['authored_outcome'])


def generic_business_failure_factory(lane, bootstrap):
    return GenericBusinessFailureWorker(lane, bootstrap)


class FirstFailureHoldingWorker(PureWorker):
    def execute(self, index, target, emit_status):
        if index == 0:
            self.bootstrap['entered'][self.lane].set()
            for entry in self.bootstrap['entered'][1:]:
                if not entry.wait(5):
                    raise RuntimeError('Other callbacks did not enter before first failure')
            raise AuthoredCommittedFailure('TOP_SECRET first child error')
        return super().execute(index, target, emit_status)


def first_failure_holding_factory(lane, bootstrap):
    return FirstFailureHoldingWorker(lane, bootstrap)


class Observations:
    def __init__(self):
        self.events = []
        self.started = {}
        self.ready = {}
        self.lock = threading.Lock()

    def observe(self, event):
        with self.lock:
            self.events.append(dict(event))

    def process_started(self, lane, pid):
        with self.lock:
            self.started[lane] = pid

    def process_ready(self, lane, pid, metadata):
        with self.lock:
            self.ready[lane] = {'pid': pid, 'metadata': metadata}

    def summary(self):
        summaries = [event for event in self.events if event['kind'] == 'summary']
        if len(summaries) != 1:
            raise AssertionError('Expected exactly one final process-generation summary')
        return summaries[0]


def background_run(function):
    outcome = {}

    def invoke():
        try:
            outcome['result'] = function()
        except BaseException as error:
            outcome['exception'] = error

    thread = threading.Thread(target=invoke, name='process-generation-test-controller')
    thread.start()
    return thread, outcome


class ProcessGenerationTests(unittest.TestCase):
    def assert_reaped(self, pids):
        self.assertTrue(pids)
        for pid in pids:
            self.assertNotEqual(pid, os.getpid())
            with self.subTest(pid=pid):
                with self.assertRaises(ProcessLookupError):
                    os.kill(pid, 0)

    def invoke(self, count, bootstrap, observations, **kwargs):
        return run_paced_processes(
            count, 100000, pure_worker_factory, bootstrap,
            on_observation=observations.observe,
            on_process_started=observations.process_started,
            on_process_ready=observations.process_ready, **kwargs)

    def test_four_fresh_spawn_workers_preserve_order_cycle_mix_and_lifecycle(self):
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        results = self.invoke(20, {'cleanups': cleanups}, observations)
        self.assertEqual([result['index'] for result in results], list(range(20)))
        self.assertEqual(set(observations.started), {0, 1, 2, 3})
        self.assertEqual(set(observations.ready), {0, 1, 2, 3})
        self.assertEqual(len(set(observations.started.values())), 4)
        for result in results:
            lane = (result['index'] // 4) % 4
            self.assertEqual(result['lane'], lane)
            self.assertEqual(result['position'], result['index'] % 4)
            self.assertEqual(result['pid'], observations.started[lane])
        for lane in range(4):
            self.assertEqual(observations.ready[lane]['pid'], observations.started[lane])
            self.assertEqual(observations.ready[lane]['metadata']['resource_mode'], 'pure-test')
        self.assertEqual(list(cleanups), [1, 1, 1, 1])
        summary = observations.summary()
        self.assertEqual(summary['requested_count'], 20)
        self.assertEqual(summary['completed_count'], 20)
        self.assertEqual(summary['failed_count'], 0)
        self.assertEqual(summary['unknown_count'], 0)
        self.assert_reaped(observations.started.values())

    def test_blocked_lane_observes_queue_bound_and_one_global_pacing_clock(self):
        from benchmarks.events import process_generation
        from multiprocessing.queues import Queue
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 4),
                     'entered': [context.Event() for _ in range(4)], 'release': context.Event()}
        observations = Observations()
        released_at_capacity = threading.Event()
        queue_full = threading.Event()
        real_pause = process_generation._coordinator_pause
        real_put = Queue.put_nowait

        def observe_full(work_queue, item):
            try:
                return real_put(work_queue, item)
            except queue.Full:
                if (threading.current_thread().name == 'process-generation-test-controller'
                        and item[0] == 17):
                    queue_full.set()
                raise

        def release_full_lane(seconds):
            if threading.current_thread().name == 'process-generation-test-controller':
                with observations.lock:
                    scheduled = {event['global_index'] for event in observations.events
                                 if event['kind'] == 'scheduled'}
                # Lane zero owns in-flight zero, plus queued 1, 2, 3, 16.
                # Its next admission (17) must wait until this controlled release.
                if (queue_full.is_set() and 16 in scheduled and 17 not in scheduled
                        and bootstrap['entered'][0].is_set()):
                    released_at_capacity.set()
                    bootstrap['release'].set()
            return real_pause(seconds)

        with patch.object(Queue, 'put_nowait', new=observe_full), \
                patch.object(process_generation, '_coordinator_pause', new=release_full_lane):
            thread, outcome = background_run(lambda: run_paced_processes(
                36, 1000, lifecycle_delay_factory, bootstrap,
                on_observation=observations.observe,
                on_process_started=observations.process_started,
                on_process_ready=observations.process_ready))
            try:
                thread.join(8)
                self.assertFalse(thread.is_alive())
            finally:
                bootstrap['release'].set()
                thread.join(8)
        self.assertNotIn('exception', outcome)
        self.assertTrue(queue_full.is_set())
        self.assertTrue(released_at_capacity.is_set())
        self.assertEqual([row['index'] for row in outcome['result']], list(range(36)))
        summary = observations.summary()
        self.assertEqual(summary['max_queue_depths'][0], 4)
        self.assertTrue(all(0 <= depth <= 4 for depth in summary['max_queue_depths']))
        waits = [event for event in observations.events if event['kind'] == 'queue_wait']
        self.assertGreaterEqual(summary['queue_wait_count'], 1)
        self.assertEqual(summary['queue_wait_count'], len(waits))
        self.assertTrue(any(event['lane'] == 0 and event['global_index'] == 17 for event in waits))
        self.assertGreater(summary['queue_wait_seconds'], 0)
        self.assertAlmostEqual(summary['queue_wait_seconds'],
                               sum(event['duration_seconds'] for event in waits))
        scheduled = sorted((event for event in observations.events if event['kind'] == 'scheduled'),
                           key=lambda event: event['global_index'])
        first_target = scheduled[0]['target_monotonic']
        for index, event in enumerate(scheduled):
            self.assertAlmostEqual(event['target_monotonic'] - first_target, index / 1000, delta=1e-7)
            self.assertGreaterEqual(event['observed_monotonic'], event['target_monotonic'])
        first_spawn = min(event['observed_monotonic'] for event in observations.events
                          if event['kind'] == 'process_started')
        last_exit = max(event['observed_monotonic'] for event in observations.events
                        if event['kind'] == 'process_exit')
        self.assertGreaterEqual(summary['duration_seconds'], last_exit - first_spawn)
        self.assertAlmostEqual(summary['actual_rate'], 36 / summary['duration_seconds'])
        self.assertTrue(summary['lifecycle_complete'])
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['channels_closed'])
        self.assertEqual(list(bootstrap['cleanups']), [1, 1, 1, 1])
        self.assert_reaped(observations.started.values())

    def test_result_capacity_one_completes_many_results_without_losing_global_order(self):
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        results = self.invoke(48, {'cleanups': cleanups}, observations, result_capacity=1)
        self.assertEqual([result['index'] for result in results], list(range(48)))
        self.assertEqual(len({result['index'] for result in results}), 48)
        self.assertEqual(list(cleanups), [1, 1, 1, 1])
        summary = observations.summary()
        self.assertEqual(summary['completed_count'], 48)
        self.assertEqual(summary['failed_count'], 0)
        self.assertEqual(summary['unknown_count'], 0)
        self.assertEqual(summary['result_capacity'], 1)
        self.assertEqual(summary['profile']['result_capacity'], 1)
        self.assertEqual(summary['result_frames'], 48)
        self.assertEqual(summary['status_frames'], 96)
        self.assertGreater(summary['max_output_credits'], 0)
        self.assertLessEqual(summary['max_output_credits'], 1)
        self.assertGreater(summary['max_pending_credit_requests'], 0)
        self.assertLessEqual(summary['max_pending_credit_requests'], 4)
        self.assert_reaped(observations.started.values())

    def test_global_start_index_changes_lane_assignment_without_changing_result_order(self):
        observations = Observations()
        results = self.invoke(12, {}, observations, start_index=3)
        self.assertEqual([result['index'] for result in results], list(range(3, 15)))
        for result in results:
            self.assertEqual(result['lane'], (result['index'] // 4) % 4)
            self.assertEqual(result['position'], result['index'] % 4)
        self.assert_reaped(observations.started.values())

    def test_partial_startup_failure_closes_ready_workers_and_preserves_safe_authored_error(self):
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 4),
                     'parent_ready': [context.Event() for _ in range(4)]}
        observations = Observations()

        def ready(lane, pid, metadata):
            observations.process_ready(lane, pid, metadata)
            bootstrap['parent_ready'][lane].set()

        with self.assertRaises(ProcessGenerationError) as caught:
            run_paced_processes(16, 100000, partial_startup_factory, bootstrap,
                                on_observation=observations.observe,
                                on_process_started=observations.process_started,
                                on_process_ready=ready)
        first = caught.exception.details['first_error']
        self.assertEqual(first['class'], 'FixtureStartupFailure')
        self.assertEqual(first['stage'], 'startup_factory')
        self.assertEqual(first['outcome'], 'not_started')
        self.assertNotIn('TOP_SECRET', str(caught.exception))
        self.assertNotIn('TOP_SECRET', repr(caught.exception.details))
        self.assertEqual(list(bootstrap['cleanups'])[:3], [1, 1, 0])
        for lane in observations.ready:
            self.assertEqual(bootstrap['cleanups'][lane], 1)
        summary = observations.summary()
        self.assertEqual(summary['started_count'], 0)
        self.assertEqual(summary['completed_count'], 0)
        self.assert_reaped(observations.started.values())

    def test_authored_after_commit_failure_retains_outcome_without_raw_exception_text(self):
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        with self.assertRaises(ProcessGenerationError) as caught:
            run_paced_processes(1, 100000, committed_failure_factory, {'cleanups': cleanups},
                                on_observation=observations.observe,
                                on_process_started=observations.process_started,
                                on_process_ready=observations.process_ready)
        first = caught.exception.details['first_error']
        self.assertEqual(first['class'], 'AuthoredCommittedFailure')
        self.assertEqual(first['stage'], 'after_commit')
        self.assertEqual(first['global_index'], 0)
        self.assertEqual(first['outcome'], 'committed')
        self.assertNotIn('TOP_SECRET', str(caught.exception))
        self.assertNotIn('TOP_SECRET', repr(caught.exception.details))
        self.assertEqual(list(cleanups), [1, 1, 1, 1])
        self.assert_reaped(observations.started.values())

    def test_generic_business_error_details_preserve_authored_outcomes_and_omit_private_text(self):
        context = multiprocessing.get_context('spawn')
        for authored_outcome, normalized in [('commit_unknown', 'unknown'),
                                              ('not_entered', 'not_started')]:
            with self.subTest(authored_outcome=authored_outcome):
                cleanups = context.Array('i', 4)
                observations = Observations()
                with self.assertRaises(ProcessGenerationError) as caught:
                    run_paced_processes(
                        1, 100000, generic_business_failure_factory,
                        {'cleanups': cleanups, 'authored_outcome': authored_outcome}, start_index=5,
                        on_observation=observations.observe,
                        on_process_started=observations.process_started,
                        on_process_ready=observations.process_ready)
                first = caught.exception.details['first_error']
                self.assertEqual(first['class'], 'OriginalDatabaseFailure')
                self.assertEqual(first['stage'], 'postgres_commit')
                self.assertEqual(first['global_index'], 5)
                self.assertEqual(first['lane'], 1)
                self.assertEqual(first['outcome'], normalized)
                self.assertEqual(first['authored_outcome'], authored_outcome)
                self.assertNotIn('TOP_SECRET', str(caught.exception))
                self.assertNotIn('TOP_SECRET', repr(caught.exception.details))
                self.assertNotIn('TOP_SECRET', repr(observations.events))
                self.assertNotIn('message', first)
                self.assertNotIn('credentials', first)
                self.assertEqual(list(cleanups), [1, 1, 1, 1])
                self.assert_reaped(observations.started.values())

    def test_error_after_result_send_preserves_completion_without_double_counting_or_replay(self):
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        starts = context.Array('i', 1)
        observations = Observations()
        with self.assertRaises(ProcessGenerationError) as caught:
            run_paced_processes(
                1, 100000, post_result_interruption_factory,
                {'cleanups': cleanups, 'starts': starts},
                on_observation=observations.observe,
                on_process_started=observations.process_started,
                on_process_ready=observations.process_ready)
        first = caught.exception.details['first_error']
        self.assertEqual(first['class'], 'AuthoredCommittedFailure')
        self.assertEqual(first['stage'], 'after_commit')
        self.assertEqual(first['global_index'], 0)
        self.assertEqual(first['outcome'], 'committed')
        self.assertNotIn('TOP_SECRET', str(caught.exception))
        self.assertNotIn('TOP_SECRET', repr(caught.exception.details))
        summary = observations.summary()
        self.assertEqual(summary['started_count'], 1)
        self.assertEqual(summary['completed_count'], 1)
        self.assertEqual(summary['failed_count'], 0)
        self.assertEqual(summary['reported_failure_count'], 1)
        self.assertEqual(summary['unknown_count'], 0)
        self.assertEqual(summary['cancelled_count'], 0)
        self.assertEqual(summary['unscheduled_count'], 0)
        self.assertEqual(len(summary['post_result_errors']), 1)
        post_result_error = summary['post_result_errors'][0]
        self.assertEqual(post_result_error['global_index'], 0)
        self.assertEqual(post_result_error['class'], first['class'])
        self.assertEqual(post_result_error['stage'], first['stage'])
        self.assertEqual(post_result_error['outcome'], 'committed')
        completed = [event for event in observations.events if event['kind'] == 'completed']
        failed = [event for event in observations.events if event['kind'] == 'failed']
        self.assertEqual([event['global_index'] for event in completed], [0])
        self.assertEqual([event['global_index'] for event in failed], [0])
        self.assertEqual(completed[0]['result']['index'], 0)
        self.assertEqual(completed[0]['result']['pid'], observations.started[0])
        self.assertEqual(list(starts), [1])
        self.assertEqual(list(cleanups), [1, 1, 1, 1])
        self.assertTrue(summary['children'][0]['cleanup_metadata']['interrupted_after_result'])
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['channels_closed'])
        self.assert_reaped(observations.started.values())

    def test_interrupt_after_real_queue_put_cancels_reserved_admission_exactly_once(self):
        from multiprocessing.queues import Queue
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        original = KeyboardInterrupt('original interrupted queue admission')
        submitted = []
        real_put = Queue.put_nowait

        def interrupt_after_put(work_queue, item):
            result = real_put(work_queue, item)
            submitted.append(item[0])
            if len(submitted) == 1:
                raise original
            return result

        with patch.object(Queue, 'put_nowait', new=interrupt_after_put):
            with self.assertRaises(KeyboardInterrupt) as caught:
                self.invoke(16, {'cleanups': cleanups}, observations)
        self.assertIs(caught.exception, original)
        self.assertEqual(submitted, [0])
        summary = observations.summary()
        self.assertEqual(summary['requested_count'], 16)
        self.assertEqual(summary['scheduled_count'], 1)
        self.assertEqual(summary['started_count'], 0)
        self.assertEqual(summary['started_indices'], [])
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(summary['failed_count'], 0)
        self.assertEqual(summary['unknown_count'], 0)
        self.assertEqual(summary['cancelled_count'], 1)
        self.assertEqual(summary['cancelled_indices'], [0])
        self.assertEqual(summary['unscheduled_count'], 15)
        self.assertEqual(summary['unscheduled_indices'], list(range(1, 16)))
        self.assertFalse(set(summary['cancelled_indices']) & set(summary['unscheduled_indices']))
        self.assertEqual(set(summary['cancelled_indices']) | set(summary['unscheduled_indices']),
                         set(range(16)))
        self.assertEqual(list(cleanups), [1, 1, 1, 1])
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['channels_closed'])
        self.assert_reaped(observations.started.values())

    def test_owned_sigkill_after_started_is_unknown_and_never_replayed(self):
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 4), 'starts': context.Array('i', 1)}
        observations = Observations()
        killed = False

        def observe(event):
            nonlocal killed
            observations.observe(event)
            if (event['kind'] == 'status' and event['global_index'] == 0
                    and event['metadata'].get('phase') == 'kill_window' and not killed):
                self.assertIn(event['pid'], observations.started.values())
                self.assertTrue(any(row['kind'] == 'started' and row['global_index'] == 0
                                    for row in observations.events))
                self.assertEqual(bootstrap['starts'][0], 1)
                killed = True
                os.kill(event['pid'], signal.SIGKILL)

        with self.assertRaises(ProcessGenerationError) as caught:
            run_paced_processes(1, 100000, kill_window_factory, bootstrap,
                                on_observation=observe,
                                on_process_started=observations.process_started,
                                on_process_ready=observations.process_ready)
        self.assertTrue(killed)
        self.assertEqual(bootstrap['starts'][0], 1)
        first = caught.exception.details['first_error']
        self.assertEqual(first['outcome'], 'unknown')
        self.assertEqual(first['global_index'], 0)
        summary = observations.summary()
        self.assertEqual(summary['unknown_count'], 1)
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(list(bootstrap['cleanups']), [0, 1, 1, 1])
        self.assert_reaped(observations.started.values())

    def test_coordinator_interrupt_waits_for_inflight_children_cleanup_and_os_reap(self):
        from benchmarks.events import process_generation
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 4), 'starts': context.Array('i', 16),
                     'entered': [context.Event() for _ in range(4)], 'release': context.Event()}
        observations = Observations()
        original = KeyboardInterrupt('original coordinator interrupt')
        interrupt_captured = threading.Event()
        interrupted = False
        real_pause = process_generation._coordinator_pause

        def interrupt_pause(seconds):
            nonlocal interrupted
            if threading.current_thread().name == 'process-generation-test-controller':
                with observations.lock:
                    starts = [event for event in observations.events if event['kind'] == 'started']
                if len(starts) >= 4 and all(entry.is_set() for entry in bootstrap['entered']):
                    if not interrupted:
                        interrupted = True
                        raise original
                    interrupt_captured.set()
            return real_pause(seconds)

        with patch.object(process_generation, '_coordinator_pause', new=interrupt_pause):
            thread, outcome = background_run(lambda: self.invoke(16, bootstrap, observations))
            try:
                self.assertTrue(interrupt_captured.wait(8))
                self.assertTrue(thread.is_alive(), 'Interrupted coordinator must still own in-flight children')
                self.assertEqual(outcome, {})
                self.assertEqual(list(bootstrap['cleanups']), [0, 0, 0, 0])
            finally:
                bootstrap['release'].set()
                thread.join(8)
        self.assertFalse(thread.is_alive())
        self.assertIs(outcome.get('exception'), original)
        self.assertEqual(list(bootstrap['cleanups']), [1, 1, 1, 1])
        self.assertEqual([index for index, count in enumerate(bootstrap['starts']) if count], [0, 4, 8, 12])
        self.assertTrue(all(count <= 1 for count in bootstrap['starts']))
        summary = observations.summary()
        self.assertEqual(summary['completed_count'], 4)
        self.assertEqual(summary['cancelled_count'], 12)
        self.assert_reaped(observations.started.values())

    def test_prior_child_error_wins_over_later_coordinator_interrupt(self):
        from benchmarks.events import process_generation
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 4),
                     'entered': [context.Event() for _ in range(4)], 'release': context.Event()}
        observations = Observations()
        later_interrupt = KeyboardInterrupt('later coordinator interrupt')
        interrupt_captured = threading.Event()
        interrupted = False
        real_pause = process_generation._coordinator_pause

        def interrupt_pause(seconds):
            nonlocal interrupted
            if threading.current_thread().name == 'process-generation-test-controller':
                with observations.lock:
                    child_failed = any(event['kind'] == 'failed' for event in observations.events)
                if child_failed:
                    if not interrupted:
                        interrupted = True
                        raise later_interrupt
                    interrupt_captured.set()
            return real_pause(seconds)

        with patch.object(process_generation, '_coordinator_pause', new=interrupt_pause):
            thread, outcome = background_run(lambda: run_paced_processes(
                16, 100000, first_failure_holding_factory, bootstrap,
                on_observation=observations.observe,
                on_process_started=observations.process_started,
                on_process_ready=observations.process_ready))
            try:
                self.assertTrue(interrupt_captured.wait(8))
                self.assertTrue(thread.is_alive())
                self.assertEqual(outcome, {})
            finally:
                bootstrap['release'].set()
                thread.join(8)
        self.assertFalse(thread.is_alive())
        error = outcome.get('exception')
        self.assertIsInstance(error, ProcessGenerationError)
        first = error.details['first_error']
        self.assertEqual(first['class'], 'AuthoredCommittedFailure')
        self.assertEqual(first['stage'], 'after_commit')
        self.assertEqual(first['global_index'], 0)
        self.assertEqual(first['outcome'], 'committed')
        self.assertNotIn('TOP_SECRET', str(error))
        self.assertEqual(list(bootstrap['cleanups']), [1, 1, 1, 1])
        self.assert_reaped(observations.started.values())

    def test_join_interrupt_is_retried_until_all_started_children_are_reaped(self):
        from multiprocessing.process import BaseProcess
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        original = KeyboardInterrupt('original process join interrupt')
        interrupted = False
        joins = []
        real_join = BaseProcess.join

        def interrupt_join(child, timeout=None):
            nonlocal interrupted
            if (threading.current_thread().name == 'process-generation-test-controller'
                    and child.pid in observations.started.values()):
                joins.append(child.pid)
                if not interrupted:
                    interrupted = True
                    raise original
            return real_join(child, timeout)

        with patch.object(BaseProcess, 'join', new=interrupt_join):
            thread, outcome = background_run(lambda: self.invoke(16, {'cleanups': cleanups}, observations))
            thread.join(8)
        self.assertFalse(thread.is_alive())
        self.assertTrue(interrupted)
        self.assertIs(outcome.get('exception'), original)
        self.assertEqual(set(joins), set(observations.started.values()))
        self.assertGreaterEqual(joins.count(joins[0]), 2)
        self.assertEqual(list(cleanups), [1, 1, 1, 1])
        self.assert_reaped(observations.started.values())

    def test_hung_child_is_terminated_then_killed_before_original_interrupt_is_raised(self):
        from benchmarks.events import process_generation
        from multiprocessing.process import BaseProcess
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        original = KeyboardInterrupt('original interrupt with an uncooperative child')
        later_interrupt = KeyboardInterrupt('later interrupted kill attempt')
        interrupted = False
        hung_entered = False
        sibling_cleanups = set()
        kill_attempts = []
        real_kill = BaseProcess.kill

        def interrupt_after_entry(event):
            nonlocal interrupted, hung_entered
            observations.observe(event)
            if (event['kind'] == 'status' and event['global_index'] == 0
                    and event['metadata'].get('phase') == 'stuck_execute'):
                hung_entered = True
            if event['kind'] == 'cleanup_complete' and event['lane'] != 0:
                sibling_cleanups.add(event['lane'])
            if hung_entered and sibling_cleanups == {1, 2, 3} and not interrupted:
                interrupted = True
                raise original

        def interrupt_first_kill(child):
            self.assertEqual(child.pid, observations.started[0])
            kill_attempts.append(child.pid)
            if len(kill_attempts) == 1:
                raise later_interrupt
            return real_kill(child)

        with patch.object(BaseProcess, 'kill', new=interrupt_first_kill), \
                patch.object(process_generation, 'FAILURE_CLEANUP_SECONDS', .05), \
                patch.object(process_generation, 'TERM_GRACE_SECONDS', .02):
            with self.assertRaises(KeyboardInterrupt) as caught:
                run_paced_processes(1, 100000, term_ignoring_factory, {'cleanups': cleanups},
                                    on_observation=interrupt_after_entry,
                                    on_process_started=observations.process_started,
                                    on_process_ready=observations.process_ready)
        self.assertIs(caught.exception, original)
        self.assertTrue(interrupted)
        self.assertEqual(sibling_cleanups, {1, 2, 3})
        self.assertGreaterEqual(len(kill_attempts), 2)
        self.assertEqual(set(kill_attempts), {observations.started[0]})
        summary = observations.summary()
        signals = [event['signal'] for event in observations.events
                   if event['kind'] == 'forced_shutdown' and event['lane'] == 0]
        self.assertEqual(signals, ['SIGTERM', 'SIGKILL'])
        self.assertEqual([event['signal'] for event in summary['forced_shutdowns']
                          if event['lane'] == 0], signals)
        child = next(child for child in summary['children'] if child['lane'] == 0)
        self.assertEqual(child['exitcode'], -signal.SIGKILL)
        self.assertFalse(child['cleanup_complete'])
        self.assertFalse(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertEqual(summary['unknown_count'], 1)
        self.assertEqual(summary['unknown_indices'], [0])
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(summary['failed_count'], 0)
        self.assertEqual(list(cleanups), [0, 1, 1, 1])
        self.assertTrue(summary['channels_closed'])
        self.assert_reaped(observations.started.values())

    def test_interrupt_after_actual_process_launch_retains_ownership_until_reaped(self):
        from benchmarks.events import process_generation
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        original = KeyboardInterrupt('original interrupted launch')
        real_start = process_generation._start_owned_process
        launched = []

        def interrupt_after_launch(process):
            real_start(process)
            launched.append(process.pid)
            if len(launched) == 1:
                raise original

        with patch.object(process_generation, '_start_owned_process', new=interrupt_after_launch):
            with self.assertRaises(KeyboardInterrupt) as caught:
                self.invoke(16, {'cleanups': cleanups}, observations)
        self.assertIs(caught.exception, original)
        self.assertEqual(len(launched), 1)
        self.assertEqual(set(observations.started.values()), set(launched))
        self.assertEqual(list(cleanups), [1, 0, 0, 0])
        summary = observations.summary()
        self.assertEqual(summary['scheduled_count'], 0)
        self.assertEqual(summary['started_count'], 0)
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(len(summary['children']), 1)
        self.assertTrue(summary['children'][0]['cleanup_complete'])
        self.assertEqual(summary['children'][0]['exitcode'], 0)
        self.assertTrue(summary['children'][0]['reaped'])
        delayed = [event for event in observations.events
                   if event['kind'] == 'process_started' and event.get('delayed_observation')]
        self.assertEqual([event['pid'] for event in delayed], launched)
        self.assert_reaped(launched)

    def assert_interrupted_credit_grant_is_drained(self, *, after_send):
        from benchmarks.events import process_generation
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 4)
        observations = Observations()
        original = KeyboardInterrupt('original credit grant interrupt')
        real_grant = process_generation._send_credit_grant
        interrupted = False
        grants = []

        def interrupt_grant(channel, sequence):
            nonlocal interrupted
            grants.append((channel.fileno(), sequence))
            if not interrupted:
                interrupted = True
                if after_send:
                    real_grant(channel, sequence)
                raise original
            return real_grant(channel, sequence)

        with patch.object(process_generation, '_send_credit_grant', new=interrupt_grant):
            with self.assertRaises(KeyboardInterrupt) as caught:
                self.invoke(16, {'cleanups': cleanups}, observations, result_capacity=1)
        self.assertIs(caught.exception, original)
        self.assertTrue(interrupted)
        self.assertEqual(list(cleanups), [1, 1, 1, 1])
        if not after_send:
            self.assertGreaterEqual(grants.count(grants[0]), 2)
        summary = observations.summary()
        self.assertEqual(summary['started_count'], 0)
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(summary['unknown_count'], 0)
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['channels_closed'])
        self.assertLessEqual(summary['max_output_credits'], 1)
        self.assertLessEqual(summary['max_pending_credit_requests'], 4)
        self.assertTrue(all(child['cleanup_complete'] and child['exitcode'] == 0
                            and child['reaped'] for child in summary['children']))
        self.assert_reaped(observations.started.values())

    def test_interrupt_before_credit_send_retries_the_owned_sequence_and_reaps_children(self):
        self.assert_interrupted_credit_grant_is_drained(after_send=False)

    def test_interrupt_after_credit_send_drains_idempotent_grants_and_reaps_children(self):
        self.assert_interrupted_credit_grant_is_drained(after_send=True)

    def test_selected_process_profiles_keep_cycle_queue_and_global_result_bounds(self):
        from benchmarks.events.process_generation import (
            frozen_process_generation_profile, frozen_process_profile)
        default = frozen_process_generation_profile()
        selected = frozen_process_generation_profile(writer_topology='writers-6')
        self.assertEqual(default, frozen_process_generation_profile(writer_topology='writers-4'))
        self.assertEqual(selected, frozen_process_profile(writer_topology='writers-6'))
        for profile, lanes, preset in [(default, 4, 'writers-4'), (selected, 6, 'writers-6')]:
            with self.subTest(preset=preset):
                self.assertEqual(profile['version'], 'spawn-lanes-v1')
                self.assertEqual(profile['writer_topology'], preset)
                self.assertEqual(profile['writer_topology_version'], 'writer-topology-v1')
                self.assertEqual(profile['lanes'], lanes)
                self.assertEqual(profile['cycle_length'], 4)
                self.assertEqual(profile['queue_capacity'], 4)
                self.assertEqual(profile['result_capacity'], 16)
                self.assertEqual(profile['assignment'], '(global_index//4)%' + str(lanes))
                self.assertEqual(profile['position'], 'global_index%4')
                self.assertTrue(profile['elapsed_includes_spawn_and_readiness'])
                self.assertTrue(profile['elapsed_includes_cleanup_and_reaping'])
                self.assertEqual(profile['failure_cleanup_seconds'], 30.0)
                self.assertEqual(profile['failure_term_grace_seconds'], 5.0)
                self.assertFalse(profile['automatic_replay'])
        selected['lanes'] = 100
        self.assertEqual(frozen_process_generation_profile(writer_topology='writers-6')['lanes'], 6)

    def test_six_fresh_spawn_workers_cover_partial_cycles_and_nonzero_start_index(self):
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 6)
        observations = Observations()
        profile = {'writer_topology': 'writers-6', 'writer_topology_version': 'writer-topology-v1'}
        results = self.invoke(29, {'cleanups': cleanups, 'profile': profile}, observations,
                              start_index=3, writer_topology='writers-6', lanes=6)
        self.assertEqual([result['index'] for result in results], list(range(3, 32)))
        self.assertEqual(set(observations.started), set(range(6)))
        self.assertEqual(set(observations.ready), set(range(6)))
        self.assertEqual(len(set(observations.started.values())), 6)
        for result in results:
            lane = (result['index'] // 4) % 6
            self.assertEqual(result['lane'], lane)
            self.assertEqual(result['position'], result['index'] % 4)
            self.assertEqual(result['pid'], observations.started[lane])
        summary = observations.summary()
        self.assertEqual(summary['per_lane_requested_counts'], [5, 8, 4, 4, 4, 4])
        self.assertEqual(summary['completed_count'], 29)
        self.assertEqual(summary['unknown_count'], 0)
        self.assertEqual(summary['profile']['writer_topology'], 'writers-6')
        self.assertEqual(summary['profile']['lanes'], 6)
        self.assertEqual(len(summary['max_queue_depths']), 6)
        self.assertEqual(summary['result_capacity'], 16)
        self.assertGreater(summary['max_output_credits'], 0)
        self.assertLessEqual(summary['max_output_credits'], 16)
        self.assertLessEqual(summary['max_pending_credit_requests'], 6)
        self.assertTrue(summary['lifecycle_complete'])
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['channels_closed'])
        self.assertTrue(all(child['cleanup_complete'] and child['exitcode'] == 0
                            and child['reaped'] for child in summary['children']))
        self.assertEqual(list(cleanups), [1] * 6)
        self.assert_reaped(observations.started.values())

    def test_six_idle_lanes_still_close_and_reap_for_empty_and_small_batches(self):
        context = multiprocessing.get_context('spawn')
        for count, allocation in [(0, [0] * 6), (3, [0, 0, 0, 0, 0, 3])]:
            with self.subTest(count=count):
                cleanups = context.Array('i', 6)
                observations = Observations()
                results = self.invoke(count, {'cleanups': cleanups}, observations,
                                      start_index=20, writer_topology='writers-6')
                self.assertEqual([result['index'] for result in results], list(range(20, 20 + count)))
                self.assertTrue(all(result['lane'] == 5 for result in results))
                summary = observations.summary()
                self.assertEqual(summary['per_lane_requested_counts'], allocation)
                self.assertEqual(summary['scheduled_count'], count)
                self.assertEqual(summary['completed_count'], count)
                self.assertEqual(summary['unknown_count'], 0)
                self.assertEqual(set(observations.started), set(range(6)))
                self.assertEqual(set(observations.ready), set(range(6)))
                self.assertEqual(len(summary['children']), 6)
                self.assertTrue(summary['lifecycle_complete'])
                self.assertTrue(summary['worker_completion_observed'])
                self.assertTrue(summary['channels_closed'])
                self.assertEqual(list(cleanups), [1] * 6)
                self.assert_reaped(observations.started.values())

    def test_six_writers_share_one_result_credit_without_losing_protocol_frames(self):
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 6)
        observations = Observations()
        results = self.invoke(72, {'cleanups': cleanups}, observations,
                              writer_topology='writers-6', result_capacity=1)
        self.assertEqual([result['index'] for result in results], list(range(72)))
        self.assertEqual(len({result['index'] for result in results}), 72)
        summary = observations.summary()
        self.assertEqual(summary['per_lane_requested_counts'], [12] * 6)
        self.assertEqual(summary['completed_count'], 72)
        self.assertEqual(summary['result_frames'], 72)
        self.assertEqual(summary['status_frames'], 144)
        self.assertEqual(summary['result_capacity'], 1)
        self.assertEqual(summary['profile']['result_capacity'], 1)
        self.assertGreater(summary['max_output_credits'], 0)
        self.assertLessEqual(summary['max_output_credits'], 1)
        self.assertLessEqual(summary['max_pending_credit_requests'], 6)
        self.assertTrue(summary['lifecycle_complete'])
        self.assertEqual(list(cleanups), [1] * 6)
        self.assert_reaped(observations.started.values())

    def test_six_blocked_writers_keep_queue_four_and_one_global_pacing_clock(self):
        from benchmarks.events import process_generation
        from multiprocessing.queues import Queue
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 6),
                     'entered': [context.Event() for _ in range(6)], 'release': context.Event()}
        observations = Observations()
        queue_full = threading.Event()
        released_at_capacity = threading.Event()
        real_pause = process_generation._coordinator_pause
        real_put = Queue.put_nowait

        def observe_full(work_queue, item):
            try:
                return real_put(work_queue, item)
            except queue.Full:
                if (threading.current_thread().name == 'process-generation-test-controller'
                        and item[0] == 25):
                    queue_full.set()
                raise

        def release_full_lane(seconds):
            if threading.current_thread().name == 'process-generation-test-controller':
                with observations.lock:
                    scheduled = {event['global_index'] for event in observations.events
                                 if event['kind'] == 'scheduled'}
                # Lane zero owns in-flight zero, plus queued 1, 2, 3, 24.
                if (queue_full.is_set() and 24 in scheduled and 25 not in scheduled
                        and all(entry.is_set() for entry in bootstrap['entered'])):
                    released_at_capacity.set()
                    bootstrap['release'].set()
            return real_pause(seconds)

        with patch.object(Queue, 'put_nowait', new=observe_full), \
                patch.object(process_generation, '_coordinator_pause', new=release_full_lane):
            thread, outcome = background_run(lambda: run_paced_processes(
                52, 1000, lifecycle_delay_factory, bootstrap, writer_topology='writers-6',
                on_observation=observations.observe,
                on_process_started=observations.process_started,
                on_process_ready=observations.process_ready))
            try:
                thread.join(8)
                self.assertFalse(thread.is_alive())
            finally:
                bootstrap['release'].set()
                thread.join(8)
        self.assertNotIn('exception', outcome)
        self.assertTrue(queue_full.is_set())
        self.assertTrue(released_at_capacity.is_set())
        self.assertEqual([result['index'] for result in outcome['result']], list(range(52)))
        summary = observations.summary()
        self.assertEqual(summary['max_queue_depths'][0], 4)
        self.assertEqual(len(summary['max_queue_depths']), 6)
        self.assertTrue(all(0 <= depth <= 4 for depth in summary['max_queue_depths']))
        waits = [event for event in observations.events if event['kind'] == 'queue_wait']
        self.assertEqual(summary['queue_wait_count'], len(waits))
        self.assertTrue(any(event['lane'] == 0 and event['global_index'] == 25 for event in waits))
        self.assertGreater(summary['queue_wait_seconds'], 0)
        self.assertAlmostEqual(summary['queue_wait_seconds'], sum(event['duration_seconds'] for event in waits))
        scheduled = sorted((event for event in observations.events if event['kind'] == 'scheduled'),
                           key=lambda event: event['global_index'])
        first_target = scheduled[0]['target_monotonic']
        for offset, event in enumerate(scheduled):
            self.assertAlmostEqual(event['target_monotonic'] - first_target, offset / 1000, delta=1e-7)
            self.assertGreaterEqual(event['observed_monotonic'], event['target_monotonic'])
        first_spawn = min(event['observed_monotonic'] for event in observations.events
                          if event['kind'] == 'process_started')
        last_exit = max(event['observed_monotonic'] for event in observations.events
                        if event['kind'] == 'process_exit')
        self.assertGreaterEqual(summary['duration_seconds'], last_exit - first_spawn)
        self.assertAlmostEqual(summary['actual_rate'], 52 / summary['duration_seconds'])
        self.assertLessEqual(summary['max_output_credits'], 16)
        self.assertLessEqual(summary['max_pending_credit_requests'], 6)
        self.assertTrue(summary['lifecycle_complete'])
        self.assertEqual(list(bootstrap['cleanups']), [1] * 6)
        self.assert_reaped(observations.started.values())

    def test_sixth_writer_sigkill_is_attributed_unknown_without_replay(self):
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 6), 'starts': context.Array('i', 21)}
        observations = Observations()
        killed = False

        def observe(event):
            nonlocal killed
            observations.observe(event)
            if (event['kind'] == 'status' and event['global_index'] == 20
                    and event['metadata'].get('phase') == 'kill_window' and not killed):
                self.assertEqual(event['lane'], 5)
                self.assertEqual(event['pid'], observations.started[5])
                self.assertEqual(bootstrap['starts'][20], 1)
                killed = True
                os.kill(event['pid'], signal.SIGKILL)

        with self.assertRaises(ProcessGenerationError) as caught:
            run_paced_processes(1, 100000, kill_window_factory, bootstrap,
                                start_index=20, writer_topology='writers-6',
                                on_observation=observe,
                                on_process_started=observations.process_started,
                                on_process_ready=observations.process_ready)
        self.assertTrue(killed)
        self.assertEqual(list(bootstrap['starts']), [0] * 20 + [1])
        first = caught.exception.details['first_error']
        self.assertEqual(first['lane'], 5)
        self.assertEqual(first['global_index'], 20)
        self.assertEqual(first['outcome'], 'unknown')
        summary = observations.summary()
        self.assertEqual(summary['unknown_indices'], [20])
        self.assertEqual(summary['started_indices'], [20])
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(summary['failed_count'], 0)
        self.assertEqual(summary['per_lane_requested_counts'], [0, 0, 0, 0, 0, 1])
        self.assertEqual(list(bootstrap['cleanups']), [1, 1, 1, 1, 1, 0])
        self.assertFalse(summary['worker_completion_observed'])
        self.assertFalse(summary['lifecycle_complete'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['channels_closed'])
        self.assertEqual(len(summary['children']), 6)
        self.assert_reaped(observations.started.values())

    def test_sixth_writer_startup_failure_closes_other_ready_owned_workers(self):
        context = multiprocessing.get_context('spawn')
        bootstrap = {'cleanups': context.Array('i', 6),
                     'parent_ready': [context.Event() for _ in range(6)]}
        observations = Observations()

        def ready(lane, pid, metadata):
            observations.process_ready(lane, pid, metadata)
            bootstrap['parent_ready'][lane].set()

        with self.assertRaises(ProcessGenerationError) as caught:
            run_paced_processes(24, 100000, sixth_startup_factory, bootstrap,
                                writer_topology='writers-6', on_observation=observations.observe,
                                on_process_started=observations.process_started, on_process_ready=ready)
        first = caught.exception.details['first_error']
        self.assertEqual(first['class'], 'FixtureStartupFailure')
        self.assertEqual(first['stage'], 'startup_factory')
        self.assertEqual(first['lane'], 5)
        self.assertEqual(first['outcome'], 'not_started')
        self.assertNotIn('TOP_SECRET', str(caught.exception))
        self.assertNotIn('TOP_SECRET', repr(caught.exception.details))
        self.assertEqual(set(observations.started), set(range(6)))
        self.assertEqual(set(observations.ready), set(range(5)))
        self.assertEqual(list(bootstrap['cleanups']), [1, 1, 1, 1, 1, 0])
        summary = observations.summary()
        self.assertEqual(summary['scheduled_count'], 0)
        self.assertEqual(summary['started_count'], 0)
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(summary['unscheduled_count'], 24)
        self.assertEqual(len(summary['children']), 6)
        self.assertTrue(all(child['cleanup_complete'] and child['exitcode'] == 0
                            and child['reaped'] for child in summary['children']))
        self.assertEqual(summary['children'][5]['cleanup_metadata'], {'worker_created': False})
        self.assertTrue(summary['channels_closed'])
        self.assert_reaped(observations.started.values())

    def test_writer_topology_mismatches_fail_before_spawning_any_process(self):
        cases = [
            {'lanes': 6}, {'lanes': True}, {'lanes': 4.0},
            {'writer_topology': 'writers-6', 'lanes': 4},
            {'writer_topology': 'writers-6', 'lanes': 6.0},
            {'writer_topology': 'writers-6', 'cycle_length': 6},
            {'writer_topology': 'writers-6', 'queue_capacity': 6},
            {'writer_topology': 'writers-8'}, {'writer_topology': 6},
            {'writer_topology': 'writers-6', 'bootstrap': {'profile': {}}},
            {'bootstrap': {'profile': {'writer_topology': 'writers-6',
                                       'writer_topology_version': 'writer-topology-v1'}}},
            {'writer_topology': 'writers-6', 'bootstrap': {'profile': {
                'writer_topology': 'writers-6', 'writer_topology_version': 'wrong-version'}}},
            {'writer_topology': 'writers-6', 'bootstrap': {'profile': {'writer_topology': 'writers-6'}}},
        ]
        with patch('multiprocessing.context.SpawnProcess.start',
                   side_effect=AssertionError('Mismatched writer topology started a child process')):
            for changed in cases:
                with self.subTest(changed=changed):
                    kwargs = {'count': 1, 'rate': 1, 'worker_factory': pure_worker_factory, 'bootstrap': {}}
                    kwargs.update(changed)
                    with self.assertRaises(ValueError):
                        run_paced_processes(**kwargs)

    def test_six_large_cleanup_receipts_arrive_intact_before_children_are_reaped(self):
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 6)
        payload = 'full-cleanup-receipt:' + 'x' * 4096
        observations = Observations()
        results = run_paced_processes(
            24, 100000, large_cleanup_factory,
            {'cleanups': cleanups, 'cleanup_payload': payload}, writer_topology='writers-6',
            on_observation=observations.observe,
            on_process_started=observations.process_started,
            on_process_ready=observations.process_ready)
        self.assertEqual([result['index'] for result in results], list(range(24)))
        frames = [row for row in observations.events if row['kind'] == 'cleanup_complete']
        self.assertEqual(len(frames), 6)
        self.assertEqual({row['lane'] for row in frames}, set(range(6)))
        for row in frames:
            with self.subTest(lane=row['lane']):
                self.assertEqual(row['pid'], observations.started[row['lane']])
                self.assertGreater(len(json.dumps(row, separators=(',', ':')).encode()), 2048)
                self.assertEqual(row['metadata']['receipt_payload'], payload)
                self.assertEqual(row['metadata']['lane'], row['lane'])
                self.assertEqual(row['metadata']['pid'], row['pid'])
                self.assertTrue(row['metadata']['closed'])
        summary = observations.summary()
        self.assertEqual(summary['cleanup_errors'], [])
        self.assertIsNone(summary['first_error'])
        self.assertEqual(summary['profile']['result_capacity'], 16)
        self.assertLessEqual(summary['max_output_credits'], 16)
        self.assertLessEqual(summary['max_pending_credit_requests'], 6)
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['lifecycle_complete'])
        self.assertTrue(summary['channels_closed'])
        for child in summary['children']:
            self.assertTrue(child['cleanup_complete'])
            self.assertEqual(child['cleanup_metadata']['receipt_payload'], payload)
            self.assertEqual(child['exitcode'], 0)
            self.assertTrue(child['reaped'])
        self.assertEqual(list(cleanups), [1] * 6)
        self.assert_reaped(observations.started.values())

    def test_socket_option_failure_closes_both_owned_endpoints_before_any_spawn(self):
        from benchmarks.events import process_generation
        for failure_call in range(1, 5):
            with self.subTest(failure_call=failure_call):
                original = OSError(55, 'controlled socket option failure')
                calls, closed, blocking = [], [], []
                observations = Observations()

                class OwnedEndpoint:
                    def __init__(self, name):
                        self.name = name

                    def setsockopt(self, level, option, value):
                        calls.append((self.name, level, option, value))
                        if len(calls) == failure_call:
                            raise original

                    def setblocking(self, value):
                        blocking.append((self.name, value))

                    def close(self):
                        closed.append(self.name)

                parent, child = OwnedEndpoint('parent'), OwnedEndpoint('child')
                expected = [
                    ('parent', socket.SOL_SOCKET, socket.SO_SNDBUF, 60000),
                    ('parent', socket.SOL_SOCKET, socket.SO_RCVBUF, 120000),
                    ('child', socket.SOL_SOCKET, socket.SO_SNDBUF, 60000),
                    ('child', socket.SOL_SOCKET, socket.SO_RCVBUF, 120000),
                ]
                with patch.object(socket, 'socketpair', return_value=(parent, child)) as pairs, \
                        patch.object(process_generation, '_start_owned_process') as launch:
                    with self.assertRaises(OSError) as caught:
                        self.invoke(1, {}, observations, writer_topology='writers-6')
                self.assertIs(caught.exception, original)
                pairs.assert_called_once_with(socket.AF_UNIX, socket.SOCK_DGRAM)
                launch.assert_not_called()
                self.assertEqual(calls, expected[:failure_call])
                self.assertEqual(closed, ['parent', 'child'])
                self.assertEqual(blocking, [])
                self.assertEqual(observations.started, {})
                summary = observations.summary()
                self.assertEqual(summary['children'], [])
                self.assertEqual(summary['scheduled_count'], 0)
                self.assertEqual(summary['started_count'], 0)
                self.assertTrue(summary['channels_closed'])
                self.assertFalse(summary['lifecycle_complete'])
                self.assertEqual(summary['first_error']['class'], 'OSError')
                self.assertEqual(summary['first_error']['stage'], 'scheduler')
                self.assertEqual(summary['first_error']['source'], 'parent')

    def test_cleanup_payload_above_original_frame_limit_fails_and_reaps_all_six_children(self):
        from benchmarks.events import process_generation
        self.assertEqual(process_generation.MAX_FRAME_BYTES, 60000)
        context = multiprocessing.get_context('spawn')
        cleanups = context.Array('i', 6)
        observations = Observations()
        with self.assertRaises(ProcessGenerationError) as caught:
            run_paced_processes(
                0, 100000, large_cleanup_factory,
                {'cleanups': cleanups, 'cleanup_payload': 'x' * 60001}, writer_topology='writers-6',
                on_observation=observations.observe,
                on_process_started=observations.process_started,
                on_process_ready=observations.process_ready)
        first = caught.exception.details['first_error']
        self.assertEqual(first['class'], 'ValueError')
        self.assertEqual(first['stage'], 'cleanup')
        self.assertEqual(first['outcome'], 'unknown')
        self.assertEqual(set(observations.started), set(range(6)))
        self.assertEqual(set(observations.ready), set(range(6)))
        self.assertFalse(any(row['kind'] == 'cleanup_complete' for row in observations.events))
        summary = observations.summary()
        self.assertEqual(summary['completed_count'], 0)
        self.assertEqual(summary['unknown_count'], 0)
        self.assertEqual(len(summary['children']), 6)
        self.assertTrue(all(child['exitcode'] == 0 and child['reaped'] for child in summary['children']))
        self.assertTrue(all(not child['cleanup_complete'] for child in summary['children']))
        self.assertFalse(summary['worker_completion_observed'])
        self.assertFalse(summary['lifecycle_complete'])
        self.assertTrue(summary['worker_processes_joined'])
        self.assertTrue(summary['channels_closed'])
        self.assertLessEqual(summary['max_output_credits'], 16)
        self.assertEqual(list(cleanups), [1] * 6)
        self.assert_reaped(observations.started.values())

    def test_invalid_arguments_fail_before_starting_any_child_process(self):
        cases = [
            {'count': -1}, {'count': True}, {'count': 1.5},
            {'rate': 0}, {'rate': -1}, {'rate': True}, {'rate': 'fast'},
            {'rate': math.nan}, {'rate': math.inf},
            {'worker_factory': None}, {'start_index': -1}, {'start_index': True},
            {'result_capacity': 0}, {'result_capacity': True},
            {'on_observation': 1}, {'on_process_started': 1}, {'on_process_ready': 1},
        ]
        with patch('multiprocessing.context.SpawnProcess.start',
                   side_effect=AssertionError('Invalid arguments started a child process')):
            for changed in cases:
                with self.subTest(changed=changed):
                    kwargs = {'count': 1, 'rate': 1, 'worker_factory': pure_worker_factory, 'bootstrap': {}}
                    kwargs.update(changed)
                    with self.assertRaises(ValueError):
                        run_paced_processes(**kwargs)


if __name__ == '__main__':
    unittest.main()
