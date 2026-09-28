"""Pure pacing, concurrency and failure accounting contracts for load lanes."""
import math
import threading
import time
import unittest
from unittest.mock import patch

from benchmarks.events.concurrent_generation import (
    allocate_lane_indices, paced_lanes_profile, run_paced_lanes,
)


class Clock:
    def __init__(self, initial=0.0, gate_target=None):
        self.value = initial
        self.lock = threading.Lock()
        self.gate_target = gate_target
        self.gate_entered = threading.Event()
        self.gate_release = threading.Event()

    def monotonic(self):
        with self.lock:
            return self.value

    def sleep(self, duration):
        with self.lock:
            target = self.value + duration
        if self.gate_target is not None and target >= self.gate_target - 1e-9:
            self.gate_entered.set()
            if not self.gate_release.wait(3):
                raise AssertionError('Test pacing gate was not released')
        with self.lock:
            self.value += duration


class Observations:
    def __init__(self):
        self.events = []
        self.lock = threading.Lock()
        self.failed = threading.Event()
        self.second_scheduled = threading.Event()

    def __call__(self, event):
        with self.lock:
            self.events.append(dict(event))
        if event['kind'] == 'failed':
            self.failed.set()
        if event['kind'] == 'scheduled' and event['global_index'] == 1:
            self.second_scheduled.set()

    def summaries(self):
        with self.lock:
            return [event for event in self.events if event['kind'] == 'summary']


def background_run(function):
    outcome = {}

    def invoke():
        try:
            outcome['result'] = function()
        except BaseException as exc:
            outcome['exception'] = exc

    thread = threading.Thread(target=invoke, name='concurrent-generation-test-controller')
    thread.start()
    return thread, outcome


class ConcurrentGenerationTests(unittest.TestCase):
    def test_default_profile_is_frozen_and_explicit(self):
        self.assertEqual(paced_lanes_profile(), {
            'version': 'parallel-lanes-v1', 'lanes': 4,
            'cycle_length': 4, 'queue_capacity': 4,
        })

    def test_allocation_keeps_each_four_event_cycle_in_one_lane(self):
        self.assertEqual(allocate_lane_indices(20), (
            (0, 1, 2, 3, 16, 17, 18, 19),
            (4, 5, 6, 7), (8, 9, 10, 11), (12, 13, 14, 15),
        ))
        self.assertEqual(allocate_lane_indices(0), ((), (), (), ()))

    def test_allocation_preserves_global_offset_fifo_and_complete_cycle_mix(self):
        allocated = allocate_lane_indices(18, lanes=3, cycle_length=4, start_index=3)
        self.assertEqual(sorted(index for lane in allocated for index in lane), list(range(3, 21)))
        for lane, indices in enumerate(allocated):
            self.assertIsInstance(indices, tuple)
            self.assertEqual(indices, tuple(sorted(indices)))
            self.assertTrue(all((index // 4) % 3 == lane for index in indices))
        for cycle in range(1, 5):
            lane = cycle % 3
            self.assertEqual([index % 4 for index in allocated[lane] if index // 4 == cycle],
                             [0, 1, 2, 3])

    def test_callback_results_and_each_lane_are_in_global_fifo_order(self):
        clock = Clock(100)
        seen = {0: [], 1: []}
        lock = threading.Lock()
        observations = Observations()

        def execute(lane, index, target):
            with lock:
                seen[lane].append(index)
            return {'index': index, 'lane': lane}

        result = run_paced_lanes(20, 1000, execute, lanes=2, cycle_length=2,
                                start_index=9, monotonic=clock.monotonic, sleep=clock.sleep,
                                on_observation=observations)
        self.assertEqual([value['index'] for value in result], list(range(9, 29)))
        self.assertEqual(tuple(tuple(seen[lane]) for lane in range(2)),
                         allocate_lane_indices(20, lanes=2, cycle_length=2, start_index=9))
        for event in observations.events:
            if event['kind'] in ('scheduled', 'started', 'completed'):
                self.assertEqual(event['lane'], (event['global_index'] // 2) % 2)
                self.assertEqual(event['position'], event['global_index'] % 2)
        summary, = observations.summaries()
        actual = summary['profile']
        frozen = summary['frozen_acceptance_profile']
        self.assertEqual((actual['lanes'], actual['cycle_length'], actual['queue_capacity']),
                         (2, 2, 4))
        self.assertEqual(actual['assignment'], '(global_index//2)%2')
        self.assertEqual(actual['position'], 'global_index%2')
        self.assertEqual((frozen['lanes'], frozen['cycle_length'], frozen['queue_capacity']),
                         (4, 4, 4))
        self.assertEqual(frozen['assignment'], '(global_index//4)%4')
        self.assertEqual(frozen['position'], 'global_index%4')
        self.assertEqual(summary['nominal_per_lane_rate'], 500)
        self.assertIn('requested global rate / lanes', summary['nominal_per_lane_rate_basis'])
        self.assertIn('average', summary['nominal_per_lane_rate_basis'])

    def test_global_pacing_uses_one_target_sequence_across_all_lanes(self):
        clock = Clock(100)
        observations = Observations()
        result = run_paced_lanes(12, 4, lambda lane, index, target: index,
                                monotonic=clock.monotonic, sleep=clock.sleep,
                                on_observation=observations)
        self.assertEqual(result, list(range(12)))
        scheduled = sorted((event for event in observations.events if event['kind'] == 'scheduled'),
                           key=lambda event: event['global_index'])
        self.assertEqual(len(scheduled), 12)
        self.assertAlmostEqual(scheduled[0]['target_monotonic'], 100)
        for previous, current in zip(scheduled, scheduled[1:]):
            self.assertAlmostEqual(current['target_monotonic'] - previous['target_monotonic'], .25)
        for event in observations.events:
            if event['kind'] == 'started':
                self.assertGreaterEqual(event['observed_monotonic'] + 1e-9, event['target_monotonic'])

    def test_independent_lanes_overlap_and_all_worker_threads_are_joined(self):
        rendezvous = threading.Barrier(4)
        threads = set()
        lock = threading.Lock()
        observations = Observations()

        def execute(lane, index, target):
            with lock:
                threads.add(threading.current_thread())
            rendezvous.wait(timeout=3)
            return index

        result = run_paced_lanes(4, 10000, execute, cycle_length=1,
                                on_observation=observations)
        self.assertEqual(result, [0, 1, 2, 3])
        self.assertEqual(len(threads), 4, 'Four callbacks must enter concurrently')
        self.assertTrue(all(not thread.is_alive() for thread in threads))
        summary, = observations.summaries()
        self.assertTrue(summary['worker_threads_joined'])
        self.assertEqual(summary['completed_count'], 4)

    def test_bounded_queue_backpressure_and_inflight_drain_are_in_summary(self):
        first_entered = threading.Event()
        first_release = threading.Event()
        last_entered = threading.Event()
        last_release = threading.Event()
        observations = Observations()

        def execute(lane, index, target):
            if index == 0:
                first_entered.set()
                if not first_release.wait(3):
                    raise AssertionError('First callback gate was not released')
            if index == 4:
                last_entered.set()
                if not last_release.wait(3):
                    raise AssertionError('Drain callback gate was not released')
            return index

        thread, outcome = background_run(lambda: run_paced_lanes(
            5, 100000, execute, lanes=1, cycle_length=1, queue_capacity=1,
            on_observation=observations))
        try:
            self.assertTrue(first_entered.wait(2))
            self.assertTrue(observations.second_scheduled.wait(2))
            time.sleep(.025)
            first_release.set()
            self.assertTrue(last_entered.wait(2))
            self.assertTrue(thread.is_alive(), 'The generator must wait for in-flight drain')
            time.sleep(.025)
        finally:
            first_release.set()
            last_release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertNotIn('exception', outcome)
        self.assertEqual(outcome['result'], [0, 1, 2, 3, 4])
        summary, = observations.summaries()
        self.assertEqual(summary['max_queue_depths'], [1])
        self.assertGreaterEqual(summary['queue_wait_count'], 1)
        self.assertGreater(summary['queue_wait_seconds'], 0)
        self.assertTrue(any(event['kind'] == 'queue_wait' for event in observations.events))
        self.assertGreaterEqual(summary['duration_seconds'], .05)
        self.assertAlmostEqual(summary['actual_rate'], 5 / summary['duration_seconds'])
        self.assertTrue(summary['worker_threads_joined'])

    def test_failure_stops_new_starts_drains_inflight_and_preserves_exception(self):
        clock = Clock(gate_target=.8)
        observations = Observations()
        original = RuntimeError('original execution failure')
        failed_entered = threading.Event()
        fail_release = threading.Event()
        inflight_entered = threading.Event()
        inflight_release = threading.Event()
        inflight_completed = threading.Event()
        starts = []
        workers = set()
        lock = threading.Lock()

        def execute(lane, index, target):
            with lock:
                starts.append(index)
                workers.add(threading.current_thread())
            if index == 0:
                failed_entered.set()
                if not fail_release.wait(3):
                    raise AssertionError('Failure gate was not released')
                raise original
            if index == 4:
                inflight_entered.set()
                if not inflight_release.wait(3):
                    raise AssertionError('In-flight gate was not released')
                inflight_completed.set()
                return index
            raise AssertionError('Queued work started after the failure stop gate')

        thread, outcome = background_run(lambda: run_paced_lanes(
            12, 10, execute, lanes=2, cycle_length=4, queue_capacity=4,
            monotonic=clock.monotonic, sleep=clock.sleep, on_observation=observations))
        try:
            self.assertTrue(clock.gate_entered.wait(2), 'Scheduling must pause before index 8')
            self.assertTrue(failed_entered.wait(2))
            self.assertTrue(inflight_entered.wait(2))
            fail_release.set()
            self.assertTrue(observations.failed.wait(2))
            clock.gate_release.set()
            self.assertTrue(thread.is_alive(), 'Original exception must wait for in-flight completion')
            self.assertFalse(inflight_completed.is_set())
        finally:
            fail_release.set()
            clock.gate_release.set()
            inflight_release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertIs(outcome.get('exception'), original)
        self.assertTrue(inflight_completed.is_set())
        self.assertEqual(sorted(starts), [0, 4])
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        summary, = observations.summaries()
        expected = {'requested_count': 12, 'scheduled_count': 8, 'started_count': 2,
                    'completed_count': 1, 'failed_count': 1, 'cancelled_count': 6,
                    'unscheduled_count': 4}
        for field, value in expected.items():
            self.assertEqual(summary[field], value, field)
        self.assertEqual(summary['first_failed_index'], 0)
        self.assertTrue(summary['worker_threads_joined'])
        self.assertEqual(summary['requested_count'], summary['completed_count'] +
                         summary['failed_count'] + summary['cancelled_count'] +
                         summary['unscheduled_count'])

    def test_zero_count_emits_zero_summary_without_creating_workers(self):
        observations = Observations()
        cleanups = []
        with patch('threading.Thread', side_effect=AssertionError('Unexpected worker creation')):
            self.assertEqual(run_paced_lanes(0, 1, lambda *args: None,
                                            on_observation=observations,
                                            on_lane_shutdown=cleanups.append), [])
        self.assertEqual(cleanups, [], 'A zero-count run has no lane resources to clean up')
        summary, = observations.summaries()
        for field in ('requested_count', 'scheduled_count', 'started_count', 'completed_count',
                      'failed_count', 'cancelled_count', 'unscheduled_count', 'actual_rate'):
            self.assertEqual(summary[field], 0, field)

    def test_cleanup_runs_once_per_lane_in_its_worker_after_final_callback(self):
        completed = {lane: [] for lane in range(4)}
        worker_threads = {}
        cleanups = []
        lock = threading.Lock()
        observations = Observations()
        main_thread = threading.current_thread()

        def execute(lane, index, target):
            with lock:
                worker_threads[lane] = threading.current_thread()
                completed[lane].append(index)
            return index

        def cleanup(lane):
            with lock:
                cleanups.append((lane, threading.current_thread(), tuple(completed[lane])))

        result = run_paced_lanes(8, 10000, execute, cycle_length=1,
                                on_lane_shutdown=cleanup, on_observation=observations)
        self.assertEqual(result, list(range(8)))
        self.assertEqual(sorted(lane for lane, _, _ in cleanups), [0, 1, 2, 3])
        for lane, thread, finished in cleanups:
            self.assertIs(thread, worker_threads[lane])
            self.assertIsNot(thread, main_thread)
            self.assertEqual(finished, (lane, lane + 4))
            self.assertFalse(thread.is_alive())
        summary, = observations.summaries()
        self.assertTrue(summary['worker_threads_joined'])
        self.assertEqual(summary['completed_count'], 8)

    def test_cleanup_failure_is_preserved_after_all_other_cleanups_and_join(self):
        rendezvous = threading.Barrier(4)
        original = RuntimeError('original cleanup failure')
        last_cleanup_entered = threading.Event()
        last_cleanup_release = threading.Event()
        cleaned = []
        workers = set()
        lock = threading.Lock()
        observations = Observations()

        def execute(lane, index, target):
            with lock:
                workers.add(threading.current_thread())
            rendezvous.wait(timeout=3)
            return index

        def cleanup(lane):
            if lane == 3:
                last_cleanup_entered.set()
                if not last_cleanup_release.wait(3):
                    raise AssertionError('Final cleanup gate was not released')
            with lock:
                cleaned.append(lane)
            if lane == 0:
                raise original

        thread, outcome = background_run(lambda: run_paced_lanes(
            4, 10000, execute, cycle_length=1, on_lane_shutdown=cleanup,
            on_observation=observations))
        try:
            self.assertTrue(last_cleanup_entered.wait(2))
            self.assertTrue(thread.is_alive(), 'Raising the cleanup error must wait for all cleanups')
            self.assertNotIn('exception', outcome)
        finally:
            last_cleanup_release.set()
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertIs(outcome.get('exception'), original)
        self.assertEqual(sorted(cleaned), [0, 1, 2, 3])
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        summary, = observations.summaries()
        self.assertEqual(summary['completed_count'], 4)
        self.assertTrue(summary['worker_threads_joined'])

    def test_execution_exception_wins_over_subsequent_cleanup_failures(self):
        rendezvous = threading.Barrier(4)
        original = RuntimeError('original callback failure')
        cleanup_error = RuntimeError('later cleanup failure')
        cleaned = []
        workers = set()
        lock = threading.Lock()
        observations = Observations()

        def execute(lane, index, target):
            with lock:
                workers.add(threading.current_thread())
            rendezvous.wait(timeout=3)
            if index == 0:
                raise original
            return index

        def cleanup(lane):
            if not observations.failed.wait(3):
                raise AssertionError('Callback failure must close the stop gate before cleanup errors')
            with lock:
                cleaned.append(lane)
            raise cleanup_error

        with self.assertRaises(RuntimeError) as captured:
            run_paced_lanes(4, 10000, execute, cycle_length=1,
                            on_lane_shutdown=cleanup, on_observation=observations)
        self.assertIs(captured.exception, original)
        self.assertEqual(sorted(cleaned), [0, 1, 2, 3])
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        summary, = observations.summaries()
        self.assertEqual(summary['failed_count'], 1)
        self.assertEqual(summary['completed_count'], 3)
        self.assertTrue(summary['worker_threads_joined'])

    def test_main_drain_interrupt_waits_for_inflight_work_cleanup_and_join(self):
        original = KeyboardInterrupt('original drain interrupt')
        entered = [threading.Event() for _ in range(4)]
        release = threading.Event()
        interrupt_captured = threading.Event()
        interrupted = False
        observations = Observations()
        journal = []
        workers = set()
        lock = threading.Lock()
        real_wait = threading.Event.wait

        def execute(lane, index, target):
            with lock:
                journal.append(('started', lane, index))
                workers.add(threading.current_thread())
            entered[lane].set()
            if not release.wait(3):
                raise AssertionError('In-flight callbacks were not released')
            with lock:
                journal.append(('completed', lane, index))
            return index

        def cleanup(lane):
            with lock:
                journal.append(('cleanup', lane, None))

        def interrupt_wait(event, timeout=None):
            nonlocal interrupted
            if threading.current_thread().name == 'concurrent-generation-test-controller' and timeout == .05:
                if not interrupted:
                    for entry in entered:
                        if not real_wait(entry, 2):
                            raise AssertionError('All four initial callbacks must be in flight before interruption')
                    interrupted = True
                    raise original
                # A second drain wait proves the first interrupt was caught and
                # its stop gate closed before the test releases any callback.
                interrupt_captured.set()
            return real_wait(event, timeout)

        with patch('threading.Event.wait', new=interrupt_wait):
            thread, outcome = background_run(lambda: run_paced_lanes(
                8, 100000, execute, cycle_length=1, on_lane_shutdown=cleanup,
                on_observation=observations))
            try:
                self.assertTrue(interrupt_captured.wait(2))
                self.assertTrue(thread.is_alive(), 'Drain interruption must not abandon live callbacks')
                self.assertEqual(outcome, {}, 'The interrupt cannot escape before real worker completion')
                with lock:
                    self.assertEqual(sorted(index for kind, _, index in journal if kind == 'started'),
                                     [0, 1, 2, 3])
                    self.assertFalse(any(kind == 'cleanup' for kind, _, _ in journal))
            finally:
                release.set()
                thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertIs(outcome.get('exception'), original)
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        self.assertEqual(sorted(index for kind, _, index in journal if kind == 'started'), [0, 1, 2, 3])
        self.assertEqual(sorted(index for kind, _, index in journal if kind == 'completed'), [0, 1, 2, 3])
        self.assertEqual(sorted(lane for kind, lane, _ in journal if kind == 'cleanup'), [0, 1, 2, 3])
        self.assertEqual(len(journal), 12, 'No queued callback or journal work can start after the stop gate')
        summary, = observations.summaries()
        self.assertEqual(summary['completed_count'], 4)
        self.assertEqual(summary['cancelled_count'], 4)
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_threads_joined'])

    def test_prior_execution_error_wins_over_later_main_drain_interrupt(self):
        original = RuntimeError('first callback failure')
        later_interrupt = KeyboardInterrupt('later main drain interrupt')
        rendezvous = threading.Barrier(4)
        release = threading.Event()
        interrupt_captured = threading.Event()
        interrupted = False
        observations = Observations()
        cleanups = []
        workers = set()
        lock = threading.Lock()
        real_wait = threading.Event.wait

        def execute(lane, index, target):
            with lock:
                workers.add(threading.current_thread())
            rendezvous.wait(timeout=3)
            if index == 0:
                raise original
            if not release.wait(3):
                raise AssertionError('Surviving callbacks were not released')
            return index

        def cleanup(lane):
            with lock:
                cleanups.append(lane)

        def interrupt_wait(event, timeout=None):
            nonlocal interrupted
            if threading.current_thread().name == 'concurrent-generation-test-controller' and timeout == .05:
                if not interrupted:
                    if not real_wait(observations.failed, 2):
                        raise AssertionError('The callback error must close the stop gate first')
                    interrupted = True
                    raise later_interrupt
                interrupt_captured.set()
            return real_wait(event, timeout)

        with patch('threading.Event.wait', new=interrupt_wait):
            thread, outcome = background_run(lambda: run_paced_lanes(
                4, 100000, execute, cycle_length=1, on_lane_shutdown=cleanup,
                on_observation=observations))
            try:
                self.assertTrue(interrupt_captured.wait(2))
                self.assertTrue(thread.is_alive())
                self.assertEqual(outcome, {})
            finally:
                release.set()
                thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertIs(outcome.get('exception'), original)
        self.assertEqual(sorted(cleanups), [0, 1, 2, 3])
        self.assertTrue(all(not worker.is_alive() for worker in workers))
        summary, = observations.summaries()
        self.assertEqual(summary['failed_count'], 1)
        self.assertEqual(summary['completed_count'], 3)
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_threads_joined'])

    def test_join_interrupt_is_preserved_and_every_real_worker_is_joined(self):
        original = KeyboardInterrupt('original join interrupt')
        rendezvous = threading.Barrier(4)
        cleanups = []
        completed = []
        join_calls = []
        snapshot_at_interrupt = {}
        interrupted = False
        lock = threading.Lock()
        observations = Observations()
        real_join = threading.Thread.join

        def execute(lane, index, target):
            rendezvous.wait(timeout=3)
            with lock:
                completed.append(index)
            return index

        def cleanup(lane):
            with lock:
                cleanups.append(lane)

        def interrupt_join(worker, timeout=None):
            nonlocal interrupted
            if threading.current_thread().name == 'concurrent-generation-test-controller' and timeout == .05:
                join_calls.append(worker)
                if not interrupted:
                    interrupted = True
                    with lock:
                        snapshot_at_interrupt.update(cleanups=list(cleanups), completed=list(completed))
                    raise original
            return real_join(worker, timeout)

        with patch('threading.Thread.join', new=interrupt_join):
            thread, outcome = background_run(lambda: run_paced_lanes(
                4, 100000, execute, cycle_length=1, on_lane_shutdown=cleanup,
                on_observation=observations))
            thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertIs(outcome.get('exception'), original)
        self.assertEqual(sorted(snapshot_at_interrupt['cleanups']), [0, 1, 2, 3])
        self.assertEqual(sorted(snapshot_at_interrupt['completed']), [0, 1, 2, 3])
        self.assertEqual(len(set(join_calls)), 4)
        self.assertGreaterEqual(join_calls.count(join_calls[0]), 2, 'Interrupted join must be retried')
        self.assertTrue(all(not worker.is_alive() for worker in join_calls))
        summary, = observations.summaries()
        self.assertTrue(summary['worker_completion_observed'])
        self.assertTrue(summary['worker_threads_joined'])

    def test_invalid_allocation_arguments_fail_before_thread_creation(self):
        cases = [(-1, {}), (True, {}), (1.5, {}), (1, {'lanes': 0}),
                 (1, {'cycle_length': 0}), (1, {'start_index': -1}),
                 (1, {'lanes': True}), (1, {'cycle_length': 1.5}),
                 (1, {'start_index': 1.5})]
        with patch('threading.Thread', side_effect=AssertionError('Unexpected worker creation')):
            for count, kwargs in cases:
                with self.subTest(count=count, kwargs=kwargs):
                    with self.assertRaises(ValueError):
                        allocate_lane_indices(count, **kwargs)

    def test_invalid_runner_arguments_fail_before_thread_creation(self):
        cases = [
            {'count': -1}, {'count': True}, {'count': 1.5},
            {'rate': 0}, {'rate': -1}, {'rate': True}, {'rate': 'fast'},
            {'rate': math.nan}, {'rate': math.inf}, {'rate': -math.inf},
            {'lanes': 0}, {'lanes': True}, {'cycle_length': 0},
            {'queue_capacity': 0}, {'queue_capacity': 1.5}, {'queue_capacity': True},
            {'start_index': -1}, {'start_index': True},
            {'execute': None}, {'monotonic': None}, {'sleep': None}, {'on_observation': 1},
            {'on_lane_shutdown': 1},
        ]
        with patch('threading.Thread', side_effect=AssertionError('Unexpected worker creation')):
            for changed in cases:
                with self.subTest(changed=changed):
                    kwargs = {'count': 1, 'rate': 1, 'execute': lambda *args: None}
                    kwargs.update(changed)
                    with self.assertRaises(ValueError):
                        run_paced_lanes(**kwargs)


if __name__ == '__main__':
    unittest.main()
