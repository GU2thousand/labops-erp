"""Bounded global pacing for independent, FIFO business-command lanes.

The callback owns thread-local application resources (including Django database
connections). This module has no application, service, or network dependencies.
"""
import math
import queue
import threading
import time

from .writer_topology import DEFAULT_WRITER_PRESET, writer_profile


PROFILE_VERSION = 'parallel-lanes-v1'
DEFAULT_LANES = 4
DEFAULT_CYCLE_LENGTH = 4
DEFAULT_QUEUE_CAPACITY = 4


def paced_lanes_profile(writer_topology=DEFAULT_WRITER_PRESET):
    """Return a fresh copy of the frozen acceptance scheduling profile."""
    selected = writer_profile(writer_topology)
    return {'version': PROFILE_VERSION, 'lanes': selected['lanes'],
            'cycle_length': DEFAULT_CYCLE_LENGTH,
            'queue_capacity': DEFAULT_QUEUE_CAPACITY}


def frozen_generation_profile(writer_topology=DEFAULT_WRITER_PRESET):
    """Describe the fixed full-workload design before runtime setup begins."""
    selected = writer_profile(writer_topology)
    return {**paced_lanes_profile(writer_topology),
            'assignment': selected['assignment'],
            'position': 'global_index%4',
            'global_pacing': 'start_monotonic + scheduled_offset / requested_rate',
            'elapsed_includes_queue_wait': True,
            'elapsed_includes_drain': True,
            'worker_resource_ownership': 'independent thread-local callback connections'}


def _integer(value, description, minimum):
    if type(value) is not int or value < minimum:
        raise ValueError(description + ' must be an integer >= ' + str(minimum))
    return value


def _allocation_arguments(count, lanes, cycle_length, start_index):
    _integer(count, 'count', 0)
    _integer(lanes, 'lanes', 1)
    _integer(cycle_length, 'cycle_length', 1)
    _integer(start_index, 'start_index', 0)


def allocate_lane_indices(count, *, lanes=DEFAULT_LANES,
                          cycle_length=DEFAULT_CYCLE_LENGTH, start_index=0):
    """Assign whole command cycles to lanes before application journals begin."""
    _allocation_arguments(count, lanes, cycle_length, start_index)
    allocated = [[] for _ in range(lanes)]
    for global_index in range(start_index, start_index + count):
        allocated[(global_index // cycle_length) % lanes].append(global_index)
    return tuple(tuple(indices) for indices in allocated)


def run_paced_lanes(count, rate, execute, *, lanes=DEFAULT_LANES,
                    cycle_length=DEFAULT_CYCLE_LENGTH,
                    queue_capacity=DEFAULT_QUEUE_CAPACITY, start_index=0,
                    monotonic=time.monotonic, sleep=time.sleep,
                    on_observation=None, on_lane_shutdown=None,
                    writer_topology=DEFAULT_WRITER_PRESET):
    """Return callback results in global-index order after every worker joins.

    Global index ``start_index + i`` targets ``start_time + i / rate`` and maps
    to lane ``(global_index // cycle_length) % lanes``. One worker per lane
    preserves each complete command cycle's FIFO order. Capacity waits and all
    in-flight completion belong to the measured duration; enqueue completion is
    never a throughput result.

    A start is atomically claimed under the same lock as the first-failure stop
    gate. Work already claimed is in flight and may finish; queued work is
    cancelled, and no further starts are claimed. The original exception object
    is raised only after every worker joins. Optional observations are serialized
    across threads and finish with an accounting summary, including partial
    scheduling when a callback fails. An observer error also stops scheduling
    and is preserved unless an earlier callback error already caused the stop.
    Optional lane shutdown runs in that lane's own thread after its final work;
    every worker's cleanup finishes before joining, including after failures.
    """
    selected = writer_profile(writer_topology)
    if writer_topology != DEFAULT_WRITER_PRESET and (lanes, cycle_length, queue_capacity) != (selected['lanes'], 4, 4):
        raise ValueError('Selected writer topology dimensions changed')
    _allocation_arguments(count, lanes, cycle_length, start_index)
    _integer(queue_capacity, 'queue_capacity', 1)
    if isinstance(rate, bool) or not isinstance(rate, (int, float)):
        raise ValueError('rate must be finite and positive')
    try:
        rate = float(rate)
    except (OverflowError, ValueError) as error:
        raise ValueError('rate must be finite and positive') from error
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError('rate must be finite and positive')
    for value, description in ((execute, 'execute'), (monotonic, 'monotonic'),
                               (sleep, 'sleep')):
        if not callable(value):
            raise ValueError(description + ' must be callable')
    if on_observation is not None and not callable(on_observation):
        raise ValueError('on_observation must be callable or None')
    if on_lane_shutdown is not None and not callable(on_lane_shutdown):
        raise ValueError('on_lane_shutdown must be callable or None')

    allocation = allocate_lane_indices(count, lanes=lanes,
                                       cycle_length=cycle_length,
                                       start_index=start_index)
    queues = [queue.Queue(maxsize=queue_capacity) for _ in range(lanes)]
    condition = threading.Condition()
    observation_lock = threading.Lock()
    stopped = threading.Event()
    scheduling_done = threading.Event()
    # A join interrupted by Python's main-thread signal handling may mark the
    # Thread stopped while its target is still running. Only the worker itself
    # can attest that callback work and thread-local cleanup have really ended.
    worker_done = [threading.Event() for _ in range(lanes)]
    scheduled = set()
    started = set()
    failed = set()
    cancelled = set()
    results = {}
    first_error = None
    error_origin = None
    first_failed_index = None
    max_queue_depths = [0] * lanes
    queue_wait_seconds = 0.0
    queue_wait_count = 0
    lane_shutdowns = {}
    started_at = monotonic()

    def stop_with_error(error, origin, global_index=None):
        nonlocal first_error, error_origin, first_failed_index
        with condition:
            if first_error is None:
                first_error = error
                error_origin = origin
                first_failed_index = global_index
            stopped.set()
            condition.notify_all()

    def observe(event):
        if on_observation is None:
            return
        try:
            with observation_lock:
                on_observation(event)
        except BaseException as error:
            stop_with_error(error, 'observation', event.get('global_index'))

    def event(kind, lane, global_index, target, observed_at=None, **extra):
        return {'kind': kind, 'lane': lane, 'global_index': global_index,
                'position': global_index % cycle_length,
                'target_monotonic': target,
                'observed_monotonic': monotonic() if observed_at is None else observed_at,
                **extra}

    def process_lane(lane):
        work_queue = queues[lane]
        while True:
            with condition:
                while work_queue.empty():
                    if scheduling_done.is_set():
                        return
                    condition.wait(timeout=.05)
                global_index, target = work_queue.get_nowait()
                condition.notify_all()
                observed_at = monotonic()
                if stopped.is_set():
                    cancelled.add(global_index)
                    disposition = 'cancelled'
                else:
                    started.add(global_index)
                    disposition = 'started'
            try:
                observe(event(disposition, lane, global_index, target, observed_at))
                if disposition == 'cancelled':
                    continue
                try:
                    result = execute(lane, global_index, target)
                except BaseException as error:
                    # Publish failure and close the start gate atomically.
                    with condition:
                        failed.add(global_index)
                        stop_with_error(error, 'execute', global_index)
                    observe(event('failed', lane, global_index, target,
                                  error_type=type(error).__name__))
                else:
                    with condition:
                        results[global_index] = result
                    observe(event('completed', lane, global_index, target))
            finally:
                work_queue.task_done()

    def worker(lane):
        try:
            try:
                process_lane(lane)
            finally:
                if on_lane_shutdown is not None:
                    try:
                        on_lane_shutdown(lane)
                    except BaseException as error:
                        stop_with_error(error, 'lane_shutdown')
                        with condition:
                            lane_shutdowns[lane] = {'passed': False,
                                                    'error_type': type(error).__name__}
                    else:
                        with condition:
                            lane_shutdowns[lane] = {'passed': True, 'error_type': None}
                    observe({'kind': 'lane_shutdown', 'lane': lane,
                             'observed_monotonic': monotonic(), **lane_shutdowns[lane]})
        finally:
            worker_done[lane].set()

    workers = []
    try:
        if count:
            for lane in range(lanes):
                thread = threading.Thread(target=worker, args=(lane,),
                                          name='paced-business-lane-' + str(lane))
                thread.start()
                workers.append(thread)
        for offset in range(count):
            global_index = start_index + offset
            lane = (global_index // cycle_length) % lanes
            target = started_at + offset / rate
            while not stopped.is_set():
                remaining = target - monotonic()
                if remaining <= 0:
                    break
                sleep(min(remaining, .05))
            if stopped.is_set():
                break
            wait_started = None
            enqueued = False
            with condition:
                while not stopped.is_set():
                    if not queues[lane].full():
                        queues[lane].put_nowait((global_index, target))
                        scheduled.add(global_index)
                        observed_at = monotonic()
                        max_queue_depths[lane] = max(max_queue_depths[lane], queues[lane].qsize())
                        enqueued = True
                        condition.notify_all()
                        break
                    if wait_started is None:
                        wait_started = monotonic()
                        queue_wait_count += 1
                    condition.wait(timeout=.05)
                if wait_started is not None:
                    waited = max(0.0, monotonic() - wait_started)
                    queue_wait_seconds += waited
                else:
                    waited = 0.0
            if wait_started is not None:
                observe(event('queue_wait', lane, global_index, target,
                              duration_seconds=waited, enqueued=enqueued))
            if not enqueued:
                break
            observe(event('scheduled', lane, global_index, target, observed_at))
    except BaseException as error:
        stop_with_error(error, 'scheduler')
    finally:
        while True:
            try:
                scheduling_done.set()
                with condition:
                    condition.notify_all()
                for lane in range(len(workers)):
                    while not worker_done[lane].is_set():
                        try:
                            worker_done[lane].wait(timeout=.05)
                        except BaseException as error:
                            stop_with_error(error, 'drain_interrupt')
                # All application work and cleanup have ended before any join.
                # Preserve a join-time interrupt and finish every actual join.
                for thread in workers:
                    while True:
                        try:
                            thread.join(timeout=.05)
                        except BaseException as error:
                            stop_with_error(error, 'join_interrupt')
                            continue
                        if not thread.is_alive():
                            break
                break
            except BaseException as error:
                # Also cover interruption of notification or state inspection,
                # rather than allowing an unfinished drain to escape finally.
                stop_with_error(error, 'drain_interrupt')

    ended_at = monotonic()
    duration = max(0.0, ended_at - started_at)
    completed = set(results)
    per_lane = []
    for lane, requested in enumerate(allocation):
        assigned = set(requested)
        per_lane.append({'lane': lane, 'requested_count': len(assigned),
                         'scheduled_count': len(scheduled & assigned),
                         'started_count': len(started & assigned),
                         'completed_count': len(completed & assigned),
                         'failed_count': len(failed & assigned),
                         'cancelled_count': len(cancelled & assigned)})
    actual_profile = {**frozen_generation_profile(writer_topology), 'lanes': lanes,
                      'cycle_length': cycle_length, 'queue_capacity': queue_capacity,
                      'assignment': f'(global_index//{cycle_length})%{lanes}',
                      'position': f'global_index%{cycle_length}'}
    summary = {'kind': 'summary', 'profile': actual_profile,
               'frozen_acceptance_profile': frozen_generation_profile(writer_topology),
               'lanes': lanes, 'cycle_length': cycle_length,
               'queue_capacity': queue_capacity, 'start_index': start_index,
               'requested_count': count, 'scheduled_count': len(scheduled),
               'requested_rate': rate,
               'nominal_per_lane_rate': rate / lanes,
               'nominal_per_lane_rate_basis': 'requested global rate / lanes; average, not independent pacing',
               'started_count': len(started), 'completed_count': len(completed),
               'failed_count': len(failed), 'cancelled_count': len(cancelled),
               'unscheduled_count': count - len(scheduled),
               'started_monotonic': started_at, 'ended_monotonic': ended_at,
               'duration_seconds': duration,
               'actual_rate': len(completed) / duration if duration else 0.0,
               'queue_wait_count': queue_wait_count,
               'queue_wait_seconds': queue_wait_seconds,
               'max_queue_depths': max_queue_depths,
               'worker_threads_joined': all(not thread.is_alive() for thread in workers),
               'worker_completion_observed': all(worker_done[lane].is_set()
                                                 for lane in range(len(workers))),
               'lane_shutdowns': [{'lane': lane, **lane_shutdowns[lane]}
                                  for lane in sorted(lane_shutdowns)],
               'first_failed_index': first_failed_index, 'error_origin': error_origin,
               'error_type': type(first_error).__name__ if first_error is not None else None,
               'scheduled_indices': sorted(scheduled), 'started_indices': sorted(started),
               'completed_indices': sorted(completed), 'failed_indices': sorted(failed),
               'cancelled_indices': sorted(cancelled), 'per_lane': per_lane,
               'passed': first_error is None and len(completed) == count}
    observe(summary)
    if first_error is not None:
        raise first_error
    return [results[global_index] for global_index in range(start_index, start_index + count)]
