"""Selected fresh, bounded spawn clients for a globally paced command batch.

The caller supplies an importable factory and a private bootstrap dictionary.
Application connections and durable journals belong to the resulting child
worker. This driver never logs bootstrap data, interprets missing acknowledgments
as rollbacks, or replays a command. Unix datagrams are local IPC: one atomic
frame per status/result prevents a killed writer corrupting a shared pipe.
"""
import json
import math
import multiprocessing
import os
import pickle
import queue
import re
import signal
import socket
import time

from .concurrent_generation import allocate_lane_indices
from .writer_topology import DEFAULT_WRITER_PRESET, resolve_profile_writer, writer_profile


PROFILE_VERSION = 'spawn-lanes-v1'
LANES = 4
CYCLE_LENGTH = 4
QUEUE_CAPACITY = 4
RESULT_CAPACITY = 16
FAILURE_CLEANUP_SECONDS = 30.0
TERM_GRACE_SECONDS = 5.0
MAX_FRAME_BYTES = 60000
_SAFE_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_.-]{0,127}\Z')
_OUTCOMES = {'committed', 'rolled_back', 'unknown', 'not_started'}
_PRIVATE_KEYS = {'password', 'credentials', 'database_config', 'bootstrap',
                 'environment', 'env', 'query', 'sql', 'traceback', 'message',
                 'database_url', 'connection_uri', 'dsn'}


class ProcessGenerationError(RuntimeError):
    """A sanitized child/protocol failure; original error messages are private."""

    def __init__(self, details):
        self.details = details
        first = details.get('first_error', details)
        self.stage = first.get('stage', 'execute')
        self.outcome = first.get('outcome', 'unknown')
        self.authored_class = first.get('class', type(self).__name__)
        super().__init__('Process generation failed at ' + _name(self.stage, 'unknown')
                         + ' (' + _name(self.authored_class, 'ProcessGenerationError') + ')')


def _process_profile(writer):
    return {'version': PROFILE_VERSION, 'lanes': writer['lanes'],
            'cycle_length': CYCLE_LENGTH, 'queue_capacity': QUEUE_CAPACITY,
            'result_capacity': RESULT_CAPACITY, 'start_method': 'spawn',
            'assignment': writer['assignment'], 'position': 'global_index%4',
            'writer_topology': writer['preset'],
            'writer_topology_version': writer['version'],
            'global_pacing': 'ready_monotonic + scheduled_offset / requested_rate',
            'elapsed_includes_spawn_and_readiness': True,
            'elapsed_includes_queue_wait': True, 'elapsed_includes_drain': True,
            'elapsed_includes_cleanup_and_reaping': True,
            'failure_cleanup_seconds': FAILURE_CLEANUP_SECONDS,
            'failure_term_grace_seconds': TERM_GRACE_SECONDS,
            'lost_result_outcome': 'unknown', 'automatic_replay': False}


def frozen_process_generation_profile(*, writer_topology=DEFAULT_WRITER_PRESET):
    return _process_profile(writer_profile(writer_topology))


frozen_process_profile = frozen_process_generation_profile


def _name(value, fallback):
    return value if isinstance(value, str) and _SAFE_NAME.fullmatch(value) else fallback


def _payload(value, depth=0):
    """Accept authored JSON evidence, rejecting objects and credential fields."""
    if depth > 12:
        raise ValueError('Evidence nesting exceeds the protocol limit')
    if value is None or type(value) in (bool, int, str):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    if isinstance(value, (tuple, list)):
        return [_payload(item, depth + 1) for item in value]
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            if not isinstance(key, str) or key.lower() in _PRIVATE_KEYS:
                raise ValueError('Evidence contains a private or invalid field')
            if any(word in key.lower() for word in ('password', 'secret', 'credential')):
                raise ValueError('Evidence contains a private field')
            result[key] = _payload(item, depth + 1)
        return result
    raise ValueError('Evidence must contain finite JSON values')


def _error_details(error, stage, lane=None, global_index=None):
    authored = getattr(error, 'details', {})
    if not isinstance(authored, dict):
        authored = {}
    authored = authored.get('first_error', authored)
    if not isinstance(authored, dict):
        authored = {}
    outcome = authored.get('outcome', getattr(error, 'outcome', 'unknown'))
    details = {'class': _name(authored.get('class', authored.get('error_type', getattr(error, 'authored_class',
                                  type(error).__name__))), type(error).__name__),
            'stage': _name(authored.get('stage', getattr(error, 'stage', stage)), stage),
            'lane': lane, 'global_index': global_index,
            'outcome': outcome if outcome in _OUTCOMES else 'unknown'}
    if outcome in ('commit_unknown', 'not_entered'):
        details['authored_outcome'] = outcome
        details['outcome'] = 'unknown' if outcome == 'commit_unknown' else 'not_started'
    return details


def _record_first(gate, stop, first_bytes, details, source):
    """Claim stop and its primary error under the same gate as command starts."""
    # Setting this first also lets other clients leave a gate whose owner died.
    stop.value = 1
    if not gate.acquire(timeout=.05):
        return False
    try:
        if first_bytes[0] == 1:
            return False
        data = json.dumps({**details, 'source': source}, allow_nan=False).encode('utf-8')
        # Byte zero publishes a fully written record, rather than exposing a
        # killed writer's incomplete JSON as a primary authored exception.
        first_bytes[1:len(data) + 1] = data
        first_bytes[len(data) + 1] = 0
        first_bytes[0] = 1
        return True
    finally:
        gate.release()


def _read_first(first_bytes):
    if first_bytes[0] != 1:
        return None
    data = bytes(first_bytes)[1:].split(b'\0', 1)[0]
    return json.loads(data)


def _coordinator_pause(seconds):
    """Short interruptible waits; separate from the worker's polling waits."""
    time.sleep(seconds)


def _start_owned_process(process):
    """Publish ownership before restoring SIGINT after OS process creation."""
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
    try:
        process.start()
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, mask)


def _send_credit_grant(channel, sequence):
    """An idempotent sequence-addressed grant; coordinator owns its ledger."""
    channel.send(json.dumps({'kind': 'credit_grant', 'sequence': sequence}).encode('utf-8'))


def _worker_main(lane, factory, bootstrap, jobs, channel, gate, stop, scheduling_done, states, depths,
                 first_bytes, cleanup_done, start_index):
    worker = None
    active_index = None
    target = None
    cleanup_passed = False
    sequence = 0
    # The parent masks SIGINT only across spawn's PID publication window.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT})
    channel.setblocking(False)

    def send(kind, **fields):
        nonlocal sequence
        sequence += 1
        frame = {'kind': kind, 'lane': lane, 'pid': os.getpid(),
                 'sequence': sequence, 'observed_monotonic': time.monotonic(), **fields}
        encoded = json.dumps(_payload(frame), allow_nan=False,
                             separators=(',', ':')).encode('utf-8')
        if len(encoded) > MAX_FRAME_BYTES:
            raise ValueError('Evidence frame exceeds the protocol limit')
        request = json.dumps({'kind': 'credit_request', 'lane': lane,
                              'pid': os.getpid(), 'sequence': sequence}).encode('utf-8')
        while True:
            try:
                channel.send(request)
                break
            except (BlockingIOError, InterruptedError):
                time.sleep(.002)
        # The parent records ownership before granting each output slot. A
        # killed child cannot lose an unrecorded semaphore acquisition, and
        # never owns more than one pending credit request at a time.
        while True:
            try:
                grant = json.loads(channel.recv(1024))
                if grant.get('kind') != 'credit_grant' or type(grant.get('sequence')) is not int:
                    raise ValueError('Invalid output credit grant')
                if grant['sequence'] < sequence:
                    continue  # Idempotent retransmit after coordinator interrupt.
                if grant['sequence'] != sequence:
                    raise ValueError('Output credit sequence does not match the request')
                break
            except (BlockingIOError, InterruptedError):
                time.sleep(.002)
        while True:
            try:
                channel.send(encoded)
                return
            except (BlockingIOError, InterruptedError):
                time.sleep(.002)

    def report_failure(error, stage, global_index=None):
        details = _error_details(error, stage, lane, global_index)
        _record_first(gate, stop, first_bytes, details, 'child')
        send('failed', global_index=global_index, error=details)

    try:
        worker = factory(lane, bootstrap)
        if not callable(getattr(worker, 'execute', None)) or not callable(getattr(worker, 'close', None)):
            raise ValueError('Factory must return an execute/close worker')
        ready = worker.ready_metadata() if callable(getattr(worker, 'ready_metadata', None)) else {}
        send('ready', metadata=ready)
        while not stop.value:
            if not gate.acquire(timeout=.02):
                continue
            try:
                try:
                    active_index, target = jobs.get_nowait()
                except queue.Empty:
                    finished = scheduling_done.value and depths[lane] == 0
                    active_index = None
                else:
                    depths[lane] -= 1
                    offset = active_index - start_index
                    if stop.value:
                        states[offset] = 5
                        disposition = 'cancelled'
                    else:
                        states[offset] = 2
                        disposition = 'started'
                    finished = False
            finally:
                gate.release()
            if active_index is None:
                if finished:
                    break
                time.sleep(.002)
                continue
            if disposition == 'cancelled':
                send('cancelled', global_index=active_index)
                active_index = None
                continue
            try:
                send('started', global_index=active_index, target_monotonic=target)

                def emit_status(metadata):
                    send('status', global_index=active_index, metadata=metadata)

                result = worker.execute(active_index, target, emit_status)
                send('completed', global_index=active_index, result=result)
                states[offset] = 3
            except BaseException as error:
                states[offset] = 4
                report_failure(error, 'execute', active_index)
                break
            finally:
                active_index = None
    except BaseException as error:
        report_failure(error, 'bootstrap', active_index)
    finally:
        try:
            if worker is not None:
                try:
                    metadata = worker.close()
                    cleanup_passed = True
                    send('cleanup_complete', metadata=metadata)
                except BaseException as error:
                    report_failure(error, 'cleanup')
                    send('cleanup_failed', error=_error_details(error, 'cleanup', lane))
            else:
                send('cleanup_complete', metadata={'worker_created': False})
                cleanup_passed = True
        finally:
            cleanup_done.value = int(cleanup_passed)
            channel.close()
            jobs.close()


def run_paced_processes(count, rate, worker_factory, bootstrap, *, start_index=0,
                        writer_topology=DEFAULT_WRITER_PRESET, lanes=None,
                        cycle_length=CYCLE_LENGTH,
                        queue_capacity=QUEUE_CAPACITY,
                        result_capacity=RESULT_CAPACITY, on_observation=None,
                        on_process_started=None, on_process_ready=None):
    """Return ordered results only after child cleanup, OS exit and channel close.

    ``factory(lane, bootstrap)`` constructs each child's private worker. Its
    ``execute(index, target, emit_status)`` owns journal/transaction ordering;
    the driver does not add acknowledgement waits to that durability boundary.
    The start gate admits commands atomically and closes on the first failure.
    All admitted callbacks may finish; queued commands are cancelled. Lost
    frames after a start remain unknown pending external journal/DB evidence.
    """
    if type(count) is not int or count < 0:
        raise ValueError('count must be a nonnegative integer')
    if type(start_index) is not int or start_index < 0:
        raise ValueError('start_index must be a nonnegative integer')
    writer = writer_profile(writer_topology)
    if lanes is None:
        lanes = writer['lanes']
    if (lanes, cycle_length, queue_capacity) != (writer['lanes'], CYCLE_LENGTH, QUEUE_CAPACITY):
        raise ValueError('The frozen process profile requires selected writer lanes, cycle four and queue four')
    if any(type(value) is not int for value in (lanes, cycle_length, queue_capacity)):
        raise ValueError('Frozen profile dimensions must be integers')
    if isinstance(rate, bool) or not isinstance(rate, (int, float)):
        raise ValueError('rate must be finite and positive')
    try:
        rate = float(rate)
    except (OverflowError, ValueError) as error:
        raise ValueError('rate must be finite and positive') from error
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError('rate must be finite and positive')
    if type(result_capacity) is not int or result_capacity < 1:
        raise ValueError('result_capacity must be a positive integer')
    if not isinstance(bootstrap, dict):
        raise ValueError('bootstrap must be a private dictionary')
    if 'profile' in bootstrap and resolve_profile_writer(bootstrap['profile']) != writer['preset']:
        raise ValueError('Bootstrap and process driver writer topologies must match')
    for callback, name in ((worker_factory, 'worker_factory'),
                           (on_observation, 'on_observation'),
                           (on_process_started, 'on_process_started'),
                           (on_process_ready, 'on_process_ready')):
        if (name == 'worker_factory' or callback is not None) and not callable(callback):
            raise ValueError(name + ' must be callable')
    try:
        pickle.dumps(worker_factory)
    except Exception as error:
        raise ValueError('worker_factory must be importable and pickle-safe') from error

    started_at = time.monotonic()
    context = multiprocessing.get_context('spawn')
    gate = context.Lock()
    stop = context.RawValue('b', 0)
    scheduling_done = context.RawValue('b', 0)
    states = context.RawArray('b', count)
    depths = context.RawArray('i', lanes)
    credits = [set() for _ in range(lanes)]  # Parent-owned, sequence-addressed.
    credit_requests = {}
    first_bytes = context.RawArray('B', 4096)
    processes = {}
    jobs = {}
    channels = {}
    child_channels = {}
    cleanup_flags = {}
    ready = {}
    completed = {}
    failed = {}
    cleanup = {}
    cancelled = set()
    started = set()
    scheduled = set()
    exitcodes = {}
    cleanup_errors = []
    main_errors = []
    fallback_error = None
    max_depths = [0] * lanes
    queue_wait_seconds = 0.0
    queue_wait_count = 0
    result_frames = 0
    status_frames = 0
    channels_closed = False
    catalogued = set()
    forced_shutdowns = []
    max_output_credits = 0
    max_pending_credit_requests = 0

    def stop_with(error, stage, lane=None, global_index=None):
        nonlocal fallback_error
        details = _error_details(error, stage, lane, global_index)
        claimed = _record_first(gate, stop, first_bytes, details, 'parent')
        main_errors.append((error, details))
        if claimed or fallback_error is None:
            fallback_error = details

    def observe(frame):
        if on_observation is not None:
            try:
                on_observation(frame)
            except BaseException as error:
                stop_with(error, 'observation', frame.get('lane'), frame.get('global_index'))

    def receive(lane):
        nonlocal result_frames, status_frames, max_pending_credit_requests
        while True:
            try:
                data = channels[lane].recv(MAX_FRAME_BYTES + 1)
            except (BlockingIOError, ConnectionResetError):
                # A datagram peer's normal close may report ECONNRESET on
                # macOS. Actual exit and missing cleanup/result frames decide
                # whether this is a transport failure; reset alone does not.
                return
            except InterruptedError:
                continue
            if not data:
                return
            frame = json.loads(data)
            if frame.get('lane') != lane or frame.get('pid') != processes[lane].pid:
                raise ValueError('Child evidence identity does not match its owned process')
            kind = frame['kind']
            sequence = frame.get('sequence')
            if type(sequence) is not int or sequence < 1:
                raise ValueError('Invalid child evidence sequence')
            if kind == 'credit_request':
                if lane in credit_requests:
                    raise ValueError('Duplicate pending output credit request')
                credit_requests[lane] = sequence
                max_pending_credit_requests = max(max_pending_credit_requests, len(credit_requests))
                continue
            if sequence not in credits[lane]:
                raise ValueError('Child emitted evidence without an output credit')
            credits[lane].remove(sequence)
            if credit_requests.get(lane) == sequence:
                # A sent grant was interrupted before removing its request.
                credit_requests.pop(lane)
            index = frame.get('global_index')
            if kind == 'ready':
                ready[lane] = frame['metadata']
                if on_process_ready is not None:
                    try:
                        on_process_ready(lane, processes[lane].pid, frame['metadata'])
                    except BaseException as error:
                        stop_with(error, 'process_ready', lane)
            elif kind == 'started':
                started.add(index)
            elif kind == 'completed':
                if index in completed:
                    raise ValueError('Duplicate callback result')
                completed[index] = frame['result']
                result_frames += 1
            elif kind == 'failed':
                if index is not None:
                    failed[index] = frame['error']
                else:
                    cleanup_errors.append(frame['error'])
            elif kind == 'cancelled':
                cancelled.add(index)
            elif kind == 'cleanup_complete':
                cleanup[lane] = frame['metadata']
            elif kind == 'cleanup_failed':
                cleanup_errors.append(frame['error'])
            elif kind == 'status':
                status_frames += 1
            else:
                raise ValueError('Unknown child protocol frame')
            observe(frame)

    def pump():
        nonlocal fallback_error, max_output_credits
        for lane in list(channels):
            if lane in processes and processes[lane].pid is not None:
                receive(lane)
        for lane, process in processes.items():
            if process.pid is None:
                continue
            code = process.exitcode
            if code is None or lane in exitcodes:
                continue
            # Datagrams are atomic and remain readable after a sender dies.
            receive(lane)
            exitcodes[lane] = code
            # Every grant is owned in this parent ledger before being sent.
            credits[lane].clear()
            credit_requests.pop(lane, None)
            unresolved = [start_index + i for i, state in enumerate(states)
                          if state in (2, 3, 4) and (start_index + i) not in completed
                          and (start_index + i) not in failed
                          and ((start_index + i) // CYCLE_LENGTH) % lanes == lane]
            if code != 0 or lane not in cleanup:
                details = {'class': 'ProcessTransportUnknown', 'stage': 'worker_exit',
                           'lane': lane, 'global_index': unresolved[0] if unresolved else None,
                           'outcome': 'unknown', 'exitcode': code}
                _record_first(gate, stop, first_bytes, details, 'transport')
                if fallback_error is None:
                    fallback_error = details
                cleanup_errors.append(details)
            observe({'kind': 'process_exit', 'lane': lane, 'pid': process.pid,
                     'exitcode': code, 'cleanup_complete': lane in cleanup,
                     'observed_monotonic': time.monotonic()})
        for lane, sequence in sorted(credit_requests.copy().items()):
            already_owned = sequence in credits[lane]
            if lane in exitcodes or (not already_owned and sum(map(len, credits)) >= result_capacity):
                continue
            credits[lane].add(sequence)
            max_output_credits = max(max_output_credits, sum(map(len, credits)))
            try:
                _send_credit_grant(channels[lane], sequence)
            except (BlockingIOError, ConnectionResetError, BrokenPipeError):
                # Keep the owned/requested sequence for idempotent retry. It
                # is reclaimed if the known peer exits before consuming it.
                pass
            else:
                credit_requests.pop(lane, None)

    def pause(seconds=.005):
        try:
            _coordinator_pause(seconds)
        except BaseException as error:
            stop_with(error, 'scheduler')

    try:
        for lane in range(lanes):
            parent_channel, child_channel = socket.socketpair(socket.AF_UNIX, socket.SOCK_DGRAM)
            channels[lane] = parent_channel
            child_channels[lane] = child_channel
            # Publish both endpoints before configuration can fail. macOS's
            # default datagram buffers are smaller than the authored frame
            # bound; receive space also needs room for transport overhead.
            for channel in (parent_channel, child_channel):
                channel.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, MAX_FRAME_BYTES)
                channel.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * MAX_FRAME_BYTES)
            parent_channel.setblocking(False)
            jobs[lane] = context.Queue(maxsize=queue_capacity)
            cleanup_flags[lane] = context.RawValue('b', 0)
            process = context.Process(target=_worker_main, name='paced-spawn-lane-' + str(lane),
                                      args=(lane, worker_factory, bootstrap, jobs[lane],
                                            child_channel, gate, stop,
                                            scheduling_done, states, depths, first_bytes,
                                            cleanup_flags[lane], start_index))
            processes[lane] = process
            _start_owned_process(process)
            child_channel.close()
            if on_process_started is not None:
                on_process_started(lane, process.pid)
            catalogued.add(lane)
            observe({'kind': 'process_started', 'lane': lane, 'pid': process.pid,
                     'observed_monotonic': time.monotonic()})
            if stop.value:
                break
        while len(ready) < lanes and not stop.value:
            pump()
            pause()
        pace_origin = time.monotonic()
        for offset in range(count):
            if stop.value:
                break
            index = start_index + offset
            lane = (index // CYCLE_LENGTH) % lanes
            target = pace_origin + offset / rate
            while not stop.value and time.monotonic() < target:
                pump()
                pause(min(.005, max(0.0, target - time.monotonic())))
            if stop.value:
                break
            wait_started = None
            while not stop.value:
                pump()
                if stop.value:
                    break
                admission_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT})
                acquired = False
                try:
                    acquired = gate.acquire(timeout=.005)
                    if not acquired:
                        enqueued = False
                    elif stop.value:
                        break
                    else:
                        # Publish an admission reservation while the start
                        # gate excludes every worker. Full rolls it back; an
                        # interrupted queue call closes the gate before any
                        # child could execute that uncertain enqueue.
                        states[offset] = 1
                        depths[lane] += 1
                        scheduled.add(index)
                        try:
                            jobs[lane].put_nowait((index, target))
                        except queue.Full:
                            states[offset] = 0
                            depths[lane] -= 1
                            scheduled.remove(index)
                            enqueued = False
                        except BaseException:
                            stop.value = 1
                            raise
                        else:
                            max_depths[lane] = max(max_depths[lane], depths[lane])
                            enqueued = True
                finally:
                    if acquired:
                        gate.release()
                    signal.pthread_sigmask(signal.SIG_SETMASK, admission_mask)
                if not acquired:
                    pause()
                    continue
                if enqueued:
                    break
                if wait_started is None:
                    wait_started = time.monotonic()
                    queue_wait_count += 1
                pause()
            if wait_started is not None:
                waited = time.monotonic() - wait_started
                queue_wait_seconds += waited
                observe({'kind': 'queue_wait', 'lane': lane, 'global_index': index,
                         'duration_seconds': waited, 'enqueued': index in scheduled})
            if index in scheduled:
                observe({'kind': 'scheduled', 'lane': lane, 'global_index': index,
                         'target_monotonic': target, 'observed_monotonic': time.monotonic()})
    except BaseException as error:
        stop_with(error, 'scheduler')
    finally:
        scheduling_done.value = 1
        for lane, process in processes.items():
            if process.pid is not None and lane not in catalogued:
                try:
                    if on_process_started is not None:
                        on_process_started(lane, process.pid)
                    catalogued.add(lane)
                except BaseException as error:
                    stop_with(error, 'process_started', lane)
                observe({'kind': 'process_started', 'lane': lane, 'pid': process.pid,
                         'delayed_observation': True, 'observed_monotonic': time.monotonic()})
        launched = {lane: process for lane, process in processes.items() if process.pid is not None}
        failure_started = None
        term_started = None
        killed_lanes = set()
        # Never trust an interrupted join or a missing result as completion.
        # Poll actual OS exits while continuing to drain every child's channel.
        while len(exitcodes) < len(launched):
            try:
                pump()
            except BaseException as error:
                stop_with(error, 'drain')
            now = time.monotonic()
            if stop.value and failure_started is None:
                failure_started = now
            if failure_started is not None and term_started is None and now - failure_started >= FAILURE_CLEANUP_SECONDS:
                term_started = now
                for lane, process in launched.items():
                    if process.exitcode is None:
                        try:
                            process.terminate()
                        except BaseException as error:
                            stop_with(error, 'forced_term', lane)
                        else:
                            forced = {'kind': 'forced_shutdown', 'lane': lane, 'pid': process.pid,
                                      'signal': 'SIGTERM', 'observed_monotonic': now}
                            forced_shutdowns.append(forced)
                            observe(forced)
            if term_started is not None and now - term_started >= TERM_GRACE_SECONDS:
                for lane, process in launched.items():
                    if process.exitcode is None and lane not in killed_lanes:
                        try:
                            process.kill()
                        except BaseException as error:
                            stop_with(error, 'forced_kill', lane)
                        else:
                            killed_lanes.add(lane)
                            forced = {'kind': 'forced_shutdown', 'lane': lane, 'pid': process.pid,
                                      'signal': 'SIGKILL', 'observed_monotonic': now}
                            forced_shutdowns.append(forced)
                            observe(forced)
            if len(exitcodes) < len(launched):
                pause()
        for lane, process in launched.items():
            while True:
                try:
                    process.join(timeout=.05)
                    break
                except BaseException as error:
                    stop_with(error, 'reap', lane)
        # Workers have really exited before any journal owner may finalize.
        for lane, work_queue in jobs.items():
            while True:
                try:
                    work_queue.close()
                    work_queue.join_thread()
                    break
                except BaseException as error:
                    stop_with(error, 'channel_close', lane)
        for channel in list(channels.values()) + list(child_channels.values()):
            while True:
                try:
                    channel.close()
                    break
                except BaseException as error:
                    stop_with(error, 'channel_close')
        channels_closed = True

    shared_started = {start_index + i for i, state in enumerate(states) if state in (2, 3, 4)}
    started |= shared_started
    cancelled |= {start_index + i for i, state in enumerate(states) if state in (1, 5)}
    unknown = started - set(completed) - set(failed)
    # Failure frames describe admitted attempts, including committed failures.
    children = [{'lane': lane, 'pid': process.pid, 'ready': lane in ready,
                 'cleanup_complete': lane in cleanup and bool(cleanup_flags[lane].value),
                 'cleanup_metadata': cleanup.get(lane), 'exitcode': exitcodes[lane],
                 'reaped': True} for lane, process in launched.items()]
    for process in processes.values():
        while True:
            try:
                process.close()
                break
            except BaseException as error:
                stop_with(error, 'process_handle_close')
    first_error = _read_first(first_bytes) or fallback_error
    elapsed = time.monotonic() - started_at
    summary = {'kind': 'summary', 'requested_count': count,
               'scheduled_count': len(scheduled), 'started_count': len(started),
               'started_indices': sorted(started),
               'completed_count': len(completed),
               'failed_count': len(set(failed) - set(completed)),
               'reported_failure_count': len(failed),
               'post_result_errors': [failed[index] for index in sorted(set(failed) & set(completed))],
               'cancelled_count': len(cancelled), 'unscheduled_count': count - len(scheduled),
               'cancelled_indices': sorted(cancelled),
               'unscheduled_indices': sorted(set(range(start_index, start_index + count)) - scheduled),
               'unknown_count': len(unknown), 'unknown_indices': sorted(unknown),
               'requested_rate': rate, 'actual_rate': len(completed) / elapsed,
               'duration_seconds': elapsed, 'max_queue_depths': max_depths,
               'queue_wait_count': queue_wait_count, 'queue_wait_seconds': queue_wait_seconds,
               'result_capacity': result_capacity, 'result_frames': result_frames,
               'max_output_credits': max_output_credits,
               'max_pending_credit_requests': max_pending_credit_requests,
               'status_frames': status_frames, 'children': children,
               'channels_closed': channels_closed,
               'worker_processes_joined': all(child['reaped'] for child in children),
               'worker_completion_observed': len(children) == lanes and all(child['cleanup_complete'] for child in children),
               'lifecycle_complete': len(children) == lanes and all(
                   child['cleanup_complete'] and child['exitcode'] == 0 for child in children),
               'per_lane_requested_counts': [len(indices) for indices in allocate_lane_indices(
                   count, lanes=lanes, cycle_length=cycle_length, start_index=start_index)],
               'first_error': first_error, 'cleanup_errors': cleanup_errors,
               'forced_shutdowns': forced_shutdowns,
               'profile': {**_process_profile(writer), 'result_capacity': result_capacity}}
    observe(summary)
    # An observer's last failure also belongs to the final result, with all
    # clients already reaped. Preserve an earlier child error over interrupts.
    first_error = _read_first(first_bytes) or fallback_error
    if first_error is not None:
        summary['first_error'] = first_error
        if first_error.get('source') == 'parent':
            for error, details in main_errors:
                if all(first_error.get(key) == value for key, value in details.items()):
                    raise error
        raise ProcessGenerationError({**summary, 'first_error': first_error})
    if len(completed) != count or not summary['lifecycle_complete']:
        raise ProcessGenerationError({**summary, 'first_error': {
            'class': 'ProcessTransportUnknown', 'stage': 'accounting',
            'global_index': min(unknown) if unknown else None, 'outcome': 'unknown'}})
    return [completed[index] for index in range(start_index, start_index + count)]
