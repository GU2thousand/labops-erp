"""Fail-closed readiness for the three-broker validation recovery drill."""
import math
import time


def validation_nodes(values):
    return (isinstance(values, list) and len(values) == 3
            and all(type(value) is int for value in values) and set(values) == {0, 1, 2})


def completed_recovery_seconds(started, budget, *, monotonic=time.monotonic):
    """Reject a successful DB predicate whose response crossed the frozen SLA."""
    elapsed = monotonic() - started
    if not math.isfinite(elapsed) or elapsed < 0 or elapsed > budget:
        raise TimeoutError('Broker recovery completed outside the frozen drain window')
    return elapsed


def recovery_errors(health, metadata, topics):
    """Check observations, including all broker-local health views.

    Redpanda's health_overview field names are retained in evidence rather than
    inferred from Kafka metadata, which may still advertise stale leaders.
    """
    errors = []
    expected_nodes = {0, 1, 2}
    for node in range(3):
        observed = health.get(str(node), {})
        if observed.get('error_type'):
            errors.append(f'broker {node}: health request failed')
            continue
        document = observed.get('body')
        if not isinstance(document, dict):
            errors.append(f'broker {node}: missing health overview')
            continue
        if document.get('is_healthy') is not True:
            errors.append(f'broker {node}: is_healthy is not true')
        nodes = document.get('all_nodes')
        if not validation_nodes(nodes):
            errors.append(f'broker {node}: cluster membership differs from three validation nodes')
        for field in ('nodes_down', 'leaderless_partitions', 'under_replicated_partitions',
                      'unhealthy_reasons', 'high_disk_usage_nodes', 'nodes_in_recovery_mode'):
            if document.get(field) != []:
                errors.append(f'broker {node}: {field} is missing or nonempty')
        for field in ('leaderless_count', 'under_replicated_count'):
            if type(document.get(field)) is not int or document[field] != 0:
                errors.append(f'broker {node}: {field} is missing or nonzero')
    if metadata.get('error_type'):
        errors.append('Kafka metadata request failed')
        return errors
    brokers = metadata.get('brokers')
    if not validation_nodes(brokers):
        errors.append('Kafka metadata does not identify the three validation brokers')
    observed_topics = metadata.get('topics', {})
    for topic_name in topics:
        topic = observed_topics.get(topic_name, {})
        if topic.get('error') is not None:
            errors.append(f'{topic_name}: topic metadata error')
        partitions = topic.get('partitions')
        if not isinstance(partitions, list) or not all(isinstance(p, dict) for p in partitions):
            errors.append(f'{topic_name}: partition metadata missing')
            continue
        if not validation_nodes([p.get('partition') for p in partitions]):
            errors.append(f'{topic_name}: expected exactly partitions 0, 1, 2')
        for partition in partitions:
            number = partition.get('partition')
            replicas, isr = partition.get('replicas'), partition.get('isr')
            if partition.get('error') is not None:
                errors.append(f'{topic_name}/{number}: partition metadata error')
            if not validation_nodes(replicas):
                errors.append(f'{topic_name}/{number}: replica assignment differs from RF3')
            if not validation_nodes(isr):
                errors.append(f'{topic_name}/{number}: full ISR has not recovered')
            if type(partition.get('leader')) is not int or partition['leader'] not in expected_nodes:
                errors.append(f'{topic_name}/{number}: live leader has not recovered')
    return errors


def wait_broker_recovery(fetch_health, fetch_metadata, topics, *, timeout=180,
                         monotonic=time.monotonic, sleep=time.sleep, on_observation=None):
    """Poll all views within one frozen deadline; fetchers share remaining time.

    Fetchers receive a positive remaining budget and must bound their network
    operations by it. Authentication/TLS failures are retained as failed views;
    they never become healthy empty observations or trigger any state repair.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('Broker recovery timeout must be finite and positive')
    started = monotonic()
    deadline = started + timeout
    attempt = 0
    while monotonic() < deadline:
        attempt += 1
        snapshot = {'attempt': attempt, 'health': {}, 'metadata': {}}
        for node in range(3):
            remaining = deadline - monotonic()
            if remaining <= 0:
                snapshot['health'][str(node)] = {'error_type': 'TimeoutError'}
                continue
            try:
                snapshot['health'][str(node)] = {'body': fetch_health(node, remaining)}
            except Exception as exc:
                # Never persist a requests exception string, which may contain
                # Basic-auth URLs or another credential-bearing diagnostic.
                snapshot['health'][str(node)] = {'error_type': type(exc).__name__}
        remaining = deadline - monotonic()
        if remaining > 0:
            try:
                snapshot['metadata'] = fetch_metadata(remaining)
            except Exception as exc:
                snapshot['metadata'] = {'error_type': type(exc).__name__}
        else:
            snapshot['metadata'] = {'error_type': 'TimeoutError'}
        snapshot['elapsed_seconds'] = monotonic() - started
        snapshot['errors'] = recovery_errors(snapshot['health'], snapshot['metadata'], topics)
        if monotonic() >= deadline:
            snapshot['errors'].append('Broker recovery deadline expired')
        snapshot['ready'] = not snapshot['errors']
        if on_observation:
            on_observation(snapshot)
        if snapshot['ready']:
            return snapshot
        remaining = deadline - monotonic()
        if remaining > 0:
            sleep(min(.25, remaining))
    raise TimeoutError('Broker health, RF3 leaders and ISR did not recover within the unchanged deadline')
