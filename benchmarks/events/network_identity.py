"""Pure validation of disposable broker network identity across real faults.

Snapshots are produced by infra/events/validation/collect.py. This module never
connects to Docker or reads runtime credentials. Only explicitly selected network
identity fields are returned in the JSON-safe proof.
"""
import ipaddress
import re


BROKERS = tuple(f'redpanda-{number}' for number in range(3))
PRIVATE_RANGES = tuple(ipaddress.ip_network(cidr) for cidr in
                       ('10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'))


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _text(value, description):
    _check(isinstance(value, str) and bool(value) and value.strip() == value,
           'Missing or invalid ' + description)
    return value


def _subnet(prefix):
    _check(isinstance(prefix, str) and len(prefix.split('.')) == 3,
           'Expected a three-octet RFC1918 IPv4 prefix')
    try:
        network = ipaddress.ip_network(prefix + '.0/24', strict=True)
    except ValueError:
        raise ValueError('Expected a valid three-octet IPv4 prefix') from None
    _check(network.version == 4 and any(network.subnet_of(item) for item in PRIVATE_RANGES),
           'Broker validation subnet must be RFC1918 private IPv4')
    return network


def stable_broker_network(snapshot, expected_prefix):
    """Return a narrow normalized snapshot, or reject any missing/mismatched identity.

    Each of the three expected services must have exactly one running container
    on its project's sole bridge network. Runtime, requested static IPAM address
    and the bridge endpoint independently corroborate its fixed .10/.11/.12 IP.
    """
    subnet = _subnet(expected_prefix)
    _check(isinstance(snapshot, dict), 'Network snapshot must be an object')
    project = _text(snapshot.get('compose_project'), 'Compose project')
    _check(re.fullmatch(r'labops_events_[a-z0-9][a-z0-9_-]{0,47}', project) is not None,
           'Network snapshot is not a disposable validation project')
    network_name = project + '_default'
    bridge = snapshot.get('bridge')
    _check(isinstance(bridge, dict), 'Missing project bridge inspection')
    _check(bridge.get('name') == network_name and bridge.get('driver') == 'bridge',
           'Project bridge name or driver mismatch')
    network_id = _text(bridge.get('network_id'), 'project bridge ID')
    pools = bridge.get('ipam_config')
    _check(isinstance(pools, list) and len(pools) == 1 and isinstance(pools[0], dict),
           'Expected exactly one project bridge IPAM pool')
    _check(pools[0].get('Subnet') == str(subnet), 'Project bridge CIDR mismatch')
    endpoints = bridge.get('containers')
    _check(isinstance(endpoints, dict), 'Missing bridge container endpoint inspection')
    services = snapshot.get('brokers')
    _check(isinstance(services, dict) and set(services) == set(BROKERS),
           'Expected exactly three broker service inspections')
    normalized = {}
    used_ids = set()
    for number, name in enumerate(BROKERS):
        expected_ip = str(subnet.network_address + 10 + number)
        service = services[name]
        _check(isinstance(service, dict), 'Missing broker service inspection')
        ids_result = service.get('container_ids_result')
        _check(isinstance(ids_result, dict) and ids_result.get('exit_code') == 0,
               'Broker container discovery failed')
        discovered = ids_result.get('output')
        _check(isinstance(discovered, str), 'Broker container discovery output missing')
        container_ids = discovered.split()
        rows = service.get('containers')
        _check(len(container_ids) == 1 and isinstance(rows, list) and len(rows) == 1,
               'Expected exactly one container for each broker')
        row = rows[0]
        _check(isinstance(row, dict), 'Broker container inspection failed')
        container_id = _text(row.get('container_id'), 'broker container ID')
        _check(container_id == container_ids[0] and container_id not in used_ids,
               'Broker container IDs mismatch or are duplicated')
        used_ids.add(container_id)
        _check(row.get('service') == name and row.get('project') == project,
               'Broker service or project label mismatch')
        _check(row.get('status') == 'running', 'Broker container is not running')
        networks = row.get('networks')
        _check(isinstance(networks, dict) and set(networks) == {network_name},
               'Broker is not attached exclusively to its project bridge')
        attachment = networks[network_name]
        _check(isinstance(attachment, dict) and attachment.get('NetworkID') == network_id,
               'Broker network ID does not match project bridge')
        _check(attachment.get('IPAddress') == expected_ip,
               'Broker runtime IPv4 address is missing or does not match fixed identity')
        requested = attachment.get('IPAMConfig')
        _check(isinstance(requested, dict) and requested.get('IPv4Address') == expected_ip,
               'Broker does not have its expected static IPAM address')
        endpoint = endpoints.get(container_id)
        _check(isinstance(endpoint, dict) and endpoint.get('IPv4Address') == expected_ip + '/24',
               'Project bridge endpoint does not corroborate broker IPv4 address')
        container_name = _text(row.get('name'), 'broker container name').lstrip('/')
        _check(endpoint.get('Name') == container_name,
               'Project bridge endpoint does not match broker container name')
        owners = [cid for cid, candidate in endpoints.items() if isinstance(candidate, dict)
                  and candidate.get('IPv4Address') == expected_ip + '/24']
        _check(owners == [container_id], 'Broker IPv4 address has conflicting bridge endpoints')
        normalized[name] = {'service': name, 'container_id': container_id,
                            'ipv4_address': expected_ip, 'network_id': network_id}
    return {'compose_project': project, 'network_name': network_name,
            'network_id': network_id, 'subnet': str(subnet), 'brokers': normalized}


def compare_broker_networks(before, after, expected_prefix):
    """Prove the same broker containers retain their IP and bridge after stop/start."""
    first = stable_broker_network(before, expected_prefix)
    second = stable_broker_network(after, expected_prefix)
    _check(first['compose_project'] == second['compose_project'],
           'Compose project changed across broker fault')
    _check(first['network_id'] == second['network_id'],
           'Project bridge network ID changed across broker fault')
    for name in BROKERS:
        _check(first['brokers'][name] == second['brokers'][name],
               'Broker container, IPv4 address or network identity changed across fault')
    return {'passed': True, 'compose_project': first['compose_project'],
            'network_name': first['network_name'], 'network_id': first['network_id'],
            'subnet': first['subnet'], 'broker_addresses_unchanged': True,
            'container_ids_unchanged': True, 'network_ids_unchanged': True,
            'before': first, 'after': second}


# Keep short aliases for callers that use a singular comparison label.
compare_broker_network = compare_broker_networks
comparison = compare_broker_networks
