"""Pure tests for broker endpoint evidence across real Docker stop/start faults."""
from copy import deepcopy
import json
import unittest

from benchmarks.events.network_identity import stable_broker_network, compare_broker_networks


PREFIX = '10.243.77'
PROJECT = 'labops_events_network_test'


def network_snapshot(prefix=PREFIX):
    network_name = PROJECT + '_default'
    snapshot = {
        'observed_at': 1,
        'compose_project': PROJECT,
        'bridge': {
            'network_id': 'network-id',
            'name': network_name,
            'driver': 'bridge',
            'ipam_config': [{'Subnet': prefix + '.0/24', 'Gateway': prefix + '.1'}],
            'containers': {},
        },
        'brokers': {},
    }
    for index in range(3):
        service = 'redpanda-' + str(index)
        container_id = 'container-' + str(index)
        name = PROJECT + '-' + service + '-1'
        address = prefix + '.' + str(10 + index)
        snapshot['bridge']['containers'][container_id] = {
            'Name': name, 'IPv4Address': address + '/24', 'IPv6Address': '', 'MacAddress': '',
        }
        snapshot['brokers'][service] = {
            'container_ids_result': {'exit_code': 0, 'output': container_id + '\n', 'stderr': ''},
            'containers': [{
                'container_id': container_id, 'name': '/' + name, 'status': 'running',
                'project': PROJECT, 'service': service,
                'networks': {network_name: {
                    'NetworkID': 'network-id', 'IPAddress': address,
                    'IPAMConfig': {'IPv4Address': address}, 'Aliases': [service],
                }},
            }],
        }
    return snapshot


def broker(snapshot, index=0):
    return snapshot['brokers']['redpanda-' + str(index)]['containers'][0]


def broker_network(snapshot, index=0):
    return broker(snapshot, index)['networks'][PROJECT + '_default']


class EventNetworkIdentityTests(unittest.TestCase):
    def test_malformed_snapshot_raises_value_error(self):
        candidates = [None, [], {}, network_snapshot(), network_snapshot()]
        candidates[-2]['bridge'] = None
        candidates[-1]['brokers']['redpanda-0']['containers'] = [None]
        for index, candidate in enumerate(candidates):
            with self.subTest(candidate=index):
                with self.assertRaises(ValueError):
                    stable_broker_network(candidate, PREFIX)

    def test_valid_baseline_normalizes_all_three_identities(self):
        snapshot = network_snapshot()
        original = deepcopy(snapshot)
        normalized = stable_broker_network(snapshot, PREFIX)
        self.assertEqual(normalized, {
            'compose_project': PROJECT, 'network_name': PROJECT + '_default',
            'network_id': 'network-id', 'subnet': PREFIX + '.0/24',
            'brokers': {
                'redpanda-' + str(index): {
                    'container_id': 'container-' + str(index),
                    'ipv4_address': PREFIX + '.' + str(10 + index),
                    'service': 'redpanda-' + str(index), 'network_id': 'network-id',
                }
                for index in range(3)
            },
        })
        self.assertEqual(snapshot, original, 'Normalization must not mutate raw evidence')

    def test_unchanged_before_after_accepts_different_observation_time(self):
        before = network_snapshot()
        after = deepcopy(before)
        after['observed_at'] = 999
        result = compare_broker_networks(before, after, PREFIX)
        self.assertTrue(result['passed'])
        self.assertTrue(result['broker_addresses_unchanged'])
        self.assertTrue(result['container_ids_unchanged'])
        self.assertTrue(result['network_ids_unchanged'])
        self.assertEqual(result['before'], stable_broker_network(before, PREFIX))
        self.assertEqual(result['after'], result['before'])

    def test_valid_overridden_private_prefix(self):
        for prefix in ('10.243.78', '172.16.77', '192.168.77'):
            with self.subTest(prefix=prefix):
                normalized = stable_broker_network(network_snapshot(prefix), prefix)
                self.assertIn(prefix + '.10', json.dumps(normalized))

    def test_swapped_runtime_ips_are_rejected(self):
        snapshot = network_snapshot()
        for index, suffix in ((0, 11), (1, 10)):
            address = PREFIX + '.' + str(suffix)
            broker_network(snapshot, index)['IPAddress'] = address
            broker_network(snapshot, index)['IPAMConfig']['IPv4Address'] = address
            snapshot['bridge']['containers']['container-' + str(index)]['IPv4Address'] = address + '/24'
        with self.assertRaises(ValueError):
            stable_broker_network(snapshot, PREFIX)

    def test_missing_or_multiple_broker_containers_are_rejected(self):
        for rows in ([], [broker(network_snapshot()), broker(network_snapshot())]):
            with self.subTest(count=len(rows)):
                snapshot = network_snapshot()
                snapshot['brokers']['redpanda-0']['containers'] = rows
                with self.assertRaises(ValueError):
                    stable_broker_network(snapshot, PREFIX)

    def test_missing_runtime_ip_cannot_be_replaced_by_requested_ip(self):
        for replacement in (None, ''):
            with self.subTest(runtime_ip=replacement):
                snapshot = network_snapshot()
                if replacement is None:
                    del broker_network(snapshot)['IPAddress']
                else:
                    broker_network(snapshot)['IPAddress'] = replacement
                with self.assertRaises(ValueError):
                    stable_broker_network(snapshot, PREFIX)

    def test_actual_subnet_must_match_requested_prefix(self):
        snapshot = network_snapshot()
        snapshot['bridge']['ipam_config'][0]['Subnet'] = '10.243.78.0/24'
        with self.assertRaises(ValueError):
            stable_broker_network(snapshot, PREFIX)

    def test_non_private_or_malformed_prefixes_are_rejected(self):
        for prefix in ('8.8.8', '172.32.77', '192.0.2', '10.243', '10.243.256', ''):
            with self.subTest(prefix=prefix):
                with self.assertRaises(ValueError):
                    stable_broker_network(network_snapshot(prefix), prefix)

    def test_compose_project_and_network_name_must_agree(self):
        for field in ('compose_project', 'bridge_name'):
            with self.subTest(field=field):
                snapshot = network_snapshot()
                if field == 'compose_project':
                    snapshot['compose_project'] = PROJECT + '_other'
                else:
                    snapshot['bridge']['name'] = PROJECT + '_other_default'
                with self.assertRaises(ValueError):
                    stable_broker_network(snapshot, PREFIX)

    def test_broker_network_id_must_match_bridge_id(self):
        snapshot = network_snapshot()
        broker_network(snapshot)['NetworkID'] = 'foreign-network'
        with self.assertRaises(ValueError):
            stable_broker_network(snapshot, PREFIX)

    def test_bridge_membership_corroborates_runtime_ip(self):
        for mutation in ('missing_member', 'different_ip', 'empty_ip'):
            with self.subTest(mutation=mutation):
                snapshot = network_snapshot()
                if mutation == 'missing_member':
                    del snapshot['bridge']['containers']['container-0']
                else:
                    snapshot['bridge']['containers']['container-0']['IPv4Address'] = (
                        PREFIX + '.99/24' if mutation == 'different_ip' else '')
                with self.assertRaises(ValueError):
                    stable_broker_network(snapshot, PREFIX)

    def test_requested_static_ip_must_match_actual_runtime_ip(self):
        snapshot = network_snapshot()
        broker_network(snapshot)['IPAMConfig']['IPv4Address'] = PREFIX + '.99'
        with self.assertRaises(ValueError):
            stable_broker_network(snapshot, PREFIX)

    def test_stopped_broker_is_not_a_healthy_baseline(self):
        snapshot = network_snapshot()
        broker(snapshot)['status'] = 'exited'
        with self.assertRaises(ValueError):
            stable_broker_network(snapshot, PREFIX)

    def test_unrelated_bridge_endpoints_do_not_change_broker_identity(self):
        before = network_snapshot()
        after = deepcopy(before)
        after['bridge']['containers']['exporter-container'] = {
            'Name': PROJECT + '-kafka-exporter-1', 'IPv4Address': PREFIX + '.50/24',
            'IPv6Address': '', 'MacAddress': '',
        }
        self.assertEqual(stable_broker_network(after, PREFIX), stable_broker_network(before, PREFIX))
        self.assertTrue(compare_broker_networks(before, after, PREFIX)['passed'])

    def test_duplicate_container_ids_are_rejected(self):
        snapshot = network_snapshot()
        broker(snapshot, 1)['container_id'] = 'container-0'
        snapshot['brokers']['redpanda-1']['container_ids_result']['output'] = 'container-0\n'
        with self.assertRaises(ValueError):
            stable_broker_network(snapshot, PREFIX)

    def test_another_bridge_endpoint_cannot_claim_broker_ip(self):
        snapshot = network_snapshot()
        snapshot['bridge']['containers']['foreign-container'] = {
            'Name': 'foreign', 'IPv4Address': PREFIX + '.10/24',
            'IPv6Address': '', 'MacAddress': '',
        }
        with self.assertRaises(ValueError):
            stable_broker_network(snapshot, PREFIX)

    def test_failed_or_inconsistent_container_discovery_is_rejected(self):
        for mutation in ('failed', 'different_id', 'missing_output'):
            with self.subTest(mutation=mutation):
                snapshot = network_snapshot()
                discovery = snapshot['brokers']['redpanda-0']['container_ids_result']
                if mutation == 'failed':
                    discovery['exit_code'] = 1
                elif mutation == 'different_id':
                    discovery['output'] = 'foreign-container\n'
                else:
                    del discovery['output']
                with self.assertRaises(ValueError):
                    stable_broker_network(snapshot, PREFIX)

    def test_container_labels_must_match_project_and_service(self):
        for label, value in (('project', 'foreign-project'), ('service', 'redpanda-1')):
            with self.subTest(label=label):
                snapshot = network_snapshot()
                broker(snapshot)[label] = value
                with self.assertRaises(ValueError):
                    stable_broker_network(snapshot, PREFIX)

    def test_changed_container_identity_after_fault_is_rejected(self):
        before = network_snapshot()
        after = deepcopy(before)
        changed = 'replacement-container'
        broker(after)['container_id'] = changed
        after['brokers']['redpanda-0']['container_ids_result']['output'] = changed + '\n'
        after['bridge']['containers'][changed] = after['bridge']['containers'].pop('container-0')
        # Each individual snapshot is coherent; a stop/start must retain the
        # container identity rather than hide a replacement behind the same IP.
        stable_broker_network(after, PREFIX)
        with self.assertRaises(ValueError):
            compare_broker_networks(before, after, PREFIX)

    def test_changed_network_identity_after_fault_is_rejected(self):
        before = network_snapshot()
        after = deepcopy(before)
        after['bridge']['network_id'] = 'replacement-network'
        for index in range(3):
            broker_network(after, index)['NetworkID'] = 'replacement-network'
        stable_broker_network(after, PREFIX)
        with self.assertRaises(ValueError):
            compare_broker_networks(before, after, PREFIX)

    def test_extra_environment_and_secret_fields_are_not_normalized(self):
        snapshot = network_snapshot()
        normalized = stable_broker_network(snapshot, PREFIX)
        snapshot['Env'] = ['KAFKA_ADMIN_PASSWORD=never-normalize-this-secret']
        snapshot['bridge']['Env'] = {'SECRET': 'never-normalize-this-secret'}
        broker(snapshot)['Env'] = ['POSTGRES_PASSWORD=never-normalize-this-secret']
        broker(snapshot)['Config'] = {'Env': ['TOKEN=never-normalize-this-secret']}
        candidate = stable_broker_network(snapshot, PREFIX)
        self.assertEqual(candidate, normalized)
        self.assertNotIn('never-normalize-this-secret', json.dumps(candidate))
        comparison = compare_broker_networks(network_snapshot(), snapshot, PREFIX)
        self.assertNotIn('never-normalize-this-secret', json.dumps(comparison))


if __name__ == '__main__':
    unittest.main()
