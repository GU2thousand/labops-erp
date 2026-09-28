"""Pure contract tests for scoped Kafka exporter consumer-group lag evidence."""
import json
import math
import unittest

from benchmarks.events.metrics_contract import require_consumer_group_lag


GROUPS = ('labops.citest.notification.v1', 'labops.citest.analytics.v1')
TOPIC = 'labops.citest.inventory.v1'
METRIC = 'kafka_consumergroup_lag'


def lag_line(group, partition, value='0', topic=TOPIC, extra_labels=None):
    labels = {'consumergroup': group, 'topic': topic, 'partition': str(partition)}
    labels.update(extra_labels or {})
    encoded = ','.join(key + '=' + json.dumps(value) for key, value in labels.items())
    return METRIC + '{' + encoded + '} ' + str(value)


def required_lines():
    return [lag_line(group, partition, str(partition))
            for group in GROUPS for partition in range(3)]


def exposition(lines):
    return '# HELP kafka_consumergroup_lag Consumer group lag\n' + \
        '# TYPE kafka_consumergroup_lag gauge\n' + '\n'.join(lines) + '\n'


class EventMetricsContractTests(unittest.TestCase):
    def check(self, lines, **kwargs):
        return require_consumer_group_lag(exposition(lines), GROUPS, TOPIC, **kwargs)

    def reject(self, lines, **kwargs):
        with self.assertRaises(ValueError):
            self.check(lines, **kwargs)

    def test_exact_two_groups_three_partitions_produces_six_samples(self):
        result = self.check(required_lines())
        self.assertTrue(result['passed'])
        self.assertEqual(result['metric'], METRIC)
        self.assertEqual(result['expected_samples'], 6)
        self.assertEqual(result['observed_samples'], 6)
        self.assertEqual(result['groups'], sorted(GROUPS))
        self.assertEqual(result['topic'], TOPIC)
        self.assertEqual(result['partitions'], [0, 1, 2])
        self.assertEqual([(sample['consumergroup'], sample['topic'], sample['partition'])
                          for sample in result['samples']],
                         [(group, TOPIC, partition)
                          for group in sorted(GROUPS) for partition in range(3)])

    def test_reordered_series_labels_and_scientific_numbers_are_valid(self):
        lines = []
        numeric = ['0e0', '1.25e+1', '2E-2', '+3.0', '4', '5.5']
        expected = {}
        for index, (group, partition) in enumerate(
                (group, partition) for group in GROUPS for partition in range(3)):
            line = (METRIC + '{instance="exporter:9308",partition="' + str(partition) +
                    '",topic=' + json.dumps(TOPIC) + ',consumergroup=' + json.dumps(group) +
                    ',rack="test"} ' + numeric[index])
            lines.append(line)
            expected[(group, partition)] = float(numeric[index])
        result = self.check(list(reversed(lines)))
        for sample in result['samples']:
            self.assertEqual(sample['value'], expected[(sample['consumergroup'], sample['partition'])])
            self.assertEqual(sample['labels']['instance'], 'exporter:9308')
            self.assertEqual(sample['labels']['rack'], 'test')

    def test_optional_numeric_timestamps_preserve_values_and_raw_evidence(self):
        for timestamp in ('100', '100.5', '-100', '1700000000123'):
            with self.subTest(timestamp=timestamp):
                lines = required_lines()
                lines[0] = '  ' + lag_line(GROUPS[0], 0, '7.5e+1') + ' ' + timestamp + '  '
                result = self.check(lines)
                sample = next(sample for sample in result['samples']
                              if sample['consumergroup'] == GROUPS[0] and sample['partition'] == 0)
                self.assertEqual(sample['value'], 75.0, 'A timestamp must never become the lag value')
                self.assertEqual(sample['raw_line'], lines[0])
                self.assertEqual(result['observed_samples'], 6)
                json.dumps(result, allow_nan=False)

    def test_middle_token_between_value_and_timestamp_fails(self):
        lines = required_lines()
        lines[0] = lag_line(GROUPS[0], 0, '0 arbitrary 100')
        self.reject(lines)

    def test_extra_suffix_after_optional_timestamp_fails(self):
        for suffix in ('0 100 extra', '0 100 200'):
            with self.subTest(suffix=suffix):
                lines = required_lines()
                lines[0] = lag_line(GROUPS[0], 0, suffix)
                self.reject(lines)

    def test_malformed_postvalue_comments_fail(self):
        for suffix in ('0 # arbitrary comment', '0 # arbitrary 100',
                       '0 100 # arbitrary comment', '0 100 # arbitrary 200'):
            with self.subTest(suffix=suffix):
                lines = required_lines()
                lines[0] = lag_line(GROUPS[0], 0, suffix)
                self.reject(lines)

    def test_unrelated_well_formed_series_do_not_count_as_required_samples(self):
        unrelated = [
            lag_line('labops.stale.notification.v1', 0, '999'),
            lag_line(GROUPS[0], 0, '999', topic='labops.citest.inventory.dlq.v1'),
            'kafka_brokers 3',
            'kafka_topic_partition_replicas{topic=' + json.dumps(TOPIC) + ',partition="0"} 3',
        ]
        result = self.check(required_lines() + unrelated)
        self.assertEqual(result['observed_samples'], 6)
        self.assertEqual(len(result['samples']), 6)
        self.assertEqual(result['total_lag_samples'], 8)
        self.assertEqual(result['ignored_lag_samples'], 2)

    def test_stale_group_samples_cannot_replace_a_required_group(self):
        lines = [lag_line(GROUPS[0], partition) for partition in range(3)]
        lines += [lag_line('labops.stale.analytics.v1', partition) for partition in range(3)]
        self.reject(lines)

    def test_dlq_topic_samples_cannot_replace_main_inventory_samples(self):
        self.reject([lag_line(group, partition, topic='labops.citest.inventory.dlq.v1')
                     for group in GROUPS for partition in range(3)])

    def test_missing_one_partition_fails_despite_unrelated_replacement(self):
        lines = required_lines()[1:]
        lines.append(lag_line('labops.stale.notification.v1', 0))
        self.reject(lines)

    def test_duplicate_required_coordinate_fails_even_with_equal_value(self):
        lines = required_lines()
        self.reject(lines + [lines[0]])

    def test_duplicate_required_coordinate_with_extra_label_also_fails(self):
        self.reject(required_lines() + [lag_line(GROUPS[0], 0, extra_labels={'instance': 'other-exporter'})])

    def test_noncanonical_required_partition_labels_fail(self):
        for partition in ('00', '+0', '-0', '0.0', ' 0', '0 ', '1e0'):
            with self.subTest(partition=partition):
                self.reject(required_lines() + [lag_line(GROUPS[0], partition)])

    def test_unexpected_partition_on_required_group_and_topic_fails(self):
        self.reject(required_lines() + [lag_line(GROUPS[0], 3)])

    def test_missing_required_label_fails(self):
        original = required_lines()[0]
        for removed in ('consumergroup', 'topic', 'partition'):
            with self.subTest(label=removed):
                labels = {'consumergroup': GROUPS[0], 'topic': TOPIC, 'partition': '0'}
                labels.pop(removed)
                line = METRIC + '{' + ','.join(key + '=' + json.dumps(value)
                    for key, value in labels.items()) + '} 0'
                self.assertNotEqual(line, original)
                self.reject([line] + required_lines()[1:])

    def test_duplicate_required_label_keys_fail(self):
        for label, value in [('consumergroup', GROUPS[0]), ('topic', TOPIC), ('partition', '0')]:
            with self.subTest(label=label):
                line = required_lines()[0].replace('} ', ',' + label + '=' + json.dumps(value) + '} ')
                self.reject(required_lines() + [line])

    def test_malformed_required_labels_fail(self):
        malformed = [
            METRIC + '{consumergroup=' + GROUPS[0] + ',topic=' + json.dumps(TOPIC) + ',partition="0"} 0',
            METRIC + '{consumergroup=' + json.dumps(GROUPS[0]) + ',topic="unterminated,partition="0"} 0',
            METRIC + '{consumergroup=' + json.dumps(GROUPS[0]) + ' topic=' + json.dumps(TOPIC) + ',partition="0"} 0',
            METRIC + '{consumergroup=' + json.dumps(GROUPS[0]) + ',topic=' + json.dumps(TOPIC) + ',partition="0" 0',
        ]
        for line in malformed:
            with self.subTest(line=line):
                self.reject(required_lines() + [line])

    def test_nonfinite_and_negative_required_values_fail(self):
        for value in ('NaN', '+Inf', '-Inf', 'Inf', '1e999', '-0.01', '-1'):
            with self.subTest(value=value):
                self.reject([lag_line(GROUPS[0], 0, value)] + required_lines()[1:])

    def test_malformed_required_value_or_exposition_fails(self):
        for suffix in ('', 'bogus', '1.0.0', '1 trailing garbage'):
            with self.subTest(suffix=suffix):
                line = required_lines()[0].rsplit(' ', 1)[0] + ' ' + suffix
                self.reject(required_lines() + [line])

    def test_empty_or_other_metrics_only_payload_cannot_satisfy_contract(self):
        for payload in ('', '# no lag samples\n', 'kafka_brokers 3\n'):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    require_consumer_group_lag(payload, GROUPS, TOPIC)

    def test_groups_must_be_exactly_two_unique_ids(self):
        for groups in ([], [GROUPS[0]], [GROUPS[0], GROUPS[0]], list(GROUPS) + ['extra']):
            with self.subTest(groups=groups):
                with self.assertRaises(ValueError):
                    require_consumer_group_lag(exposition(required_lines()), groups, TOPIC)

    def test_expected_partitions_must_be_exact_inventory_partition_contract(self):
        for partitions in ((), (0, 1), (0, 1, 2, 3), (0, 0, 1), ('0', '1', '2')):
            with self.subTest(partitions=partitions):
                self.reject(required_lines(), expected_partitions=partitions)

    def test_proof_is_json_safe_finite_and_preserves_exact_raw_lines(self):
        lines = required_lines()
        lines[0] = '  ' + lag_line(GROUPS[0], 0, '1.0e+2', extra_labels={'rack': 'test'}) + '  '
        payload = exposition(lines)
        result = require_consumer_group_lag(payload, GROUPS, TOPIC)
        encoded = json.dumps(result, allow_nan=False)
        self.assertEqual(json.loads(encoded), result)
        self.assertEqual({sample['raw_line'] for sample in result['samples']}, set(lines))
        for sample in result['samples']:
            self.assertIsInstance(sample['partition'], int)
            self.assertIsInstance(sample['value'], float)
            self.assertTrue(math.isfinite(sample['value']))
            self.assertGreaterEqual(sample['value'], 0)
            self.assertEqual(sample['labels']['consumergroup'], sample['consumergroup'])
            self.assertEqual(sample['labels']['topic'], sample['topic'])
            self.assertEqual(sample['labels']['partition'], str(sample['partition']))
        self.assertEqual(payload, exposition(lines))


if __name__ == '__main__':
    unittest.main()
