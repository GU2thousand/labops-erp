"""Pinned Redpanda topic request/effective-config and safe failure evidence."""
from concurrent.futures import Future
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AlterConfigOpType
from django.test import SimpleTestCase

from benchmarks.events.recovery_matrix import _RecoveryMatrix, _kafka_diagnostics, _scope


def completed(value=None, error=None):
    future = Future()
    if error is not None:
        future.set_exception(error)
    else:
        future.set_result(value)
    return future


class RecoveryTopicContractTests(SimpleTestCase):
    def matrix(self):
        matrix = object.__new__(_RecoveryMatrix)
        matrix.run_id = 'topic-contract'
        matrix.main_topic = 'labops.topic-contract.inventory.v1'
        matrix.topics = set()
        matrix.cases = []
        matrix.last_kafka_operation = None
        matrix.h = SimpleNamespace(settings=SimpleNamespace(
            KAFKA_DLQ_TOPIC='labops.topic-contract.inventory.dlq.v1'))
        matrix.admin = Mock()
        matrix.admin.list_topics.return_value = SimpleNamespace(topics={})
        matrix._wait_topic = Mock()
        return matrix

    def test_create_uses_valid_topic_override_and_real_replication_contract(self):
        matrix = self.matrix()
        captured = []

        def create(topics, **options):
            topic = topics[0]
            captured.append(topic)
            # Pinned v26.2.2 validators.h rejects the global-only disabled mode
            # before topic registration. This mock fails the formerly sent
            # request rather than merely asserting a copied config dictionary.
            error = (KafkaException(KafkaError(KafkaError.INVALID_CONFIG))
                     if topic.config['write.caching'] == 'disabled' else None)
            return {topic.topic: completed(error=error)}
        matrix.admin.create_topics.side_effect = create
        scope = _scope(matrix.run_id, 'retention')
        config = matrix._create_topic(scope['topic'], retention_ms=-1, segment_bytes=1048576)
        self.assertEqual(config['write.caching'], 'false')
        self.assertNotIn('min.insync.replicas', config)
        self.assertEqual(config['segment.bytes'], '1048576')
        self.assertEqual(config['retention.ms'], '-1')
        self.assertEqual((captured[0].num_partitions, captured[0].replication_factor), (1, 3))
        self.assertEqual(matrix.topics, {scope['topic']})
        matrix._wait_topic.assert_called_once_with(scope['topic'])

    def configured_matrix(self, actual_overrides=None, before_overrides=None):
        matrix = self.matrix()
        topic = _scope(matrix.run_id, 'retention')['topic']
        config = {'cleanup.policy': 'delete', 'retention.ms': '1000', 'segment.bytes': '1048576',
                  'write.caching': 'false', 'compression.type': 'producer'}
        # DescribeConfigs intentionally omits the no-op min.insync.replicas.
        before = {**config, 'retention.ms': '-1', 'write.caching': 'disabled',
                  **(before_overrides or {})}
        after = {**config, 'write.caching': 'disabled', **(actual_overrides or {})}
        descriptions = iter([before, after])
        # v26.2.2 config_utils.h rejects any explicit write.caching override
        # in AlterConfigs when its cluster default is disabled. Reject the
        # formerly used request, while testing the real incremental API type.
        matrix.admin.alter_configs.side_effect = KafkaException(KafkaError(KafkaError.INVALID_CONFIG))

        def incremental(resources, **_):
            resource = resources[0]
            entries = resource.incremental_configs
            valid = (len(entries) == 1 and entries[0].name == 'retention.ms'
                     and entries[0].incremental_operation == AlterConfigOpType.SET)
            error = None if valid else KafkaException(KafkaError(KafkaError.INVALID_CONFIG))
            return {resource: completed(error=error)}

        matrix.admin.incremental_alter_configs.side_effect = incremental
        matrix.admin.describe_configs.side_effect = lambda resources, **_: {
            resources[0]: completed({key: SimpleNamespace(value=value)
                                     for key, value in next(descriptions).items()})}
        return matrix, topic, config

    def test_alter_accepts_global_disabled_effective_value_without_noop_property(self):
        matrix, topic, config = self.configured_matrix()
        evidence = matrix._alter_topic(topic, config)
        matrix.admin.alter_configs.assert_not_called()
        resource = matrix.admin.incremental_alter_configs.call_args.args[0][0]
        self.assertEqual(resource.set_config_dict, {})
        self.assertEqual([(entry.name, entry.value, entry.incremental_operation)
                          for entry in resource.incremental_configs],
                         [('retention.ms', '1000', AlterConfigOpType.SET)])
        self.assertEqual(evidence['api'], 'incremental_alter_configs')
        self.assertEqual(evidence['updates'], {'retention.ms': '1000'})
        self.assertEqual(evidence['observed_before']['retention.ms'], '-1')
        self.assertEqual(evidence['observed_after']['retention.ms'], '1000')
        self.assertEqual(evidence['observed_after']['write.caching'], 'disabled')
        self.assertEqual(evidence['config'], config)
        self.assertEqual(matrix.admin.describe_configs.call_count, 2)

    def test_alter_never_accepts_enabled_caching_or_changes_other_exact_gates(self):
        for bad in ({'write.caching': 'true'}, {'retention.ms': '60000'},
                    {'segment.bytes': '2097152'}, {'cleanup.policy': 'compact'},
                    {'compression.type': 'gzip'}):
            with self.subTest(bad=bad):
                matrix, topic, config = self.configured_matrix(bad)
                with self.assertRaises(AssertionError):
                    matrix._alter_topic(topic, config)

    def test_alter_refuses_existing_fixed_property_drift_before_any_update(self):
        for bad in ({'write.caching': 'true'}, {'segment.bytes': '2097152'},
                    {'cleanup.policy': 'compact'}, {'compression.type': 'gzip'}):
            with self.subTest(bad=bad):
                matrix, topic, config = self.configured_matrix(before_overrides=bad)
                with self.assertRaises(AssertionError):
                    matrix._alter_topic(topic, config)
                matrix.admin.incremental_alter_configs.assert_not_called()
                matrix.admin.alter_configs.assert_not_called()
                self.assertEqual(matrix.last_kafka_operation['step'], 'verify_topic_config_before_alter')

    def test_alter_restores_unlimited_retention_with_only_incremental_set(self):
        matrix, topic, config = self.configured_matrix(
            {'retention.ms': '-1'}, {'retention.ms': '1000'})
        config['retention.ms'] = '-1'
        evidence = matrix._alter_topic(topic, config)
        self.assertEqual(evidence['updates'], {'retention.ms': '-1'})
        resource = matrix.admin.incremental_alter_configs.call_args.args[0][0]
        self.assertEqual([(entry.name, entry.value, entry.incremental_operation)
                          for entry in resource.incremental_configs],
                         [('retention.ms', '-1', AlterConfigOpType.SET)])
        matrix.admin.alter_configs.assert_not_called()

    def test_alter_skips_already_applied_retention_but_still_checks_every_property(self):
        matrix, topic, config = self.configured_matrix(before_overrides={'retention.ms': '1000'})
        evidence = matrix._alter_topic(topic, config)
        self.assertEqual(evidence['updates'], {})
        self.assertEqual(matrix.admin.describe_configs.call_count, 2)
        matrix.admin.incremental_alter_configs.assert_not_called()
        matrix.admin.alter_configs.assert_not_called()

    def test_alter_rejection_preserves_safe_native_error_and_exact_incremental_operation(self):
        matrix, topic, config = self.configured_matrix()
        private = 'sasl.password=private-password synthetic-original-payload'
        error = KafkaException(KafkaError(KafkaError.INVALID_CONFIG, private))
        matrix.admin.incremental_alter_configs.side_effect = lambda resources, **_: {
            resources[0]: completed(error=error)}
        matrix.restart = lambda: matrix._alter_topic(topic, config)
        matrix.h.events = []
        matrix.h.args = SimpleNamespace(drain_timeout=1)
        matrix.h.drained = Mock()
        matrix.h.api = SimpleNamespace(safe_error=lambda _: 'KafkaException')
        matrix._pause_main = Mock()
        matrix._resume_main = Mock()
        with TemporaryDirectory() as directory:
            matrix.h.evidence = Path(directory)
            cases = matrix.run()
            self.assertFalse(cases[0]['passed'])
            self.assertEqual(cases[0]['kafka_error_code'], KafkaError.INVALID_CONFIG)
            operation = cases[0]['last_kafka_operation']
            self.assertEqual(operation['step'], 'alter_topic_config')
            self.assertEqual(operation['api'], 'incremental_alter_configs')
            self.assertEqual(operation['updates'], {'retention.ms': '1000'})
            self.assertEqual(operation['observed_before']['write.caching'], 'disabled')
            self.assertNotIn('observed_after', operation)
            for name in ('recovery-matrix.json', 'errors.jsonl'):
                output = (Path(directory) / name).read_text()
                self.assertNotIn(private, output)
                self.assertNotIn('private-password', output)
        matrix.admin.alter_configs.assert_not_called()

    def test_create_rejection_preserves_native_code_step_and_never_private_text(self):
        matrix = self.matrix()
        private = 'sasl.password=private-password synthetic-original-payload'
        error = KafkaException(KafkaError(KafkaError.INVALID_CONFIG, private))
        matrix.admin.create_topics.side_effect = lambda topics, **_: {
            topics[0].topic: completed(error=error)}
        matrix.h.events = []
        matrix.h.args = SimpleNamespace(drain_timeout=1)
        matrix.h.drained = Mock()
        matrix.h.api = SimpleNamespace(safe_error=lambda _: 'KafkaException')
        matrix._pause_main = Mock()
        matrix._resume_main = Mock()
        with TemporaryDirectory() as directory:
            matrix.h.evidence = Path(directory)
            cases = matrix.run()
            self.assertFalse(cases[0]['passed'])
            self.assertEqual(cases[0]['kafka_error_code'], KafkaError.INVALID_CONFIG)
            self.assertEqual(cases[0]['kafka_error_name'], 'INVALID_CONFIG')
            self.assertEqual(cases[0]['last_kafka_operation']['step'], 'create_topic')
            self.assertEqual(cases[0]['last_kafka_operation']['config']['write.caching'], 'false')
            self.assertEqual(matrix.topics, set())
            for name in ('recovery-matrix.json', 'errors.jsonl'):
                output = (Path(directory) / name).read_text()
                self.assertIn('"kafka_error_code": 40', output)
                self.assertNotIn(private, output)
                self.assertNotIn('private-password', output)
            self.assertFalse(json.loads((Path(directory) / 'recovery-matrix.json').read_text())['passed'])
        matrix._resume_main.assert_called_once()

    def test_generic_exception_cannot_be_mislabelled_as_native_kafka_error(self):
        self.assertEqual(_kafka_diagnostics(RuntimeError('private-password')), {})

    def retention_observation(self, responses):
        matrix = self.matrix()
        topic = _scope(matrix.run_id, 'retention')['topic']
        clock = SimpleNamespace(now=0)
        pending = iter(responses)

        def watermarks(_topic, *, timeout):
            elapsed, low = next(pending)
            clock.now += elapsed
            return {'low': low, 'high': 20}

        matrix._watermarks = Mock(side_effect=watermarks)
        with TemporaryDirectory() as directory, patch(
                'benchmarks.events.recovery_matrix.time.monotonic', side_effect=lambda: clock.now), patch(
                'benchmarks.events.recovery_matrix.time.sleep',
                side_effect=lambda seconds: setattr(clock, 'now', clock.now + seconds)) as sleep:
            matrix.h.evidence = Path(directory)
            report = matrix._wait_retention_cleanup(topic, 3)
            persisted = json.loads((Path(directory) / 'retention-policy.json').read_text())
        return report, persisted, matrix._watermarks, sleep

    def test_retention_late_cleaned_watermark_cannot_pass_and_is_preserved(self):
        # The second request begins at 74s and returns a cleaned watermark at
        # 80s. The former pre-call check incorrectly accepted this result.
        report, persisted, reads, _ = self.retention_observation([(73, 0), (6, 8)])
        self.assertFalse(report['policy_cleanup_observed'])
        self.assertEqual(report['elapsed_seconds'], 80)
        self.assertEqual([call.kwargs['timeout'] for call in reads.call_args_list], [10, 1])
        self.assertEqual(report['watermark_samples'][-1], {
            'low': 8, 'high': 20, 'elapsed_seconds': 80,
            'query_timeout_seconds': 1, 'within_deadline': False})
        self.assertEqual(persisted, report)

    def test_retention_exact_deadline_boundary_passes_but_any_later_result_fails(self):
        for delay, passed in ((1, True), (1.000001, False)):
            with self.subTest(delay=delay):
                report, persisted, reads, _ = self.retention_observation([(73, 0), (delay, 8)])
                self.assertEqual(report['policy_cleanup_observed'], passed)
                self.assertEqual(report['watermark_samples'][-1]['within_deadline'], passed)
                self.assertEqual(reads.call_args.kwargs['timeout'], 1)
                self.assertEqual(persisted, report)

    def test_retention_no_cleanup_stops_at_original_deadline_and_bounds_sleep(self):
        report, persisted, reads, sleep = self.retention_observation([(74.5, 0)])
        self.assertFalse(report['policy_cleanup_observed'])
        self.assertEqual(report['elapsed_seconds'], 75)
        self.assertEqual(reads.call_count, 1)
        sleep.assert_called_once_with(0.5)
        self.assertEqual(persisted, report)

    def test_retention_watermark_native_timeout_subtracts_consumer_construction(self):
        matrix = self.matrix()
        topic = _scope(matrix.run_id, 'retention')['topic']
        clock = SimpleNamespace(now=0)
        client = Mock()
        client.get_watermark_offsets.return_value = (0, 20)
        matrix._consumer_config = Mock(return_value={})

        def create(_config):
            clock.now += 2
            return client

        with patch('confluent_kafka.Consumer', side_effect=create), patch(
                'benchmarks.events.recovery_matrix.time.monotonic', side_effect=lambda: clock.now):
            self.assertEqual(matrix._watermarks(topic, timeout=3), {'low': 0, 'high': 20})
        self.assertEqual(client.get_watermark_offsets.call_args.kwargs,
                         {'timeout': 1, 'cached': False})
        client.close.assert_called_once()

    def test_retention_watermark_expired_construction_budget_never_starts_native_read(self):
        matrix = self.matrix()
        topic = _scope(matrix.run_id, 'retention')['topic']
        clock = SimpleNamespace(now=0)
        client = Mock()
        matrix._consumer_config = Mock(return_value={})

        def create(_config):
            clock.now += 4
            return client

        with patch('confluent_kafka.Consumer', side_effect=create), patch(
                'benchmarks.events.recovery_matrix.time.monotonic', side_effect=lambda: clock.now):
            with self.assertRaises(AssertionError):
                matrix._watermarks(topic, timeout=3)
        client.get_watermark_offsets.assert_not_called()
        client.close.assert_called_once()

    def test_retention_query_error_preserves_safe_error_evidence_and_stays_failed(self):
        matrix = self.matrix()
        topic = _scope(matrix.run_id, 'retention')['topic']
        private = 'sasl.password=private-password synthetic-original-payload'
        matrix._watermarks = Mock(side_effect=KafkaException(KafkaError(KafkaError._TIMED_OUT, private)))
        with TemporaryDirectory() as directory, patch(
                'benchmarks.events.recovery_matrix.time.monotonic', return_value=0):
            matrix.h.evidence = Path(directory)
            with self.assertRaises(KafkaException):
                matrix._wait_retention_cleanup(topic, 3)
            output = (Path(directory) / 'retention-policy.json').read_text()
        persisted = json.loads(output)
        self.assertFalse(persisted['policy_cleanup_observed'])
        self.assertEqual(persisted['error_type'], 'KafkaException')
        self.assertEqual(persisted['kafka_error_code'], KafkaError._TIMED_OUT)
        self.assertNotIn(private, output)
        self.assertNotIn('private-password', output)
