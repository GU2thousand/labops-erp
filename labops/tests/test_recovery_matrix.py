"""Pinned Redpanda topic request/effective-config and safe failure evidence."""
from concurrent.futures import Future
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock

from confluent_kafka import KafkaError, KafkaException
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

    def configured_matrix(self, actual_overrides=None):
        matrix = self.matrix()
        topic = _scope(matrix.run_id, 'retention')['topic']
        config = {'cleanup.policy': 'delete', 'retention.ms': '1000', 'segment.bytes': '1048576',
                  'write.caching': 'false', 'compression.type': 'producer'}
        # DescribeConfigs intentionally omits the no-op min.insync.replicas.
        actual = {**config, 'write.caching': 'disabled', **(actual_overrides or {})}
        matrix.admin.alter_configs.side_effect = lambda resources, **_: {
            resources[0]: completed()}
        matrix.admin.describe_configs.side_effect = lambda resources, **_: {
            resources[0]: completed({key: SimpleNamespace(value=value) for key, value in actual.items()})}
        return matrix, topic, config

    def test_alter_accepts_global_disabled_effective_value_without_noop_property(self):
        matrix, topic, config = self.configured_matrix()
        matrix._alter_topic(topic, config)
        resource = matrix.admin.alter_configs.call_args.args[0][0]
        self.assertEqual(resource.set_config_dict['write.caching'], 'false')
        self.assertNotIn('min.insync.replicas', resource.set_config_dict)

    def test_alter_never_accepts_enabled_caching_or_changes_other_exact_gates(self):
        for bad in ({'write.caching': 'true'}, {'retention.ms': '60000'},
                    {'segment.bytes': '2097152'}, {'cleanup.policy': 'compact'}):
            with self.subTest(bad=bad):
                matrix, topic, config = self.configured_matrix(bad)
                with self.assertRaises(AssertionError):
                    matrix._alter_topic(topic, config)

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
