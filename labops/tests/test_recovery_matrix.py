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


def topic_metadata(topic, *, identity=None, partition_id=0, topic_error=None,
                   partition_error=None, isrs=(0, 1, 2)):
    part = SimpleNamespace(id=partition_id, error=partition_error, leader=2,
                           replicas=[0, 1, 2], isrs=list(isrs))
    return SimpleNamespace(topics={topic: SimpleNamespace(topic=identity or topic,
        error=topic_error, partitions={0: part})})


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

    def readiness_matrix(self):
        matrix = self.matrix()
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        matrix.h.evidence = Path(directory.name)
        matrix._consumer_config = Mock(return_value={})
        topic = _scope(matrix.run_id, 'recreate')['topic']
        clock = SimpleNamespace(now=0)
        client = Mock()
        client.list_topics.return_value = topic_metadata(topic)
        matrix.admin.list_topics.return_value = topic_metadata(topic)
        return matrix, topic, clock, client

    def clock_context(self, clock):
        return patch('benchmarks.events.recovery_matrix.time.monotonic', side_effect=lambda: clock.now)

    def clock_sleep(self, clock):
        return patch('benchmarks.events.recovery_matrix.time.sleep',
                     side_effect=lambda seconds: setattr(clock, 'now', clock.now + seconds))

    def test_watermark_leader_transition_refreshes_same_client_and_retains_native_code(self):
        matrix, topic, clock, client = self.readiness_matrix()
        private = 'sasl.password=private-password synthetic-original-payload'
        transition = KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION, private))
        replies = iter([transition, (0, 0)])

        def query(_partition, **_):
            clock.now += .2
            result = next(replies)
            if isinstance(result, Exception):
                raise result
            return result

        def metadata(**_):
            clock.now += .1
            return topic_metadata(topic)

        client.get_watermark_offsets.side_effect = query
        client.list_topics.side_effect = metadata
        with self.clock_context(clock), self.clock_sleep(clock), patch(
                'confluent_kafka.Consumer', return_value=client) as consumer:
            self.assertEqual(matrix._watermarks(topic, timeout=3), {'low': 0, 'high': 0})
        consumer.assert_called_once()
        client.list_topics.assert_called_once()
        self.assertEqual(client.list_topics.call_args.kwargs, {'timeout': 2.65})
        self.assertEqual(client.get_watermark_offsets.call_count, 2)
        self.assertEqual(client.get_watermark_offsets.call_args_list[0].kwargs,
                         {'timeout': 3, 'cached': False})
        self.assertAlmostEqual(client.get_watermark_offsets.call_args_list[1].kwargs['timeout'], 2.55)
        for call in client.get_watermark_offsets.call_args_list:
            self.assertEqual((call.args[0].topic, call.args[0].partition), (topic, 0))
        client.close.assert_called_once()
        output = (matrix.h.evidence / 'errors.jsonl').read_text()
        evidence = json.loads(output)
        self.assertEqual(evidence['kafka_error_code'], 6)
        self.assertEqual(evidence['kafka_error_name'], 'NOT_LEADER_FOR_PARTITION')
        self.assertFalse(evidence['kafka_error_retriable'])
        self.assertFalse(evidence['kafka_error_fatal'])
        self.assertEqual(evidence['step'], 'read_topic_watermarks')
        self.assertNotIn('private-password', output)
        self.assertNotIn('synthetic-original-payload', output)
        self.assertEqual(matrix.last_kafka_operation['step'], 'read_topic_watermarks')
        self.assertTrue(matrix.last_kafka_operation['within_deadline'])

    def test_persistent_watermark_code_six_stops_on_original_budget_and_remains_native_failure(self):
        matrix, topic, clock, client = self.readiness_matrix()
        transition = KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))

        def query(_partition, **_):
            clock.now += 1
            raise transition

        client.get_watermark_offsets.side_effect = query
        with self.clock_context(clock), self.clock_sleep(clock), patch(
                'confluent_kafka.Consumer', return_value=client), self.assertRaises(KafkaException) as raised:
            matrix._watermarks(topic, timeout=3)
        self.assertIs(raised.exception, transition)
        self.assertEqual(_kafka_diagnostics(raised.exception)['kafka_error_code'], 6)
        self.assertEqual(client.get_watermark_offsets.call_count, 3)
        self.assertEqual(client.list_topics.call_count, 2)
        self.assertLess(client.get_watermark_offsets.call_args.kwargs['timeout'], 1)
        client.close.assert_called_once()
        self.assertAlmostEqual(matrix.last_kafka_operation['elapsed_seconds'], 3.3)
        self.assertEqual(matrix.last_kafka_operation['query_elapsed_seconds'], 1)
        self.assertEqual(matrix.last_kafka_operation['cleanup_seconds'], 0)
        self.assertFalse(matrix.last_kafka_operation['within_deadline'])
        errors = [json.loads(line) for line in (matrix.h.evidence / 'errors.jsonl').read_text().splitlines()]
        self.assertEqual([row['kafka_error_code'] for row in errors], [6, 6, 6])
        self.assertEqual(errors[-1]['query_elapsed_seconds'], 1)
        self.assertFalse(errors[-1]['within_deadline'])

    def test_watermark_authentication_and_every_other_native_code_fail_without_retry(self):
        for code in (KafkaError.TOPIC_AUTHORIZATION_FAILED, KafkaError.GROUP_AUTHORIZATION_FAILED,
                     KafkaError._AUTHENTICATION, KafkaError.LEADER_NOT_AVAILABLE, KafkaError._TIMED_OUT):
            with self.subTest(code=code):
                matrix, topic, clock, client = self.readiness_matrix()
                error = KafkaException(KafkaError(code))
                client.get_watermark_offsets.side_effect = error
                with self.clock_context(clock), self.clock_sleep(clock) as sleep, patch(
                        'confluent_kafka.Consumer', return_value=client), self.assertRaises(KafkaException) as raised:
                    matrix._watermarks(topic, timeout=3)
                self.assertIs(raised.exception, error)
                client.get_watermark_offsets.assert_called_once()
                client.list_topics.assert_not_called()
                client.close.assert_called_once()
                sleep.assert_not_called()

    def test_fatal_code_six_is_terminal_even_though_its_numeric_code_matches(self):
        for stage in ('query', 'metadata_refresh', 'metadata_wait'):
            with self.subTest(stage=stage):
                matrix, topic, clock, client = self.readiness_matrix()
                fatal = KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION,
                                                 'private-password', fatal=True))
                if stage == 'metadata_wait':
                    matrix.admin.list_topics.side_effect = fatal
                    with self.clock_context(clock), self.clock_sleep(clock) as sleep, self.assertRaises(KafkaException) as raised:
                        _RecoveryMatrix._wait_topic(matrix, topic, timeout=3)
                    matrix.admin.list_topics.assert_called_once()
                    sleep.assert_not_called()
                else:
                    client.get_watermark_offsets.side_effect = (
                        fatal if stage == 'query' else KafkaException(
                            KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION)))
                    client.list_topics.side_effect = fatal
                    with self.clock_context(clock), self.clock_sleep(clock), patch(
                            'confluent_kafka.Consumer', return_value=client), self.assertRaises(KafkaException) as raised:
                        matrix._watermarks(topic, timeout=3)
                    client.get_watermark_offsets.assert_called_once()
                    client.close.assert_called_once()
                self.assertIs(raised.exception, fatal)
                self.assertEqual(_kafka_diagnostics(raised.exception)['kafka_error_code'], 6)
                self.assertTrue(_kafka_diagnostics(raised.exception)['kafka_error_fatal'])

    def test_cleanup_error_preserves_primary_exception_and_also_fails_an_otherwise_successful_read(self):
        for failed_query in (True, False):
            with self.subTest(failed_query=failed_query):
                matrix, topic, clock, client = self.readiness_matrix()
                primary = KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED,
                                                    'primary-private-password'))
                cleanup = RuntimeError('cleanup-private-password')
                if failed_query:
                    client.get_watermark_offsets.side_effect = primary
                else:
                    client.get_watermark_offsets.return_value = (0, 0)
                client.close.side_effect = cleanup
                expected = KafkaException if failed_query else RuntimeError
                with self.clock_context(clock), self.clock_sleep(clock), patch(
                        'confluent_kafka.Consumer', return_value=client), self.assertRaises(expected) as raised:
                    matrix._watermarks(topic, timeout=3)
                self.assertIs(raised.exception, primary if failed_query else cleanup)
                output = (matrix.h.evidence / 'errors.jsonl').read_text()
                evidence = json.loads(output)
                self.assertEqual(evidence['error_type'], 'RuntimeError')
                self.assertEqual(evidence['primary_error_type'], 'KafkaException' if failed_query else None)
                self.assertNotIn('primary-private-password', output)
                self.assertNotIn('cleanup-private-password', output)
                self.assertEqual(matrix.last_kafka_operation['step'],
                                 'read_topic_watermarks' if failed_query else 'close_watermark_consumer')
                client.get_watermark_offsets.assert_called_once()
                client.close.assert_called_once()

    def test_watermark_refresh_preserves_topic_partition_identity_auth_and_full_isr(self):
        for bad, expected in (
                ({'identity': 'different-topic'}, AssertionError),
                ({'partition_id': 1}, AssertionError),
                ({'topic_error': KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)}, KafkaException),
                ({'partition_error': KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)}, KafkaException),
                ({'isrs': (0, 1)}, KafkaException),
                ({'isrs': (0, 1, 1)}, KafkaException)):
            with self.subTest(bad=bad):
                matrix, topic, clock, client = self.readiness_matrix()
                client.get_watermark_offsets.side_effect = KafkaException(
                    KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))
                client.list_topics.return_value = topic_metadata(topic, **bad)
                with self.clock_context(clock), self.clock_sleep(clock), patch(
                        'confluent_kafka.Consumer', return_value=client), self.assertRaises(expected):
                    matrix._watermarks(topic, timeout=3)
                client.get_watermark_offsets.assert_called_once()
                client.close.assert_called_once()
        matrix, _, _, _ = self.readiness_matrix()
        with patch('confluent_kafka.Consumer') as consumer, self.assertRaises(ValueError):
            matrix._watermarks(matrix.main_topic)
        consumer.assert_not_called()

    def test_watermark_late_success_refresh_or_cleanup_never_passes(self):
        for stage in ('query', 'metadata_refresh', 'cleanup'):
            with self.subTest(stage=stage):
                matrix, topic, clock, client = self.readiness_matrix()

                def query(_partition, **_):
                    if stage == 'metadata_refresh':
                        raise KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))
                    clock.now += 3.1 if stage == 'query' else 1
                    return (0, 0)

                client.get_watermark_offsets.side_effect = query
                if stage == 'metadata_refresh':
                    def metadata(**_):
                        clock.now += 3
                        return topic_metadata(topic)
                    client.list_topics.side_effect = metadata
                if stage == 'cleanup':
                    client.close.side_effect = lambda: setattr(clock, 'now', clock.now + 3)
                with self.clock_context(clock), self.clock_sleep(clock), patch(
                        'confluent_kafka.Consumer', return_value=client), self.assertRaises(AssertionError):
                    matrix._watermarks(topic, timeout=3)
                client.close.assert_called_once()
                client.get_watermark_offsets.assert_called_once()

    def test_metadata_wait_retries_only_code_six_and_requires_full_ready_identity(self):
        matrix, topic, clock, _ = self.readiness_matrix()
        replies = iter([KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION)),
                        topic_metadata(topic, isrs=(0, 1)), topic_metadata(topic)])

        def metadata(**_):
            clock.now += .2
            response = next(replies)
            if isinstance(response, Exception):
                raise response
            return response

        matrix.admin.list_topics.side_effect = metadata
        with self.clock_context(clock), self.clock_sleep(clock):
            _RecoveryMatrix._wait_topic(matrix, topic, timeout=3)
        self.assertEqual(matrix.admin.list_topics.call_count, 3)
        self.assertEqual(matrix.admin.list_topics.call_args_list[0].kwargs, {'timeout': 3})
        self.assertTrue(matrix.last_kafka_operation['within_deadline'])
        self.assertEqual(json.loads((matrix.h.evidence / 'errors.jsonl').read_text())['kafka_error_code'], 6)

    def test_metadata_wait_auth_wrong_identity_and_persistent_transition_cannot_pass(self):
        for bad, expected in (
                (KafkaException(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)), KafkaException),
                (topic_metadata('placeholder', identity='wrong-topic'), AssertionError),
                (topic_metadata('placeholder', partition_id=1), AssertionError),
                (topic_metadata('placeholder', topic_error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED)), KafkaException)):
            with self.subTest(bad=bad):
                matrix, topic, clock, _ = self.readiness_matrix()
                if isinstance(bad, Exception):
                    matrix.admin.list_topics.side_effect = bad
                else:
                    bad.topics[topic] = bad.topics.pop('placeholder')
                    if bad.topics[topic].topic == 'placeholder':
                        bad.topics[topic].topic = topic
                    matrix.admin.list_topics.return_value = bad
                with self.clock_context(clock), self.clock_sleep(clock), self.assertRaises(expected):
                    _RecoveryMatrix._wait_topic(matrix, topic, timeout=3)
                matrix.admin.list_topics.assert_called_once()
        matrix, topic, clock, _ = self.readiness_matrix()
        transition = KafkaException(KafkaError(KafkaError.NOT_LEADER_FOR_PARTITION))

        def persistent(**_):
            clock.now += 1
            raise transition

        matrix.admin.list_topics.side_effect = persistent
        with self.clock_context(clock), self.clock_sleep(clock), self.assertRaises(KafkaException) as raised:
            _RecoveryMatrix._wait_topic(matrix, topic, timeout=3)
        self.assertIs(raised.exception, transition)
        self.assertEqual(matrix.admin.list_topics.call_count, 3)
        self.assertAlmostEqual(matrix.last_kafka_operation['elapsed_seconds'], 3.3)
        self.assertEqual(matrix.last_kafka_operation['query_elapsed_seconds'], 1)
        self.assertFalse(matrix.last_kafka_operation['within_deadline'])

    def test_metadata_wait_late_full_isr_response_never_passes(self):
        matrix, topic, clock, _ = self.readiness_matrix()

        def late(**_):
            clock.now += 3.1
            return topic_metadata(topic)

        matrix.admin.list_topics.side_effect = late
        with self.clock_context(clock), self.clock_sleep(clock), self.assertRaises(AssertionError):
            _RecoveryMatrix._wait_topic(matrix, topic, timeout=3)
        self.assertFalse(matrix.last_kafka_operation['within_deadline'])
        matrix.admin.list_topics.assert_called_once_with(timeout=3)

    def test_terminal_watermark_errors_stamp_query_and_secondary_cleanup_durations(self):
        for code, fatal in ((KafkaError.TOPIC_AUTHORIZATION_FAILED, False),
                            (KafkaError.NOT_LEADER_FOR_PARTITION, True)):
            with self.subTest(code=code, fatal=fatal):
                matrix, topic, clock, client = self.readiness_matrix()
                primary = KafkaException(KafkaError(code, 'primary-private-password', fatal=fatal))

                def query(_partition, **_):
                    clock.now += .4
                    raise primary

                def cleanup():
                    clock.now += .3
                    raise RuntimeError('cleanup-private-password')

                client.get_watermark_offsets.side_effect = query
                client.close.side_effect = cleanup
                with self.clock_context(clock), self.clock_sleep(clock) as sleep, patch(
                        'confluent_kafka.Consumer', return_value=client), self.assertRaises(KafkaException) as raised:
                    matrix._watermarks(topic, timeout=.5)
                self.assertIs(raised.exception, primary)
                operation = matrix.last_kafka_operation
                self.assertEqual(operation['step'], 'read_topic_watermarks')
                self.assertEqual(operation['query_timeout_seconds'], .5)
                self.assertAlmostEqual(operation['query_elapsed_seconds'], .4)
                self.assertAlmostEqual(operation['cleanup_seconds'], .3)
                self.assertAlmostEqual(operation['elapsed_seconds'], .7)
                self.assertAlmostEqual(operation['cleanup_elapsed_seconds'], .7)
                self.assertEqual(operation['construction_seconds'], 0)
                self.assertFalse(operation['within_deadline'])
                self.assertEqual(operation['secondary_cleanup_error_type'], 'RuntimeError')
                output = (matrix.h.evidence / 'errors.jsonl').read_text()
                evidence = json.loads(output)
                self.assertAlmostEqual(evidence['cleanup_seconds'], .3)
                self.assertAlmostEqual(evidence['elapsed_seconds'], .7)
                self.assertFalse(evidence['within_deadline'])
                self.assertNotIn('primary-private-password', output)
                self.assertNotIn('cleanup-private-password', output)
                client.get_watermark_offsets.assert_called_once()
                client.list_topics.assert_not_called()
                sleep.assert_not_called()

    def test_terminal_metadata_errors_stamp_actual_request_time_and_deadline(self):
        for code, fatal in ((KafkaError.TOPIC_AUTHORIZATION_FAILED, False),
                            (KafkaError.NOT_LEADER_FOR_PARTITION, True)):
            for delay in (.8, 1.2):
                with self.subTest(code=code, fatal=fatal, delay=delay):
                    matrix, topic, clock, _ = self.readiness_matrix()
                    error = KafkaException(KafkaError(code, fatal=fatal))

                    def query(**_):
                        clock.now += delay
                        raise error

                    matrix.admin.list_topics.side_effect = query
                    with self.clock_context(clock), self.clock_sleep(clock) as sleep, self.assertRaises(KafkaException) as raised:
                        _RecoveryMatrix._wait_topic(matrix, topic, timeout=1)
                    self.assertIs(raised.exception, error)
                    operation = matrix.last_kafka_operation
                    self.assertEqual(operation['step'], 'wait_topic_full_isr')
                    self.assertEqual(operation['query_timeout_seconds'], 1)
                    self.assertAlmostEqual(operation['query_elapsed_seconds'], delay)
                    self.assertAlmostEqual(operation['elapsed_seconds'], delay)
                    self.assertEqual(operation['within_deadline'], delay <= 1)
                    matrix.admin.list_topics.assert_called_once_with(timeout=1)
                    sleep.assert_not_called()

    def test_watermark_constructor_failure_stamps_elapsed_without_inventing_query_or_cleanup(self):
        matrix, topic, clock, _ = self.readiness_matrix()
        error = KafkaException(KafkaError(KafkaError._AUTHENTICATION))

        def construct(_config):
            clock.now += .4
            raise error

        with self.clock_context(clock), patch('confluent_kafka.Consumer', side_effect=construct), self.assertRaises(KafkaException) as raised:
            matrix._watermarks(topic, timeout=1)
        self.assertIs(raised.exception, error)
        operation = matrix.last_kafka_operation
        self.assertEqual(operation['step'], 'construct_watermark_consumer')
        self.assertAlmostEqual(operation['construction_seconds'], .4)
        self.assertAlmostEqual(operation['elapsed_seconds'], .4)
        self.assertEqual(operation['cleanup_seconds'], 0)
        self.assertTrue(operation['within_deadline'])
        self.assertNotIn('query_elapsed_seconds', operation)
        self.assertNotIn('query_timeout_seconds', operation)

    def test_constructor_failure_keeps_zero_cleanup_with_advancing_clock_reads(self):
        matrix, topic, clock, _ = self.readiness_matrix()
        error = KafkaException(KafkaError(KafkaError._AUTHENTICATION))

        def read_clock():
            clock.now += .001
            return clock.now

        def construct(_config):
            clock.now += .4
            raise error

        with patch('benchmarks.events.recovery_matrix.time.monotonic', side_effect=read_clock), patch(
                'confluent_kafka.Consumer', side_effect=construct) as constructor, self.assertRaises(KafkaException) as raised:
            matrix._watermarks(topic, timeout=1)
        self.assertIs(raised.exception, error)
        constructor.assert_called_once()
        operation = matrix.last_kafka_operation
        self.assertAlmostEqual(operation['construction_seconds'], .401)
        self.assertGreater(operation['elapsed_seconds'], operation['construction_seconds'])
        self.assertEqual(operation['cleanup_seconds'], 0)
        self.assertTrue(operation['within_deadline'])
        self.assertNotIn('query_elapsed_seconds', operation)
        self.assertNotIn('query_timeout_seconds', operation)

    def test_watermark_success_and_late_cleanup_stamp_complete_separate_durations(self):
        for close_delay, passed in ((.15, True), (.8, False)):
            with self.subTest(close_delay=close_delay):
                matrix, topic, clock, client = self.readiness_matrix()

                def construct(_config):
                    clock.now += .1
                    return client

                def query(_partition, **_):
                    clock.now += .25
                    return (0, 0)

                client.get_watermark_offsets.side_effect = query
                client.close.side_effect = lambda: setattr(clock, 'now', clock.now + close_delay)
                with self.clock_context(clock), patch('confluent_kafka.Consumer', side_effect=construct):
                    if passed:
                        self.assertEqual(matrix._watermarks(topic, timeout=1), {'low': 0, 'high': 0})
                    else:
                        with self.assertRaises(AssertionError):
                            matrix._watermarks(topic, timeout=1)
                operation = matrix.last_kafka_operation
                self.assertAlmostEqual(operation['construction_seconds'], .1)
                self.assertAlmostEqual(operation['query_timeout_seconds'], .9)
                self.assertAlmostEqual(operation['query_elapsed_seconds'], .25)
                self.assertAlmostEqual(operation['cleanup_seconds'], close_delay)
                self.assertAlmostEqual(operation['elapsed_seconds'], .35 + close_delay)
                self.assertEqual(operation['within_deadline'], passed)
