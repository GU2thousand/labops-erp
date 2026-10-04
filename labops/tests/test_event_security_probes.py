import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from confluent_kafka import KafkaError
from django.test import SimpleTestCase

from benchmarks.events.security import (run_security_probes, _category,
                                        _broker_authentication_rejection)


MODERN_REJECTION = ('redpanda-0-1      | WARN  2026-09-28 03:03:20,151 [shard 0:kafk] kafka - '
                    'connection_context.cc:1132 - Error processing request: kafka::kafka_authentication_exception '
                    '(requests.cc:198 - Unexpected auth request 3 expected handshake)')
START = '2026-09-28T03:03:20+00:00'
END = '2026-09-28T03:03:21.300000+00:00'


class SecurityProbeTests(SimpleTestCase):
    def test_exact_current_redpanda_rejection_is_recognized(self):
        self.assertTrue(_broker_authentication_rejection(MODERN_REJECTION, START, END))

    def test_unrelated_request_generic_disconnect_and_missing_class_remain_inconclusive(self):
        for line in (MODERN_REJECTION.replace('request 3 ', 'request 36 '),
                     MODERN_REJECTION.replace('request 3 ', 'request 18 '),
                     MODERN_REJECTION.replace('kafka::kafka_authentication_exception', 'kafka::runtime_error'),
                     MODERN_REJECTION.replace('Unexpected auth request 3 expected handshake', 'Connection timed out'),
                     'Unexpected auth request 3 expected handshake'):
            with self.subTest(line=line):
                self.assertFalse(_broker_authentication_rejection(line, START, END))

    def test_historical_or_unstamped_rejection_cannot_pass(self):
        for line in (MODERN_REJECTION.replace('03:03:20,151', '03:03:19,999'),
                     MODERN_REJECTION.replace('03:03:20,151', '03:03:21,301'),
                     MODERN_REJECTION.replace('2026-09-28 03:03:20,151', 'no-timestamp')):
            with self.subTest(line=line):
                self.assertFalse(_broker_authentication_rejection(line, START, END))

    def test_transport_timeout_and_speculative_sasl_hint_do_not_prove_denial(self):
        for message in ('Failed to get metadata: Local: Broker transport failure',
                        'Disconnected: broker might require SASL authentication',
                        'Connection timed out',
                        'Unexpected auth request 3 expected handshake'):
            with self.subTest(message=message):
                self.assertEqual(_category(KafkaError._TRANSPORT, message), 'other')

    def run_probes(self, *, broker_logs=MODERN_REJECTION, anonymous_success=False, healthy_control=True):
        configs = {name: {'bootstrap.servers': 'unused.test:9092', 'security.protocol': 'SASL_SSL',
                          'sasl.username': name, 'sasl.password': 'private-fixture-' + name}
                   for name in ('publisher', 'notification', 'admin')}
        windows = []
        def logs(case, start, end):
            windows.append((case, start, end))
            return broker_logs
        configs['broker_log_reader'] = logs
        ticks = iter([datetime.fromisoformat(START), datetime.fromisoformat(END)])
        class ProbeDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                return next(ticks)

        def metadata(config, topic, observations, timeout):
            if config['security.protocol'] == 'SSL':
                observations.error(KafkaError(KafkaError._TRANSPORT, 'Local: Broker transport failure'))
                return anonymous_success
            if config.get('sasl.password', '').startswith('invalid-security-probe-'):
                observations.error(KafkaError(KafkaError._AUTHENTICATION, 'SASL authentication failed'))
                return False
            if str(config.get('ssl.ca.location', '')).endswith('security-probe-untrusted-ca.crt'):
                observations.error(KafkaError(KafkaError._SSL, 'Broker certificate verify failed'))
                return False
            return healthy_control

        def group(config, topic, group_id, observations, timeout):
            if group_id.startswith('other.'):
                observations.error(KafkaError(KafkaError.GROUP_AUTHORIZATION_FAILED, 'Group authorization failed'))
                return False
            return True

        def forbidden_write(config, topic, run_id, observations, timeout):
            observations.error(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, 'Topic authorization failed'))
            return False

        def forbidden_create(config, admin_config, topic, observations, timeout):
            observations.error(KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED, 'Unauthorized'))
            return False

        with TemporaryDirectory() as directory, \
             patch('benchmarks.events.security.datetime', ProbeDateTime), \
             patch('benchmarks.events.security._metadata', metadata), \
             patch('benchmarks.events.security._group_access', group), \
             patch('benchmarks.events.security._forbidden_write', forbidden_write), \
             patch('benchmarks.events.security._forbidden_create', forbidden_create):
            report = run_security_probes(configs, 'run.inventory.v1', 'run123', directory)
            self.assertEqual(report, json.loads((Path(directory) / 'security-probes.json').read_text()))
        self.assertEqual(windows, [('anonymous', START, END)])
        return report

    def test_full_gate_accepts_scoped_modern_rejection_and_preserves_raw_line(self):
        report = self.run_probes()
        self.assertTrue(report['passed'])
        self.assertEqual((report['passed_cases'], report['total_cases']), (6, 6))
        anonymous = next(case for case in report['cases'] if case['name'] == 'anonymous')
        evidence = next(item for item in anonymous['observations'] if item['source'] == 'broker_log')
        self.assertEqual(evidence['message'], MODERN_REJECTION)
        self.assertEqual(evidence['probe_window_start_utc'], START)
        self.assertEqual(evidence['probe_window_end_utc'], END)
        self.assertNotIn('private-fixture-', json.dumps(report))

    def test_full_gate_keeps_bare_transport_historical_log_and_success_as_failures(self):
        cases = [dict(broker_logs=''),
                 dict(broker_logs=MODERN_REJECTION.replace('03:03:20,151', '03:03:19,999')),
                 dict(anonymous_success=True)]
        for kwargs in cases:
            with self.subTest(kwargs=kwargs):
                report = self.run_probes(**kwargs)
                self.assertFalse(report['passed'])
                self.assertEqual(report['passed_cases'], 5)
                self.assertFalse(report['cases'][0]['denial_confirmed'])

    def test_specific_denials_do_not_override_failed_health_control(self):
        report = self.run_probes(healthy_control=False)
        self.assertEqual(report['passed_cases'], 6)
        self.assertFalse(report['passed'])
