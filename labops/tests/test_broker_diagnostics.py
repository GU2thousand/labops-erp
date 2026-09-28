from confluent_kafka import KafkaError
from django.test import SimpleTestCase, override_settings

from labops.events import broker_error, classify_failure, safe_error, retry_delay
from infra.events.validation.prepare import VALIDATION_RETRY_SECONDS, VALIDATION_RETRY_JITTER, validate_ipv4_prefix


class BrokerDiagnosticTests(SimpleTestCase):
    def test_numeric_delivery_codes_survive_without_broker_text(self):
        for code, category in [(KafkaError._MSG_TIMED_OUT, 'transient'),
                               (KafkaError.TOPIC_AUTHORIZATION_FAILED, 'authorization'),
                               (KafkaError.MSG_SIZE_TOO_LARGE, 'permanent')]:
            with self.subTest(code=code):
                result = broker_error(KafkaError(code, 'private-payload-and-password'))
                self.assertEqual(result.kafka_error_code, code)
                self.assertEqual(classify_failure(result), category)
                self.assertIn(f'kafka_code={code}', safe_error(result))
                self.assertNotIn('private', str(result))
                self.assertNotIn('password', safe_error(result))

    def test_no_delivery_callback_has_explicit_unknown_code(self):
        result = broker_error(None)
        self.assertIsNone(result.kafka_error_code)
        self.assertEqual(safe_error(result), 'broker_delivery_failed')
        self.assertEqual(classify_failure(result), 'transient')

    @override_settings(EVENT_RETRY_SECONDS=VALIDATION_RETRY_SECONDS,
                       EVENT_RETRY_JITTER=VALIDATION_RETRY_JITTER)
    def test_frozen_ci_policy_covers_outage_and_has_bounded_natural_recovery(self):
        # Even the shortest schedule cannot exhaust during a 600-second outage.
        self.assertGreater(sum(VALIDATION_RETRY_SECONDS), 600)
        for attempt in range(1, len(VALIDATION_RETRY_SECONDS) + 1):
            delay = retry_delay(attempt, 'fixed-validation-event')
            self.assertGreaterEqual(delay, VALIDATION_RETRY_SECONDS[attempt - 1])
            self.assertLessEqual(delay, 72)

    def test_validation_network_accepts_only_three_octet_private_prefixes(self):
        for prefix in ('10.243.77', '172.20.4', '192.168.32'):
            self.assertEqual(validate_ipv4_prefix(prefix), prefix)
        for prefix in ('8.8.8', '127.0.0', '10.243.77.0/24', '10.256.1', '10.01.1'):
            with self.subTest(prefix=prefix), self.assertRaises(ValueError):
                validate_ipv4_prefix(prefix)
