from pathlib import Path
from tempfile import TemporaryDirectory

from django.core.exceptions import ImproperlyConfigured
from django.test import SimpleTestCase, override_settings

from labops.kafka_config import (producer_config, consumer_config, source_key, consumer_group,
                                PRODUCER_ALLOWLIST, CONSUMER_ALLOWLIST)


@override_settings(KAFKA_REQUIRE_SECURITY=False, KAFKA_SECURITY_PROTOCOL='PLAINTEXT',
                   KAFKA_BOOTSTRAP_SERVERS='broker-a:9092,broker-b:9092',
                   KAFKA_SOURCE_CLUSTER_ID='test-cluster', KAFKA_SOURCE_STREAM_GENERATION='7')
class KafkaConfigTests(SimpleTestCase):
    def test_allowlist_and_durability_defaults(self):
        producer = producer_config()
        consumer = consumer_config('analytics')
        self.assertLessEqual(producer.keys(), PRODUCER_ALLOWLIST)
        self.assertLessEqual(consumer.keys(), CONSUMER_ALLOWLIST)
        self.assertTrue(producer['enable.idempotence'])
        self.assertEqual(producer['acks'], 'all')
        self.assertFalse(producer['allow.auto.create.topics'])
        self.assertFalse(consumer['enable.auto.commit'])
        self.assertFalse(consumer['enable.auto.offset.store'])
        self.assertEqual(producer_config('dlq')['message.max.bytes'], 2097152)

    @override_settings(KAFKA_GROUP_PREFIX='labops.run123')
    def test_source_generation_and_group_identity(self):
        self.assertEqual(consumer_group('notification'), 'labops.run123.notification.v1')
        first = source_key('inventory', 2, 300)
        with override_settings(KAFKA_SOURCE_STREAM_GENERATION='8'):
            second = source_key('inventory', 2, 300)
        self.assertEqual(first, 'test-cluster:7:inventory:2:300')
        self.assertNotEqual(first, second)

    def test_secure_role_secret_and_hostname_verification(self):
        with TemporaryDirectory() as directory:
            ca = Path(directory) / 'ca.pem'
            secret = Path(directory) / 'password'
            ca.write_text('certificate fixture')
            secret.write_text('private-password\n')
            with override_settings(KAFKA_REQUIRE_SECURITY=True, KAFKA_SECURITY_PROTOCOL='SASL_SSL',
                                   KAFKA_SSL_CA_LOCATION=str(ca),
                                   KAFKA_PUBLISHER_SASL_USERNAME='publisher',
                                   KAFKA_PUBLISHER_SASL_PASSWORD_FILE=str(secret)):
                config = producer_config()
                self.assertEqual(config['sasl.username'], 'publisher')
                self.assertEqual(config['sasl.password'], 'private-password')
                self.assertTrue(config['enable.ssl.certificate.verification'])
                self.assertEqual(config['ssl.endpoint.identification.algorithm'], 'https')
                with self.assertRaises(ImproperlyConfigured):
                    consumer_config('analytics')

    @override_settings(KAFKA_REQUIRE_SECURITY=True)
    def test_secure_environment_rejects_plaintext(self):
        with self.assertRaisesMessage(ImproperlyConfigured, 'requires SASL_SSL'):
            producer_config()

    @override_settings(KAFKA_SECURITY_PROTOCOL='SASL_SSL', KAFKA_SSL_CA_LOCATION='/missing/ca')
    def test_missing_ca_has_safe_error(self):
        with self.assertRaisesMessage(ImproperlyConfigured, 'trusted CA file'):
            producer_config()

    @override_settings(KAFKA_SECURITY_PROTOCOL='SASL_PLAINTEXT', KAFKA_SASL_USERNAME='user',
                       KAFKA_SASL_PASSWORD_FILE='/missing/secret', KAFKA_SASL_PASSWORD='secret-must-not-leak')
    def test_missing_secret_error_never_exposes_inline_secret(self):
        with self.assertRaises(ImproperlyConfigured) as failure:
            producer_config()
        self.assertNotIn('secret-must-not-leak', str(failure.exception))

    def test_invalid_budget_bounds_are_rejected(self):
        for override in ({'EVENT_LEASE_SECONDS': 19}, {'KAFKA_MAX_POLL_INTERVAL_MS': 40000},
                         {'KAFKA_PRODUCER_QUEUE_WAIT_SECONDS': float('nan')},
                         {'KAFKA_PUBLISH_FLUSH_SECONDS': float('inf')},
                         {'EVENT_RETRY_JITTER': float('nan')},
                         {'WORKER_SHUTDOWN_TIMEOUT_SECONDS': 20},
                         {'KAFKA_REQUEST_TIMEOUT_MS': 20000},
                         {'KAFKA_PRODUCER_RETRIES': 0}):
            with self.subTest(override=override), override_settings(**override):
                with self.assertRaises(ImproperlyConfigured):
                    producer_config()

    @override_settings(KAFKA_DLQ_MESSAGE_MAX_BYTES=1048576)
    def test_dlq_budget_covers_maximum_raw_poison(self):
        with self.assertRaisesMessage(ImproperlyConfigured, 'base64 raw poison'):
            producer_config('dlq')

    def test_invalid_identifiers_and_addresses_rejected(self):
        for override in ({'KAFKA_SOURCE_CLUSTER_ID': 'bad:cluster'},
                         {'KAFKA_GROUP_PREFIX': 'bad/group'},
                         {'KAFKA_BOOTSTRAP_SERVERS': 'http://broker:9092'},
                         {'KAFKA_BOOTSTRAP_SERVERS': 'broker:99999'},
                         {'KAFKA_SECURITY_PROTOCOL': 'unknown'}):
            with self.subTest(override=override), override_settings(**override):
                with self.assertRaises(ImproperlyConfigured):
                    producer_config()
