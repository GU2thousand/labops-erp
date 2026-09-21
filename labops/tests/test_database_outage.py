from unittest.mock import patch
from django.db import InterfaceError, OperationalError
from django.test import TestCase, override_settings
from labops.tests.test_acceptance import Fixture
from labops.models import AuditEvent, CommandResult, StockMovement, OutboxEvent


@override_settings(DEBUG=False)
class DatabaseOutageTests(Fixture, TestCase):
    def assert_outage(self, response):
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['error']['code'], 'DATABASE_UNAVAILABLE')
        self.assertEqual(response['Retry-After'], '5')
        self.assertEqual(response['Cache-Control'], 'no-store')
        self.assertNotIn('private-host-secret', response.content.decode())

    def test_authenticated_read_and_login_session_failure(self):
        client = self.client_for(self.admin)
        for path in ['/api/v1/items', '/login/', '/']:
            with self.subTest(path=path), patch('django.contrib.sessions.backends.db.SessionStore.load', side_effect=OperationalError('private-host-secret')):
                self.assert_outage(client.get(path))

    def test_command_failure_rolls_back_and_key_remains_retryable(self):
        batch = self.stock(10)
        client = self.client_for(self.admin)
        counts = lambda: tuple(m.objects.count() for m in [StockMovement, AuditEvent, CommandResult, OutboxEvent])
        before = counts()
        from labops.inventory import services
        real_post = services.post
        def unavailable(*args, **kwargs):
            real_post(*args, **kwargs)
            raise InterfaceError('private-host-secret')
        with patch.object(services, 'post', side_effect=unavailable):
            self.assert_outage(self.api_post(client, 'stock/issues', self.issue_data(batch, 1), 'outage-key'))
        self.assertEqual(before, counts())
        first = self.api_post(client, 'stock/issues', self.issue_data(batch, 1), 'outage-key')
        second = self.api_post(client, 'stock/issues', self.issue_data(batch, 1), 'outage-key')
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.json()['data'], first.json()['data'])
        self.assertEqual(StockMovement.objects.filter(idempotency_key='outage-key').count(), 1)
