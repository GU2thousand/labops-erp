from unittest.mock import patch,MagicMock
import redis
from django.test import SimpleTestCase
from labops.cache import cached_catalog,rate_limit
from labops.common import BusinessError

class RedisPolicyTests(SimpleTestCase):
    def test_cache_failure_uses_authoritative_loader(self):
        broker=MagicMock();broker.get.side_effect=redis.ConnectionError('down')
        with patch('labops.cache.client',return_value=broker):
            self.assertEqual(cached_catalog(lambda:[{'code':'DB'}]),[{'code':'DB'}])
    def test_sensitive_rate_limit_fails_closed(self):
        broker=MagicMock();broker.eval.side_effect=redis.ConnectionError('down')
        with patch('labops.cache.client',return_value=broker):
            for name in ['login','imports','reports']:
                with self.assertRaises(BusinessError) as exc:rate_limit(name,'user',10,60)
                self.assertEqual(exc.exception.status,503)
    def test_limit_exhaustion_returns_429(self):
        broker=MagicMock();broker.eval.return_value=0
        with patch('labops.cache.client',return_value=broker):
            with self.assertRaises(BusinessError) as exc:rate_limit('imports','user',10,60)
            self.assertEqual(exc.exception.status,429)
