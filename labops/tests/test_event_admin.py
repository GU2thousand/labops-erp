import argparse
import io
import json
import os
from unittest.mock import Mock, patch

from django.test import SimpleTestCase
import requests

from infra.events.admin import (
    ADMIN_OPERATION_SECONDS, AdminUserAPI, ReconcileError, ROLES,
    admin_endpoints, admin_origin, main, mismatches, topic_value_matches, users_reconcile,
)


ENDPOINTS = [f'https://127.0.0.1:{port}' for port in (19644, 29644, 39644)]
USER_PATH = '/v1/security/users'


def response(status=200, document=None, location=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(document).encode()
    result._content_consumed = True
    if location is not None:
        result.headers['Location'] = location
    return result


class AdminEndpointTrustTests(SimpleTestCase):
    def test_single_endpoint_remains_valid_without_allowlist(self):
        with patch.dict(os.environ, {'KAFKA_ADMIN_URL': ENDPOINTS[0]}, clear=True):
            self.assertEqual(admin_endpoints(), ENDPOINTS[:1])

    def test_explicit_allowlist_starts_with_primary(self):
        with patch.dict(os.environ, {'KAFKA_ADMIN_URL': ENDPOINTS[1],
                'KAFKA_ADMIN_TRUSTED_URLS': ','.join(ENDPOINTS)}, clear=True):
            self.assertEqual(admin_endpoints(), [ENDPOINTS[1], ENDPOINTS[0], ENDPOINTS[2]])

    def test_rejects_unsafe_configured_origins_without_echoing_input(self):
        values = ['', 'http://broker:9644', 'https://user:secret@broker:9644',
                  'https://@broker:9644', 'https://:@broker:9644',
                  'https://broker/path', 'https://broker?secret=value',
                  'https://broker#secret', 'https://broker?', 'https://broker#',
                  'https://broker:0', 'https://broker:65536', 'https://broker:bad',
                  'https://broker\\attacker', 'https://broker\n', 'https://bro%6ber']
        for value in values:
            with self.subTest(value=value):
                with self.assertRaises(ReconcileError) as error:
                    admin_origin(value)
                if value:
                    self.assertNotIn(value, str(error.exception))

    def test_rejects_duplicate_and_equivalent_origins(self):
        for configured in [ENDPOINTS[0] + ',' + ENDPOINTS[0],
                           'https://broker,https://broker:443']:
            with self.subTest(configured=configured), patch.dict(os.environ,
                    {'KAFKA_ADMIN_URL': configured.split(',')[0],
                     'KAFKA_ADMIN_TRUSTED_URLS': configured}, clear=True):
                with self.assertRaisesRegex(ReconcileError, 'unique origins'):
                    admin_endpoints()

    def test_rejects_missing_primary_and_excessive_allowlist(self):
        for configured in [ENDPOINTS[1], ','.join(f'https://broker-{i}' for i in range(11))]:
            with self.subTest(configured=configured), patch.dict(os.environ,
                    {'KAFKA_ADMIN_URL': ENDPOINTS[0],
                     'KAFKA_ADMIN_TRUSTED_URLS': configured}, clear=True):
                with self.assertRaises(ReconcileError):
                    admin_endpoints()


class AdminLeaderRoutingTests(SimpleTestCase):
    def setUp(self):
        self.session = Mock(spec=requests.Session)
        self.session.auth = ('admin', 'private-admin-password')
        self.session.verify = '/trusted/ca.crt'
        self.api = AdminUserAPI(self.session, ENDPOINTS)

    def test_observed_self_307_routes_to_next_endpoint_and_keeps_successful_endpoint(self):
        self.session.request.side_effect = [
            response(307, location=ENDPOINTS[0] + USER_PATH + '?redirect=1'),
            response(200), response(200, ['analytics'])]
        body = {'username': 'analytics', 'password': 'private-user-password', 'algorithm': 'SCRAM-SHA-256'}
        self.api.request('POST', USER_PATH, json=body)
        self.api.request('GET', USER_PATH)
        calls = self.session.request.call_args_list
        self.assertEqual([call.args for call in calls], [
            ('POST', ENDPOINTS[0] + USER_PATH), ('POST', ENDPOINTS[1] + USER_PATH),
            ('GET', ENDPOINTS[1] + USER_PATH)])
        for call in calls:
            self.assertFalse(call.kwargs['allow_redirects'])
            self.assertGreater(call.kwargs['timeout'].total, 0)
            self.assertLessEqual(call.kwargs['timeout'].total, ADMIN_OPERATION_SECONDS)
        self.assertEqual(calls[0].kwargs['json'], body)
        self.assertEqual(calls[1].kwargs['json'], body)
        self.assertEqual(self.session.verify, '/trusted/ca.crt')
        self.assertEqual(self.session.auth, ('admin', 'private-admin-password'))

    def test_trusted_leader_is_preferred_and_put_method_path_body_preserved(self):
        path = USER_PATH + '/analytics'
        self.session.request.side_effect = [
            response(307, location=ENDPOINTS[2] + path + '?redirect=2'), response(200)]
        body = {'password': 'private-password', 'algorithm': 'SCRAM-SHA-256'}
        self.api.request('PUT', path, json=body)
        self.assertEqual([call.args for call in self.session.request.call_args_list],
                         [('PUT', ENDPOINTS[0] + path), ('PUT', ENDPOINTS[2] + path)])
        self.assertEqual(self.session.request.call_args.kwargs['json'], body)
        self.assertEqual(self.api.active, ENDPOINTS[2])

    def test_untrusted_or_changed_redirect_never_forwards_credentials_or_password(self):
        locations = ['https://attacker.example' + USER_PATH + '?redirect=1',
            'http://127.0.0.1:29644' + USER_PATH + '?redirect=1',
            'https://admin:private@127.0.0.1:29644' + USER_PATH + '?redirect=1',
            'https://@127.0.0.1:29644' + USER_PATH + '?redirect=1',
            'https://:@127.0.0.1:29644' + USER_PATH + '?redirect=1',
            'https://127.0.0.1:9644' + USER_PATH + '?redirect=1',
            ENDPOINTS[1] + '/other?redirect=1', ENDPOINTS[1] + USER_PATH,
            ENDPOINTS[1] + USER_PATH + '?redirect=0',
            ENDPOINTS[1] + USER_PATH + '?redirect=-1',
            ENDPOINTS[1] + USER_PATH + '?redirect=1&redirect=2',
            ENDPOINTS[1] + USER_PATH + '?redirect=1&secret=private',
            ENDPOINTS[1] + USER_PATH + '?redirect=1#private',
            ENDPOINTS[1] + USER_PATH + '?redirect=1#',
            ENDPOINTS[1] + USER_PATH + '?redirect=1\n',
            '//127.0.0.1:29644' + USER_PATH + '?redirect=1',
            USER_PATH + '?redirect=1', 'https://[broken', '']
        for location in locations:
            with self.subTest(location=location):
                self.session.request.reset_mock()
                self.session.request.side_effect = None
                self.session.request.return_value = response(307, location=location)
                with self.assertRaisesRegex(ReconcileError, 'destination is not trusted') as error:
                    self.api.request('POST', USER_PATH, json={'password': 'private-password'})
                self.assertEqual(self.session.request.call_count, 1)
                self.assertEqual(self.session.request.call_args.args[1], ENDPOINTS[0] + USER_PATH)
                self.assertNotIn('private', str(error.exception))
                self.assertNotIn('https://', str(error.exception))

    def test_redirect_cycle_attempts_each_endpoint_once(self):
        self.session.request.side_effect = [response(307, location=endpoint + USER_PATH + '?redirect=1')
                                            for endpoint in ENDPOINTS]
        with self.assertRaisesRegex(ReconcileError, 'exhausted trusted endpoints'):
            self.api.request('POST', USER_PATH, json={'password': 'private-password'})
        self.assertEqual([call.args[1] for call in self.session.request.call_args_list],
                         [endpoint + USER_PATH for endpoint in ENDPOINTS])

    def test_no_allowlist_expansion_when_single_endpoint_redirects(self):
        api = AdminUserAPI(self.session, ENDPOINTS[:1])
        self.session.request.return_value = response(307, location=ENDPOINTS[0] + USER_PATH + '?redirect=1')
        with self.assertRaisesRegex(ReconcileError, 'exhausted trusted endpoints'):
            api.request('POST', USER_PATH)
        self.assertEqual(self.session.request.call_count, 1)

    def test_deadline_before_first_attempt_sends_nothing(self):
        with patch('infra.events.admin.time.monotonic', side_effect=[100, 115]):
            with self.assertRaisesRegex(ReconcileError, 'deadline exceeded'):
                self.api.request('POST', USER_PATH)
        self.session.request.assert_not_called()

    def test_cumulative_deadline_shrinks_timeout_and_rejects_late_success(self):
        self.session.request.side_effect = [
            response(307, location=ENDPOINTS[0] + USER_PATH + '?redirect=1'), response(200)]
        with patch('infra.events.admin.time.monotonic', side_effect=[100, 100, 110, 111, 116]):
            with self.assertRaisesRegex(ReconcileError, 'deadline exceeded'):
                self.api.request('POST', USER_PATH)
        self.assertEqual([call.kwargs['timeout'].total for call in self.session.request.call_args_list], [15, 4])
        self.assertEqual(self.api.active, ENDPOINTS[0])

    def test_tls_and_network_errors_fail_closed_with_redacted_error(self):
        for exception in [requests.exceptions.SSLError, requests.exceptions.ConnectionError,
                          requests.exceptions.Timeout]:
            with self.subTest(exception=exception):
                self.session.request.reset_mock()
                self.session.request.side_effect = exception('https://secret:password@attacker.example')
                with self.assertRaises(ReconcileError) as error:
                    self.api.request('POST', USER_PATH)
                self.assertEqual(self.session.request.call_count, 1)
                self.assertEqual(str(error.exception), 'Admin API request failed: ' + exception.__name__)

    def test_auth_and_other_http_statuses_never_route(self):
        for status in (301, 302, 308, 401, 403, 500, 503):
            with self.subTest(status=status):
                self.session.request.reset_mock()
                self.session.request.return_value = response(status, location=ENDPOINTS[1] + USER_PATH + '?redirect=1')
                self.assertEqual(self.api.request('POST', USER_PATH).status_code, status)
                self.assertEqual(self.session.request.call_count, 1)
                self.assertEqual(self.api.active, ENDPOINTS[0])


class AdminUserProvisioningTests(SimpleTestCase):
    def setUp(self):
        self.users = {name: 'new-' + name + '-private-password' for name in ('admin', *ROLES)}
        self.config = {'sasl.username': 'admin', 'sasl.password': 'old-private-admin-password',
                       'sasl.mechanism': 'SCRAM-SHA-256', 'ssl.ca.location': '/trusted/ca.crt'}
        self.args = argparse.Namespace(identities='private.json', rotate_existing=True, verify_only=False,
                                     env_file=None, command='users', development_rf1=False, report=None)
        self.environment = {'KAFKA_ADMIN_URL': ENDPOINTS[0],
                            'KAFKA_ADMIN_TRUSTED_URLS': ','.join(ENDPOINTS)}

    def test_real_session_adapter_keeps_declared_ca_auth_and_routing_under_poisoned_environment(self):
        users = self.users
        for ca_variable in ('REQUESTS_CA_BUNDLE', 'CURL_CA_BUNDLE'):
            with self.subTest(ca_variable=ca_variable):
                observed = []

                class RecordingAdapter(requests.adapters.BaseAdapter):
                    def send(self, prepared, **kwargs):
                        observed.append((prepared, kwargs))
                        result = response(200, list(users) if prepared.method == 'GET' else None)
                        result.request = prepared
                        result.url = prepared.url
                        return result

                    def close(self):
                        pass

                session = requests.Session()
                session.mount('https://', RecordingAdapter())
                poisoned = {**self.environment, ca_variable: '/attacker/ca.crt',
                            'HTTPS_PROXY': 'http://attacker.invalid:8080',
                            'ALL_PROXY': 'http://attacker.invalid:8080', 'NO_PROXY': '',
                            'NETRC': '/attacker/netrc'}
                with patch.dict(os.environ, poisoned, clear=True), \
                        patch('infra.events.admin.load_identities', return_value=self.users), \
                        patch('infra.events.admin.client_config', return_value=self.config), \
                        patch('requests.Session', return_value=session), \
                        patch('requests.sessions.get_netrc_auth', side_effect=AssertionError('netrc used')) as netrc:
                    users_reconcile(self.args, {})
                netrc.assert_not_called()
                self.assertFalse(session.trust_env)
                self.assertEqual(len(observed), len(self.users) + 2)
                for prepared, settings in observed:
                    self.assertEqual(settings['verify'], '/trusted/ca.crt')
                    self.assertEqual(settings['proxies'], {})
                    self.assertTrue(prepared.url.startswith(ENDPOINTS[0] + USER_PATH))
                    password = self.users['admin'] if prepared is observed[-1][0] else self.config['sasl.password']
                    self.assertEqual(prepared.headers['Authorization'], requests.auth._basic_auth_str('admin', password))
                session.close()

    def test_admin_rotates_last_and_final_get_uses_new_credentials_on_latest_leader(self):
        session = Mock(spec=requests.Session)
        session.headers = {}
        observed = []
        redirected = set()

        def execute(method, url, **kwargs):
            observed.append((method, url, session.auth, session.verify, kwargs))
            if method == 'GET':
                return response(200, list(self.users))
            username = url.rsplit('/', 1)[-1]
            if username == 'analytics' and username not in redirected:
                redirected.add(username)
                return response(307, location=ENDPOINTS[0] + USER_PATH + '/analytics?redirect=1')
            if username == 'admin' and username not in redirected:
                redirected.add(username)
                return response(307, location=ENDPOINTS[2] + USER_PATH + '/admin?redirect=1')
            return response(200)

        session.request.side_effect = execute
        report = {}
        with patch.dict(os.environ, self.environment, clear=True), \
                patch('infra.events.admin.load_identities', return_value=self.users), \
                patch('infra.events.admin.client_config', return_value=self.config), \
                patch('requests.Session', return_value=session):
            users_reconcile(self.args, report)
        writes = [item for item in observed if item[0] == 'PUT']
        self.assertEqual([item[1].rsplit('/', 1)[-1] for item in writes][-2:], ['admin', 'admin'])
        for item in writes:
            self.assertEqual(item[2], ('admin', self.config['sasl.password']))
            self.assertEqual(item[3], '/trusted/ca.crt')
            self.assertFalse(item[4]['allow_redirects'])
        self.assertTrue(all(item[1].startswith(ENDPOINTS[1]) for item in writes[2:-2]))
        self.assertEqual(observed[-1][:3], ('GET', ENDPOINTS[2] + USER_PATH,
                                          ('admin', self.users['admin'])))
        self.assertEqual(report['rotated'][-1], 'admin')
        self.assertEqual(set(report['rotated']), set(self.users))
        self.assertFalse(report['credentials_verified'])

    def test_verify_only_preserves_existing_users_without_mutations(self):
        self.args.verify_only = True
        session = Mock(spec=requests.Session)
        session.headers = {}
        session.request.return_value = response(200, list(self.users))
        report = {}
        with patch.dict(os.environ, self.environment, clear=True), \
                patch('infra.events.admin.load_identities', return_value=self.users), \
                patch('infra.events.admin.client_config', return_value=self.config), \
                patch('requests.Session', return_value=session):
            users_reconcile(self.args, report)
        self.assertEqual([call.args[0] for call in session.request.call_args_list], ['GET', 'GET'])
        self.assertEqual(report['rotated'], [])

    def test_failure_report_never_echoes_response_body_credentials_or_location(self):
        session = Mock(spec=requests.Session)
        session.headers = {}
        session.request.side_effect = [response(200, []),
            response(403, {'password': 'echoed-private-password'},
                     'https://private:password@attacker.example/secret')]
        output = io.StringIO()
        with patch.dict(os.environ, self.environment, clear=True), \
                patch('infra.events.admin.parse_args', return_value=self.args), \
                patch('infra.events.admin.load_identities', return_value=self.users), \
                patch('infra.events.admin.client_config', return_value=self.config), \
                patch('requests.Session', return_value=session), patch('sys.stdout', output):
            self.assertEqual(main(), 1)
        report = json.loads(output.getvalue())
        self.assertEqual(report['error'], 'Admin user operation failed for analytics: HTTP 403')
        for secret in ('password', 'private', 'https://', 'attacker'):
            self.assertNotIn(secret, output.getvalue())
        self.assertEqual(session.request.call_count, 2)


class RedpandaConfigContractTests(SimpleTestCase):
    def test_effective_cluster_disabled_satisfies_topic_false_only(self):
        self.assertTrue(topic_value_matches('write.caching', 'false', 'disabled'))
        self.assertTrue(topic_value_matches('write.caching', 'false', 'false'))
        self.assertFalse(topic_value_matches('write.caching', 'false', 'true'))
        self.assertFalse(topic_value_matches('write.caching', 'false', None))
        self.assertFalse(topic_value_matches('write.caching', 'disabled', 'false'))
        self.assertFalse(topic_value_matches('cleanup.policy', 'false', 'disabled'))

    def test_alias_does_not_hide_other_topic_or_replication_mismatches(self):
        wanted=[{'name':'inventory','partitions':1,'replication_factor':3,
                 'config':{'write.caching':'false','cleanup.policy':'delete'}}]
        observed={'inventory':{'partitions':[{'partition':0,'leader':0,'replicas':[0,1,2],'isr':[0,1,2]}],
                               'config':{'write.caching':{'value':'disabled'},'cleanup.policy':{'value':'delete'}}}}
        self.assertEqual(mismatches(wanted,observed,full_isr=True),[])
        observed['inventory']['config']['cleanup.policy']['value']='compact'
        self.assertEqual(len(mismatches(wanted,observed)),1)
        observed['inventory']['partitions'][0]['replicas']=[0]
        self.assertEqual(len(mismatches(wanted,observed)),2)
