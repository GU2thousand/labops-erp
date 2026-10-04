from copy import deepcopy
from django.test import SimpleTestCase
from benchmarks.events.health import recovery_errors, wait_broker_recovery, completed_recovery_seconds


class BrokerRecoveryReadinessTests(SimpleTestCase):
    topics = ('run.inventory.v1', 'run.inventory.dlq.v1')

    def test_successful_count_response_after_frozen_drain_deadline_is_rejected(self):
        # A DB predicate can start before its deadline and return True late.
        with self.assertRaises(TimeoutError):
            completed_recovery_seconds(100, 900, monotonic=lambda: 1000.001)
        self.assertEqual(completed_recovery_seconds(100, 900, monotonic=lambda: 1000), 900)

    def test_completion_evidence_uses_response_time_not_later_artifact_collection(self):
        self.assertEqual(completed_recovery_seconds(100, 900, monotonic=lambda: 999.5), 899.5)

    def health(self):
        return {'is_healthy': True, 'all_nodes': [0, 1, 2], 'nodes_down': [],
                'leaderless_partitions': [], 'under_replicated_partitions': [],
                'unhealthy_reasons': [], 'high_disk_usage_nodes': [],
                'nodes_in_recovery_mode': [], 'leaderless_count': 0,
                'under_replicated_count': 0}

    def metadata(self):
        return {'brokers': [0, 1, 2], 'topics': {
            name: {'error': None, 'partitions': [
                {'partition': number, 'leader': number, 'replicas': [0, 1, 2],
                 'isr': [0, 1, 2], 'error': None} for number in range(3)]}
            for name in self.topics}}

    def views(self):
        return {str(node): {'body': self.health()} for node in range(3)}

    def test_three_advertised_brokers_cannot_hide_the_observed_stale_local_raft_view(self):
        health = self.views()
        health['2']['body'].update(is_healthy=False, nodes_down=[0, 1],
            leaderless_partitions=['kafka/run.inventory.v1/1'], leaderless_count=1,
            unhealthy_reasons=['nodes_down', 'leaderless_partitions'])
        errors = recovery_errors(health, self.metadata(), self.topics)
        self.assertTrue(any('broker 2' in value and 'nodes_down' in value for value in errors))
        self.assertTrue(any('leaderless_partitions' in value for value in errors))

    def test_every_broker_view_and_exact_health_fields_are_required(self):
        for node in range(3):
            for field in ('is_healthy', 'all_nodes', 'nodes_down', 'leaderless_partitions',
                          'under_replicated_partitions', 'leaderless_count', 'under_replicated_count'):
                with self.subTest(node=node, field=field):
                    health = self.views()
                    del health[str(node)]['body'][field]
                    self.assertTrue(recovery_errors(health, self.metadata(), self.topics))
        health = self.views(); del health['1']
        self.assertTrue(recovery_errors(health, self.metadata(), self.topics))

    def test_both_topics_need_full_live_rf3_leaders_and_isr(self):
        changes = ({'leader': -1}, {'leader': 99}, {'replicas': [0, 1]},
                   {'replicas': [0, 1, 1]}, {'isr': [0, 1]}, {'error': 'NOT_LEADER_OR_FOLLOWER'})
        for name in self.topics:
            for change in changes:
                with self.subTest(topic=name, change=change):
                    metadata = self.metadata()
                    metadata['topics'][name]['partitions'][1].update(change)
                    self.assertTrue(recovery_errors(self.views(), metadata, self.topics))
            metadata = self.metadata(); metadata['topics'][name]['partitions'].pop()
            self.assertTrue(recovery_errors(self.views(), metadata, self.topics))
            metadata = self.metadata(); del metadata['topics'][name]
            self.assertTrue(recovery_errors(self.views(), metadata, self.topics))

    def test_false_zero_duplicate_membership_and_missing_count_do_not_pass(self):
        for value in (False, None, '0', 1):
            with self.subTest(count=value):
                health = self.views(); health['0']['body']['leaderless_count'] = value
                self.assertTrue(recovery_errors(health, self.metadata(), self.topics))
        for nodes in ([0, 1, 1], [False, 1, 2], [0, 1, 99], [{}, 1, 2]):
            with self.subTest(nodes=nodes):
                health = self.views(); health['0']['body']['all_nodes'] = nodes
                self.assertTrue(recovery_errors(health, self.metadata(), self.topics))

    def run_wait(self, health_fetcher=None, metadata_fetcher=None, timeout=1):
        self.now = 0; self.observations = []; self.calls = []
        def health(node, remaining):
            self.calls.append(('health', node, remaining))
            return health_fetcher(node, remaining) if health_fetcher else self.health()
        def metadata(remaining):
            self.calls.append(('metadata', remaining))
            return metadata_fetcher(remaining) if metadata_fetcher else self.metadata()
        def pause(seconds): self.now += seconds
        return wait_broker_recovery(health, metadata, self.topics, timeout=timeout,
            monotonic=lambda: self.now, sleep=pause, on_observation=self.observations.append)

    def test_ready_evidence_contains_all_raw_views_and_topic_assignments(self):
        result = self.run_wait()
        self.assertTrue(result['ready'])
        self.assertEqual(result['health'], self.views())
        self.assertEqual(result['metadata'], self.metadata())
        self.assertEqual(self.observations, [result])
        self.assertEqual(len(self.calls), 4)

    def test_unhealthy_polls_recover_only_after_all_views_match(self):
        def health(node, remaining):
            value = self.health()
            if self.now < .5 and node == 1:
                value.update(is_healthy=False, under_replicated_count=1,
                             under_replicated_partitions=['kafka/run.inventory.v1/1'])
            return value
        result = self.run_wait(health)
        self.assertEqual([item['ready'] for item in self.observations], [False, False, True])
        self.assertEqual(result['elapsed_seconds'], .5)
        self.assertEqual(self.observations[0]['health']['1']['body']['under_replicated_count'], 1)

    def test_tls_errors_remain_failures_and_never_persist_exception_credentials(self):
        def health(node, remaining):
            if node == 2:
                raise ConnectionError('https://admin:credential@example.test')
            return self.health()
        with self.assertRaises(TimeoutError): self.run_wait(health)
        self.assertEqual(self.now, 1)
        self.assertEqual(len(self.observations), 4)
        self.assertEqual(self.observations[-1]['health']['2'], {'error_type': 'ConnectionError'})
        self.assertNotIn('credential', str(self.observations))

    def test_shared_deadline_prevents_metadata_or_later_requests_extending_timeout(self):
        def health(node, remaining):
            self.assertAlmostEqual(remaining, .5 - self.now)
            self.now += min(.2, remaining)
            return self.health()
        with self.assertRaises(TimeoutError): self.run_wait(health, timeout=.5)
        self.assertAlmostEqual(self.now, .5)
        self.assertEqual([call[0] for call in self.calls], ['health', 'health', 'health'])
        self.assertEqual(self.observations[-1]['metadata'], {'error_type': 'TimeoutError'})
        self.assertFalse(self.observations[-1]['ready'])

    def test_a_late_healthy_metadata_response_cannot_be_accepted(self):
        def metadata(remaining):
            self.now += remaining
            return deepcopy(self.metadata())
        with self.assertRaises(TimeoutError): self.run_wait(metadata_fetcher=metadata)
        self.assertEqual(self.now, 1)
        self.assertIn('Broker recovery deadline expired', self.observations[-1]['errors'])
