from uuid import UUID

from django.test import SimpleTestCase

from benchmarks.events.delivery_contract import acknowledged_offsets_coverage, qualify_delivery_observations


class DeliveryEvidenceContractTests(SimpleTestCase):
    topic = 'isolated.inventory.v1'
    source = ('isolated-cluster', 'run-20260927')
    ids = tuple(str(UUID(int=value)) for value in (1, 2))

    def row(self, event_id, consumer, offset, *, partition=0, topic=None, source=None, result=True):
        cluster, generation = source or self.source
        return {'event_id': event_id, 'consumer': consumer,
                'delivery_key': f'{cluster}:{generation}:{topic or self.topic}:{partition}:{offset}',
                'received_at': 1.0, 'completed_at': 2.0, 'result': result}

    def rows(self, *, count=3, ids=None):
        return [self.row(event_id, consumer, ordinal * 10 + offset)
                for ordinal, event_id in enumerate(ids or self.ids)
                for consumer in ('notification', 'analytics') for offset in range(count)]

    def qualify(self, rows, *, count=3, ids=None, required_coordinates=None):
        return qualify_delivery_observations(iter(rows), ids or self.ids, self.topic, count,
                                             expected_source_identity=self.source,
                                             required_coordinates=required_coordinates)

    def new_acknowledgements(self):
        return {event_id: [{'partition': 0, 'offset': ordinal * 10 + offset}
                          for offset in (3, 4)]
                for ordinal, event_id in enumerate(self.ids)}

    def test_each_event_and_consumer_requires_distinct_coordinates(self):
        rows = self.rows()
        rows[0]['payload'] = {'private': 'must-not-be-retained'}
        rows[0]['log_file'] = 'notification-deliveries.jsonl'
        rows[0]['line_number'] = 8
        proof = self.qualify(rows)
        self.assertTrue(proof['passed'])
        self.assertEqual(proof['expected_total_distinct_coordinates'], 12)
        self.assertEqual(proof['qualified_total_distinct_coordinates'], 12)
        self.assertEqual(proof['covered_pair_count'], 4)
        self.assertEqual(proof['pairs'][0]['coordinates'], [
            {'topic': self.topic, 'partition': 0, 'offset': offset} for offset in range(3)])
        self.assertEqual(proof['pairs'][0]['raw_observations'][0]['line_number'], 8)
        self.assertNotIn('payload', proof['pairs'][0]['raw_observations'][0])
        self.assertNotIn('must-not-be-retained', str(proof))

    def test_repeated_log_rows_cannot_substitute_for_duplicate_publishes(self):
        rows = self.rows(count=1) * 20
        proof = self.qualify(rows)
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['total_observation_rows'], 80)
        self.assertEqual(proof['qualified_total_distinct_coordinates'], 4)
        self.assertEqual(len(proof['missing_pairs']), 4)
        self.assertTrue(all(pair['distinct_coordinates'] == 1 and pair['shortfall'] == 2
                            and pair['repeated_successful_coordinate_observations'] == 19
                            for pair in proof['pairs']))

    def test_concentrated_deliveries_cannot_cover_missing_events_or_consumers(self):
        # The old aggregate count passed this fixture: twelve rows for one pair.
        rows = [self.row(self.ids[0], 'notification', offset) for offset in range(12)]
        proof = self.qualify(rows)
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['total_observation_rows'], proof['expected_total_distinct_coordinates'])
        self.assertEqual(proof['covered_pair_count'], 1)
        self.assertEqual(proof['qualified_total_distinct_coordinates'], 3)
        self.assertEqual(len(proof['missing_pairs']), 3)
        self.assertEqual(proof['pairs'][0]['excess_distinct_coordinates'], 9)

    def test_unrelated_ids_scoped_topics_and_old_source_generations_do_not_qualify(self):
        rows = self.rows(count=1)
        unrelated = str(UUID(int=3))
        for event_id in self.ids:
            for consumer in ('notification', 'analytics'):
                rows.extend(self.row(event_id, consumer, offset, topic='isolated.recovery.v1')
                            for offset in range(10))
                rows.extend(self.row(event_id, consumer, offset, source=('isolated-cluster', 'old-run'))
                            for offset in range(10))
        rows.extend(self.row(unrelated, 'analytics', offset) for offset in range(10))
        proof = self.qualify(rows)
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['qualified_total_distinct_coordinates'], 4)
        self.assertEqual(proof['ignored_observation_reasons'],
                         {'unrelated_topic': 40, 'unrelated_source_identity': 40, 'unrelated_event_id': 10})

    def test_failed_processing_is_retained_without_qualifying_a_coordinate(self):
        rows = self.rows(count=1, ids=self.ids[:1])
        rows[0]['result'] = False
        proof = self.qualify(rows, count=1, ids=self.ids[:1])
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['pairs'][0]['raw_observations'], [rows[0]])
        self.assertEqual(proof['pairs'][0]['distinct_coordinates'], 0)
        rows.append(dict(rows[0], result=True))
        self.assertTrue(self.qualify(rows, count=1, ids=self.ids[:1])['passed'])

    def test_main_redelivery_requires_one_coordinate_for_both_consumers(self):
        proof = self.qualify(self.rows(count=1), count=1)
        self.assertTrue(proof['passed'])
        self.assertEqual(proof['expected_total_distinct_coordinates'], 4)
        self.assertEqual(proof['qualified_total_distinct_coordinates'], 4)

    def test_historical_three_coordinates_cannot_substitute_for_two_new_acknowledgements(self):
        proof = self.qualify(self.rows(), required_coordinates=self.new_acknowledgements())
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['qualified_total_distinct_coordinates'], 12)
        self.assertEqual(proof['expected_acknowledged_coordinate_observations'], 8)
        self.assertEqual(proof['observed_acknowledged_coordinate_observations'], 0)
        self.assertEqual(len(proof['missing_pairs']), 4)
        self.assertTrue(all(pair['shortfall'] == 0 and len(pair['missing_acknowledged_coordinates']) == 2
                            for pair in proof['pairs']))
        self.assertEqual(proof['pairs'][0]['missing_acknowledged_coordinates'], [
            {'topic': self.topic, 'partition': 0, 'offset': offset} for offset in (3, 4)])

    def test_one_consumer_missing_one_actual_acknowledgement_fails(self):
        rows = self.rows(count=5)
        rows = [row for row in rows if not (row['event_id'] == self.ids[1]
                and row['consumer'] == 'analytics' and row['delivery_key'].endswith(':14'))]
        proof = self.qualify(rows, required_coordinates=self.new_acknowledgements())
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['covered_pair_count'], 3)
        self.assertEqual(proof['observed_acknowledged_coordinate_observations'], 7)
        self.assertEqual(proof['missing_pairs'], [{'event_id': self.ids[1], 'consumer': 'analytics',
            'shortfall': 0, 'missing_acknowledged_coordinates': [
                {'topic': self.topic, 'partition': 0, 'offset': 14}]}])

    def test_both_consumers_observing_all_new_acknowledgements_pass(self):
        proof = self.qualify(self.rows(count=5), required_coordinates=self.new_acknowledgements())
        self.assertTrue(proof['passed'])
        self.assertEqual(proof['expected_acknowledged_coordinate_observations'], 8)
        self.assertEqual(proof['observed_acknowledged_coordinate_observations'], 8)
        self.assertTrue(all(not pair['missing_acknowledged_coordinates'] for pair in proof['pairs']))

    def test_acknowledgement_with_failed_processing_cannot_qualify(self):
        rows = self.rows(count=5)
        for row in rows:
            if row['event_id'] == self.ids[0] and row['consumer'] == 'analytics' and row['delivery_key'].endswith(':4'):
                row['result'] = False
        proof = self.qualify(rows, required_coordinates=self.new_acknowledgements())
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['observed_acknowledged_coordinate_observations'], 7)

    def test_acknowledgement_denominator_requires_exact_ids_and_distinct_valid_coordinates(self):
        valid = self.new_acknowledgements()
        cases = [{}, {self.ids[0]: valid[self.ids[0]]}, dict(valid, extra=[]),
                 {self.ids[0]: [], self.ids[1]: valid[self.ids[1]]},
                 {self.ids[0]: [valid[self.ids[0]][0]] * 2, self.ids[1]: valid[self.ids[1]]},
                 {event_id: [{'partition': 0, 'offset': 7}] for event_id in self.ids}]
        for partition, offset in ((True, 3), (3, 3), (0, -1), (0, True), (0, '3'),
                                  (0, 3.0), (0, 2 ** 63)):
            cases.append({self.ids[0]: [{'partition': partition, 'offset': offset}],
                          self.ids[1]: valid[self.ids[1]]})
        cases.append({self.ids[0]: [{'topic': self.topic, 'partition': 0, 'offset': 3}],
                      self.ids[1]: valid[self.ids[1]]})
        for coordinates in cases:
            with self.subTest(coordinates=coordinates), self.assertRaises(ValueError):
                self.qualify(self.rows(), required_coordinates=coordinates)

    def test_malformed_required_coordinates_and_identity_fail(self):
        template = self.rows()[0]
        invalid_rows = [dict(template, consumer='replay'), dict(template, consumer=None),
                        dict(template, result='true'), dict(template, received_at=float('nan')),
                        dict(template, completed_at=10 ** 400), dict(template, line_number=True)]
        prefix = ':'.join((*self.source, self.topic)) + ':'
        for key in ('cluster:gen:topic:0', prefix + '00:1',
                    prefix + '0:-1', prefix + '0:9223372036854775808', prefix + '3:1',
                    'bad cluster:gen:' + self.topic + ':0:1'):
            invalid_rows.append(dict(template, delivery_key=key))
        for row in invalid_rows:
            with self.subTest(row=row), self.assertRaises(ValueError):
                self.qualify([row])

    def test_a_coordinate_cannot_claim_two_event_ids_for_one_consumer(self):
        rows = [self.row(event_id, 'analytics', 7) for event_id in self.ids]
        with self.assertRaisesMessage(ValueError, 'claims multiple event IDs'):
            self.qualify(rows)

    def test_a_coordinate_cannot_claim_different_ids_across_consumers(self):
        rows = [self.row(self.ids[0], 'notification', 7), self.row(self.ids[1], 'analytics', 7)]
        with self.assertRaisesMessage(ValueError, 'claims multiple event IDs'):
            self.qualify(rows)

    def test_invalid_denominator_cannot_vacuously_pass(self):
        for ids, count in (([], 3), ([self.ids[0], self.ids[0]], 3), (['bad-id'], 3),
                           (self.ids, 0), (self.ids, True)):
            with self.subTest(ids=ids, count=count), self.assertRaises(ValueError):
                qualify_delivery_observations([], ids, self.topic, count)

    def test_full_tier_denominator_accepts_streamed_ten_thousand_events(self):
        ids = [str(UUID(int=index + 1)) for index in range(10_000)]
        rows = (self.row(event_id, consumer, index * 3 + offset)
                for index, event_id in enumerate(ids)
                for consumer in ('notification', 'analytics') for offset in range(3))
        proof = qualify_delivery_observations(rows, ids, self.topic, 3,
                                              expected_source_identity=self.source,
                                              required_coordinates={event_id: [
                                                  {'partition': 0, 'offset': index * 3 + offset}
                                                  for offset in (1, 2)]
                                                  for index, event_id in enumerate(ids)})
        self.assertTrue(proof['passed'])
        self.assertEqual(proof['expected_pair_count'], 20_000)
        self.assertEqual(proof['expected_total_distinct_coordinates'], 60_000)
        self.assertEqual(proof['qualified_total_distinct_coordinates'], 60_000)
        self.assertEqual(proof['total_observation_rows'], 60_000)
        self.assertEqual(proof['expected_acknowledged_coordinate_observations'], 40_000)
        self.assertEqual(proof['observed_acknowledged_coordinate_observations'], 40_000)


class DurableAcknowledgedOffsetCoverageTests(SimpleTestCase):
    ids = tuple(str(UUID(int=value)) for value in (1, 2))

    def acknowledgements(self):
        return {self.ids[0]: [{'partition': 0, 'offset': 3}, {'partition': 0, 'offset': 4}],
                self.ids[1]: [{'partition': 1, 'offset': 8}, {'partition': 2, 'offset': 10}]}

    def snapshot(self):
        return {consumer: {'0': 5, '1': 9, '2': 11} for consumer in ('notification', 'analytics')}

    def test_next_offsets_beyond_all_actual_acknowledgements_pass(self):
        snapshot = self.snapshot()
        proof = acknowledged_offsets_coverage(snapshot, self.acknowledgements())
        self.assertTrue(proof['passed'])
        self.assertEqual(proof['raw_next_offsets'], snapshot)
        self.assertEqual(proof['required_max_acknowledged_offsets'], {'0': 4, '1': 8, '2': 10})
        self.assertEqual(proof['required_next_offsets'], {'0': 5, '1': 9, '2': 11})
        self.assertEqual(proof['acknowledged_broker_record_count'], 4)
        self.assertEqual(proof['expected_consumer_partition_checks'], 6)
        self.assertEqual(proof['covered_consumer_partition_checks'], 6)
        self.assertEqual(proof['missing'], [])

    def test_equal_ack_offset_is_not_a_durable_completion_cursor(self):
        snapshot = self.snapshot()
        snapshot['analytics']['0'] = 4
        proof = acknowledged_offsets_coverage(snapshot, self.acknowledgements())
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['covered_consumer_partition_checks'], 5)
        self.assertEqual(proof['missing'], [{'consumer': 'analytics', 'partition': 0,
            'maximum_acknowledged_offset': 4, 'required_next_offset': 5,
            'observed_next_offset': 4, 'passed': False}])

    def test_invalid_offset_is_retained_as_uncovered(self):
        snapshot = self.snapshot()
        snapshot['notification']['2'] = -1001
        proof = acknowledged_offsets_coverage(snapshot, self.acknowledgements())
        self.assertFalse(proof['passed'])
        self.assertEqual(proof['missing'][0]['observed_next_offset'], -1001)

    def test_unrelated_partitions_need_not_have_zero_lag(self):
        coordinates = {self.ids[0]: [{'partition': 0, 'offset': 3}, {'partition': 0, 'offset': 4}]}
        snapshot = {consumer: {'0': 50, '1': 0, '2': -1001}
                    for consumer in ('notification', 'analytics')}
        proof = acknowledged_offsets_coverage(snapshot, coordinates)
        self.assertTrue(proof['passed'])
        self.assertEqual(proof['required_next_offsets'], {'0': 5})
        self.assertEqual(proof['expected_consumer_partition_checks'], 2)
        self.assertEqual(proof['raw_next_offsets'], snapshot)

    def test_snapshot_group_partition_shape_and_offsets_are_strict(self):
        cases = [{}, {'notification': self.snapshot()['notification']},
                 dict(self.snapshot(), replay=self.snapshot()['notification']),
                 {'notification': {'0': 5, '1': 9}, 'analytics': self.snapshot()['analytics']}]
        for invalid in (True, '5', 5.0, -1000, -1, 2 ** 63):
            snapshot = self.snapshot()
            snapshot['notification']['0'] = invalid
            cases.append(snapshot)
        for snapshot in cases:
            with self.subTest(snapshot=snapshot), self.assertRaises(ValueError):
                acknowledged_offsets_coverage(snapshot, self.acknowledgements())

    def test_no_acknowledgements_cannot_vacuously_pass(self):
        for coordinates in (None, {}, {'bad-id': [{'partition': 0, 'offset': 3}]},
                            {self.ids[0]: []}):
            with self.subTest(coordinates=coordinates), self.assertRaises(ValueError):
                acknowledged_offsets_coverage(self.snapshot(), coordinates)
