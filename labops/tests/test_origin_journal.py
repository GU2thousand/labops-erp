"""Pure contracts for exclusive origin evidence and conservative DB recovery."""
from copy import deepcopy
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

from benchmarks.events.generation_journal import GenerationJournal, numeric_profile
from benchmarks.events.origin_journal import CompositeGenerationJournal, OriginJournal


RUN_ID = 'origin-journal-test'
ACTOR_ID = str(UUID(int=700))
REQUEST_HASH = 'a' * 64
PAYLOAD_HASH = 'b' * 64
OTHER_HASH = 'c' * 64


def requested_profile(**overrides):
    values = {
        'events': 90000, 'rate': 50.0, 'duration': 1800.0,
        'fault_repetitions': 20, 'fault_events': 30000, 'duplicate_events': 10000,
        'poison_events': 100, 'broker_fault_seconds': 300.0, 'outage_seconds': 600.0,
        'consumer_outage_seconds': 600.0, 'drain_timeout': 900.0,
        'runtime_diagnostics_enabled': True,
    }
    values.update(overrides)
    return values


def original_ids(global_index):
    return str(UUID(int=1000 + global_index * 2)), str(UUID(int=1001 + global_index * 2))


def database_facts(command):
    """Normalized independent ORM facts, with no broker payload or secret text."""
    movement_id, event_id = original_ids(command['global_index'])
    return {
        'database_observed': True, 'command_key': command['command_key'],
        'session_settled': True, 'database_scope_matches': True,
        'source_context_matches': True,
        'movement_rows': [{
            'id': movement_id, 'idempotency_key': command['command_key'],
            'status': 'POSTED', 'type': command['kind'], 'actor_id': ACTOR_ID,
            'version': 1, 'request_hash': REQUEST_HASH, 'context_matches': True,
        }],
        'outbox_rows': [{
            'id': event_id, 'dedupe_key': 'inventory:' + movement_id,
            'aggregate_type': 'stockmovement', 'aggregate_id': movement_id,
            'aggregate_version': 1,
            'event_type': 'inventory.' + command['kind'].lower() + '.posted',
            'transport': 'kafka', 'schema_version': 1, 'payload_hash': PAYLOAD_HASH,
            'computed_payload_hash': PAYLOAD_HASH, 'schema_valid': True,
            'ledger_links_valid': True,
        }],
        'marker_hashes': [PAYLOAD_HASH],
    }


def absent_database_facts(command):
    facts = database_facts(command)
    facts.update(movement_rows=[], outbox_rows=[])
    return facts


class OriginJournalTests(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self._group = 0

    def composite(self):
        self._group += 1
        directory = self.root / ('group-' + str(self._group))
        parent = GenerationJournal(directory / 'parent', RUN_ID, requested_profile())
        self.addCleanup(parent.finalize)
        return CompositeGenerationJournal(parent)

    def plan(self, composite, *, origin_id='origin_0', lane=0, indices=(0, 1, 2)):
        return {
            'path': composite.parent.evidence_dir.parent / origin_id,
            'run_id': RUN_ID, 'origin_id': origin_id, 'label': 'steady_lane_' + str(lane),
            'lane': lane, 'indices': list(indices), 'rate': 12.5,
            'context': {
                'actor_id': ACTOR_ID, 'source_generation': RUN_ID,
                'database_scope_digest': 'd' * 64, 'source_context_digest': 'e' * 64,
            },
        }

    def origin(self, composite, *, origin_id='origin_0', lane=0, indices=(0, 1, 2)):
        value = self.plan(composite, origin_id=origin_id, lane=lane, indices=indices)
        composite.add_origin_plan(value)
        child = OriginJournal(
            value['path'], run_id=RUN_ID, origin_id=origin_id,
            profile=requested_profile(), label=value['label'], lane=lane,
            indices=value['indices'], rate=value['rate'], context=value['context'],
        )
        self.addCleanup(child.finalize)
        batch = child.begin_batch(len(indices), value['rate'], value['label'])
        return child, batch

    def identify(self, child, batch, global_index):
        ordinal = child.attempt(batch, global_index=global_index)
        movement_id, event_id = original_ids(global_index)
        child.commit(batch, ordinal, movement_id, request_hash=REQUEST_HASH)
        child.identify_event(batch, ordinal, event_id, payload_hash=PAYLOAD_HASH)
        return ordinal, movement_id, event_id

    def origin_batch(self, composite, origin_id='origin_0'):
        return next(batch for batch in composite.summary()['batches']
                    if batch.get('origin_id') == origin_id)

    def assert_counts(self, summary, **expected):
        self.assertEqual({name: summary[name] for name in expected}, expected)

    def test_full_origin_freezes_exact_profile_and_command_identity(self):
        composite = self.composite()
        child, batch = self.origin(composite, indices=(0, 1, 2, 3))
        frozen = json.loads((child.directory / 'origin-plan.json').read_text())
        self.assertEqual(frozen['requested_numeric_profile'], numeric_profile(requested_profile()))
        self.assertEqual(len(frozen['requested_numeric_profile']), 12)
        self.assertIs(frozen['requested_numeric_profile']['runtime_diagnostics_enabled'], True)
        self.assertEqual(frozen['commands'], [
            {'ordinal': index + 1, 'global_index': index, 'command_key': f'{RUN_ID}:{index}',
             'kind': kind}
            for index, kind in enumerate(('RECEIPT', 'ISSUE', 'TRANSFER', 'REVERSAL'))
        ])
        for index in range(4):
            self.identify(child, batch, index)
        child.finish_success(batch)
        child.finalize()
        observed = self.origin_batch(composite)
        self.assert_counts(observed, requested=4, attempted=4, committed=4,
                           identified_events=4, unattempted=0, failed_before_commit=0,
                           commit_unknown=0, integrity_failed=0)
        self.assertEqual(observed['status'], 'succeeded')
        self.assertFalse(composite.summary()['journal_database_atomic'])
        self.assertTrue(composite.summary()['database_reconciliation_required'])
        raw = [json.loads(line) for line in (child.directory / 'origin-journal.jsonl').read_text().splitlines()]
        self.assertEqual([row['sequence'] for row in raw], list(range(1, len(raw) + 1)))

    def test_frozen_reservation_rejects_wrong_lanes_types_and_namespace_reuse(self):
        invalid_plans = (
            {'indices': [0, 0]}, {'indices': [1, 0]}, {'indices': [4]},
            {'indices': [True]}, {'lane': True}, {'lane': 4}, {'rate': True},
            {'rate': float('nan')}, {'origin_id': 'bad origin'},
        )
        for overrides in invalid_plans:
            with self.subTest(overrides=overrides):
                composite = self.composite()
                value = self.plan(composite)
                value.update(overrides)
                with self.assertRaises(ValueError):
                    composite.add_origin_plan(value)
                self.assertEqual(composite.summary()['totals']['requested'], 0)
        for field, invalid in (('events', True), ('rate', float('nan')),
                               ('duration', float('inf')), ('runtime_diagnostics_enabled', 1)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                OriginJournal(self.root / ('invalid-' + field), run_id=RUN_ID, origin_id='invalid',
                              profile=requested_profile(**{field: invalid}), label='steady',
                              lane=0, indices=[0], rate=12.5)
        composite = self.composite()
        child, batch = self.origin(composite)
        with self.assertRaises(ValueError):
            composite.add_origin_plan(self.plan(composite))
        overlapping = self.plan(composite, origin_id='other_origin')
        with self.assertRaises(ValueError):
            composite.add_origin_plan(overlapping)
        foreign = self.plan(composite, origin_id='foreign_origin', lane=1, indices=(4,))
        foreign['run_id'] = 'foreign-run'
        with self.assertRaises(ValueError):
            composite.add_origin_plan(foreign)
        with self.assertRaises(FileExistsError):
            OriginJournal(child.directory, run_id=RUN_ID, origin_id='origin_0',
                          profile=requested_profile(), label='steady_lane_0',
                          lane=0, indices=[0, 1, 2], rate=12.5)
        with self.assertRaises(ValueError):
            child.attempt(batch, global_index=1)
        self.assertEqual(child.summary()['totals']['attempted'], 0)

    def test_attempted_child_kill_or_eof_is_unknown_without_rollback_proof(self):
        for error_type in ('ChildKilled', 'ChildEOF'):
            with self.subTest(error_type=error_type):
                composite = self.composite()
                child, batch = self.origin(composite)
                ordinal = child.attempt(batch, global_index=0)
                child.finish_failure(batch, 'child_transport', error_type, attempt_id=ordinal)
                child.finalize()
                composite.note_transport('origin_0', started_indices=[0], error_type=error_type)
                observed = self.origin_batch(composite)
                self.assert_counts(observed, requested=3, attempted=1, committed=0,
                                   failed_before_commit=0, commit_unknown=1, unattempted=2)
                self.assertEqual(observed['status'], 'failed')
                self.assertEqual(observed['failure']['outcome'], 'commit_unknown')
                self.assertEqual(observed['raw_totals']['commit_unknown'], 1)

    def test_only_proven_rollback_or_not_entered_can_mark_precommit_failure(self):
        for outcome in ('rollback_proven', 'not_entered'):
            with self.subTest(outcome=outcome):
                composite = self.composite()
                child, batch = self.origin(composite)
                ordinal = child.attempt(batch)
                child.finish_failure(batch, 'business_transaction', 'OperationalError',
                                     attempt_id=ordinal, outcome=outcome)
                child.finalize()
                self.assert_counts(self.origin_batch(composite), requested=3, attempted=1,
                                   committed=0, failed_before_commit=1, commit_unknown=0, unattempted=2)

    def test_postcommit_observation_failure_preserves_ids_and_partial_counts(self):
        composite = self.composite()
        child, batch = self.origin(composite)
        self.identify(child, batch, 0)
        ordinal = child.attempt(batch, global_index=1)
        movement_id, _ = original_ids(1)
        child.commit(batch, ordinal, movement_id, request_hash=REQUEST_HASH)
        for outcome in ('rollback_proven', 'not_entered'):
            with self.subTest(outcome=outcome), self.assertRaises(ValueError):
                child.finish_failure(batch, 'event_lookup', 'OperationalError',
                                     attempt_id=ordinal, outcome=outcome)
        child.finish_failure(batch, 'event_lookup', 'OperationalError', attempt_id=ordinal)
        child.finalize()
        self.assert_counts(self.origin_batch(composite), requested=3, attempted=2, committed=2,
                           identified_events=1, post_commit_observation_failed=1,
                           failed_before_commit=0, unattempted=1, pending_observation=1)
        observed = composite.committed_attempts()
        self.assertEqual(observed[1]['movement_id'], movement_id)
        self.assertIsNone(observed[1]['event_id'])

    def test_attempt_and_commit_are_fsynced_before_external_observation(self):
        composite = self.composite()
        child, batch = self.origin(composite, indices=(0,))
        path = child.directory / 'origin-journal.jsonl'
        real_fsync = os.fsync
        durable_actions = []

        def synced(fd):
            durable_actions.append(json.loads(path.read_text().splitlines()[-1]))
            real_fsync(fd)

        movement_id, event_id = original_ids(0)
        with patch('benchmarks.events.origin_journal.os.fsync', side_effect=synced):
            ordinal = child.attempt(batch)
            self.assertEqual(durable_actions[-1]['action'], 'command_attempted')
            child.commit(batch, ordinal, movement_id, request_hash=REQUEST_HASH)

            def observe_event():
                self.assertEqual(durable_actions[-1]['action'], 'command_committed')
                self.assertEqual(durable_actions[-1]['movement_id'], movement_id)
                self.assertEqual(child.summary()['totals']['committed'], 1)
                child.identify_event(batch, ordinal, event_id, payload_hash=PAYLOAD_HASH)

            observe_event()
            self.assertEqual([row['action'] for row in durable_actions],
                             ['command_attempted', 'command_committed'])
        self.assertEqual(composite.committed_attempts()[0]['event_id'], event_id)

    def test_commit_fsync_failure_retains_commit_and_contiguous_failure_records(self):
        composite = self.composite()
        child, batch = self.origin(composite, indices=(0,))
        ordinal = child.attempt(batch)
        movement_id, _ = original_ids(0)
        with patch('benchmarks.events.origin_journal.os.fsync', side_effect=OSError('TOP_SECRET fsync')):
            with self.assertRaises(OSError):
                child.commit(batch, ordinal, movement_id, request_hash=REQUEST_HASH)
        self.assertEqual(child.summary()['totals']['committed'], 1)
        child.finish_failure(batch, 'journal_commit_fsync', 'OSError', attempt_id=ordinal)
        child.finalize()
        observed = self.origin_batch(composite)
        self.assert_counts(observed, requested=1, attempted=1, committed=1,
                           failed_before_commit=0, post_commit_observation_failed=1,
                           identified_events=0, pending_observation=1, commit_unknown=0)
        self.assertFalse(observed['evidence_errors'])
        self.assertEqual(composite.committed_attempts()[0]['movement_id'], movement_id)
        path = child.directory / 'origin-journal.jsonl'
        records = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual([row['sequence'] for row in records], list(range(1, len(records) + 1)))
        self.assertEqual([row['action'] for row in records], [
            'origin_frozen', 'batch_started', 'command_attempted', 'command_committed',
            'command_failed', 'batch_failed', 'origin_finalized',
        ])
        self.assertNotIn('TOP_SECRET', path.read_text())

    def test_incomplete_finalization_keeps_pending_commit_and_rejects_false_success(self):
        composite = self.composite()
        child, batch = self.origin(composite)
        ordinal = child.attempt(batch)
        with self.assertRaises(ValueError):
            child.finish_success(batch)
        with self.assertRaises(ValueError):
            child.finish_failure(batch, 'generation', 'RuntimeError')
        with self.assertRaises(ValueError):
            child.attempt(batch)
        child.commit(batch, ordinal, original_ids(0)[0], request_hash=REQUEST_HASH)
        with self.assertRaises(ValueError):
            child.commit(batch, ordinal, original_ids(0)[0])
        child.finalize()
        observed = self.origin_batch(composite)
        self.assert_counts(observed, requested=3, attempted=1, committed=1,
                           pending_observation=1, failed_before_commit=0, unattempted=2)
        self.assertEqual(observed['status'], 'interrupted')
        before = (child.directory / 'origin-journal.jsonl').read_bytes()
        self.assertEqual(child.finalize()['totals']['requested'], 3)
        self.assertEqual((child.directory / 'origin-journal.jsonl').read_bytes(), before)
        with self.assertRaises(RuntimeError):
            child.identify_event(batch, ordinal, original_ids(0)[1])

    def test_corrupt_tail_retains_frozen_denominator_and_known_valid_prefix(self):
        corruptions = ('incomplete', 'invalid_json', 'empty_record', 'nonobject_record',
                       'duplicate_ordinal', 'nan_timestamp', 'duplicate_json_keys',
                       'schema_version', 'sequence')
        for corruption in corruptions:
            with self.subTest(corruption=corruption):
                composite = self.composite()
                child, batch = self.origin(composite)
                self.identify(child, batch, 0)
                child.attempt(batch, global_index=1)
                child.finalize()
                path = child.directory / 'origin-journal.jsonl'
                records = [json.loads(line) for line in path.read_text().splitlines()]
                prefix = records[:-1]  # Replace finalization with a damaged next boundary.
                tail = {key: prefix[-1][key] for key in
                        ('schema_version', 'run_id', 'origin_id', 'plan_digest', 'recorded_at', 'batch_id')}
                tail.update(sequence=len(prefix) + 1, action='command_committed', ordinal=2,
                            movement_id=original_ids(1)[0], request_hash=REQUEST_HASH)
                if corruption == 'duplicate_ordinal':
                    tail = {**prefix[-1], 'sequence': len(prefix) + 1}
                elif corruption == 'nan_timestamp':
                    tail['recorded_at'] = float('nan')
                elif corruption == 'schema_version':
                    tail['schema_version'] = True
                elif corruption == 'sequence':
                    tail['sequence'] += 1
                encoded = json.dumps(tail, sort_keys=True) + '\n'
                if corruption == 'incomplete':
                    encoded = '{"sequence":'
                elif corruption == 'invalid_json':
                    encoded = '{this is not JSON}\n'
                elif corruption == 'empty_record':
                    encoded = '\n'
                elif corruption == 'nonobject_record':
                    encoded = '[]\n'
                elif corruption == 'duplicate_json_keys':
                    encoded = encoded.replace('"action": "command_committed"',
                        '"action": "command_committed", "action": "command_committed"', 1)
                path.write_text(''.join(json.dumps(row) + '\n' for row in prefix) + encoded)
                observed = self.origin_batch(composite)
                self.assert_counts(observed, requested=3, attempted=2, committed=1,
                                   identified_events=1, failed_before_commit=0)
                self.assert_counts(observed['raw_totals'], requested=3, attempted=2,
                                   committed=1, identified_events=1, unattempted=1)
                self.assertTrue(observed['evidence_errors'])
                self.assertEqual(observed['status'], 'failed')
                self.assertEqual(composite.committed_attempts()[0]['movement_id'], original_ids(0)[0])

    def test_corrupt_saved_profile_cannot_erase_known_journal_prefix(self):
        for corruption in ('numeric_value', 'nan_profile', 'boolean_type', 'duplicate_keys'):
            with self.subTest(corruption=corruption):
                composite = self.composite()
                child, batch = self.origin(composite)
                self.identify(child, batch, 0)
                child.finalize()
                path = child.directory / 'origin-plan.json'
                saved = json.loads(path.read_text())
                profile = saved['requested_numeric_profile']
                if corruption == 'numeric_value':
                    profile['events'] = 1
                elif corruption == 'nan_profile':
                    profile['duration'] = float('nan')
                elif corruption == 'boolean_type':
                    profile['runtime_diagnostics_enabled'] = 1
                encoded = json.dumps(saved, sort_keys=True) + '\n'
                if corruption == 'duplicate_keys':
                    encoded = encoded.replace('"events": 90000',
                                              '"events": 90000, "events": 90000', 1)
                path.write_text(encoded)
                observed = self.origin_batch(composite)
                self.assert_counts(observed, requested=3, attempted=1, committed=1, identified_events=1)
                self.assertEqual(composite.summary()['requested_numeric_profile'], requested_profile())
                self.assertTrue(observed['evidence_errors'])
                self.assertEqual(observed['status'], 'failed')

    def test_direct_batch_rollback_cannot_overturn_active_recorded_commit(self):
        for identified, outcome in ((False, 'rollback_proven'), (False, 'not_entered'),
                                    (True, 'not_entered')):
            with self.subTest(identified=identified, outcome=outcome):
                composite = self.composite()
                child, batch = self.origin(composite, indices=(0, 1))
                ordinal = child.attempt(batch)
                movement_id, event_id = original_ids(0)
                child.commit(batch, ordinal, movement_id, request_hash=REQUEST_HASH)
                if identified:
                    child.identify_event(batch, ordinal, event_id, payload_hash=PAYLOAD_HASH)
                child.finalize()
                path = child.directory / 'origin-journal.jsonl'
                prefix = [json.loads(line) for line in path.read_text().splitlines()][:-1]
                tail = {key: prefix[-1][key] for key in
                        ('schema_version', 'run_id', 'origin_id', 'plan_digest', 'recorded_at', 'batch_id')}
                tail.update(sequence=len(prefix) + 1, action='batch_failed', stage='pacing',
                            error_type='RuntimeError', outcome=outcome)
                path.write_text(''.join(json.dumps(row) + '\n' for row in prefix + [tail]))
                observed = self.origin_batch(composite)
                self.assert_counts(observed, requested=2, attempted=1, committed=1,
                                   identified_events=int(identified), failed_before_commit=0)
                self.assertEqual(bool(observed['evidence_errors']), not identified)
                self.assertEqual(observed['status'], 'failed')
                self.assertEqual(composite.committed_attempts()[0]['movement_id'], movement_id)

    def test_missing_journal_distinguishes_never_started_from_transport_started(self):
        composite = self.composite()
        composite.add_origin_plan(self.plan(composite))
        untouched = self.origin_batch(composite)
        self.assertFalse(untouched['journal_present'])
        self.assert_counts(untouched, requested=3, attempted=0, unattempted=3,
                           attempted_unknown=0, commit_unknown=0, failed_before_commit=0)
        composite.note_transport('origin_0', execution_permitted=False, error_type='ChildStartupFailure')
        never_allowed = self.origin_batch(composite)
        self.assert_counts(never_allowed, requested=3, attempted=0, unattempted=3,
                           attempted_unknown=0, commit_unknown=0, failed_before_commit=0)
        composite.note_transport('origin_0', started_indices=[0], error_type='ChildEOF')
        started = self.origin_batch(composite)
        # EOF can also hide the STARTED notification for every permitted command.
        self.assert_counts(started, requested=3, attempted=0, unattempted=0,
                           attempted_unknown=3, commit_unknown=3, failed_before_commit=0)
        self.assertTrue(started['requested_partition_complete'])
        self.assert_counts(started['raw_totals'], requested=3, attempted=0, unattempted=3)
        composite.note_transport('origin_0', started_indices=[1], error_type='ChildKilled')
        self.assertEqual(self.origin_batch(composite)['transport_failure']['started_indices'], [0, 1])
        with self.assertRaises(ValueError):
            composite.note_transport('origin_0', started_indices=[4])
        with self.assertRaises(ValueError):
            composite.note_transport('origin_0', execution_permitted=1)
        composite.note_transport('origin_0', execution_permitted=False)
        self.assertTrue(self.origin_batch(composite)['transport_failure']['execution_permitted'])
        composite.reconcile_origin('origin_0', settled=True,
            query_callback=lambda plan, command: database_facts(command) if command['global_index'] == 0
                else absent_database_facts(command))
        recovered = self.origin_batch(composite)
        self.assert_counts(recovered, requested=3, attempted=1, committed=1, identified_events=1,
                           attempted_unknown=2, unattempted=0, database_recovered_committed=1,
                           database_identified_events=1, commit_unknown=0)
        self.assertEqual(recovered['raw_totals']['attempted'], 0)
        self.assertEqual(composite.committed_attempts()[0]['movement_id'], original_ids(0)[0])
        self.assertEqual(composite.committed_attempts()[0]['payload_hash'], PAYLOAD_HASH)

    def test_unsettled_owned_session_never_invokes_database_callback(self):
        composite = self.composite()
        child, batch = self.origin(composite, indices=(0, 1))
        child.attempt(batch)
        child.finalize()
        callback = Mock(side_effect=AssertionError('Unsettled transaction must not be queried'))
        annotations = composite.reconcile_origin('origin_0', settled=False, query_callback=callback)
        callback.assert_not_called()
        self.assertEqual(len(annotations), 2)
        self.assertTrue(all(row['state'] == 'commit_unknown'
                            and row['error_type'] == 'OwnedSessionUnsettled'
                            and row['session_settled'] is False for row in annotations))
        self.assert_counts(self.origin_batch(composite), requested=2, committed=0,
                           failed_before_commit=0, database_recovered_committed=0, commit_unknown=2)
        with self.assertRaises(ValueError):
            composite.reconcile_origin('origin_0', settled=1, query_callback=callback)
        callback.assert_not_called()

    def test_settled_absence_proves_no_commit_without_inventing_attempts(self):
        composite = self.composite()
        child, batch = self.origin(composite)
        ordinal = child.attempt(batch)
        child.finish_failure(batch, 'child_transport', 'ChildEOF', attempt_id=ordinal)
        child.finalize()
        path = child.directory / 'origin-journal.jsonl'
        before = path.read_bytes()
        composite.note_transport('origin_0', started_indices=[0, 1])
        annotations = composite.reconcile_origin('origin_0', settled=True,
            query_callback=lambda plan, command: absent_database_facts(command))
        self.assertTrue(all(row['state'] == 'no_business_commit' for row in annotations))
        observed = self.origin_batch(composite)
        self.assert_counts(observed, requested=3, attempted=1, committed=0, failed_before_commit=1,
                           attempted_unknown=1, unattempted=1, commit_unknown=0)
        self.assert_counts(observed['raw_totals'], failed_before_commit=0, commit_unknown=1)
        self.assertEqual(path.read_bytes(), before)

    def test_exact_key_recovery_retains_original_ids_hashes_raw_counts_and_failure(self):
        for recorded_commit, expected_hash_provided in ((False, False), (True, False), (False, True)):
            with self.subTest(recorded_commit=recorded_commit, expected_hash_provided=expected_hash_provided):
                composite = self.composite()
                child, batch = self.origin(composite, indices=(0,))
                ordinal = child.attempt(batch)
                movement_id, event_id = original_ids(0)
                if recorded_commit:
                    child.commit(batch, ordinal, movement_id, request_hash=REQUEST_HASH)
                child.finish_failure(batch, 'event_lookup', 'ChildEOF', attempt_id=ordinal)
                child.finalize()
                path = child.directory / 'origin-journal.jsonl'
                before = path.read_bytes()
                def query(plan, command):
                    facts = database_facts(command)
                    if expected_hash_provided:
                        facts['movement_rows'][0]['expected_request_hash'] = REQUEST_HASH
                    return facts

                callback = Mock(side_effect=query)
                annotations = composite.reconcile_origin('origin_0', settled=True, query_callback=callback)
                callback.assert_called_once()
                observed = self.origin_batch(composite)
                self.assert_counts(observed, requested=1, attempted=1, committed=1, identified_events=1,
                                   failed_before_commit=0, database_identified_events=1,
                                   database_recovered_committed=int(not recorded_commit), commit_unknown=0)
                self.assertEqual(observed['status'], 'failed')
                self.assertEqual(observed['failure']['error_type'], 'ChildEOF')
                self.assertEqual(observed['raw_totals']['committed'], int(recorded_commit))
                self.assertEqual(observed['raw_totals']['identified_events'], 0)
                recovered = composite.committed_attempts()[0]
                self.assertEqual((recovered['movement_id'], recovered['event_id']), (movement_id, event_id))
                self.assertEqual((recovered['request_hash'], recovered['payload_hash']), (REQUEST_HASH, PAYLOAD_HASH))
                self.assertEqual(annotations[0]['command_key'], RUN_ID + ':0')
                self.assertEqual(annotations[0]['state'], 'committed_recovered')
                self.assertIs(annotations[0]['request_hash_independently_verified'], expected_hash_provided)
                self.assertIs(annotations[0]['journal_request_hash_matches_database'], recorded_commit)
                self.assertEqual(annotations[0]['request_hash_reference'],
                    'expected_input_hash' if expected_hash_provided else
                    'child_commit_record' if recorded_commit else 'unavailable')
                self.assertEqual(annotations[0]['source_context_verification'], 'configuration_only')
                self.assertEqual(path.read_bytes(), before)
                composite.finalize()
                self.assertEqual(path.read_bytes(), before)

    def test_foreign_context_hash_cardinality_schema_and_marker_conflicts_fail_closed(self):
        conflicts = (
            (('command_key',), 'foreign:0'), (('source_context_matches',), False),
            (('database_scope_matches',), 1), (('session_settled',), False),
            (('movement_rows', 0, 'idempotency_key'), RUN_ID + ':99'),
            (('movement_rows', 0, 'actor_id'), str(UUID(int=999))),
            (('movement_rows', 0, 'context_matches'), False),
            (('movement_rows', 0, 'type'), 'ISSUE'),
            (('movement_rows', 0, 'status'), 'DRAFT'),
            (('movement_rows', 0, 'version'), True),
            (('movement_rows', 0, 'request_hash'), 'INVALID'),
            (('movement_rows', 0, 'expected_request_hash'), OTHER_HASH),
            (('outbox_rows', 0, 'aggregate_id'), str(UUID(int=999))),
            (('outbox_rows', 0, 'dedupe_key'), 'inventory:foreign'),
            (('outbox_rows', 0, 'aggregate_type'), 'other'),
            (('outbox_rows', 0, 'aggregate_version'), 2),
            (('outbox_rows', 0, 'event_type'), 'inventory.issue.posted'),
            (('outbox_rows', 0, 'transport'), 'rabbitmq'),
            (('outbox_rows', 0, 'schema_version'), True),
            (('outbox_rows', 0, 'schema_valid'), 1),
            (('outbox_rows', 0, 'ledger_links_valid'), False),
            (('outbox_rows', 0, 'computed_payload_hash'), OTHER_HASH),
            (('movement_rows',), []), (('outbox_rows',), []),
            (('marker_hashes',), [OTHER_HASH]), (('marker_hashes',), ['INVALID']),
            (('marker_hashes',), {PAYLOAD_HASH: True}),
            (('marker_hashes',), (PAYLOAD_HASH,)),
        )
        for location, invalid in conflicts:
            with self.subTest(location=location, invalid=invalid):
                composite = self.composite()
                child, batch = self.origin(composite, indices=(0,))
                ordinal = child.attempt(batch)
                child.finish_failure(batch, 'child_transport', 'ChildEOF', attempt_id=ordinal)
                child.finalize()

                def query(plan, command):
                    facts = database_facts(command)
                    target = facts
                    for key in location[:-1]:
                        target = target[key]
                    target[location[-1]] = deepcopy(invalid)
                    return facts

                annotation = composite.reconcile_origin('origin_0', settled=True, query_callback=query)[0]
                self.assertEqual(annotation['state'], 'integrity_failed')
                self.assert_counts(self.origin_batch(composite), requested=1, attempted=1,
                                   committed=0, integrity_failed=1, database_recovered_committed=0,
                                   failed_before_commit=0)
        for collection in ('movement_rows', 'outbox_rows'):
            with self.subTest(duplicated=collection):
                composite = self.composite()
                composite.add_origin_plan(self.plan(composite, indices=(0,)))

                def duplicate_query(plan, command):
                    facts = database_facts(command)
                    facts[collection].append(deepcopy(facts[collection][0]))
                    return facts

                row = composite.reconcile_origin('origin_0', settled=True, query_callback=duplicate_query)[0]
                self.assertEqual(row['state'], 'integrity_failed')

    def test_database_conflicts_never_replace_recorded_ids_or_rollback_evidence(self):
        for conflict in ('movement_id', 'event_id', 'request_hash', 'payload_hash', 'rollback', 'missing_rows'):
            with self.subTest(conflict=conflict):
                composite = self.composite()
                child, batch = self.origin(composite, indices=(0,))
                ordinal = child.attempt(batch)
                movement_id, event_id = original_ids(0)
                saved_movement = str(UUID(int=999)) if conflict == 'movement_id' else movement_id
                saved_event = str(UUID(int=998)) if conflict == 'event_id' else event_id
                if conflict != 'rollback':
                    child.commit(batch, ordinal, saved_movement,
                                 request_hash=OTHER_HASH if conflict == 'request_hash' else REQUEST_HASH)
                    if conflict in ('event_id', 'payload_hash'):
                        child.identify_event(batch, ordinal, saved_event,
                                             payload_hash=OTHER_HASH if conflict == 'payload_hash' else PAYLOAD_HASH)
                child.finish_failure(batch, 'event_lookup', 'ChildEOF', attempt_id=ordinal,
                                     outcome='rollback_proven' if conflict == 'rollback' else 'commit_unknown')
                child.finalize()
                query = absent_database_facts if conflict == 'missing_rows' else database_facts
                row = composite.reconcile_origin('origin_0', settled=True,
                    query_callback=lambda plan, command: query(command))[0]
                self.assertEqual(row['state'], 'integrity_failed')
                observed = self.origin_batch(composite)
                self.assert_counts(observed, committed=int(conflict != 'rollback'), integrity_failed=1,
                                   database_recovered_committed=0, database_identified_events=0)
                self.assertEqual(observed['raw_totals']['failed_before_commit'], int(conflict == 'rollback'))
                if conflict != 'rollback':
                    recorded = composite.committed_attempts()[0]
                    self.assertEqual(recorded['movement_id'], saved_movement)
                    if conflict in ('event_id', 'payload_hash'):
                        self.assertEqual(recorded['event_id'], saved_event)

    def test_database_unavailability_and_callback_errors_keep_unknown_and_safe_metadata(self):
        for condition in ('not_observed', 'truthy_observed', 'incomplete', 'exception'):
            with self.subTest(condition=condition):
                composite = self.composite()
                composite.add_origin_plan(self.plan(composite, indices=(0,)))
                composite.note_transport('origin_0', started_indices=[0])

                def query(plan, command):
                    if condition == 'exception':
                        raise LookupError('TOP_SECRET password=private business-payload')
                    facts = database_facts(command)
                    if condition == 'not_observed':
                        return None
                    if condition == 'truthy_observed':
                        facts['database_observed'] = 1
                    if condition == 'incomplete':
                        facts['movement_rows'] = None
                    return facts

                row = composite.reconcile_origin('origin_0', settled=True, query_callback=query)[0]
                self.assertEqual(row['state'], 'commit_unknown')
                self.assert_counts(self.origin_batch(composite), requested=1, attempted=0, committed=0,
                                   commit_unknown=1, attempted_unknown=1, failed_before_commit=0)
                if condition == 'exception':
                    self.assertEqual(row['error_type'], 'LookupError')
                raw = composite.reconciliation_path.read_text()
                for secret in ('TOP_SECRET', 'password=', 'business-payload'):
                    self.assertNotIn(secret, raw)

    def test_serial_parent_and_four_origins_keep_namespaces_and_profile_unmultiplied(self):
        composite = self.composite()
        serial = composite.begin_batch(1, 50, 'serial_probe')
        ordinal = composite.attempt(serial)
        composite.commit(serial, ordinal, original_ids(100)[0])
        composite.identify_event(serial, ordinal, original_ids(100)[1])
        composite.finish_success(serial)
        saved_children = []
        for lane in range(4):
            indices = tuple(range(lane * 4, lane * 4 + 4))
            child, batch = self.origin(composite, origin_id='origin_' + str(lane), lane=lane, indices=indices)
            for index in indices:
                self.identify(child, batch, index)
            child.finish_success(batch)
            child.finalize()
            path = child.directory / 'origin-journal.jsonl'
            saved_children.append((path, path.read_bytes()))
        report = composite.finalize()
        self.assert_counts(report['totals'], requested=17, attempted=17, committed=17,
                           identified_events=17, unattempted=0, commit_unknown=0, integrity_failed=0)
        self.assertEqual(report['requested_numeric_profile'], requested_profile())
        self.assertEqual(len({batch['batch_id'] for batch in report['batches']}), 5)
        self.assertEqual([batch['requested'] for batch in report['batches']], [1, 4, 4, 4, 4])
        self.assertTrue(all(batch['status'] == 'succeeded' for batch in report['batches']))
        committed = composite.committed_attempts()
        self.assertEqual(len({row['movement_id'] for row in committed}), 17)
        self.assertEqual(len({row['event_id'] for row in committed}), 17)
        self.assertEqual(json.loads(composite.parent.summary_path.read_text()), report)
        for path, content in saved_children:
            self.assertEqual(path.read_bytes(), content)

    def test_copy_snapshots_and_process_owner_guard_protect_evidence(self):
        composite = self.composite()
        child, batch = self.origin(composite, indices=(0,))
        ordinal = child.attempt(batch)
        path = child.directory / 'origin-journal.jsonl'
        before = path.read_bytes()
        operations = (
            lambda: child.begin_batch(1, 12.5, 'steady_lane_0'),
            lambda: child.attempt(batch),
            lambda: child.commit(batch, ordinal, original_ids(0)[0]),
            lambda: child.identify_event(batch, ordinal, original_ids(0)[1]),
            lambda: child.finish_failure(batch, 'child_transport', 'ChildEOF', attempt_id=ordinal),
            lambda: child.finish_success(batch), child.finalize,
        )
        with patch('benchmarks.events.origin_journal.os.getpid', return_value=os.getpid() + 1):
            for operation in operations:
                with self.assertRaises(RuntimeError):
                    operation()
        self.assertEqual(path.read_bytes(), before)
        copied_plan = child.plan
        copied_plan['indices'][0] = 99
        copied_plan['commands'][0]['command_key'] = 'mutated:99'
        copied_plan['requested_numeric_profile']['events'] = 1
        self.assertEqual(child.plan['indices'], [0])
        self.assertEqual(child.plan['commands'][0]['command_key'], RUN_ID + ':0')
        self.assertEqual(child.plan['requested_numeric_profile'], requested_profile())

        def query(plan, command):
            self.assertNotIn('indices', plan)
            self.assertNotIn('commands', plan)
            self.assertEqual(len(plan['plan_digest']), 64)
            facts = database_facts(command)
            plan['requested_numeric_profile']['events'] = 99
            plan['context']['actor_id'] = str(UUID(int=999))
            command['command_key'] = 'mutated:99'
            return facts

        annotations = composite.reconcile_origin('origin_0', settled=True, query_callback=query)
        annotations[0]['movement_id'] = str(UUID(int=999))
        report = composite.summary()
        report['requested_numeric_profile']['events'] = 1
        report['batches'][0]['requested'] = 99
        report['batches'][0]['raw_totals']['attempted'] = 99
        records = composite.committed_attempts()
        records[0]['movement_id'] = str(UUID(int=999))
        records[0]['reconciliation']['payload_hash'] = OTHER_HASH
        records[0]['payload_hash'] = OTHER_HASH
        fresh = composite.summary()
        self.assertEqual(fresh['requested_numeric_profile'], requested_profile())
        self.assert_counts(fresh['batches'][0], requested=1, attempted=1, committed=1)
        self.assertEqual(fresh['batches'][0]['raw_totals']['attempted'], 1)
        recovered = composite.committed_attempts()[0]
        self.assertEqual(recovered['movement_id'], original_ids(0)[0])
        self.assertEqual(recovered['command_key'], RUN_ID + ':0')
        self.assertEqual(recovered['payload_hash'], PAYLOAD_HASH)
        self.assertEqual(recovered['reconciliation']['payload_hash'], PAYLOAD_HASH)
        self.assertEqual(path.read_bytes(), before)
