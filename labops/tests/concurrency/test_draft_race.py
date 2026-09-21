"""Draft editing must lock the document selected by its URL, even with extra input."""
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest import skipUnless
from unittest.mock import patch

from django.db import connection, connections, close_old_connections, transaction
from django.test import TransactionTestCase, override_settings

from labops.common import BusinessError
from labops.inventory import services as inventory
from labops.models import StockMovement
from labops.projects.services import write_project, project_action, write_task, task_action
from labops.tests.test_acceptance import Fixture


@skipUnless(connection.vendor == 'postgresql', 'PostgreSQL concurrency acceptance')
@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class DraftPostingRaceTests(Fixture, TransactionTestCase):
    def test_extra_draft_id_cannot_redirect_edit_lock(self):
        with transaction.atomic():
            batch = self.stock(20)
        original_data = self.issue_data(batch, 3)
        draft = inventory.issue_draft(self.admin, original_data, self.rid)

        project = write_project(self.admin, {'code': 'P2', 'name': 'Second project'}, self.rid)
        project = project_action(self.admin, project.pk, 'transition', {
            'expected_version': project.version, 'target_status': 'ACTIVE'}, self.rid)
        task = write_task(self.admin, {'project_id': str(project.pk), 'title': 'Other task',
                                     'assignee_id': str(self.admin.pk)}, self.rid)
        task = task_action(self.admin, task.pk, 'transition', {
            'expected_version': task.version, 'target_status': 'IN_PROGRESS'}, self.rid)
        other_data = {**self.issue_data(batch, 7), 'task_id': str(task.pk)}
        other_draft = inventory.issue_draft(self.admin, other_data, self.rid)
        edit_data = {**other_data, 'expected_version': draft.version, 'draft_id': str(other_draft.pk)}

        posting_validated = threading.Event()
        release_posting = threading.Event()
        editing_started = threading.Event()
        edit_read_document = threading.Event()
        release_edit = threading.Event()
        real_version, real_obj = inventory.version, inventory.obj

        def version(record, data):
            real_version(record, data)
            if threading.current_thread().name.startswith('poster') and record.pk == draft.pk:
                posting_validated.set()
                if not release_posting.wait(10):
                    raise RuntimeError('Timed out waiting for concurrent editor')

        def obj(model, ident):
            record = real_obj(model, ident)
            if threading.current_thread().name.startswith('editor') and model is StockMovement and str(ident) == str(draft.pk):
                edit_read_document.set()
                if not release_edit.wait(10):
                    raise RuntimeError('Timed out waiting for posting to commit')
            return record

        def run(posting):
            close_old_connections()
            try:
                if posting:
                    return inventory.issue(self.store, {'draft_id': str(draft.pk),
                        'expected_version': draft.version}, 'post-draft', self.rid)
                editing_started.set()
                return inventory.issue_draft(self.admin, edit_data, self.rid, draft.pk)
            except BusinessError as exc:
                return exc.code
            finally:
                connections.close_all()

        with patch.object(inventory, 'version', version), patch.object(inventory, 'obj', obj), \
             ThreadPoolExecutor(max_workers=1, thread_name_prefix='poster') as posters, \
             ThreadPoolExecutor(max_workers=1, thread_name_prefix='editor') as editors:
            posting = posters.submit(run, True)
            try:
                self.assertTrue(posting_validated.wait(5))
                editing = editors.submit(run, False)
                self.assertTrue(editing_started.wait(5))
                read_while_posting = edit_read_document.wait(.5)
            finally:
                release_posting.set()
            try:
                posted = posting.result(timeout=10)
            finally:
                release_edit.set()
            edit_result = editing.result(timeout=10)

        self.assertFalse(read_while_posting, 'Editor read an unlocked draft during posting')
        self.assertIsInstance(posted, StockMovement)
        self.assertEqual(edit_result, 'DOCUMENT_LOCKED')
        draft.refresh_from_db()
        self.assertEqual(draft.status, 'POSTED')
        self.assertEqual(draft.lines.get().delta_qty, -3)
        self.assertEqual(inventory.reconcile(), [])
