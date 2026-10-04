"""Add an ordered active route index without changing publisher eligibility."""
from contextlib import contextmanager

from django.db import migrations, models
from django.db.models import Q
from django.db.utils import NotSupportedError


INDEX_NAME = 'outbox_active_created_id_idx'
TABLE_NAME = 'labops_outboxevent'
SCHEMA_NAME = 'public'
# PostgreSQL's canonical expression for this fixed Q condition. Fail closed if
# the catalog contains a different expression; do not guess index ownership.
EXPECTED_PREDICATE = (
    "(((transport)::text = 'kafka'::text) AND ((status)::text = ANY "
    "((ARRAY['PENDING'::character varying, 'PROCESSING'::character varying])::text[])))"
)


def target_table(schema_editor):
    with schema_editor.connection.cursor() as cursor:
        cursor.execute('''SELECT c.oid, c.relkind, to_regclass(%s)::oid
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s''',
            [TABLE_NAME, SCHEMA_NAME, TABLE_NAME])
        row = cursor.fetchone()
    if row is None or row[1] != 'r' or row[0] != row[2]:
        raise RuntimeError('Active outbox index requires the expected public table')
    return row[0]


def index_catalog(schema_editor):
    with schema_editor.connection.cursor() as cursor:
        cursor.execute('''SELECT c.relkind, i.indrelid, a.amname,
                i.indisunique, i.indisprimary, i.indisvalid, i.indisready, i.indislive,
                i.indnkeyatts, i.indnatts, i.indexprs IS NULL,
                i.indkey::smallint[], i.indoption::smallint[],
                pg_get_expr(i.indpred, i.indrelid),
                CASE WHEN i.indexrelid IS NOT NULL THEN pg_get_indexdef(c.oid, 1, false) END,
                CASE WHEN i.indexrelid IS NOT NULL THEN pg_get_indexdef(c.oid, 2, false) END,
                EXISTS (SELECT 1 FROM pg_constraint WHERE conindid = c.oid)
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_index i ON i.indexrelid = c.oid
            LEFT JOIN pg_am a ON a.oid = c.relam
            WHERE n.nspname = %s AND c.relname = %s''', [SCHEMA_NAME, INDEX_NAME])
        return cursor.fetchone()


def require_owned_index(schema_editor, table_oid):
    row = index_catalog(schema_editor)
    with schema_editor.connection.cursor() as cursor:
        cursor.execute('''SELECT attname, attnum FROM pg_attribute
            WHERE attrelid = %s AND attname IN ('created_at', 'id')
              AND NOT attisdropped''', [table_oid])
        attributes = dict(cursor.fetchall())
    keys = [attributes.get('created_at'), attributes.get('id')]
    expected = ('i', table_oid, 'btree', False, False, True, True, True,
                2, 2, True, keys, [0, 0], EXPECTED_PREDICATE, 'created_at', 'id', False)
    if row != expected:
        raise RuntimeError('Active outbox index catalog does not match this migration')


@contextmanager
def qualified_table(model, schema_editor):
    original = model._meta.db_table
    if original != TABLE_NAME:
        raise RuntimeError('Unexpected table for the active outbox index')
    quote = schema_editor.quote_name
    model._meta.db_table = f'{quote(SCHEMA_NAME)}.{quote(TABLE_NAME)}'
    try:
        yield
    finally:
        model._meta.db_table = original


class AddActiveOutboxIndex(migrations.AddIndex):
    """Keep AddIndex state/router behavior with vendor-specific database DDL."""
    atomic = False

    def check_transaction(self, schema_editor):
        if schema_editor.connection.in_atomic_block:
            raise NotSupportedError('Concurrent outbox index migration must run outside a transaction')

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if schema_editor.connection.vendor == 'sqlite':
            return super().database_forwards(app_label, schema_editor, from_state, to_state)
        model = to_state.apps.get_model(app_label, self.model_name)
        if not self.allow_migrate_model(schema_editor.connection.alias, model):
            return
        if schema_editor.connection.vendor != 'postgresql':
            raise NotSupportedError('Active outbox index supports PostgreSQL and SQLite')
        self.check_transaction(schema_editor)
        if not schema_editor.collect_sql:
            target_table(schema_editor)
            if index_catalog(schema_editor) is not None:
                raise RuntimeError('Active outbox index name already exists; inspect it before recovery')
        with qualified_table(model, schema_editor):
            schema_editor.add_index(model, self.index, concurrently=True)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if schema_editor.connection.vendor == 'sqlite':
            return super().database_backwards(app_label, schema_editor, from_state, to_state)
        model = from_state.apps.get_model(app_label, self.model_name)
        if not self.allow_migrate_model(schema_editor.connection.alias, model):
            return
        if schema_editor.connection.vendor != 'postgresql':
            raise NotSupportedError('Active outbox index supports PostgreSQL and SQLite')
        self.check_transaction(schema_editor)
        if not schema_editor.collect_sql:
            require_owned_index(schema_editor, target_table(schema_editor))
        quote = schema_editor.quote_name
        schema_editor.execute(f'DROP INDEX CONCURRENTLY {quote(SCHEMA_NAME)}.{quote(INDEX_NAME)}')


class Migration(migrations.Migration):
    atomic = False
    dependencies = [('labops', '0006_event_immutability')]
    operations = [AddActiveOutboxIndex(model_name='outboxevent', index=models.Index(
        fields=['created_at', 'id'], name=INDEX_NAME,
        condition=Q(transport='kafka') & Q(status__in=['PENDING', 'PROCESSING'])))]
