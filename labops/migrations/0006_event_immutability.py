"""Database enforcement for retained inventory and failure identity.

PostgreSQL is the supported production store. SQLite demo mode retains runtime
checksum detection but intentionally does not claim database DML immutability.
"""
from django.db import migrations


def install_guards(apps, schema_editor):
    if schema_editor.connection.vendor != 'postgresql':
        return
    schema_editor.execute("""
        CREATE FUNCTION labops_outbox_immutable_content() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            IF OLD.payload_hash IS NOT NULL AND OLD.payload_hash <> '' AND (
                NEW.id IS DISTINCT FROM OLD.id OR
                NEW.event_type IS DISTINCT FROM OLD.event_type OR
                NEW.schema_version IS DISTINCT FROM OLD.schema_version OR
                NEW.aggregate_type IS DISTINCT FROM OLD.aggregate_type OR
                NEW.aggregate_id IS DISTINCT FROM OLD.aggregate_id OR
                NEW.aggregate_version IS DISTINCT FROM OLD.aggregate_version OR
                NEW.created_at IS DISTINCT FROM OLD.created_at OR
                NEW.payload_json::text IS DISTINCT FROM OLD.payload_json::text OR
                NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
            ) THEN
                RAISE EXCEPTION 'Retained OutboxEvent content is immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END $$
    """)
    schema_editor.execute("""
        CREATE TRIGGER labops_outbox_no_content_mutation
        BEFORE UPDATE ON labops_outboxevent
        FOR EACH ROW EXECUTE FUNCTION labops_outbox_immutable_content()
    """)
    schema_editor.execute("""
        CREATE FUNCTION labops_failure_immutable_content() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            IF OLD.original_hash IS NOT NULL AND OLD.original_hash <> '' AND (
                NEW.id IS DISTINCT FROM OLD.id OR
                NEW.consumer_name IS DISTINCT FROM OLD.consumer_name OR
                NEW.source_cluster IS DISTINCT FROM OLD.source_cluster OR
                NEW.source_generation IS DISTINCT FROM OLD.source_generation OR
                NEW.delivery_key IS DISTINCT FROM OLD.delivery_key OR
                NEW.created_at IS DISTINCT FROM OLD.created_at OR
                NEW.envelope::text IS DISTINCT FROM OLD.envelope::text OR
                NEW.original_hash IS DISTINCT FROM OLD.original_hash
            ) THEN
                RAISE EXCEPTION 'Retained FailedDelivery evidence is immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END $$
    """)
    schema_editor.execute("""
        CREATE TRIGGER labops_failure_no_content_mutation
        BEFORE UPDATE ON labops_faileddelivery
        FOR EACH ROW EXECUTE FUNCTION labops_failure_immutable_content()
    """)
    schema_editor.execute("""
        CREATE FUNCTION labops_processed_immutable_identity() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
            IF OLD.payload_hash IS NOT NULL AND OLD.payload_hash <> '' AND (
                NEW.id IS DISTINCT FROM OLD.id OR
                NEW.consumer_name IS DISTINCT FROM OLD.consumer_name OR
                NEW.event_id IS DISTINCT FROM OLD.event_id OR
                NEW.created_at IS DISTINCT FROM OLD.created_at OR
                NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
            ) THEN
                RAISE EXCEPTION 'Retained ProcessedEvent identity is immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END $$
    """)
    schema_editor.execute("""
        CREATE TRIGGER labops_processed_no_identity_mutation
        BEFORE UPDATE ON labops_processedevent
        FOR EACH ROW EXECUTE FUNCTION labops_processed_immutable_identity()
    """)


def remove_guards(apps, schema_editor):
    if schema_editor.connection.vendor != 'postgresql':
        return
    for table, trigger, function in (
        ('labops_outboxevent', 'labops_outbox_no_content_mutation', 'labops_outbox_immutable_content'),
        ('labops_faileddelivery', 'labops_failure_no_content_mutation', 'labops_failure_immutable_content'),
        ('labops_processedevent', 'labops_processed_no_identity_mutation', 'labops_processed_immutable_identity'),
    ):
        schema_editor.execute(f'DROP TRIGGER IF EXISTS {trigger} ON {table}')
        schema_editor.execute(f'DROP FUNCTION IF EXISTS {function}()')


class Migration(migrations.Migration):
    dependencies = [('labops', '0005_event_contracts')]
    operations = [migrations.RunPython(install_guards, remove_guards)]
