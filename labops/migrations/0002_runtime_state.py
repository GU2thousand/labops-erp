from django.db import migrations

def init(apps,schema_editor): apps.get_model('labops','RuntimeState').objects.get_or_create(pk=1)
class Migration(migrations.Migration):
    dependencies=[('labops','0001_initial')]
    operations=[migrations.RunPython(init,migrations.RunPython.noop)]
