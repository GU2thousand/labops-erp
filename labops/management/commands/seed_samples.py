from django.core.management.base import BaseCommand
from django.db import transaction
from labops.models import User,TestCatalog,LabOrder,Project,Task
from labops.samples.services import create_order,register,transition
class Command(BaseCommand):
    @transaction.atomic
    def handle(self,*a,**kw):
        admin=User.objects.filter(email='admin@labops.local').first()
        if not admin:return
        for code,name,typ in [('TEST-NUT','Micronutrient Testing (Simulated)','SERUM'),('TEST-MOL','Molecular Assay Validation (Simulated)','BLOOD'),('TEST-QC','QC Sample Testing (Simulated)','PLASMA')]:TestCatalog.objects.get_or_create(code=code,defaults=dict(name=name,sample_type=typ,created_by=admin,updated_by=admin))
        if LabOrder.objects.exists():return
        p=Project.objects.filter(code='PRJ-2026-001').first()
        if not p:return
        t=Task.objects.filter(project=p,status='IN_PROGRESS').first()
        o=create_order(admin,dict(project_id=str(p.id),task_id=str(t.id),test_id=str(TestCatalog.objects.get(code='TEST-NUT').id)),'demo-samples')
        for n in range(3):
            s=register(admin,dict(order_id=str(o.id),barcode=f'SIM-2609-{n+1:04}'),'demo-samples')
            if n>0:s=transition(admin,s.id,dict(expected_version=s.version,target_status='RECEIVED'),'demo-samples')
            if n==2:transition(admin,s.id,dict(expected_version=s.version,target_status='PROCESSING'),'demo-samples')
        self.stdout.write('Fictional samples created.')
