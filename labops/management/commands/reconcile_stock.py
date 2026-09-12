from django.core.management.base import BaseCommand,CommandError
from labops.inventory.services import reconcile
class Command(BaseCommand):
    def handle(self,*a,**kw):
        result=reconcile()
        if result: raise CommandError(str(result))
        self.stdout.write('OK: every stock balance matches the immutable ledger.')
