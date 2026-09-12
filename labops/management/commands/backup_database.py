import sqlite3
from pathlib import Path
from django.conf import settings
from django.core.management.base import BaseCommand,CommandError
from django.utils import timezone
class Command(BaseCommand):
    help='Consistent SQLite snapshot using SQLite backup API; never copy a live database file.'
    def add_arguments(self,p):p.add_argument('--output')
    def handle(self,*a,**kw):
        db=settings.DATABASES['default']
        if db['ENGINE']!='django.db.backends.sqlite3':raise CommandError('PostgreSQL: use pg_dump with your deployment backup policy.')
        target=Path(kw.get('output') or settings.BASE_DIR/'backups'/f'labops-{timezone.localdate()}.sqlite3')
        target.parent.mkdir(parents=True,exist_ok=True)
        if target.exists():self.stdout.write('Backup already exists; not overwritten.');return
        target.touch(mode=0o600,exist_ok=False)
        try:
            with sqlite3.connect(db['NAME']) as source,sqlite3.connect(target) as destination:
                source.backup(destination)
                if destination.execute('PRAGMA integrity_check').fetchone()[0]!='ok':raise CommandError('Backup integrity check failed')
        except Exception:
            target.unlink(missing_ok=True);raise
        self.stdout.write(f'Created verified SQLite backup: {target}')
