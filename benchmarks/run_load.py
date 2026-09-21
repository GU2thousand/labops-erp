"""Clone a benchmark template per tier, start Gunicorn, run k6, reconcile, clean up.
Requires a PostgreSQL role with CREATEDB and a seed_benchmark template database.
"""
import argparse,json,os,signal,subprocess,time,uuid
from pathlib import Path
from urllib.parse import urlparse,urlunparse
import urllib.request
import psycopg
from psycopg import sql

p=argparse.ArgumentParser()
p.add_argument('--template-url',required=True)
p.add_argument('--tiers',default='20,50,100,200')
p.add_argument('--duration',default='30s')
p.add_argument('--port',type=int,default=18766)
a=p.parse_args()
root=Path(__file__).resolve().parents[1];out=root/'benchmarks/results';out.mkdir(exist_ok=True)
parsed=urlparse(a.template_url);template=parsed.path.lstrip('/');admin_url=urlunparse(parsed._replace(path='/postgres'))
metadata={'tiers':[],'gunicorn_workers':2,'threads_per_worker':4,'duration':a.duration,'template':template,'k6':subprocess.check_output(['k6','version'],text=True).strip()}
with psycopg.connect(admin_url,autocommit=True) as admin:
    for count in map(int,a.tiers.split(',')):
        database='labops_load_'+uuid.uuid4().hex[:12]
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(sql.Identifier(database),sql.Identifier(template)))
        env={**os.environ,'DATABASE_URL':urlunparse(parsed._replace(path='/'+database)),'LABOPS_DB_MODE':'postgres','REDIS_URL':'','OTEL_EXPORTER_OTLP_ENDPOINT':''}
        log=(out/f'gunicorn-{count}.log').open('w')
        process=subprocess.Popen([str(root/'.venv/bin/gunicorn'),'config.wsgi:application','--bind',f'127.0.0.1:{a.port}','--workers','2','--threads','4'],cwd=root,env=env,stdout=log,stderr=log)
        try:
            for _ in range(100):
                if process.poll() is not None:raise RuntimeError('Gunicorn failed to start')
                try:
                    urllib.request.urlopen(f'http://127.0.0.1:{a.port}/login/',timeout=1);break
                except OSError:time.sleep(.1)
            with (out/f'k6-{count}.log').open('w') as report:
                result=subprocess.run(['k6','run','--summary-export',str(out/f'k6-{count}.json'),str(root/'benchmarks/load.js')],cwd=root,env={**env,'BASE_URL':f'http://127.0.0.1:{a.port}','VUS':str(count),'DURATION':a.duration,'RUN_ID':database},stdout=report,stderr=report)
            reconciliation=subprocess.run([str(root/'.venv/bin/python'),'manage.py','reconcile_stock'],cwd=root,env=env,capture_output=True,text=True)
            metadata['tiers'].append({'vus':count,'k6_exit_code':result.returncode,'reconcile_exit_code':reconciliation.returncode,'reconcile':reconciliation.stdout.strip()})
            (out/'load-environment.json').write_text(json.dumps(metadata,indent=2))
            if result.returncode or reconciliation.returncode:raise RuntimeError(f'{count} VU run failed; inspect results')
        finally:
            process.terminate()
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:process.kill();process.wait()
            log.close()
            admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
        print(f'{count} VU completed and reconciled',flush=True)
