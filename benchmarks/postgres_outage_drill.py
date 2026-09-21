"""Destructive PostgreSQL outage drill, ONLY against a dedicated disposable server.

The explicit --confirm-stop-container must exactly match --container. Never point
this at an application/shared database server: all its databases are interrupted.
A unique database is created and dropped; credentials come from DATABASE_URL.
Example: DATABASE_URL=postgresql://.../postgres .venv/bin/python \
 benchmarks/postgres_outage_drill.py --container labops-upgrade-postgres \
 --confirm-stop-container labops-upgrade-postgres --port 8003
"""
import argparse, hashlib, http.client as http_client, json, os, secrets, subprocess, sys, time, uuid
from http.cookies import SimpleCookie
from pathlib import Path
from urllib.parse import urlparse, urlunparse, urlencode
import psycopg
from psycopg import sql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', required=True)
    parser.add_argument('--confirm-stop-container', required=True)
    parser.add_argument('--port', type=int, default=8003)
    parser.add_argument('--output', default='benchmarks/results/postgres-outage.json')
    args = parser.parse_args()
    if args.container != args.confirm_stop_container:
        parser.error('Explicit stop confirmation must match the dedicated container')
    details = json.loads(subprocess.check_output(['docker','inspect',args.container]))[0]
    if details['Config'].get('Labels', {}).get('com.docker.compose.project'):
        parser.error('Refusing to stop a Compose service; use a dedicated disposable PostgreSQL container')
    parsed = urlparse(os.environ['DATABASE_URL'])
    if parsed.hostname not in ('127.0.0.1','localhost'):
        parser.error('Only loopback dedicated database servers are supported')
    ports = details['HostConfig']['PortBindings'].get('5432/tcp', [])
    if not any(str(parsed.port or 5432) == p['HostPort'] for p in ports):
        parser.error('DATABASE_URL port does not match the dedicated container')
    root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(root))
    database = 'labops_outage_' + uuid.uuid4().hex[:12]
    admin_url = urlunparse(parsed._replace(path='/postgres'))
    db_url = urlunparse(parsed._replace(path='/'+database, query='connect_timeout=2'))
    started = details['State']['Running']
    process = None; created = False; cookies = {}
    result = {'scenario':'full PostgreSQL service outage through real HTTP', 'database':database,
              'debug':False, 'gunicorn_workers':2, 'checks':{}, 'requests':[]}
    def docker(action):
        subprocess.run(['docker',action,args.container],check=True,stdout=subprocess.DEVNULL)
    def ready():
        for _ in range(100):
            try:
                with psycopg.connect(admin_url,connect_timeout=1): return
            except psycopg.Error: time.sleep(.2)
        raise RuntimeError('PostgreSQL did not recover')
    def http(method,path,body=None,key=None):
        conn = http_client.HTTPConnection('127.0.0.1',args.port,timeout=10)
        headers = {'Cookie':'; '.join(f'{k}={v}' for k,v in cookies.items())}
        if body is not None:
            headers['Content-Type'] = 'application/json' if isinstance(body,dict) else 'application/x-www-form-urlencoded'
            if isinstance(body,dict): body=json.dumps(body)
            headers['X-CSRFToken']=cookies.get('csrftoken','')
        if key: headers['Idempotency-Key']=key
        before=time.monotonic();conn.request(method,path,body,headers);response=conn.getresponse();raw=response.read()
        for k,v in response.getheaders():
            if k.lower()=='set-cookie':
                jar=SimpleCookie();jar.load(v);cookies.update({k:x.value for k,x in jar.items()})
        entry={'method':method,'path':path,'status':response.status,'elapsed_ms':round((time.monotonic()-before)*1000,2)}
        if response.status==503:
            value=json.loads(raw);entry['code']=value['error']['code']
            assert entry['code']=='DATABASE_UNAVAILABLE' and response.getheader('Retry-After')=='5',entry
            assert parsed.hostname.encode() not in raw and b'OperationalError' not in raw
        result['requests'].append(entry);conn.close()
        return response.status,raw
    try:
        if not started: docker('start')
        ready()
        with psycopg.connect(admin_url,autocommit=True) as conn:
            conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(database)));created=True
        env={**os.environ,'DATABASE_URL':db_url,'LABOPS_DB_MODE':'postgres','LABOPS_DEBUG':'0',
             'LABOPS_SECRET_KEY':secrets.token_urlsafe(48),'REDIS_URL':'','OTEL_EXPORTER_OTLP_ENDPOINT':'',
             'LABOPS_EVENT_TRANSPORT':'local','DJANGO_SETTINGS_MODULE':'config.settings'}
        for name in list(env):
            if name.startswith('POSTGRES_'):
                env.pop(name)
                os.environ.pop(name, None)
        os.environ.update(env)
        import django
        django.setup()
        from django.core.management import call_command
        from django.db import connection, connections
        from labops.models import StockBalance, Task, StockMovement, CommandResult, OutboxEvent, AuditEvent
        from labops.inventory.services import reconcile
        call_command('migrate',verbosity=0);call_command('seed_demo',verbosity=0)
        balance=StockBalance.objects.filter(on_hand_qty__gte=1).first()
        task=Task.objects.filter(status='IN_PROGRESS').first()
        data={'task_id':str(task.pk),'lines':[{'batch_id':str(balance.batch_id),'warehouse_id':str(balance.warehouse_id),'qty':'0.001'}]}
        key='postgres-outage-'+uuid.uuid4().hex
        def snapshot():
            values={}
            with connection.cursor() as cur:
                for table in sorted(t for t in connection.introspection.table_names() if t.startswith('labops_')):
                    cur.execute(sql.SQL('SELECT * FROM {} ORDER BY 1').format(sql.Identifier(table)))
                    values[table]=hashlib.sha256(repr(cur.fetchall()).encode()).hexdigest()
            return values
        log=root/args.output;log.parent.mkdir(parents=True,exist_ok=True)
        with log.with_suffix('.server.log').open('w') as stream:
            process=subprocess.Popen([str(root/'.venv/bin/gunicorn'),'config.wsgi:application','--bind',f'127.0.0.1:{args.port}','--workers','2'],cwd=root,env=env,stdout=stream,stderr=stream)
            for _ in range(100):
                try:
                    assert http('GET','/login/')[0]==200;break
                except (OSError,AssertionError):time.sleep(.1)
            else:
                raise RuntimeError('Gunicorn failed to become ready')
            assert http('POST','/login/',urlencode({'email':'admin@labops.local','password':'LabOpsDemo!2026','csrfmiddlewaretoken':cookies['csrftoken']}))[0]==302
            assert http('GET','/api/v1/items')[0]==200
            before=snapshot(); counts={m.__name__:m.objects.count() for m in (StockMovement,CommandResult,OutboxEvent,AuditEvent)}
            connections.close_all();docker('stop')
            assert http('GET','/api/v1/items')[0]==503
            assert http('POST','/api/v1/stock/issues',data,key)[0]==503
            assert http('GET','/')[0]==503
            docker('start');ready()
            after=snapshot();assert before==after
            result['checks']['all_business_tables_unchanged_after_failed_requests']=True
            result['checks']['business_table_count']=len(before)
            assert http('GET','/api/v1/items')[0]==200
            status,first=http('POST','/api/v1/stock/issues',data,key);assert status==200,(status,first)
            after_first={m.__name__:m.objects.count() for m in (StockMovement,CommandResult,OutboxEvent,AuditEvent)}
            status,second=http('POST','/api/v1/stock/issues',data,key);assert status==200,(status,second)
            assert json.loads(first)['data']==json.loads(second)['data']
            assert after_first=={m.__name__:m.objects.count() for m in (StockMovement,CommandResult,OutboxEvent,AuditEvent)}
            assert StockMovement.objects.filter(idempotency_key=key).count()==1
            assert not reconcile()
            result['checks'].update({'retry_same_key_exactly_one_movement':True,'duplicate_retry_no_additional_side_effects':True,'reconciliation_differences':0,'recovery_count_deltas':{k:after_first[k]-counts[k] for k in counts}})
            result['passed']=True
    finally:
        if process:
            process.terminate()
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:process.kill();process.wait()
        docker('start');ready()
        if created:
            if 'connections' in locals():connections.close_all()
            with psycopg.connect(admin_url,autocommit=True) as conn:
                conn.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(database)))
        if not started:docker('stop')
        current = json.loads(subprocess.check_output(['docker', 'inspect', args.container]))[0]['State']['Running']
        result['cleanup']={'disposable_database_removed':created, 'initial_container_running':started, 'container_running_restored':current == started}
        (root/args.output).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))

if __name__=='__main__': main()
