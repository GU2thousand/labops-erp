import json, platform, statistics, time
from pathlib import Path
from django.core.management.base import BaseCommand
from django.db import connection
from django.test import Client
from django.test.utils import CaptureQueriesContext
from labops.models import User

class Command(BaseCommand):
    help='Measure authenticated Django request latency/query counts; not network load testing.'
    def add_arguments(self,p):
        p.add_argument('--output',required=True)
        p.add_argument('--samples',type=int,default=30)
    def handle(self,*args,**opts):
        client=Client();client.force_login(User.objects.get(email='benchmark@labops.local'))
        report={'kind':'in-process authenticated HTTP handler benchmark','python':platform.python_version(),'database':connection.vendor,'samples':opts['samples'],'results':{}}
        with connection.cursor() as cursor:
            if connection.vendor=='postgresql':cursor.execute('SELECT version()');report['database_version']=cursor.fetchone()[0]
        for endpoint in ['inventory','reports','movements']:
            url='/api/v1/'+endpoint
            for _ in range(3):client.get(url)
            values=[]
            for _ in range(opts['samples']):
                start=time.perf_counter();response=client.get(url);values.append((time.perf_counter()-start)*1000)
                if response.status_code!=200:raise RuntimeError(response.content.decode())
            with CaptureQueriesContext(connection) as queries:client.get(url)
            values.sort();report['results'][endpoint]={'p50_ms':round(statistics.median(values),3),'p95_ms':round(values[min(len(values)-1,int(len(values)*.95))],3),'query_count':len(queries),'samples_ms':values}
            if connection.vendor=='postgresql':
                plans=[]
                with connection.cursor() as cursor:
                    for q in queries.captured_queries:
                        if q['sql'].lstrip().upper().startswith(('SELECT','WITH')):
                            cursor.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+q['sql']);plans.append({'sql':q['sql'],'plan':cursor.fetchone()[0]})
                report['results'][endpoint]['query_plans']=plans
        output=Path(opts['output']);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2,default=str))
        self.stdout.write(json.dumps({k:{a:b for a,b in v.items() if a not in ['query_plans','samples_ms']} for k,v in report['results'].items()},indent=2))
