"""Bounded-label Prometheus metrics and optional OTLP traces."""
import os
import re
import time
from contextlib import contextmanager
from prometheus_client import Counter, Histogram, CollectorRegistry, REGISTRY, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.core import GaugeMetricFamily
from opentelemetry import trace
from django.db import connection
from django.http import HttpResponse

HTTP = Histogram('http_request_duration_seconds', 'HTTP latency', ['method','route','status'])
DB = Histogram('db_query_duration_seconds', 'Database statement latency', ['operation'])
TRANSACTION = Histogram('db_transaction_duration_seconds', 'Business command duration', ['command'])
REPLAYS = Counter('idempotency_replays_total', 'Replayed commands')
CACHE = Counter('cache_requests_total', 'Cache outcomes', ['result'])
CACHE_LATENCY = Histogram('cache_request_duration_seconds', 'Cache read latency', ['result'])
RATE = Counter('rate_limit_rejections_total', 'Rejected operations', ['operation'])
tracer = trace.get_tracer('labops')

@contextmanager
def command_span(name):
    with TRANSACTION.labels(name).time(), tracer.start_as_current_span('command.'+name):
        yield

class MetricsMiddleware:
    def __init__(self,get_response):self.get_response=get_response
    def __call__(self,request):
        if request.path == '/metrics':return self.get_response(request)
        started=time.perf_counter()
        def measured(execute,sql,params,many,context):
            operation=sql.lstrip().split(' ',1)[0].upper()
            if operation not in {'SELECT','INSERT','UPDATE','DELETE','COMMIT','BEGIN','SAVEPOINT','RELEASE','ROLLBACK'}:operation='OTHER'
            with DB.labels(operation).time():return execute(sql,params,many,context)
        with connection.execute_wrapper(measured):response=self.get_response(request)
        route=request.resolver_match.route if request.resolver_match else 'unmatched'
        if route=='api/v1/<path:route>':
            # Whitelist the resource; never use user-controlled IDs in metric labels.
            resource=request.path.split('/')[3] if len(request.path.split('/'))>3 else ''
            from .api import MODELS
            route='api/'+(resource if resource in {*MODELS,'inventory','reports','stock','dashboard','references','me','system'} else 'other')
        HTTP.labels(request.method,route,str(response.status_code)).observe(time.perf_counter()-started)
        return response

class DatabaseMetrics:
    def collect(self):
        from django.utils import timezone
        from .models import OutboxEvent,FailedDelivery
        from .inventory.services import reconcile
        for name,description,value in [
            ('outbox_backlog','Unpublished Kafka events',OutboxEvent.objects.filter(transport='kafka').exclude(status='PUBLISHED').count()),
            ('outbox_publish_failures','Kafka events with failed attempts',OutboxEvent.objects.filter(transport='kafka',attempts__gt=0).exclude(status='PUBLISHED').count()),
            ('consumer_failures','Unresolved consumer deliveries',FailedDelivery.objects.exclude(status='RESOLVED').count()),
            ('inventory_reconciliation_failures','Mismatched ledger balances',len(reconcile())),
        ]:
            metric=GaugeMetricFamily(name,description);metric.add_metric([],value);yield metric
        oldest=OutboxEvent.objects.filter(transport='kafka').exclude(status='PUBLISHED').order_by('created_at').first()
        metric=GaugeMetricFamily('outbox_oldest_pending_seconds','Age of oldest unpublished event')
        metric.add_metric([],max(0,(timezone.now()-oldest.created_at).total_seconds()) if oldest else 0);yield metric

def metrics(request):
    import hmac
    from django.conf import settings
    token=os.environ.get('METRICS_TOKEN','')
    if token:
        if not hmac.compare_digest(request.headers.get('Authorization',''),'Bearer '+token):return HttpResponse(status=403)
    elif not settings.DEBUG:return HttpResponse(status=403)
    if os.environ.get('PROMETHEUS_MULTIPROC_DIR'):
        from prometheus_client import multiprocess
        registry=CollectorRegistry();multiprocess.MultiProcessCollector(registry)
    else:
        registry=CollectorRegistry()
        for metric in [HTTP,DB,TRANSACTION,REPLAYS,CACHE,CACHE_LATENCY,RATE]:registry.register(metric)
    registry.register(DatabaseMetrics())
    return HttpResponse(generate_latest(registry),content_type=CONTENT_TYPE_LATEST)

def configure_tracing():
    if not os.environ.get('OTEL_EXPORTER_OTLP_ENDPOINT'):return
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.django import DjangoInstrumentor
    from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
    provider=TracerProvider(resource=Resource.create({'service.name':os.environ.get('OTEL_SERVICE_NAME','labops')}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    DjangoInstrumentor().instrument();PsycopgInstrumentor().instrument()


def traced_consumer(fn):
    from functools import wraps
    from opentelemetry.propagate import extract
    @wraps(fn)
    def wrapped(consumer,event,*args,**kwargs):
        with tracer.start_as_current_span('consumer.'+consumer,context=extract(event.get('trace_context',{})),kind=trace.SpanKind.CONSUMER):
            return fn(consumer,event,*args,**kwargs)
    return wrapped
