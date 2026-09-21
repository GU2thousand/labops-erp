"""Optional Redis. Cache outages fall back to DB; sensitive rate limits fail closed."""
import hashlib
import json
import os
import time
from functools import lru_cache
import redis
from .telemetry import CACHE,CACHE_LATENCY,RATE

@lru_cache
def client():
    url=os.environ.get('REDIS_URL')
    return redis.Redis.from_url(url,socket_connect_timeout=.2,socket_timeout=.2,decode_responses=True) if url else None

def cached_catalog(loader):
    broker=client()
    if broker is None:return loader()
    start=time.perf_counter();result='miss'
    try:
        generation=broker.get('labops:catalog:generation') or '0'
        key='labops:catalog:'+generation
        raw=broker.get(key)
        if raw is not None:
            result='hit';return json.loads(raw)
        data=loader();broker.setex(key,60,json.dumps(data));return data
    except (redis.RedisError,ValueError,TypeError):
        result='unavailable';return loader()
    finally:
        CACHE.labels(result).inc();CACHE_LATENCY.labels(result).observe(time.perf_counter()-start)

def invalidate_catalog():
    broker=client()
    if broker is None:return
    try:broker.incr('labops:catalog:generation')
    except redis.RedisError:pass  # TTL bounds stale results after failed invalidation.

RATE_LUA='''
local t=redis.call('TIME')
local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
local capacity=tonumber(ARGV[1])
local interval=tonumber(ARGV[2])*1000
local values=redis.call('HMGET',KEYS[1],'tokens','at')
local tokens=tonumber(values[1]) or capacity
local at=tonumber(values[2]) or now
tokens=math.min(capacity,tokens+math.max(0,now-at)*capacity/interval)
local allowed=0
if tokens>=1 then tokens=tokens-1 allowed=1 end
redis.call('HSET',KEYS[1],'tokens',tokens,'at',now)
redis.call('PEXPIRE',KEYS[1],interval*2)
return allowed
'''

def rate_limit(operation,identity,capacity,seconds):
    broker=client()
    if broker is None:return  # Optional; existing DB login protection remains.
    from .common import fail
    key=hashlib.sha256(str(identity).encode()).hexdigest()
    try:allowed=broker.eval(RATE_LUA,1,f'labops:rate:{operation}:{key}',capacity,seconds)
    except redis.RedisError:
        fail('RATE_LIMIT_UNAVAILABLE','Rate limiter is unavailable. Try again shortly.',503)
    if not allowed:
        RATE.labels(operation).inc();fail('RATE_LIMITED','Too many requests. Try again later.',429)
