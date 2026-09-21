#!/bin/sh
set -eu
if [ "${1:-}" = web ]; then
    export PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-/tmp/labops-prometheus}"
    mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
    # Gunicorn workers share counters; clear stale shards once per master start.
    rm -f "$PROMETHEUS_MULTIPROC_DIR"/*.db
    exec gunicorn config.wsgi:application --config /app/infra/gunicorn.conf.py
fi
exec "$@"
