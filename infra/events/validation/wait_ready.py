"""Wait for authenticated RF3 cluster health and PostgreSQL startup."""
import argparse
import json
import os
from pathlib import Path
import time
import requests
import psycopg


def load_env(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        if line and not line.startswith('#'):
            key, value = line.split('=', 1)
            values[key] = value
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file', required=True)
    parser.add_argument('--timeout', type=int, default=180)
    args = parser.parse_args()
    env = load_env(args.env_file)
    end = time.monotonic() + args.timeout
    last = 'not ready'
    while time.monotonic() < end:
        try:
            response = requests.get(env['KAFKA_ADMIN_URL'] + '/v1/cluster/health_overview', auth=(env['KAFKA_ADMIN_USERNAME'], env['KAFKA_ADMIN_PASSWORD']), verify=env['KAFKA_SSL_CA_LOCATION'], timeout=5, allow_redirects=False)
            response.raise_for_status()
            health = response.json()
            if not health.get('is_healthy') or len(health.get('all_nodes', [])) != 3:
                last = 'waiting for three healthy brokers'
            else:
                with psycopg.connect(env['DATABASE_URL'], connect_timeout=3) as database:
                    database.execute('SELECT 1')
                print(json.dumps({'healthy': True, 'brokers': health['all_nodes'], 'database_ready': True}))
                return
        except Exception as exc:
            # Never echo connection URLs, passwords or exception payloads.
            last = type(exc).__name__
        time.sleep(2)
    raise SystemExit('Cluster readiness timed out: ' + last)


if __name__ == '__main__':
    main()
