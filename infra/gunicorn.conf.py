import os

bind = '0.0.0.0:8000'
workers = int(os.environ.get('WEB_CONCURRENCY', '2'))
accesslog = '-'
errorlog = '-'
timeout = 60
# Each worker initializes its own tracing exporter after fork.
preload_app = False


def child_exit(server, worker):
    from prometheus_client import multiprocess
    multiprocess.mark_process_dead(worker.pid)
