import os, secrets
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent.parent
DEBUG = os.environ.get('LABOPS_DEBUG', '1') == '1'
secret_file = BASE_DIR / '.local-secret'
if not os.environ.get('LABOPS_SECRET_KEY') and DEBUG and not secret_file.exists():
    fd = os.open(secret_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as out: out.write(secrets.token_urlsafe(64))
SECRET_KEY = os.environ.get('LABOPS_SECRET_KEY') or (secret_file.read_text() if DEBUG else '')
if not SECRET_KEY: raise RuntimeError('LABOPS_SECRET_KEY is required')
ALLOWED_HOSTS = os.environ.get('LABOPS_ALLOWED_HOSTS', 'localhost,127.0.0.1,testserver').split(',')
INSTALLED_APPS = ['django.contrib.auth','django.contrib.contenttypes','django.contrib.sessions','django.contrib.staticfiles','labops']
MIDDLEWARE = ['labops.telemetry.MetricsMiddleware','whitenoise.middleware.WhiteNoiseMiddleware','django.middleware.security.SecurityMiddleware','django.contrib.sessions.middleware.SessionMiddleware','django.middleware.common.CommonMiddleware','django.middleware.csrf.CsrfViewMiddleware','django.contrib.auth.middleware.AuthenticationMiddleware','django.middleware.clickjacking.XFrameOptionsMiddleware']
ROOT_URLCONF = 'config.urls'
TEMPLATES = [{'BACKEND':'django.template.backends.django.DjangoTemplates','DIRS':[BASE_DIR/'labops/templates'],'APP_DIRS':True,'OPTIONS':{'context_processors':['django.template.context_processors.request','django.contrib.auth.context_processors.auth','django.template.context_processors.csrf']}}]
from urllib.parse import urlparse, unquote, parse_qs
DB_MODE = os.environ.get('LABOPS_DB_MODE', 'postgres')
if DB_MODE == 'sqlite-demo':
    DATABASES = {'default': {'ENGINE':'django.db.backends.sqlite3','NAME':os.environ.get('LABOPS_DB', str(BASE_DIR/'labops.sqlite3')),'OPTIONS':{'timeout':30,'transaction_mode':'IMMEDIATE'}}}
elif DB_MODE == 'postgres':
    parsed = urlparse(os.environ.get('DATABASE_URL', 'postgresql://labops:labops-local@127.0.0.1:55432/labops'))
    if parsed.scheme not in {'postgres', 'postgresql'}: raise RuntimeError('DATABASE_URL must use PostgreSQL')
    DATABASES = {'default': {'ENGINE':'django.db.backends.postgresql',
        'NAME':os.environ.get('POSTGRES_DB', unquote(parsed.path.lstrip('/'))),
        'USER':os.environ.get('POSTGRES_USER', unquote(parsed.username or 'labops')),
        'PASSWORD':os.environ.get('POSTGRES_PASSWORD', unquote(parsed.password or '')),
        'HOST':os.environ.get('POSTGRES_HOST', parsed.hostname or '127.0.0.1'),
        'PORT':os.environ.get('POSTGRES_PORT', str(parsed.port or 5432)),
        'CONN_MAX_AGE': int(os.environ.get('DB_CONN_MAX_AGE', '0')),
        'OPTIONS': {k:v[-1] for k,v in parse_qs(parsed.query).items() if k in {'sslmode', 'connect_timeout'}}}}
else: raise RuntimeError('LABOPS_DB_MODE must be postgres or sqlite-demo')
AUTH_USER_MODEL = 'labops.User'
AUTH_PASSWORD_VALIDATORS = [{'NAME':'django.contrib.auth.password_validation.MinimumLengthValidator','OPTIONS':{'min_length':10}},{'NAME':'django.contrib.auth.password_validation.CommonPasswordValidator'}]
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'America/New_York'
USE_TZ = True
STATIC_URL = '/static/'
STATIC_ROOT = BASE_DIR/'staticfiles'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = 'Lax'
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG
CSRF_FAILURE_VIEW = 'labops.api.csrf_failure'
SECURE_CONTENT_TYPE_NOSNIFF = True
X_FRAME_OPTIONS = 'SAMEORIGIN'
DATA_UPLOAD_MAX_MEMORY_SIZE = 6 * 1024 * 1024
FILE_UPLOAD_MAX_MEMORY_SIZE = 5 * 1024 * 1024
LOGIN_URL = '/login/'
LOGGING = {'version':1,'disable_existing_loggers':False,'handlers':{'console':{'class':'logging.StreamHandler'}},'loggers':{'labops':{'handlers':['console'],'level':'INFO'}}}

if os.environ.get("LABOPS_TEST_DB"):
    DATABASES["default"]["TEST"] = {"NAME": os.environ["LABOPS_TEST_DB"]}

EVENT_TRANSPORT = os.environ.get('LABOPS_EVENT_TRANSPORT', 'local')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:19092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'labops.inventory.v1')
KAFKA_DLQ_TOPIC = os.environ.get('KAFKA_DLQ_TOPIC', 'labops.inventory.dlq.v1')
EVENT_RETRY_SECONDS = [60, 300, 900, 3600]
