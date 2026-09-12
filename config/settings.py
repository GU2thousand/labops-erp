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
MIDDLEWARE = ['django.middleware.security.SecurityMiddleware','django.contrib.sessions.middleware.SessionMiddleware','django.middleware.common.CommonMiddleware','django.middleware.csrf.CsrfViewMiddleware','django.contrib.auth.middleware.AuthenticationMiddleware','django.middleware.clickjacking.XFrameOptionsMiddleware']
ROOT_URLCONF = 'config.urls'
TEMPLATES = [{'BACKEND':'django.template.backends.django.DjangoTemplates','DIRS':[BASE_DIR/'labops/templates'],'APP_DIRS':True,'OPTIONS':{'context_processors':['django.template.context_processors.request','django.contrib.auth.context_processors.auth','django.template.context_processors.csrf']}}]
DATABASES = {'default': {'ENGINE':'django.db.backends.sqlite3','NAME':os.environ.get('LABOPS_DB', str(BASE_DIR/'labops.sqlite3')),'OPTIONS':{'timeout':30,'transaction_mode':'IMMEDIATE'}}}
if os.environ.get('POSTGRES_DB'):
    DATABASES = {'default': {'ENGINE':'django.db.backends.postgresql','NAME':os.environ['POSTGRES_DB'],'USER':os.environ.get('POSTGRES_USER','labops'),'PASSWORD':os.environ.get('POSTGRES_PASSWORD',''),'HOST':os.environ.get('POSTGRES_HOST','127.0.0.1'),'PORT':os.environ.get('POSTGRES_PORT','5432')}}
AUTH_USER_MODEL = 'labops.User'
AUTH_PASSWORD_VALIDATORS = [{'NAME':'django.contrib.auth.password_validation.MinimumLengthValidator','OPTIONS':{'min_length':10}},{'NAME':'django.contrib.auth.password_validation.CommonPasswordValidator'}]
LANGUAGE_CODE = 'zh-hans'
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
