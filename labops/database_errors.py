"""Stable outage responses, including database-backed session/authentication failures."""
import logging
import uuid

from django.db import InterfaceError, OperationalError
from django.http import JsonResponse
from django.utils.deprecation import MiddlewareMixin


def database_unavailable(request_id=None):
    request_id = request_id or str(uuid.uuid4())
    logging.getLogger('labops').warning('database_unavailable request_id=%s', request_id)
    response = JsonResponse({
        'error': {'code': 'DATABASE_UNAVAILABLE',
                  'message': 'Database temporarily unavailable. Please retry shortly.',
                  'field_errors': {}},
        'request_id': request_id,
    }, status=503)
    response['Retry-After'] = '5'
    response['Cache-Control'] = 'no-store'
    return response


class DatabaseUnavailableMiddleware(MiddlewareMixin):
    def process_exception(self, request, exception):
        if isinstance(exception, (OperationalError, InterfaceError)):
            return database_unavailable()
        return None
