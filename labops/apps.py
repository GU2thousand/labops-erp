from django.apps import AppConfig
class LabopsConfig(AppConfig):
    name = 'labops'

    def ready(self):
        from .telemetry import configure_tracing
        configure_tracing()
