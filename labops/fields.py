from decimal import Decimal
from django.db import models
class Fixed6Field(models.BigIntegerField):
    """Exact NUMERIC(18,6) semantics using micro-units on SQLite and PostgreSQL."""
    def from_db_value(self, value, expression, connection):
        return None if value is None else Decimal(value) / Decimal(1000000)
    def to_python(self, value):
        return None if value is None else Decimal(str(value))
    def get_prep_value(self, value):
        if value is None: return None
        number = Decimal(str(value))
        scaled = number * 1000000
        if not number.is_finite() or scaled != scaled.to_integral_value() or abs(scaled) >= 10**18:
            raise ValueError('Quantity or cost exceeds the NUMERIC(18,6) range')
        return int(scaled)
