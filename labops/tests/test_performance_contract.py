from decimal import Decimal,localcontext
from django.test import TestCase,override_settings
from django.db import connection
from django.test.utils import CaptureQueriesContext
from labops.tests.test_acceptance import Fixture
from labops.models import *
from labops.queries import inventory_overview,costs
from labops.inventory.services import issue

@override_settings(PASSWORD_HASHERS=['django.contrib.auth.hashers.MD5PasswordHasher'])
class QueryContractTests(Fixture,TestCase):
    def test_inventory_query_count_does_not_grow_with_items(self):
        Item.objects.bulk_create([Item(code=f'Q{i}',name='Query item',base_uom='EA',created_by=self.admin) for i in range(50)])
        self.stock(10)
        with CaptureQueriesContext(connection) as queries:rows=inventory_overview()
        self.assertEqual(len(rows),51);self.assertLessEqual(len(queries),2)
    def test_movement_page_has_bounded_queries(self):
        for _ in range(25):self.stock(10)
        client=self.client_for(self.admin)
        with CaptureQueriesContext(connection) as queries:response=client.get('/api/v1/movements?page_size=20')
        self.assertEqual(response.status_code,200);self.assertLessEqual(len(queries),8)
    def test_micro_units_and_large_product_are_exact(self):
        batch=self.stock(10);batch.unit_cost=Decimal('999999999999.123456');batch.save()
        rows=inventory_overview()
        self.assertEqual(Decimal(rows[0]['value']),Decimal('9999999999991.234560'))
        issue(self.store,self.issue_data(batch,'0.000001'),'micro',self.rid)
        self.assertEqual(Decimal(costs(self.admin)[0]['material_cost']),Decimal('999999.999999123456'))
