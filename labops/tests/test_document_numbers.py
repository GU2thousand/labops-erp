from datetime import date
from unittest.mock import patch
from uuid import UUID

from django.test import SimpleTestCase
from labops.common import number
from labops.models import LabOrder, PurchaseOrder, PurchaseRequest, Receipt, StockMovement


class DocumentNumberTests(SimpleTestCase):
    first = UUID('01234567-89ab-4cde-8fab-0123456789ab')
    second = UUID('01234567-fedc-4ba9-8fed-fedcba987654')

    def test_distinct_uuids_with_the_same_first_32_bits_do_not_collide(self):
        self.assertEqual(self.first.hex[:8], self.second.hex[:8],
                         'The regression fixture must collide with the former short suffix')
        with patch('labops.common.timezone.localdate', return_value=date(2026, 9, 28)), \
             patch('labops.common.uuid.uuid4', side_effect=[self.first, self.second]):
            left, right = number('STK'), number('STK')
        self.assertNotEqual(left, right)
        self.assertEqual(left, 'STK-260928-' + self.first.hex.upper())
        self.assertEqual(right, 'STK-260928-' + self.second.hex.upper())

    def test_every_current_prefix_fits_its_actual_existing_model_field(self):
        uses = ((StockMovement, 'movement_no', 'STK'), (StockMovement, 'movement_no', 'ISS'),
                (PurchaseRequest, 'request_no', 'PR'), (PurchaseOrder, 'order_no', 'PO'),
                (Receipt, 'receipt_no', 'RCV'), (LabOrder, 'order_no', 'LAB'))
        for model, field_name, prefix in uses:
            with self.subTest(model=model.__name__, prefix=prefix), \
                 patch('labops.common.timezone.localdate', return_value=date(2026, 9, 28)), \
                 patch('labops.common.uuid.uuid4', return_value=self.first):
                value = number(prefix)
                field = model._meta.get_field(field_name)
                self.assertEqual(field.max_length, 64)
                self.assertEqual(len(value), len(prefix) + 40)
                self.assertLessEqual(len(value), field.max_length)
                self.assertEqual(field.clean(value, None), value)

    def test_existing_short_document_numbers_remain_valid_without_rewriting(self):
        for model, field_name, prefix in ((StockMovement, 'movement_no', 'STK'),
                (PurchaseRequest, 'request_no', 'PR'), (PurchaseOrder, 'order_no', 'PO'),
                (Receipt, 'receipt_no', 'RCV'), (LabOrder, 'order_no', 'LAB')):
            with self.subTest(model=model.__name__):
                value = prefix + '-260927-01234567'
                self.assertEqual(model._meta.get_field(field_name).clean(value, None), value)
