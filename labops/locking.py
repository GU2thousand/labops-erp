"""Transaction-scoped locks. PostgreSQL is the concurrency reference backend.

Order: catalog gate, command key, opening gate, document key, project, request,
order, receipt/movement, balance keys (sorted). SQLite uses BEGIN IMMEDIATE.
"""
import hashlib
from django.db import connection
from . import models as m


def advisory(key, *, shared=False):
    if connection.vendor != 'postgresql':
        return
    number = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], 'big', signed=True)
    function = 'pg_advisory_xact_lock_shared' if shared else 'pg_advisory_xact_lock'
    with connection.cursor() as cursor:
        cursor.execute(f'SELECT {function}(%s)', [number])


def rows(model, ids):
    return list(model.objects.filter(pk__in={x for x in ids if x}).order_by('pk').select_for_update())


def command_locks(name, values):
    data = values.get('data') or {}
    ident = values.get('id')
    projects, tasks, requests, orders, receipts, movements = set(), set(), set(), set(), set(), set()
    if name in {'opening', 'issue', 'post_receipt'}:
        # Only the initial close competes with opening stock. No hot singleton
        # UPDATE remains on subsequent issues/receipts.
        state = m.RuntimeState.objects.get(pk=1)
        if name == 'opening' or not state.opening_closed:
            state = m.RuntimeState.objects.select_for_update().get(pk=1)
            if name != 'opening' and not state.opening_closed:
                state.opening_closed = True
                state.save(update_fields=['opening_closed'])
    if name in {'write_project', 'project_action'}:
        projects.add(ident)
    if name in {'write_task', 'task_action'}:
        tasks.add(ident)
        projects.add(data.get('project_id'))
    if name in {'issue', 'issue_draft'}:
        # Draft edits use the URL/document id; an unrelated payload field must
        # never redirect the lock away from the document being changed.
        draft = ident if name == 'issue_draft' else data.get('draft_id')
        if draft:
            advisory('document:' + str(draft))
            movements.add(draft)
            tasks.update(m.StockMovementLine.objects.filter(movement_id=draft).values_list('task_id', flat=True))
        tasks.add(data.get('task_id'))
    if name in {'write_request', 'request_action'}:
        requests.add(ident)
        projects.add(data.get('project_id'))
    if name == 'write_order':
        line_ids = [x.get('request_line_id') for x in data.get('lines', [])]
        requests.update(m.RequestLine.objects.filter(pk__in=line_ids).values_list('request_id', flat=True))
    if name == 'order_action':
        orders.add(ident)
    if name == 'create_receipt':
        orders.add(data.get('order_id'))
    if name == 'post_receipt':
        receipts.add(ident)
    if name == 'reverse':
        movements.add(ident)
        receipts.update(m.StockMovement.objects.filter(pk=ident).values_list('receipt_id', flat=True))
    if name == 'create_order':  # sample lab order
        projects.add(data.get('project_id'))
    if name in {'register', 'order_transition'}:
        projects.update(m.LabOrder.objects.filter(pk=ident or data.get('order_id')).values_list('project_id', flat=True))
    if name == 'transition':  # sample
        projects.update(m.Sample.objects.filter(pk=ident).values_list('order__project_id', flat=True))
    orders.update(m.Receipt.objects.filter(pk__in=receipts - {None}).values_list('order_id', flat=True))
    requests.update(m.OrderLine.objects.filter(order_id__in=orders - {None}).values_list('request_line__request_id', flat=True))
    projects.update(m.PurchaseRequest.objects.filter(pk__in=requests - {None}).values_list('project_id', flat=True))
    projects.update(m.Task.objects.filter(pk__in=tasks - {None}).values_list('project_id', flat=True))
    for model, ids in [(m.Project, projects), (m.Task, tasks), (m.PurchaseRequest, requests),
                       (m.PurchaseOrder, orders), (m.Receipt, receipts), (m.StockMovement, movements)]:
        if ids - {None}:
            rows(model, ids)
    if name == 'queue_import':
        rows(m.ImportJob, [ident])
    if name == 'adjustment':
        advisory(f"balance:{data.get('batch_id')}:{data.get('warehouse_id')}")
        list(m.StockBalance.objects.filter(batch_id=data.get('batch_id'), warehouse_id=data.get('warehouse_id')).select_for_update())
