import uuid
from django.db import models
from django.db.models import Q
from django.contrib.auth.models import AbstractUser
from django.utils import timezone
from .fields import Fixed6Field

def fk(model, **kw): return models.ForeignKey(model, on_delete=models.PROTECT, **kw)
def optfk(model, **kw): return fk(model, null=True, blank=True, **kw)
class User(AbstractUser):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    email = models.EmailField(unique=True)
    name = models.CharField(max_length=160)
    version = models.PositiveIntegerField(default=1)
    def save(self, *a, **kw):
        self.email = self.email.strip().lower()
        self.username = self.email
        super().save(*a, **kw)
class Base(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    created_at = models.DateTimeField(default=timezone.now, db_index=True)
    class Meta: abstract = True
class Editable(Base):
    created_by = optfk(User, related_name='+')
    updated_by = optfk(User, related_name='+')
    updated_at = models.DateTimeField(auto_now=True)
    version = models.PositiveIntegerField(default=1)
    class Meta: abstract = True
class RuntimeState(models.Model):
    id = models.PositiveIntegerField(primary_key=True, default=1)
    opening_closed = models.BooleanField(default=False)
class Master(Editable):
    code = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=160)
    is_active = models.BooleanField(default=True)
    class Meta: abstract = True
class Item(Master):
    base_uom = models.CharField(max_length=16, choices=[(x,x) for x in ['EA','ML','G','KIT']])
    reorder_qty = Fixed6Field(default=0)
    class Meta: constraints = [models.CheckConstraint(condition=Q(reorder_qty__gte=0),name='item_reorder_nonnegative')]
class Supplier(Master):
    email = models.EmailField(blank=True)
    contact_name = models.CharField(max_length=160, blank=True)
class Warehouse(Master):
    location = models.TextField(blank=True)
class Project(Editable):
    code = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=160)
    owner = fk(User, related_name='owned_projects')
    status = models.CharField(max_length=32, default='DRAFT', db_index=True)
    start_date = models.DateField(null=True, blank=True)
    due_date = models.DateField(null=True, blank=True)
    budget_amount = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    members = models.ManyToManyField(User, through='ProjectMember')
class ProjectMember(Base):
    project = fk(Project)
    user = fk(User)
    class Meta: constraints = [models.UniqueConstraint(fields=['project','user'],name='project_member_unique')]
class Task(Editable):
    project = fk(Project, related_name='tasks')
    title = models.CharField(max_length=160)
    description = models.TextField(blank=True)
    assignee = optfk(User, related_name='tasks')
    status = models.CharField(max_length=32, default='TODO')
    priority = models.CharField(max_length=32, default='NORMAL')
    due_date = models.DateField(null=True, blank=True)
    blocked_reason = models.TextField(blank=True)
    class Meta: indexes = [models.Index(fields=['project','status','due_date'])]
class TaskComment(Base):
    task = fk(Task, related_name='comments')
    author = fk(User)
    body = models.TextField()
class PurchaseRequest(Editable):
    request_no = models.CharField(max_length=64, unique=True)
    project = optfk(Project)
    reason = models.TextField()
    status = models.CharField(max_length=32, default='DRAFT', db_index=True)
    submitted_at = models.DateTimeField(null=True)
    approved_by = optfk(User, related_name='+')
    approved_at = models.DateTimeField(null=True)
    decision_reason = models.TextField(blank=True)
class RequestLine(Base):
    request = fk(PurchaseRequest, related_name='lines')
    line_no = models.PositiveIntegerField()
    item = fk(Item)
    qty = Fixed6Field()
    needed_by = models.DateField()
    class Meta:
        constraints = [models.UniqueConstraint(fields=['request','line_no'],name='request_line_unique'),models.CheckConstraint(condition=Q(qty__gt=0),name='request_qty_positive')]
class PurchaseOrder(Editable):
    order_no = models.CharField(max_length=64, unique=True)
    supplier = fk(Supplier)
    status = models.CharField(max_length=32, default='DRAFT', db_index=True)
    ordered_at = models.DateTimeField(null=True)
class OrderLine(Base):
    order = fk(PurchaseOrder, related_name='lines')
    line_no = models.PositiveIntegerField()
    request_line = fk(RequestLine, related_name='order_lines')
    qty = Fixed6Field()
    unit_price = Fixed6Field()
    class Meta:
        constraints = [models.UniqueConstraint(fields=['order','line_no'],name='order_line_unique'),models.CheckConstraint(condition=Q(qty__gt=0)&Q(unit_price__gt=0),name='order_positive')]
class Receipt(Editable):
    receipt_no = models.CharField(max_length=64, unique=True)
    order = fk(PurchaseOrder, related_name='receipts')
    status = models.CharField(max_length=32, default='DRAFT')
    received_at = models.DateTimeField(default=timezone.now)
    posted_at = models.DateTimeField(null=True)
    posted_by = optfk(User, related_name='+')
class Batch(Editable):
    item = fk(Item, related_name='batches')
    batch_no = models.CharField(max_length=64)
    supplier_lot = models.CharField(max_length=128, blank=True)
    expires_on = models.DateField(null=True, blank=True, db_index=True)
    unit_cost = Fixed6Field()
    origin = models.CharField(max_length=32)
    class Meta:
        constraints = [models.UniqueConstraint(fields=['item','batch_no'],name='batch_unique'),models.CheckConstraint(condition=Q(unit_cost__gte=0),name='batch_cost_nonnegative')]
class ReceiptLine(Base):
    receipt = fk(Receipt, related_name='lines')
    line_no = models.PositiveIntegerField()
    order_line = fk(OrderLine, related_name='receipt_lines')
    batch = models.OneToOneField(Batch, on_delete=models.PROTECT)
    warehouse = fk(Warehouse)
    qty = Fixed6Field()
    class Meta:
        constraints = [models.UniqueConstraint(fields=['receipt','line_no'],name='receipt_line_unique'),models.CheckConstraint(condition=Q(qty__gt=0),name='receipt_qty_positive')]
class StockMovement(Editable):
    movement_no = models.CharField(max_length=64, unique=True)
    type = models.CharField(max_length=32)
    status = models.CharField(max_length=32, default='DRAFT')
    receipt = models.OneToOneField(Receipt, on_delete=models.PROTECT, null=True, blank=True, related_name='movement')
    reversal_of = models.OneToOneField('self', on_delete=models.PROTECT, null=True, blank=True, related_name='reversal')
    reason = models.TextField(blank=True)
    idempotency_key = models.CharField(max_length=128, unique=True, null=True, blank=True)
    request_hash = models.CharField(max_length=64, blank=True)
    posted_at = models.DateTimeField(null=True)
    posted_by = optfk(User, related_name='+')
class StockMovementLine(Base):
    movement = fk(StockMovement, related_name='lines')
    line_no = models.PositiveIntegerField()
    batch = fk(Batch, related_name='movement_lines')
    warehouse = fk(Warehouse)
    delta_qty = Fixed6Field()
    unit_cost = Fixed6Field()
    task = optfk(Task, related_name='movement_lines')
    receipt_line = optfk(ReceiptLine)
    reversal_of_line = models.OneToOneField('self', on_delete=models.PROTECT, null=True, blank=True, related_name='reversal')
    transfer_pair_no = models.PositiveIntegerField(null=True)
    class Meta:
        constraints = [models.UniqueConstraint(fields=['movement','line_no'],name='movement_line_unique'),models.CheckConstraint(condition=~Q(delta_qty=0)&Q(unit_cost__gte=0),name='movement_line_nonzero')]
        indexes = [models.Index(fields=['batch','warehouse'])]
class StockBalance(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    batch = fk(Batch, related_name='balances')
    warehouse = fk(Warehouse)
    on_hand_qty = Fixed6Field(default=0)
    version = models.PositiveIntegerField(default=1)
    updated_at = models.DateTimeField(auto_now=True)
    class Meta:
        constraints = [models.UniqueConstraint(fields=['batch','warehouse'],name='balance_unique'),models.CheckConstraint(condition=Q(on_hand_qty__gte=0),name='balance_nonnegative')]
class ImportJob(Editable):
    entity_type = models.CharField(max_length=32)
    mode = models.CharField(max_length=32)
    file_name = models.CharField(max_length=255)
    file_sha256 = models.CharField(max_length=64)
    status = models.CharField(max_length=32, default='UPLOADED')
    total_rows = models.PositiveIntegerField(default=0)
    success_rows = models.PositiveIntegerField(default=0)
    failed_rows = models.PositiveIntegerField(default=0)
    idempotency_key = models.CharField(max_length=128, unique=True)
    started_at = models.DateTimeField(null=True)
    finished_at = models.DateTimeField(null=True)
class ImportRow(Base):
    job = fk(ImportJob, related_name='rows')
    row_no = models.PositiveIntegerField()
    input_json = models.JSONField()
    normalized_json = models.JSONField(null=True)
    errors_json = models.JSONField(default=list)
    status = models.CharField(max_length=32, default='PENDING')
    result_id = models.UUIDField(null=True)
    expected_version = models.PositiveIntegerField(null=True)
    class Meta: constraints = [models.UniqueConstraint(fields=['job','row_no'],name='import_row_unique')]
class OutboxEvent(Base):
    event_type = models.CharField(max_length=64)
    transport = models.CharField(max_length=16, default='local')
    schema_version = models.PositiveIntegerField(default=1)
    aggregate_version = models.PositiveIntegerField(default=1)
    published_at = models.DateTimeField(null=True)
    lease_token = models.UUIDField(null=True)
    aggregate_type = models.CharField(max_length=64)
    aggregate_id = models.UUIDField()
    payload_json = models.JSONField(default=dict)
    # NULL denotes an unaudited legacy record; it must never imply a trusted hash.
    payload_hash = models.CharField(max_length=64, null=True, blank=True)
    dedupe_key = models.CharField(max_length=255, unique=True)
    status = models.CharField(max_length=32, default='PENDING')
    attempts = models.PositiveIntegerField(default=0)
    next_attempt_at = models.DateTimeField(default=timezone.now)
    locked_until = models.DateTimeField(null=True)
    last_error = models.TextField(blank=True)
    processed_at = models.DateTimeField(null=True)
    class Meta:
        indexes = [models.Index(fields=['status','next_attempt_at']),
                   models.Index(fields=['transport', 'status', 'next_attempt_at'], name='outbox_route_due_idx'),
                   models.Index(fields=['aggregate_type', 'aggregate_id', 'aggregate_version'], name='outbox_aggregate_version_idx'),
                   models.Index(fields=['created_at', 'id'], name='outbox_active_created_id_idx',
                       condition=Q(transport='kafka') & Q(status__in=['PENDING', 'PROCESSING']))]
        constraints = [models.UniqueConstraint(
            fields=['aggregate_type', 'aggregate_id', 'aggregate_version'],
            condition=Q(aggregate_type='stockmovement', event_type__in=[
                f'inventory.{kind}.posted' for kind in ['opening', 'receipt', 'issue', 'transfer', 'adjustment', 'reversal']]),
            name='inventory_aggregate_version_uniq')]
class Notification(Base):
    event = fk(OutboxEvent)
    user = fk(User)
    title = models.CharField(max_length=160)
    body = models.TextField()
    read_at = models.DateTimeField(null=True)
    class Meta:
        constraints = [models.UniqueConstraint(fields=['event','user'],name='notification_unique')]
        indexes = [models.Index(fields=['user','read_at'])]
class AuditEvent(Base):
    actor = optfk(User)
    entity_type = models.CharField(max_length=64)
    entity_id = models.UUIDField()
    action = models.CharField(max_length=64)
    before_json = models.JSONField(null=True)
    after_json = models.JSONField(null=True)
    request_id = models.CharField(max_length=64)
    reason = models.TextField(blank=True)
    class Meta: indexes = [models.Index(fields=['entity_type','entity_id','created_at'])]
class CommandResult(Base):
    key = models.CharField(max_length=128, unique=True)
    request_hash = models.CharField(max_length=64)
    result_json = models.JSONField()
class LoginAttempt(models.Model):
    key = models.CharField(max_length=64, primary_key=True)
    failures = models.PositiveIntegerField(default=0)
    blocked_until = models.DateTimeField(null=True)

class TestCatalog(Master):
    sample_type = models.CharField(max_length=32)
class LabOrder(Editable):
    order_no = models.CharField(max_length=64, unique=True)
    test = fk(TestCatalog)
    project = fk(Project)
    task = optfk(Task)
    status = models.CharField(max_length=32, default='OPEN')
class Sample(Editable):
    order = fk(LabOrder, related_name='samples')
    barcode = models.CharField(max_length=64, unique=True)
    status = models.CharField(max_length=32, default='REGISTERED')
    collected_at = models.DateTimeField(null=True)
    received_at = models.DateTimeField(null=True)
    storage_warehouse = optfk(Warehouse)
class SampleEvent(Base):
    sample = fk(Sample, related_name='events')
    actor = fk(User)
    from_status = models.CharField(max_length=32)
    to_status = models.CharField(max_length=32)
    reason = models.TextField(blank=True)
    occurred_at = models.DateTimeField(default=timezone.now)


class ProcessedEvent(Base):
    consumer_name = models.CharField(max_length=64)
    event_id = models.UUIDField()
    payload_hash = models.CharField(max_length=64, null=True, blank=True)
    class Meta:
        constraints = [models.UniqueConstraint(fields=['consumer_name', 'event_id'], name='consumer_event_unique')]

class InventoryProjection(models.Model):
    batch = fk(Batch)
    warehouse = fk(Warehouse)
    quantity = Fixed6Field(default=0)
    class Meta:
        constraints = [models.UniqueConstraint(fields=['batch', 'warehouse'], name='projection_balance_unique')]

class FailedDelivery(Base):
    consumer_name = models.CharField(max_length=64)
    delivery_key = models.CharField(max_length=600)
    source_cluster = models.CharField(max_length=128, default='legacy', blank=True)
    source_generation = models.CharField(max_length=128, default='legacy', blank=True)
    failure_class = models.CharField(max_length=32, default='transient')
    envelope = models.JSONField(default=dict)
    original_hash = models.CharField(max_length=64, null=True, blank=True)
    attempts = models.PositiveIntegerField(default=1)
    status = models.CharField(max_length=16, default='RETRY')
    next_attempt_at = models.DateTimeField(default=timezone.now)
    lease_token = models.UUIDField(null=True, blank=True)
    locked_until = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True)
    dlq_published_at = models.DateTimeField(null=True)
    dlq_lease_token = models.UUIDField(null=True, blank=True)
    dlq_locked_until = models.DateTimeField(null=True, blank=True)
    dlq_attempts = models.PositiveIntegerField(default=0)
    dlq_next_attempt_at = models.DateTimeField(default=timezone.now)
    resolved_at = models.DateTimeField(null=True)
    resolution_note = models.TextField(blank=True)
    class Meta:
        constraints = [models.UniqueConstraint(fields=['consumer_name', 'source_cluster', 'source_generation', 'delivery_key'], name='consumer_source_delivery_unique')]
        indexes = [models.Index(fields=['status', 'next_attempt_at']),
                   models.Index(fields=['status', 'dlq_published_at', 'dlq_next_attempt_at'], name='delivery_dlq_due_idx')]


class AppendOnlyAuditQuerySet(models.QuerySet):
    def update(self, **kwargs):
        raise ValueError('DeliveryAudit is append-only')
    def delete(self):
        raise ValueError('DeliveryAudit is append-only')
    def bulk_update(self, objs, fields, batch_size=None):
        raise ValueError('DeliveryAudit is append-only')
    def bulk_create(self, objs, batch_size=None, ignore_conflicts=False, update_conflicts=False,
                    update_fields=None, unique_fields=None):
        if update_conflicts:
            raise ValueError('DeliveryAudit is append-only')
        return super().bulk_create(objs, batch_size=batch_size, ignore_conflicts=ignore_conflicts,
                                   update_conflicts=False, update_fields=update_fields, unique_fields=unique_fields)


class DeliveryAudit(Base):
    """Append-only recovery evidence. PostgreSQL also enforces this with a trigger."""
    delivery = optfk(FailedDelivery, related_name='audit_entries')
    outbox = optfk(OutboxEvent, related_name='delivery_audit_entries')
    actor = optfk(User, related_name='delivery_audit_entries')
    actor_label = models.CharField(max_length=160, blank=True)
    action = models.CharField(max_length=64)
    outcome = models.CharField(max_length=32)
    reason = models.TextField(blank=True)
    before_json = models.JSONField(null=True, blank=True)
    after_json = models.JSONField(null=True, blank=True)
    original_hash = models.CharField(max_length=64, null=True, blank=True)
    authorization_json = models.JSONField(default=dict)
    objects = AppendOnlyAuditQuerySet.as_manager()
    class Meta:
        indexes = [models.Index(fields=['delivery', 'created_at'], name='delivery_audit_time_idx'),
                   models.Index(fields=['outbox', 'created_at'], name='outbox_audit_time_idx')]
    def save(self, *, force_insert=False, force_update=False, using=None, update_fields=None):
        if not self._state.adding or force_update or update_fields is not None:
            raise ValueError('DeliveryAudit is append-only')
        # A new Python object carrying an existing primary key must attempt an
        # INSERT and fail uniqueness, rather than Django's usual UPDATE fallback.
        return super().save(force_insert=True, using=using)
    def delete(self, *args, **kwargs):
        raise ValueError('DeliveryAudit is append-only')
