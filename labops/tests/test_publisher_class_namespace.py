"""Static class capability proofs; no broker records or database SQL.

Synthetic classes isolate namespace/MRO semantics. The production-policy
cases retain a real native producer and a positive admission baseline, so a
refusal cannot pass merely because an unrelated capability was unavailable.
"""
from contextlib import ExitStack
import inspect
import sys
from types import GetSetDescriptorType, MemberDescriptorType, ModuleType
from unittest.mock import Mock, patch

from confluent_kafka.cimpl import Producer
from django.db import connection
from django.db.backends.postgresql.base import DatabaseWrapper
from django.db.backends.postgresql import psycopg_any
from django.db.models import Field, IntegerField
from django.db.models.manager import BaseManager, Manager
from django.test import SimpleTestCase
import psycopg

from labops import events, worker_metrics
from labops.management.commands import publish_events as command
from labops.models import OutboxEvent
from labops.publisher_shards import PublisherShardOwner
from labops.tests.test_publisher_budget_reuse_admission import ordinary_connection_capability, plain_tracing
from labops.worker_metrics import OperationDeadlineExceeded, PublisherBudgetAdmission, StopController


class NamespaceControl(BaseException):
    pass


def forbidden_dispatch(*args, **kwargs):
    # A modified function must be refused before its code executes. A control
    # would escape ordinary fallback if this body were dispatched accidentally.
    raise BaseException('Modified capability must never be dispatched')


class PublisherClassNamespaceTests(SimpleTestCase):
    def capture(self, *roots):
        snapshot = worker_metrics._publisher_class_capture(roots)
        self.assertIsNotNone(snapshot)
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        return snapshot

    def test_plain_inherited_and_metaclass_resolution_has_complete_unique_closure(self):
        marker, meta_marker = object(), object()
        class Meta(type):
            from_meta = meta_marker
        class Base:
            inherited = marker
        class Child(Base, metaclass=Meta):
            local_none = None
        snapshot = self.capture(Child, Child)
        kinds = [entry[0] for entry in snapshot[1]]
        self.assertEqual(len(kinds), len({id(kind) for kind in kinds}))
        self.assertTrue(all(any(kind is expected for kind in kinds) for expected in (Child, Base, Meta, type, object)))
        for _ in range(3):
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
            self.assertIs(worker_metrics._publisher_class_resolve(snapshot, Child, 'inherited'), marker)
            self.assertIs(worker_metrics._publisher_class_resolve(snapshot, Child, 'from_meta'), meta_marker)
            self.assertIsNone(worker_metrics._publisher_class_resolve(snapshot, Child, 'local_none'))
            self.assertIs(worker_metrics._publisher_class_resolve(snapshot, Child, 'absent'), worker_metrics._PUBLISHER_CLASS_MISSING)

    def test_added_deleted_replaced_and_none_bindings_are_distinct_and_restore(self):
        original = object()
        class Root:
            value = original
        snapshot = self.capture(Root)
        with patch.object(Root, 'value', object()):
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        del Root.value
        try:
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        finally:
            Root.value = original
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        with patch.object(Root, 'new_none', None, create=True):
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
            fresh = self.capture(Root)
            self.assertIsNone(worker_metrics._publisher_class_resolve(fresh, Root, 'new_none'))
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)

    def test_same_inherited_descriptor_rebound_locally_is_new_presence(self):
        marker = object()
        class Base:
            value = marker
        class Root(Base):
            pass
        snapshot = self.capture(Root)
        self.assertNotIn('value', vars(Root))
        with patch.object(Root, 'value', marker):
            self.assertIs(inspect.getattr_static(Root, 'value'), marker)
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        self.assertNotIn('value', vars(Root))
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        # Reassigning an existing binding to the same object changes no fact.
        Base.value = marker
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)

    def test_ancestor_property_changes_are_refused_without_descriptor_getter(self):
        getter = Mock(side_effect=AssertionError('Descriptor getter must not run'))
        descriptor = property(lambda self: getter())
        class Base:
            value = descriptor
        class Root(Base):
            pass
        snapshot = self.capture(Root)
        self.assertIs(worker_metrics._publisher_class_resolve(snapshot, Root, 'value'), descriptor)
        with patch.object(Base, 'unrelated', descriptor, create=True):
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        with patch.object(Base, 'value', property(lambda self: getter())):
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        getter.assert_not_called()

    def test_changed_base_order_and_restored_fresh_mro_use_member_identities(self):
        marker = object()
        class Left:
            value = marker
        class Right:
            value = marker
        class Root(Left, Right):
            pass
        snapshot = self.capture(Root)
        original_bases, original_mro = Root.__bases__, Root.__mro__
        try:
            Root.__bases__ = (Right, Left)
            self.assertIs(inspect.getattr_static(Root, 'value'), marker)
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        finally:
            Root.__bases__ = original_bases
        self.assertIsNot(Root.__mro__, original_mro)
        self.assertTrue(all(actual is expected for actual, expected in zip(Root.__mro__, original_mro)))
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)

    def test_metaclass_ancestor_namespace_and_mro_changes_are_included(self):
        class MetaLeft(type):
            pass
        class MetaRight(type):
            pass
        class Meta(MetaLeft, MetaRight):
            pass
        class Root(metaclass=Meta):
            pass
        snapshot = self.capture(Root)
        with patch.object(MetaLeft, 'unrelated', None, create=True):
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        original_bases = Meta.__bases__
        try:
            Meta.__bases__ = (MetaRight, MetaLeft)
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        finally:
            Meta.__bases__ = original_bases
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)

    def test_metaclass_mro_property_before_capture_refuses_then_late_change_restores_absence(self):
        getter = Mock(side_effect=AssertionError('Metaclass MRO property must not run'))
        class PreexistingMeta(type):
            __mro__ = property(lambda self: getter())
        class PreexistingRoot(metaclass=PreexistingMeta):
            pass
        getter.assert_not_called()
        self.assertIsNone(worker_metrics._publisher_class_capture((PreexistingRoot,)))
        getter.assert_not_called()
        # Native type's __mro__ slot is readonly. Exercise a late inherited
        # shadow through compatible heap-metaclass bases, without writing it.
        class HeapNormalParent(type):
            pass
        class HeapMroTrapParent(type):
            __mro__ = property(lambda self: getter())
        class Meta(HeapNormalParent):
            pass
        class Root(metaclass=Meta):
            pass
        getter.assert_not_called()
        self.assertNotIn('__mro__', vars(Meta))
        snapshot = self.capture(Root)
        original_bases, original_mro = Meta.__bases__, Meta.__mro__
        try:
            Meta.__bases__ = (HeapMroTrapParent,)
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
            self.assertIsNone(worker_metrics._publisher_class_capture((Root,)))
            getter.assert_not_called()
        finally:
            Meta.__bases__ = original_bases
        self.assertIs(Meta.__bases__, original_bases)
        self.assertEqual(len(Meta.__mro__), len(original_mro))
        self.assertTrue(all(actual is expected for actual, expected in zip(Meta.__mro__, original_mro)))
        self.assertNotIn('__mro__', vars(Meta))
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        getter.assert_not_called()

    def test_unsupported_metaclass_hooks_before_capture_are_never_called(self):
        for name in ('__getattribute__', '__getattr__', '__hash__', '__eq__', '__dict__'):
            with self.subTest(hook=name):
                getter = Mock(side_effect=AssertionError('Unknown metaclass hook must not run'))
                value = property(lambda self: getter()) if name == '__dict__' else lambda *args: getter()
                meta = type('UnknownMeta', (type,), {name: value})
                root = meta('Root', (), {})
                self.assertIsNone(worker_metrics._publisher_class_capture((root,)))
                getter.assert_not_called()

    def test_new_metaclass_getters_after_capture_are_rejected_without_calls(self):
        class Meta(type):
            pass
        class Root(metaclass=Meta):
            pass
        snapshot = self.capture(Root)
        for name in ('__getattribute__', '__getattr__'):
            with self.subTest(hook=name):
                getter = Mock(side_effect=AssertionError('Changed metaclass hook must not run'))
                value = lambda *args: getter()
                with patch.object(Meta, name, value, create=True):
                    self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
                self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
                getter.assert_not_called()
        dict_getter = Mock(side_effect=AssertionError('Changed metaclass dictionary property must not run'))
        class HeapNormalParent(type):
            pass
        class HeapDictTrapParent(type):
            __dict__ = property(lambda self: dict_getter())
        class DictMeta(HeapNormalParent):
            pass
        class DictRoot(metaclass=DictMeta):
            pass
        dict_getter.assert_not_called()
        dict_snapshot = self.capture(DictRoot)
        original_bases, original_mro = DictMeta.__bases__, DictMeta.__mro__
        self.assertNotIn('__dict__', vars(DictMeta))
        try:
            DictMeta.__bases__ = (HeapDictTrapParent,)
            self.assertIs(worker_metrics._publisher_class_unchanged(dict_snapshot), False)
            self.assertIsNone(worker_metrics._publisher_class_capture((DictRoot,)))
            dict_getter.assert_not_called()
        finally:
            DictMeta.__bases__ = original_bases
        self.assertIs(DictMeta.__bases__, original_bases)
        self.assertEqual(len(DictMeta.__mro__), len(original_mro))
        self.assertTrue(all(actual is expected for actual, expected in zip(DictMeta.__mro__, original_mro)))
        self.assertNotIn('__dict__', vars(DictMeta))
        self.assertIs(worker_metrics._publisher_class_unchanged(dict_snapshot), True)
        dict_getter.assert_not_called()

    def test_namespace_values_are_compared_by_identity_without_equality_or_hash(self):
        calls = Mock(side_effect=AssertionError('Namespace values must not be compared or hashed'))
        class Value:
            def __eq__(self, other):
                return calls('equal')
            def __hash__(self):
                return calls('hash')
        value = Value()
        class Root:
            binding = value
        snapshot = self.capture(Root)
        with patch.object(Root, 'binding', Value()):
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), False)
        self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        calls.assert_not_called()

    def test_unsupported_root_and_oversized_namespace_refuse_without_attribute_access(self):
        getter = Mock(side_effect=AssertionError('Nonclass metadata must not be read'))
        class UnknownRoot:
            def __getattribute__(self, name):
                return getter(name)
        self.assertIsNone(worker_metrics._publisher_class_capture((UnknownRoot(),)))
        self.assertIsNone(worker_metrics._publisher_class_capture([]))
        getter.assert_not_called()
        root = type('LargeNamespace', (), {f'value_{index}': None for index in range(4097)})
        self.assertIsNone(worker_metrics._publisher_class_capture((root,)))

    def test_ordinary_read_errors_refuse_but_baseexception_preserves_identity(self):
        class Root:
            pass
        snapshot = self.capture(Root)
        for operation, expected in (
                (lambda: worker_metrics._publisher_class_capture((Root,)), None),
                (lambda: worker_metrics._publisher_class_unchanged(snapshot), False)):
            for kind in (RuntimeError, AttributeError):
                with self.subTest(operation=expected, ordinary=kind.__name__):
                    reader = Mock(side_effect=kind('PRIVATE read failure'))
                    with patch.object(worker_metrics, '_publisher_class_read', reader):
                        self.assertIs(operation(), expected)
                    reader.assert_called_once()
            for kind in (NamespaceControl, OperationDeadlineExceeded):
                with self.subTest(operation=expected, control=kind.__name__):
                    first = kind('PRIVATE control')
                    reader = Mock(side_effect=first)
                    with patch.object(worker_metrics, '_publisher_class_read', reader), self.assertRaises(kind) as raised:
                        operation()
                    self.assertIs(raised.exception, first)
                    reader.assert_called_once()

    def test_direct_capture_resolve_and_recheck_dispatch_no_static_inspector(self):
        marker = object()
        class Root:
            value = marker
        inspector = Mock(side_effect=NamespaceControl('Static inspector must not be dispatched'))
        with patch.object(inspect, 'getattr_static', inspector):
            snapshot = self.capture(Root)
            self.assertIs(worker_metrics._publisher_class_resolve(snapshot, Root, 'value'), marker)
            self.assertIs(worker_metrics._publisher_class_unchanged(snapshot), True)
        inspector.assert_not_called()


class PublisherClassNamespaceProductionTests(SimpleTestCase):
    def setUp(self):
        super().setUp()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(ordinary_connection_capability())
        self.stop = self.stack.enter_context(StopController())
        self.stack.enter_context(plain_tracing())
        configuration = dict(connection.settings_dict)
        configuration.update(ENGINE='django.db.backends.postgresql', OPTIONS={}, AUTOCOMMIT=True)
        self.database = DatabaseWrapper(configuration, alias='default')
        self.stack.enter_context(patch('django.db.connections', {'default': self.database}))
        self.broker, self.owner = events.producer(), PublisherShardOwner(0, 1)
        self.assertIs(type(self.broker.client), Producer)
        self.policy = PublisherBudgetAdmission(command._budget_aliases())
        self.assertIs(self.policy.valid, True)
        self.assertIs(self.plain(), True, 'Native policy positive baseline is required')
        self.assertIsNone(self.database.connection)

    def plain(self):
        return self.policy.plain(self.broker, self.owner, command._budget_aliases(), self.stop)

    def refused(self):
        self.assertIs(self.plain(), False)
        self.assertIsNone(self.database.connection)

    def invalid_constructor(self):
        aliases = command._budget_aliases()
        changed = PublisherBudgetAdmission(aliases)
        self.assertIs(changed.valid, False)
        self.assertIs(changed.aliases, aliases)
        self.assertIs(changed.plain(self.broker, self.owner, aliases, self.stop), False)
        self.assertIsNone(self.database.connection)
        return changed

    def test_unchanged_native_policy_repeatedly_admits_without_opening_session(self):
        mro = type.__dict__['__mro__']
        kind = type(mro)
        self.assertTrue(kind is GetSetDescriptorType or kind is MemberDescriptorType)
        self.assertIs(worker_metrics._PUBLISHER_TYPE_MRO, mro)
        self.assertIs(worker_metrics._PUBLISHER_TYPE_MRO_KIND, kind)
        self.assertIs(worker_metrics._PUBLISHER_TYPE_MRO_SLOT, kind.__dict__['__get__'])
        for _ in range(3):
            self.assertIs(worker_metrics._publisher_namespace_ready(), True)
            self.assertIs(self.plain(), True)
            self.assertIsNone(self.database.connection)

    def test_real_manager_and_field_construction_keeps_original_global_admission(self):
        global_policy = command._BUDGET_ADMISSION
        self.assertIsNotNone(global_policy)
        self.assertIs(global_policy.valid, True)
        self.assertIs(global_policy.plain(self.broker, self.owner, command._budget_aliases(), self.stop), True)
        counters = ((BaseManager, 'creation_counter'), (Field, 'creation_counter'), (Field, 'auto_creation_counter'))
        originals = [(owner, name, vars(owner)[name]) for owner, name in counters]
        manager, field, automatic = Manager(), IntegerField(), IntegerField(auto_created=True)
        self.assertIs(type(manager), Manager)
        self.assertIs(type(field), IntegerField)
        self.assertIs(type(automatic), IntegerField)
        self.assertEqual(vars(BaseManager)['creation_counter'], originals[0][2] + 1)
        self.assertEqual(vars(Field)['creation_counter'], originals[1][2] + 1)
        self.assertEqual(vars(Field)['auto_creation_counter'], originals[2][2] - 1)
        # Real constructors leave their normal bookkeeping advances in place;
        # the same captured policies must keep admitting without a reset.
        self.assertIs(command._BUDGET_ADMISSION, global_policy)
        self.assertIs(global_policy.plain(self.broker, self.owner, command._budget_aliases(), self.stop), True)
        self.assertIs(self.plain(), True)
        self.assertIsNone(self.database.connection)

    def test_counter_poison_and_deletion_refuse_without_getters_or_numeric_hooks(self):
        counters = ((BaseManager, 'creation_counter'), (Field, 'creation_counter'), (Field, 'auto_creation_counter'))
        for owner, name in counters:
            with self.subTest(owner=owner.__name__, counter=name):
                original = vars(owner)[name]
                calls = Mock(side_effect=NamespaceControl('Counter extension must not run'))
                class UnknownInt(int):
                    def __lt__(self, other):
                        return calls('less')
                    def __gt__(self, other):
                        return calls('greater')
                    def __int__(self):
                        return calls('int')
                for label, value in (('bool', True), ('integer_subclass', UnknownInt(original)),
                        ('property', property(lambda self: calls('get')))):
                    with self.subTest(poison=label), patch.object(owner, name, value):
                        self.refused()
                        self.invalid_constructor()
                    self.assertIs(vars(owner)[name], original)
                    self.assertIs(self.plain(), True)
                try:
                    delattr(owner, name)
                    self.refused()
                    self.invalid_constructor()
                finally:
                    setattr(owner, name, original)
                calls.assert_not_called()
                self.assertIs(vars(owner)[name], original)
                self.assertIs(self.plain(), True)

    def test_counter_wrong_direction_relative_to_captured_value_refuses_and_restores(self):
        for owner, name, direction in ((BaseManager, 'creation_counter', 1),
                (Field, 'creation_counter', 1), (Field, 'auto_creation_counter', -1)):
            with self.subTest(owner=owner.__name__, counter=name):
                entry = next(entry for entry in self.policy.class_namespaces[1] if entry[0] is owner)
                captured = next(value for key, value in entry[3] if key == name)
                original = vars(owner)[name]
                self.assertIs(type(captured), int)
                with patch.object(owner, name, captured - direction):
                    self.refused()
                self.assertIs(vars(owner)[name], original)
                self.assertIs(self.plain(), True)

    def test_same_named_counters_on_other_concrete_classes_remain_namespace_changes(self):
        for owner in (Manager, IntegerField):
            with self.subTest(owner=owner.__name__):
                self.assertNotIn('creation_counter', vars(owner))
                inherited = inspect.getattr_static(owner, 'creation_counter')
                calls = Mock(side_effect=NamespaceControl('Other-class counter property must not run'))
                for label, value in (('same_integer', inherited), ('property', property(lambda self: calls()))):
                    with self.subTest(binding=label), patch.object(owner, 'creation_counter', value):
                        self.refused()
                    self.assertNotIn('creation_counter', vars(owner))
                    self.assertIs(self.plain(), True)
                calls.assert_not_called()

    def test_optional_empty_slot_cache_and_same_identity_content_are_checked_each_record(self):
        present = '__slotnames__' in vars(Manager)
        original = vars(Manager).get('__slotnames__')
        cache = []
        try:
            setattr(Manager, '__slotnames__', cache)
            self.assertIs(self.plain(), True)
            aliases = command._budget_aliases()
            captured = PublisherBudgetAdmission(aliases)
            self.assertIs(captured.valid, True)
            self.assertIs(worker_metrics._publisher_class_resolve(captured.class_namespaces, Manager, '__slotnames__'), cache)
            self.assertIs(captured.plain(self.broker, self.owner, aliases, self.stop), True)
            cache.append('unexpected_slot')
            self.assertIs(vars(Manager)['__slotnames__'], cache)
            self.refused()
            self.assertIs(captured.plain(self.broker, self.owner, aliases, self.stop), False)
            cache.clear()
            self.assertIs(captured.plain(self.broker, self.owner, aliases, self.stop), True)
            self.assertIs(self.plain(), True)
            setattr(Manager, '__slotnames__', [])
            self.assertIs(captured.plain(self.broker, self.owner, aliases, self.stop), True)
            self.assertIs(self.plain(), True)
            delattr(Manager, '__slotnames__')
            self.assertIs(captured.plain(self.broker, self.owner, aliases, self.stop), True)
            self.assertIs(self.plain(), True)
        finally:
            if present:
                setattr(Manager, '__slotnames__', original)
            elif '__slotnames__' in vars(Manager):
                delattr(Manager, '__slotnames__')
        self.assertEqual('__slotnames__' in vars(Manager), present)
        if present:
            self.assertIs(vars(Manager)['__slotnames__'], original)
        self.assertIs(self.plain(), True)

    def test_slot_cache_nonempty_custom_type_and_property_refuse_without_hooks(self):
        calls = Mock(side_effect=NamespaceControl('Slot cache extension must not run'))
        class UnknownList(list):
            def __len__(self):
                return calls('length')
            def __iter__(self):
                return calls('iterate')
            def __bool__(self):
                return calls('truth')
        present, original = '__slotnames__' in vars(Manager), vars(Manager).get('__slotnames__')
        for label, value in (('nonempty', ['slot']), ('tuple', ()), ('custom_list', UnknownList()),
                ('property', property(lambda self: calls('get')))):
            with self.subTest(cache=label), patch.object(Manager, '__slotnames__', value, create=True):
                self.refused()
                self.invalid_constructor()
            self.assertEqual('__slotnames__' in vars(Manager), present)
            if present:
                self.assertIs(vars(Manager)['__slotnames__'], original)
            self.assertIs(self.plain(), True)
        calls.assert_not_called()

    def test_bookkeeping_registry_is_exact_and_inherited_descriptor_intersections_are_known(self):
        expected = ((BaseManager, 'creation_counter', 1), (Field, 'creation_counter', 1),
            (Field, 'auto_creation_counter', -1), (Manager, '__slotnames__', 0))
        rules = worker_metrics._PUBLISHER_BOOKKEEPING_REFS
        self.assertIs(type(rules), tuple)
        self.assertEqual(len(rules), 4)
        for actual, required in zip(rules, expected):
            self.assertIs(type(actual), tuple)
            self.assertIs(actual[0], required[0])
            self.assertEqual(actual[1:], required[1:])
        snapshot = worker_metrics._publisher_class_capture((Manager, IntegerField))
        self.assertIsNotNone(snapshot)
        for kind, name in ((Manager, 'creation_counter'), (IntegerField, 'creation_counter'),
                (IntegerField, 'auto_creation_counter'), (Manager, '__slotnames__')):
            self.assertIs(worker_metrics._publisher_binding_is_bookkeeping(snapshot, kind, name), True)
        self.assertIs(worker_metrics._publisher_binding_is_bookkeeping(snapshot, Manager, 'get_queryset'), False)
        class Other:
            creation_counter = 0
        other = worker_metrics._publisher_class_capture((Other,))
        self.assertIsNotNone(other)
        self.assertIs(worker_metrics._publisher_binding_is_bookkeeping(other, Other, 'creation_counter'), False)
        with patch.object(Other, 'creation_counter', 1):
            self.assertIs(worker_metrics._publisher_class_unchanged(other), False)
        self.assertIs(worker_metrics._publisher_class_unchanged(other), True)
        for kind, name, _ in self.policy.descriptors:
            self.assertIs(worker_metrics._publisher_binding_is_bookkeeping(self.policy.class_namespaces, kind, name), False)

    def test_constructor_rejects_inherited_bookkeeping_registry_binding_with_matching_native_value(self):
        capture_code = worker_metrics._publisher_class_capture.__code__
        constructor_code = PublisherBudgetAdmission.__init__.__code__
        previous, calls, injected, returned, active = sys.getprofile(), [], [], [], []
        def observe(frame, event, argument):
            if frame.f_code is capture_code:
                caller = frame.f_back
                if event == 'call' and caller is not None and caller.f_code is constructor_code:
                    policy = caller.f_locals['self']
                    calls.append(policy)
                    if len(calls) == 3:
                        registry = policy.descriptors
                        original_registry = tuple(registry)
                        binding = (Manager, 'creation_counter', vars(BaseManager)['creation_counter'])
                        injected.append((policy, registry, original_registry, binding))
                        registry.append(binding)
                        active.append(frame)
                elif event == 'return' and active and frame is active[0]:
                    returned.append(argument)
                    active.clear()
            if previous is not None:
                previous(frame, event, argument)
        aliases = command._budget_aliases()
        try:
            try:
                sys.setprofile(observe)
                changed = PublisherBudgetAdmission(aliases)
            finally:
                sys.setprofile(previous)
            self.assertIs(sys.getprofile(), previous)
            self.assertEqual(len(calls), 3)
            self.assertEqual(len(injected), 1)
            self.assertEqual(len(returned), 1)
            policy, registry, _, binding = injected[0]
            self.assertIs(policy, changed)
            self.assertIs(changed.descriptors, registry)
            self.assertIs(registry[-1], binding)
            final = returned[0]
            self.assertIsNotNone(final)
            self.assertIs(worker_metrics._publisher_binding_is_bookkeeping(final, Manager, 'creation_counter'), True)
            self.assertIs(worker_metrics._publisher_class_resolve(final, Manager, 'creation_counter'), binding[2])
            self.assertIs(changed.valid, False)
            self.assertIs(changed.aliases, aliases)
            self.assertIs(changed.plain(self.broker, self.owner, aliases, self.stop), False)
            self.assertIs(self.plain(), True)
            self.assertIsNone(self.database.connection)
        finally:
            # Restore the private test registry even if a control interrupts
            # capture or an assertion; class namespaces never change here.
            for _, registry, original_registry, _ in injected:
                registry[:] = original_registry

    def test_original_inspector_runtime_calls_keep_every_field_binding_without_class_loop(self):
        original = inspect.getattr_static
        code, calls = original.__code__, []
        previous = sys.getprofile()
        def observe(frame, event, argument):
            if event == 'call' and frame.f_code is code:
                calls.append((frame.f_locals.get('obj'), frame.f_locals.get('attr')))
            if previous is not None:
                previous(frame, event, argument)
        try:
            sys.setprofile(observe)
            admitted = self.plain()
        finally:
            sys.setprofile(previous)
        self.assertIs(sys.getprofile(), previous)
        self.assertIs(admitted, True)
        self.assertEqual(len(self.policy.field_descriptors), 133)
        for field, name, _ in self.policy.field_descriptors:
            self.assertTrue(any(obj is field and attribute == name for obj, attribute in calls), name)
        self.assertLess(len(calls), len(self.policy.descriptors))
        self.assertIsNone(self.database.connection)
        business_code = events.send.__code__
        try:
            events.send.__code__ = forbidden_dispatch.__code__
            self.refused()
        finally:
            events.send.__code__ = business_code
        self.assertIs(self.plain(), True)

    def test_transient_inspector_capture_mismatch_refuses_after_exact_namespace_restoration(self):
        entries = self.policy.class_namespaces[1]
        wrapper_entry = next(entry for entry in entries if entry[0] is DatabaseWrapper)
        owner = next(ancestor for ancestor in wrapper_entry[2]
            if any(name == 'cursor' for name, _ in next(entry[3] for entry in entries if entry[0] is ancestor)))
        original = next(value for name, value in next(entry[3] for entry in entries if entry[0] is owner)
            if name == 'cursor')
        wrong = worker_metrics._publisher_class_resolve(self.policy.class_namespaces, DatabaseWrapper, 'close')
        self.assertIs(vars(owner)['cursor'], original)
        self.assertIsNot(wrong, original)
        inspector_code, previous = inspect.getattr_static.__code__, sys.getprofile()
        calls, returned, active = [], [], []
        def observe(frame, event, argument):
            if frame.f_code is inspector_code:
                if (event == 'call' and frame.f_locals.get('obj') is DatabaseWrapper
                        and frame.f_locals.get('attr') == 'cursor'):
                    calls.append(frame)
                    if len(calls) == 1:
                        active.append(frame)
                        setattr(owner, 'cursor', wrong)
                elif event == 'return' and active and frame is active[0]:
                    returned.append(argument)
                    setattr(owner, 'cursor', original)
                    active.clear()
            if previous is not None:
                previous(frame, event, argument)
        aliases = command._budget_aliases()
        try:
            sys.setprofile(observe)
            changed = PublisherBudgetAdmission(aliases)
        finally:
            try:
                sys.setprofile(previous)
            finally:
                # A control/error can arrive before the matching return hook.
                setattr(owner, 'cursor', original)
        self.assertIs(sys.getprofile(), previous)
        self.assertIs(vars(owner)['cursor'], original)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(returned), 1)
        self.assertIs(returned[0], wrong)
        self.assertTrue(any(kind is DatabaseWrapper and name == 'cursor' and value is wrong
            for kind, name, value in changed.descriptors))
        self.assertIs(changed.valid, False)
        self.assertIs(changed.aliases, aliases)
        self.assertIs(changed.plain(self.broker, self.owner, aliases, self.stop), False)
        self.assertIs(self.plain(), True)
        self.assertIsNone(self.database.connection)

    def test_inspect_and_types_module_subclasses_refuse_before_dict_getter(self):
        for module in (inspect, inspect.types, worker_metrics._publisher_field_module,
                worker_metrics._publisher_manager_module):
            with self.subTest(module=module.__name__):
                getter = Mock(side_effect=NamespaceControl('Unknown module dictionary getter must not run'))
                class UnknownModule(ModuleType):
                    __dict__ = property(lambda self: getter())
                original_class = type(module)
                try:
                    module.__class__ = UnknownModule
                    self.refused()
                    self.invalid_constructor()
                finally:
                    module.__class__ = original_class
                getter.assert_not_called()
                self.assertIs(self.plain(), True)

    def test_native_builtin_resolution_shadows_are_refused_before_unknown_calls(self):
        for name in ('type', 'id', 'int'):
            with self.subTest(builtin=name):
                self.assertNotIn(name, vars(worker_metrics))
                extension = Mock(side_effect=NamespaceControl('Shadowed builtin must never be dispatched'))
                with patch.object(worker_metrics, name, extension, create=True):
                    self.refused()
                    self.invalid_constructor()
                extension.assert_not_called()
                self.assertNotIn(name, vars(worker_metrics))
                self.assertIs(self.plain(), True)

    def test_driver_class_aliases_before_policy_init_refuse_without_custom_metaclass_calls(self):
        for module, name in ((psycopg, 'Connection'), (psycopg_any, 'BaseTzLoader')):
            with self.subTest(module=module.__name__, alias=name):
                getter = Mock(side_effect=NamespaceControl('Unknown driver metaclass must not run'))
                class UnknownMeta(type):
                    def __getattribute__(self, attribute):
                        return getter(attribute)
                unknown = UnknownMeta('UnknownDriverClass', (), {})
                getter.assert_not_called()
                with patch.object(module, name, unknown):
                    changed = PublisherBudgetAdmission(command._budget_aliases())
                    self.assertIs(changed.valid, False)
                    self.assertIs(changed.plain(self.broker, self.owner, command._budget_aliases(), self.stop), False)
                    self.assertIsNone(self.database.connection)
                getter.assert_not_called()
                self.assertIs(self.plain(), True)

    def test_modelbase_meta_property_before_policy_init_never_executes_and_restores_absence(self):
        meta = type(OutboxEvent)
        self.assertNotIn('_meta', vars(meta))
        getter = Mock(side_effect=NamespaceControl('ModelBase metadata getter must never run'))
        with patch.object(meta, '_meta', property(lambda self: getter()), create=True):
            changed = PublisherBudgetAdmission(command._budget_aliases())
            self.assertIs(changed.valid, False)
            self.assertIs(changed.plain(self.broker, self.owner, command._budget_aliases(), self.stop), False)
            self.refused()
        getter.assert_not_called()
        self.assertNotIn('_meta', vars(meta))
        self.assertIs(self.plain(), True)
        self.assertIsNone(self.database.connection)

    def test_unrelated_native_class_namespace_change_is_conservative_refusal_then_restores(self):
        getter = Mock(side_effect=NamespaceControl('Unrelated class property must never run'))
        with patch.object(DatabaseWrapper, '_publisher_unrelated', property(lambda self: getter()), create=True):
            self.refused()
        getter.assert_not_called()
        self.assertNotIn('_publisher_unrelated', vars(DatabaseWrapper))
        self.assertIs(self.plain(), True)

    def test_replaced_helper_or_ready_guard_is_refused_before_dispatch_and_constructor_use(self):
        for name in ('_publisher_class_read', '_publisher_class_resolve', '_publisher_bookkeeping_names',
                '_publisher_binding_is_bookkeeping', '_publisher_class_capture', '_publisher_class_unchanged', '_publisher_namespace_ready'):
            with self.subTest(helper=name):
                extension = Mock(side_effect=NamespaceControl('Unknown helper must not run'))
                with patch.object(worker_metrics, name, extension):
                    self.refused()
                    self.invalid_constructor()
                extension.assert_not_called()
                self.assertIs(self.plain(), True)
        for name in ('_PUBLISHER_TYPE_DICT_GET', '_PUBLISHER_TYPE_MRO_GET',
                '_PUBLISHER_TYPE_MRO_KIND', '_PUBLISHER_TYPE_MRO_SLOT', '_PUBLISHER_BOOKKEEPING_REFS',
                '_PUBLISHER_BASE_MANAGER', '_PUBLISHER_MANAGER', '_PUBLISHER_FIELD'):
            with self.subTest(native=name):
                extension = Mock(side_effect=NamespaceControl('Unknown native capability must not run'))
                with patch.object(worker_metrics, name, extension):
                    self.refused()
                    self.invalid_constructor()
                extension.assert_not_called()
                self.assertIs(self.plain(), True)

    def test_original_inspector_alias_code_defaults_and_metadata_mutations_are_refused(self):
        original = inspect.getattr_static
        replacement = Mock(side_effect=NamespaceControl('Unknown static inspector must not run'))
        with patch.object(inspect, 'getattr_static', replacement):
            self.refused()
            self.invalid_constructor()
        replacement.assert_not_called()
        code, defaults, attributes = original.__code__, original.__defaults__, dict(original.__dict__)
        try:
            original.__code__ = forbidden_dispatch.__code__
            self.refused()
            self.invalid_constructor()
        finally:
            original.__code__ = code
        self.assertIs(self.plain(), True)
        try:
            original.__defaults__ = (object(),)
            self.refused()
        finally:
            original.__defaults__ = defaults
        self.assertIs(self.plain(), True)
        try:
            original.__dict__['_publisher_test_extension'] = object()
            self.refused()
        finally:
            original.__dict__.clear()
            original.__dict__.update(attributes)
        self.assertIs(self.plain(), True)

    def test_helper_in_place_code_and_unchanged_business_fingerprint_guards_remain_active(self):
        for function in (worker_metrics._publisher_class_unchanged, worker_metrics._publisher_bookkeeping_names,
                worker_metrics._publisher_binding_is_bookkeeping, worker_metrics._publisher_namespace_ready, events.send):
            with self.subTest(function=function.__name__):
                code = function.__code__
                try:
                    function.__code__ = forbidden_dispatch.__code__
                    self.refused()
                finally:
                    function.__code__ = code
                self.assertIs(self.plain(), True)
        self.assertIsNone(self.database.connection)
