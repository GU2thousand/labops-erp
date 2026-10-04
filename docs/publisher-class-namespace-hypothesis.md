# Publisher class namespace and MRO hypothesis

This change replaces only the repeated class-descriptor validation loop in the
ordinary publisher budget admission policy. It does not change publisher SQL,
the public budget helper, retained-session cleanup, deadlines, owner checks,
leases, broker acknowledgement, writeback, batch limits, topology or acceptance
thresholds. The prior budget-reuse hypothesis remains a separate documented
behavior change.

The sealed local metadata diagnostic at source `0a877e7` recorded 1,671 class
binding checks per admission, with 149 distinct descriptor values. Those values
are not interchangeable bindings: the original `(kind, name, descriptor)` list
is retained in full. Its Python 3.11.15 runtime reported 10 logical CPUs; physical
cores were unmeasured. Hosted validation uses Python 3.12/ARM64 and four logical
CPUs, and the local PostgreSQL test runtime uses Python 3.14. Unopened metadata
timing does not quantify real-session admission, database, publishing or online
capacity costs, and does not establish a numerical improvement from this change.

## Selected implementation

Before the first model metadata read or original static-inspector call, the
policy uses owned native type dictionary and MRO descriptors to inspect the
actual Outbox model and all class roots used by the original descriptor checks.
The closure includes every MRO member, its actual metaclass, and that metaclass's
MRO recursively. Classes are deduplicated by `id` with strong references, without
class hashing or equality. Namespace keys must be exact strings.

Unknown metaclass lookup, hashing, equality, `__getattr__`, `__dict__` or `__mro__`
shadowing is refused before the original inspector can encounter it. The actual
model's `_meta` value is derived from native copied class state; any metaclass
`_meta` binding is refused before a data descriptor could execute. The expected
value must be the ordinary Django `Options` instance type.

The native `__dict__` getset descriptor and `__mro__` descriptor have separate
exact type, owner, slot and reference checks. The MRO descriptor's actual kind is
captured and must be exactly the native getset or member descriptor type; its
actual native getter slot is pinned as well. Their captured native bound
getters are checked before dispatch. No regular `kind.__dict__`, `kind.__mro__`,
`.mro()` or unknown class getter is used to bootstrap the closure.

The owned inspector function, helper functions, native bound getters, sentinel,
type aliases and relevant algorithm module references are checked before any
original inspector invocation. Function code, defaults, keyword defaults and
attributes remain part of these checks. The Python 3.14 weakref cache wrapper and
its wrapped function are included. Changed inspector or private helper methods
are refused before invocation; the readiness function itself has a captured
callable/code/default gate in its callers. Ordinary capability failures select
the existing public path, and `BaseException` controls propagate unchanged.

The captured state is a tuple `(roots, entries)`. Every entry contains its class,
actual metaclass, ordered MRO members and a private copied tuple of namespace
key/value references. Expected state is not a live mappingproxy. Each record
rereads the complete actual namespaces and MROs, compares key presence and value
identity subject only to the four concrete Django bookkeeping rules below, and
compares ordered MRO member identities. Dictionary insertion order
and the identity of the MRO tuple do not determine admission.

Construction checks an initial and expanded closure, then rederives every
original descriptor binding from the final copied state and checks stability.
Any selected original binding that resolves through a declared bookkeeping
owner/name is refused, including inherited bindings; the exception cannot
replace a selected descriptor identity check.
A descriptor seen during mutation and then restored cannot be accepted unless
its captured result agrees with that final state. This consistency check is not
an atomic guarantee against arbitrary concurrent monkeypatching.

Bounds are 1,024 closure classes, 64 members in an MRO, 4,096 keys in any class
namespace and 65,536 copied keys across the closure. Unsupported shapes or bounds
select fallback before budget SQL. Ordinary capabilities on the supported
runtimes must pass positive validation rather than merely pass refusal tests.

## Preserved checks and stricter fallback

Exactly four concrete-class namespace entries use explicit ordinary Django
bookkeeping rules. `BaseManager.creation_counter` and `Field.creation_counter`
must remain present, exact native integers and greater than or equal to their
captured values. `Field.auto_creation_counter` must remain present, an exact
native integer and less than or equal to its captured value. These are bounds
relative to the capture, not a stored history proving that consecutive reads
never reverse direction. `Manager.__slotnames__` may be absent or an exact empty
native list. Its contents are checked every record, including when the list
identity is unchanged. Properties, boolean or integer subclasses, absent
counters, values beyond the opposite bound, nonempty cache lists and list
subclasses are refused without evaluating unknown getters or comparisons.
No similarly named entry on another class receives an exception. All other
namespace keys retain exact presence and value identity checks.

The immutable rule table, its exact concrete owner references, the relevant
Django module bindings and the new helper code/default/attribute fingerprints
are owned capabilities checked before helper dispatch. The native `int`
capability is included in the builtin identity gate. Neither admission nor
construction primes a cache through an unknown getter or refreshes the policy.

These rules address ordinary source paths rather than test-only state. Django
command checks read `Options.managers`, which copies managers; Python
`copyreg._slotnames` then caches an empty list on `Manager`. A root-owned
unopened metadata diagnostic at `20260929T111338Z-e9abc7` observed this one
namespace change after test-runner model checks, with no database calls and
unchanged source. The ordinary CLI follows the same manager-copy source chain.
The cache-insertion stack was not captured by that diagnostic; the copy chain
is a source-supported explanation of the observed delta, rather than a measured
insertion trace.
Separately, `events.owned_event().exists()` builds a `Value(1)` annotation;
its output-field resolution creates an `IntegerField`, advancing
`Field.creation_counter`. `BaseManager` construction and auto-created fields
update the other two counters. Those counter paths are source-confirmed;
the new real PostgreSQL ownership-query proof still requires root execution.

All original function fingerprint occurrences remain, including duplicates.
The sealed Python 3.11 baseline had 736 occurrences and 204 distinct functions;
the implementation does not deduplicate or cache those checks. All original
field-instance descriptor checks remain; that baseline had 133 checks over
19 fields. Driver adapters, actual physical session identity, tracing, callback,
manager, converter and instance override checks remain in the original path.
Additional private helper and inspector fingerprints protect the new machinery.

Namespaces outside those four rules are deliberately stricter than selected
descriptor equality.
Adding an unrelated class attribute, deleting a binding, adding a present `None`,
or rebinding the same inherited descriptor locally can now select the public
per-record budget path. Restoring the original key presence and value/MRO member
identities can re-admit the policy. The empty manager cache is checked by content;
other copied namespace values freeze references, not arbitrary mutable contents.
Retained function and field checks keep their
existing responsibility for the mutable state they inspect.

The Django test fixture restores the inherited `ensure_connection` binding on
its captured original namespace owner. It does not add an identical descriptor
to the PostgreSQL subclass. Only the identified Django test wrapper closure is
temporarily removed; unknown wrappers remain refused. Original wrapper objects,
class key absence and an identified bound instance cursor are restored exactly,
including exceptional fixture exits. The production policy has no test exemption
and is not refreshed to conceal test-framework mutations.

## Required evidence

Focused proofs cover native positive admission, actual PostgreSQL client/server
binding modes, class/ancestor/metaclass namespace and MRO changes, absent versus
present `None`, shared descriptor values at different bindings, restored MRO
members, unknown getters before and after construction, native capability and
inspector replacement, in-place code changes, exact control propagation, and
continued function/field validation. A reversible call observer verifies that
the original descriptor loop is absent while remaining inspector field checks
still execute. Synthetic helper tests isolate these state properties; actual
policy tests require a positive native producer baseline and no opened session.
Bookkeeping proofs retain the original global policy while ordinary manager and
field constructors advance counters; those ordinary advances remain in place.
Poisoned, missing or backwards counters,
other-class changes and cache replacement or in-place content mutations must
refuse before unknown callbacks and re-admit after exact restoration. A separate
PostgreSQL test executes two real ownership queries in each binding mode and
checks actual field-counter advancement and the unchanged global policy before,
between and after them. It does not send broker records or establish native ACK
or publication success; sustained real publication remains a hosted proof.

For this revised candidate, the implementation agents have performed only
source inspection and AST syntax checks. New affected/full PostgreSQL tests, a separately bounded local metadata
diagnostic, hosted gates, default RF3 correctness smoke and formal capacity
acceptance are pending root-owned execution. Existing failures and the sealed
baseline diagnostic remain unchanged. Passing local tests or metadata checks
alone does not qualify online capacity.

The first actual affected PostgreSQL run failed with 329 methods discovered and
run. Its ordinary admission fixtures failed because the initial policy had an
empty descriptor registry. A separate unopened Python 3.14 metadata diagnostic
confirmed the inspector provenance checks passed and identified one rejected
native predicate: that runtime's `type.__dict__['__mro__']` is a getset descriptor,
while the initial implementation required a member descriptor. The narrow
revision captures the actual native kind and getter slot without permitting an
unknown descriptor type or changing any other guard. Separate readonly-special
metaclass test construction errors were also corrected without dropping cases.
The failed run and diagnostic remain preserved; the revised implementation's
Python 3.11/3.12 compatibility and actual PostgreSQL results still require new
execution, and are not inferred from the diagnostic.

The next actual affected run discovered and ran 329 methods. The native
descriptor-kind correction made initial construction valid, but 13 original
static admission tests and two real binding subcases still refused the original
global policy. Fresh-policy construction tests passed. The preserved metadata
diagnostic identified the ordinary empty `Manager.__slotnames__` cache as the
global namespace difference. Rebuilding the policy or undoing that cache in the
test fixture would conceal an ordinary CLI transition; this revision instead
declares the four bounded bookkeeping rules above. No production test-wrapper
exemption, policy refresh, counter/cache reset in positive fixtures, SQL change or budget
lifecycle change is introduced.
