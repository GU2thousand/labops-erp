# Publisher ordinary class namespace comparison

The publisher admission policy rereads the complete native class namespace and
MRO closure for every record. This change affects only how
`_publisher_class_unchanged` compares those copied namespace facts. It leaves the
native reader, capability guards, construction consistency checks, and every
original descriptor, function, and field registration in place.

An entry proof uses captured native type/length/tuple/string/dictionary
capabilities and the native dictionary get method before any potentially changed
global builtin is called. Module and builtin dictionaries must have exact native
string keys. Their captured binding facts must agree. The reader must retain its
exact function identity, code, positional defaults, absent keyword defaults, and
empty native attribute dictionary. Its global dictionary, native type dictionary
and MRO descriptors, supported descriptor kinds, getter owner/slot/name, and
captured bound native getters must agree before its body is dispatched.

The unchanged owned reader establishes the current native tuple/pair shapes,
exact string keys, MRO shape, and bounds. The comparison therefore does not repeat
those current pair checks. Expected entries, MRO/item containers, and every
expected pair and key still receive exact native shape checks on every call;
neither a caller-supplied mapping proxy nor an unproved token is trusted. Names
agree in the same order, and values agree by object identity. Values are never
compared for equality or hashed. Every class is still reread on every record.
There is no cached admission decision or native-ID projection.

The current namespace still comes from the existing owned native type dictionary
descriptor. The reader verifies its actual mapping proxy, exact string keys,
native MRO shape, and existing bounds. The metaclass and all ordered MRO member
identities must agree before namespace comparison. Every ancestor and metaclass
entry remains part of the closure; matching one resolved descriptor is not
sufficient to admit a changed namespace.

Exactly the captured `BaseManager`, `Manager`, and `Field` objects retain the
former dictionary comparison and four bookkeeping rules. Their input pair and
key shapes are checked before dictionary construction. Creation counters retain
their exact integer types and captured bounds. The optional Manager slot cache
must still be absent or an exact empty list on every read, including when its
identity is unchanged. No similarly named entry on another class receives an
exception. This branch remains insensitive to native dictionary insertion order.

Ordinary key order is deliberately conservative. Deleting and reinserting a key
can preserve all names and value identities while moving its position. That
change selects the existing public per-record budget helper. Equal native string
names represented by different string objects remain compatible. This stricter
fallback adds no admitted extension and changes no public budget operation.
Restoring a value alone does not restore its former key position.

The existing helper/readiness fingerprints, native lookup/hash/equality checks,
early and final construction closure checks, complete descriptor rederivation,
global and function metadata checks, field-instance inspections, adapters,
tracing/context/signals/router checks, manager and wrapper checks, actual session
identity, dedicated ownership, lease/deadline, native acknowledgement, receipt,
writeback, and close behavior remain unchanged. There is no function factoring,
SQL, cross-record admission cache, resource, or topology change. Ordinary
inspection exceptions select fallback; `BaseException` controls propagate.
The checks retain their existing non-atomic limit for concurrent malicious
monkeypatching.

Ordinary comparisons avoid constructing two dictionaries and dispatching an
empty bookkeeping rule loop. The entry proof and expected pair/key validation
also add work. The
independent cost of dictionary allocation and the net cost of this change are
not established by source inspection. This algorithm makes no local speedup,
hosted capacity, or production performance prediction.

Focused tests cover native positive comparison, value identity without hooks,
name/presence/order facts, ancestor and metaclass coverage, malformed expected
and read shapes, exact bookkeeping owner compatibility, and the original global
policy's actual PostgreSQL public budget fallback and timeout restoration. They
do not send broker records or establish native acknowledgement performance.
Local correctness and metadata timings remain separate from the required hosted
correctness and fixed formal capacity gates.

The direct synthetic recheck now refuses an unknown reader replacement before
its body, even if a controlled Mock would return a well-shaped result or raise a
control signal. The original test method retains this explicit zero-call refusal
and exercises ordinary errors and exact control-object propagation inside the
actual owned reader with a bounded test-only trace injection. The former direct
capture seam is unchanged. Production helper/readiness guards remain active.
