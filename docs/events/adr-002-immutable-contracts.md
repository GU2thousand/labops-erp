# ADR 002: Immutable whole-envelope JSON v1 identity

Status: accepted. Decision date: 2026-09-27.

Retain JSON inventory v1 and explicit validation at creation and consumption.
The six current types and their known fields are the complete first-release
contract. A consumer validates the envelope before creating a dedupe marker or
any database effect. Unsupported version/type and illegal fields are isolated
durably before offset acknowledgement.

## Canonical checksum

The checksum is SHA256 over the **whole retained JSON envelope** encoded with
sorted object keys, compact separators, UTF-8, `ensure_ascii=False` and
`allow_nan=False`. This is the project's documented Python JSON canonicalization,
not a claim of RFC 8785/JCS compliance. The hash includes event ID/type, aggregate,
versions, timestamp, every payload field and both trace carriers. Object key order
does not affect it; arrays, strings and all field values do. Transport source
coordinates outside the retained envelope are stored separately.

Store the original outbox hash and consumer marker hash. Same ID/same hash is a
duplicate. Same ID/different hash is a conflict that must be quarantined and
audited, never silently accepted as a duplicate. Retrying/replaying retains the
original ID and complete content, including original timestamp and trace fields.
Do not refresh tracing in the envelope, mutate recipients or normalize decimal
strings during a retry. A new compensation command produces a legitimate new
business event and links back to the original failure.

Legacy rows may begin with an explicitly unknown/null hash. Backfill each from
the retained original outbox envelope; a legacy marker without an original
outbox remains unaudited and is not upgraded into trustworthy content by guessing.
Migration failure/conflict counts need review before consumer cutover.

## Compatibility

Known optional fields in v1 are envelope `trace_context` and payload
`_trace_context`; fixtures cover presence and absence. Unknown new fields are
rejected. A producer cannot add arbitrary optional fields and claim old consumers
will ignore them. An additive field requires a reviewed consumer-first rollout
and expanded compatible contract/fixtures before producing it, or an explicitly
new schema/topic version.

Removing/renaming a field, changing meaning/precision, adding a required field,
or introducing a noncompatible event type requires v2 and an isolated shadow
consumer. Compare shadow projection/dedupe outcomes against the v1/ledger baseline,
retain both streams and freeze cutover watermarks before selecting v2. Do not
overwrite v1 messages or force old consumers to process v2.

Sensitive recipients, traces, bodies and raw poison bytes are permission-restricted
evidence. Metrics labels use bounded codes and roles, not raw values. Schema
Registry remains optional; this release uses checked-in schemas and fixtures.
