"""Frozen fault-workload qualification; no clocks, I/O, or mutable state.

The full profile uses an explicitly requested denominator of at least 30,000,
50 commands/second, and a generation window no more than 5% late. Every input
and completed-command count must equal that exact predeclared denominator.
The lower measured-rate bound follows the same window rule (50 / 1.05); the
upper pacing bound is 50 * 1.05. Smoke proves its own declared input coverage,
but never qualifies capacity, regardless of its size or timing.
"""
from collections.abc import Mapping
import math

FULL_MINIMUM_EVENTS = 30000
FULL_TARGET_RATE = 50.0
GENERATION_TOLERANCE = .05
RATE_UPPER_TOLERANCE = .05
_MEASUREMENT_REL_TOLERANCE = 1e-9
_ARITHMETIC_COMPARISON_ULPS = 4


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        return value if math.isfinite(value) else None
    except OverflowError:
        return None


def _count(value):
    return value if type(value) is int and value >= 0 and _number(value) is not None else None


def _positive(value):
    number = _number(value)
    return number if number is not None and number > 0 else None


def _nonnegative(value):
    number = _number(value)
    return number if number is not None and number >= 0 else None


def _same_measurement(reported, recomputed):
    return (reported is not None and recomputed is not None
            and math.isclose(reported, recomputed, rel_tol=_MEASUREMENT_REL_TOLERANCE,
                             abs_tol=_MEASUREMENT_REL_TOLERANCE))


def _invalid_display(value):
    try:
        return str(value)[:120]
    except (ValueError, OverflowError, RecursionError):
        # Extremely large Python integers can exceed its text conversion
        # limit; malformed observations must still produce failure evidence.
        return '[unrepresentable scalar]'


def _at_most(value, bound, *operand_scales):
    if value is None or bound is None:
        return False
    if value <= bound:
        return True
    # Subtracting elapsed - expected amplifies rounding at the ~600-second
    # operand scale. This allows representation equality, not another rate or
    # duration percentage. The frozen 5% thresholds remain unchanged.
    rounding = _ARITHMETIC_COMPARISON_ULPS * max(
        math.ulp(float(number)) for number in (value, bound, *operand_scales) if number is not None)
    return value - bound <= rounding


def qualify_fault_workload(workload, *, tier, requested_events, configured_rate,
                           actual_input_count, requested_min_outage_seconds,
                           measured_outage_seconds):
    """Return JSON-safe evidence; invalid observations fail without being hidden.

    ``workload`` is the untouched result of ``Harness.generate``. The separate
    ``actual_input_count`` comes from the retained IDs, and cannot be substituted
    by the generator's self-reported count. Measured outage covers generation
    and is checked independently against its requested minimum. In particular,
    a 300-second minimum is not described as an exact 300-second outage when the
    30,000-command generation itself takes approximately 600 seconds.

    Callers persist this result before raising on ``passed=False``. Tolerances
    have no caller override. Smoke can pass coverage/measurement checks while
    ``capacity_qualified`` always remains false.
    """
    valid_workload = isinstance(workload, Mapping)
    raw = workload if valid_workload else {}
    requested_count = _count(requested_events)
    requested_rate = _positive(configured_rate)
    minimum_outage = _nonnegative(requested_min_outage_seconds)
    observed_count = _count(actual_input_count)
    reported_count = _count(raw.get('input'))
    completed_count = _count(raw.get('completed_commands'))
    elapsed = _positive(raw.get('elapsed_seconds'))
    target_rate = _positive(raw.get('target_rate'))
    measured_rate = _positive(raw.get('actual_command_rate'))
    lateness = _nonnegative(raw.get('schedule_lateness_seconds'))
    outage = _nonnegative(measured_outage_seconds)
    recomputed_rate = _number(observed_count / elapsed) if observed_count is not None and elapsed else None
    expected = _number(requested_count / requested_rate) if requested_count is not None and requested_rate else None
    recomputed_lateness = max(0, elapsed - expected) if elapsed is not None and expected is not None else None
    full_expected = _number(requested_count / FULL_TARGET_RATE) if requested_count is not None else None
    full_maximum = _number(full_expected * (1 + GENERATION_TOLERANCE)) if full_expected is not None else None
    full_lateness = _number(full_expected * GENERATION_TOLERANCE) if full_expected is not None else None

    checks = {
        'valid_tier': isinstance(tier, str) and tier in {'full', 'smoke'},
        'valid_workload_object': valid_workload,
        'valid_requested_events': requested_count is not None and requested_count > 0,
        'valid_configured_rate': requested_rate is not None,
        'exact_input_and_completed_denominator': (requested_count is not None and requested_count > 0
            and observed_count == reported_count == completed_count == requested_count),
        'finite_generation_elapsed_seconds': elapsed is not None,
        'finite_actual_command_rate': measured_rate is not None,
        'finite_schedule_lateness_seconds': lateness is not None,
        'reported_target_rate_matches_configured': target_rate is not None and target_rate == requested_rate,
        'reported_rate_matches_recomputed': _same_measurement(measured_rate, recomputed_rate),
        'reported_lateness_matches_recomputed': _same_measurement(lateness, recomputed_lateness),
        'finite_requested_min_outage_seconds': minimum_outage is not None,
        'finite_measured_outage_seconds': outage is not None,
        'outage_reached_requested_minimum': (outage is not None and minimum_outage is not None
                                            and outage >= minimum_outage),
        'outage_contains_generation_window': outage is not None and elapsed is not None and outage >= elapsed,
    }
    full_checks = {
        'full_requested_events_minimum': requested_count is not None and requested_count >= FULL_MINIMUM_EVENTS,
        'full_configured_rate': requested_rate == FULL_TARGET_RATE,
        'full_generation_window': _at_most(elapsed, full_maximum),
        'full_schedule_lateness': _at_most(lateness, full_lateness, full_expected, elapsed),
        'full_actual_rate_lower_bound': _at_most(FULL_TARGET_RATE / (1 + GENERATION_TOLERANCE), recomputed_rate),
        'full_actual_rate_upper_bound': _at_most(recomputed_rate, FULL_TARGET_RATE * (1 + RATE_UPPER_TOLERANCE)),
    }
    checks.update({name: passed if tier == 'full' else None for name, passed in full_checks.items()})
    failed = [name for name, passed in checks.items() if passed is False]
    passed = not failed
    capacity_qualified = tier == 'full' and passed
    qualification = ('SMOKE_CAPACITY_UNQUALIFIED' if tier == 'smoke' else
                     'FULL_CAPACITY_QUALIFIED' if capacity_qualified else 'FULL_CAPACITY_FAILED')
    invalid = {}
    scalar_values = {
        'requested_events': (requested_events, requested_count),
        'configured_rate': (configured_rate, requested_rate),
        'actual_input_count': (actual_input_count, observed_count),
        'workload.input': (raw.get('input'), reported_count),
        'workload.completed_commands': (raw.get('completed_commands'), completed_count),
        'workload.elapsed_seconds': (raw.get('elapsed_seconds'), elapsed),
        'workload.target_rate': (raw.get('target_rate'), target_rate),
        'workload.actual_command_rate': (raw.get('actual_command_rate'), measured_rate),
        'workload.schedule_lateness_seconds': (raw.get('schedule_lateness_seconds'), lateness),
        'requested_min_outage_seconds': (requested_min_outage_seconds, minimum_outage),
        'measured_outage_seconds': (measured_outage_seconds, outage),
    }
    for name, (original, usable) in scalar_values.items():
        if usable is None:
            invalid[name] = {'type': type(original).__name__, 'value': _invalid_display(original)}
    return {
        'contract_version': 1, 'acceptance_tier': tier if isinstance(tier, str) else None,
        'qualification': qualification, 'passed': passed, 'capacity_qualified': capacity_qualified,
        'requested': {'input_events': requested_count, 'configured_rate': requested_rate,
                      'minimum_outage_seconds': minimum_outage},
        'observed': {'actual_input_events': observed_count, 'reported_input_events': reported_count,
                     'completed_commands': completed_count, 'generation_elapsed_seconds': elapsed,
                     'reported_target_rate': target_rate, 'reported_actual_command_rate': measured_rate,
                     'recomputed_actual_command_rate': recomputed_rate,
                     'reported_schedule_lateness_seconds': lateness,
                     'recomputed_schedule_lateness_seconds': recomputed_lateness,
                     'measured_outage_seconds': outage},
        'frozen_gate': {'applied': tier == 'full', 'minimum_full_requested_events': FULL_MINIMUM_EVENTS,
                        'full_target_rate': FULL_TARGET_RATE, 'generation_tolerance_fraction': GENERATION_TOLERANCE,
                        'rate_upper_tolerance_fraction': RATE_UPPER_TOLERANCE,
                        'numeric_comparison_ulps': _ARITHMETIC_COMPARISON_ULPS,
                        'expected_generation_seconds': full_expected if tier == 'full' else None,
                        'maximum_generation_seconds': full_maximum if tier == 'full' else None,
                        'maximum_schedule_lateness_seconds': full_lateness if tier == 'full' else None,
                        'minimum_actual_command_rate': FULL_TARGET_RATE / (1 + GENERATION_TOLERANCE) if tier == 'full' else None,
                        'maximum_actual_command_rate': FULL_TARGET_RATE * (1 + RATE_UPPER_TOLERANCE) if tier == 'full' else None},
        'checks': checks, 'failure_codes': failed, 'invalid_values': invalid,
    }
