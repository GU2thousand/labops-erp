"""Pure qualification of exact-run Kafka exporter consumer lag evidence.

Raw exporter exposition remains the caller's artifact. The returned proof retains
each accepted series line and parsed value, so a six-sample denominator can be
audited without confusing another run's groups or the DLQ topic with inventory.
"""
import math
import re

from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.openmetrics.parser import text_string_to_metric_families as openmetrics_families


LAG_METRIC = 'kafka_consumergroup_lag'
LAG_LINE = re.compile(r'^\s*kafka_consumergroup_lag(?:\{|\s|$)')


def _check(condition, message):
    if not condition:
        raise ValueError(message)


def _identifier(value, description):
    _check(isinstance(value, str) and bool(value) and value.strip() == value
           and all(character.isprintable() for character in value),
           'Missing or malformed ' + description)
    return value


def require_consumer_group_lag(payload, expected_groups, expected_topic,
                               expected_partitions=(0, 1, 2)):
    """Require exactly two groups times three finite, nonnegative lag samples.

    Unrelated well-formed series are retained in the total/ignored denominator
    but cannot satisfy coverage. Duplicate required coordinates fail even when
    they differ by extra labels. Malformed exposition or required series raises
    ValueError; this function never contacts the exporter or mutates evidence.
    """
    _check(isinstance(payload, str), 'Exporter exposition must be text')
    _check(isinstance(expected_groups, (tuple, list, set, frozenset)),
           'Expected consumer groups must be a collection of two IDs')
    groups = tuple(_identifier(value, 'expected consumer group ID')
                   for value in expected_groups)
    _check(len(groups) == 2 and len(set(groups)) == 2,
           'Expected exactly two distinct consumer group IDs')
    groups = tuple(sorted(groups))
    topic = _identifier(expected_topic, 'expected inventory topic')
    _check(isinstance(expected_partitions, (tuple, list))
           and tuple(expected_partitions) == (0, 1, 2)
           and all(type(value) is int for value in expected_partitions),
           'Expected inventory partitions must be exactly 0, 1 and 2')
    partitions = tuple(expected_partitions)
    # Validate complete exposition first, including malformed labels, duplicate
    # label names, and invalid numeric tokens. The public parser is already an
    # application dependency; no custom Prometheus label grammar is introduced.
    try:
        families = list(text_string_to_metric_families(payload))
    except (ValueError, TypeError) as exc:
        raise ValueError('Malformed exporter exposition') from exc
    parsed_count = sum(sample.name == LAG_METRIC for family in families
                       for sample in family.samples)
    required = {(group, topic, partition) for group in groups for partition in partitions}
    observed = {}
    total = 0
    for raw_line in payload.splitlines():
        if not LAG_LINE.match(raw_line):
            continue
        try:
            samples = [sample for family in text_string_to_metric_families(raw_line)
                       for sample in family.samples]
        except (ValueError, TypeError) as exc:
            raise ValueError('Malformed consumer group lag series') from exc
        _check(len(samples) == 1 and samples[0].name == LAG_METRIC,
               'Malformed consumer group lag series')
        sample = samples[0]
        labels = dict(sample.labels)
        total += 1
        _check(all(label in labels and isinstance(labels[label], str) and labels[label]
                   for label in ('consumergroup', 'topic', 'partition')),
               'Consumer group lag series is missing required labels')
        if labels['consumergroup'] not in groups or labels['topic'] != topic:
            continue
        # The legacy Prometheus parser accepts some malformed suffixes by using
        # the first numeric token as value and the last token as timestamp.
        # Materializing the public OpenMetrics parser validates the complete
        # required sample line, including any optional timestamp. Keep the
        # Prometheus-parsed value below, since this remains Prometheus evidence.
        try:
            list(openmetrics_families(raw_line.strip() + '\n# EOF\n'))
        except (ValueError, TypeError) as exc:
            raise ValueError('Malformed required consumer group lag sample syntax') from exc
        _check(labels['partition'] in {'0', '1', '2'},
               'Required consumer group lag partition must be canonical 0, 1 or 2')
        partition = int(labels['partition'])
        coordinate = (labels['consumergroup'], labels['topic'], partition)
        _check(coordinate not in observed, 'Duplicate required consumer group lag coordinate')
        value = float(sample.value)
        _check(math.isfinite(value) and value >= 0,
               'Required consumer group lag must be finite and nonnegative')
        observed[coordinate] = {'consumergroup': labels['consumergroup'],
                                'topic': labels['topic'], 'partition': partition,
                                'value': value, 'labels': labels, 'raw_line': raw_line}
    _check(total == parsed_count, 'Consumer group lag exposition cannot be reconciled to raw lines')
    missing = required - observed.keys()
    _check(not missing,
           'Missing required consumer group lag samples: ' +
           ', '.join(f'{group}/{sample_topic}/{partition}'
                     for group, sample_topic, partition in sorted(missing)))
    return {'passed': True, 'metric': LAG_METRIC, 'groups': list(groups),
            'topic': topic, 'partitions': list(partitions),
            'expected_samples': len(required), 'observed_samples': len(observed),
            'total_lag_samples': total, 'ignored_lag_samples': total - len(observed),
            'samples': [observed[coordinate] for coordinate in sorted(observed)]}
