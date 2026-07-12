"""Pure (Spark-free) text-matching + id logic for identity resolution — SPEC §6.

These functions carry the *algorithmic* core of identity resolution: Jaro-Winkler
string similarity, the weighted multi-attribute match scorer, and the deterministic
``master_customer_id`` generator. They deliberately depend only on ``constants`` (no
pyspark), so they are unit-testable in isolation — see ``tests/test_text_match.py``.

``glue/common/identity.py`` imports these and wraps ``weighted_match_confidence`` and
``master_id`` as Spark UDFs; the Spark ``Column`` normalisers live there.
"""

import uuid

from common.constants import (
    MASTER_ID_NAMESPACE,
    MATCH_WEIGHTS,
    UNRESOLVED_SENTINEL,
)


def jaro(s1: str, s2: str) -> float:
    """Jaro similarity in [0, 1]."""
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0
    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    # transpositions
    transpositions = 0
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    transpositions //= 2
    m = float(matches)
    return (m / len1 + m / len2 + (m - transpositions) / m) / 3.0


def jaro_winkler(s1: str, s2: str, prefix_scale: float = 0.1) -> float:
    """Jaro-Winkler similarity in [0, 1]. ``None``/empty inputs -> 0.0."""
    if not s1 or not s2:
        return 0.0
    j = jaro(s1, s2)
    # common prefix up to 4 chars
    prefix = 0
    for a, b in zip(s1, s2):
        if a == b:
            prefix += 1
        else:
            break
        if prefix == 4:
            break
    return j + prefix * prefix_scale * (1 - j)


def weighted_match_confidence(
    l_name, r_name, l_dob, r_dob, l_phone, r_phone, l_email, r_email, l_eid, r_eid
) -> float:
    """Weighted similarity across the five identity attributes (SPEC §6).

    * name contributes ``weight * jaro_winkler``
    * dob/phone/email/eid contribute their full weight on exact match, else 0
    * only fields present on BOTH sides count toward the denominator, so the
      score is renormalised to [0, 1] over available evidence.
    """
    w = MATCH_WEIGHTS
    total_weight = 0.0
    score = 0.0

    if l_name and r_name:
        total_weight += w["name"]
        score += w["name"] * jaro_winkler(l_name.lower(), r_name.lower())

    for field, lv, rv in (
        ("dob", l_dob, r_dob),
        ("phone", l_phone, r_phone),
        ("email", l_email, r_email),
        ("eid", l_eid, r_eid),
    ):
        if lv and rv:
            total_weight += w[field]
            if str(lv) == str(rv):
                score += w[field]

    if total_weight == 0.0:
        return 0.0
    return round(score / total_weight, 4)


def master_id(internal_customer_uuid: str) -> str:
    """Deterministic UUIDv5 master id (SPEC §6.1). ``None`` -> UNRESOLVED sentinel."""
    if not internal_customer_uuid:
        return UNRESOLVED_SENTINEL
    return str(uuid.uuid5(MASTER_ID_NAMESPACE, str(internal_customer_uuid)))
