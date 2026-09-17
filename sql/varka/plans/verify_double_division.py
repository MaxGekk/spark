#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""The admission check behind task 88, section 2.19 of PLAN_MILESTONE_5.md.

The Vector API has no integer division and no multiply-high, so every division
Varka does today is a range-narrowed magic multiply. Task 88 asks whether a
division through double lanes - `(double) v` in, a multiply by the reciprocal or
a true divide, `D2I`/`D2L` back - is exact over the dividends Varka's divisions
actually see, in which case it needs no magic, no correction and no range guard.

Section 2.19 argues it is, and quotes a sampled check. This script does what that
section says the admission check owes: it verifies the claim exhaustively where
the dividend range allows, at the dividends where a failure must first appear
where it does not, and it states the error constant the argument rests on. It
checks two lowerings separately, because they are not equally exact:

  RECIP    trunc(v * fl(1/d))   two roundings, the cheap form (a multiply)
  DIV      trunc(v / d)         one rounding, the slow form (a divide)

Why two lowerings can differ. When d divides v, DIV returns the integer k exactly
(the quotient is representable and the divide is correctly rounded). RECIP
computes fl(k * fl(1/d)): fl(1/d) carries a relative error e1 with |e1| <= 2^-53,
and when e1 < 0 the product k*(1+e1) can round to the double just below k, so
truncation returns k-1. The textbook case is d = 49: 49 * fl(1/49) is
0.9999999999999999 in IEEE double, and trunc gives 0. Whether a divisor is
affected depends on the sign and size of its reciprocal's rounding error against
the binade of every k in range, which no argument settles and this script checks.

For a non-multiple the argument is the one 2.19 gives: the true quotient v/d
lies at least 1/d from every integer, the computed value is within
|v/d| * (2^-52 for RECIP, 2^-53 for DIV) of it, so truncation cannot cross an
integer while |v| < 2^52 (RECIP) or 2^53 (DIV). That is the constant: the bound
is on the DIVIDEND, not the divisor, and it is 2^52 for the form that matters.

Run: python3 sql/varka/plans/verify_double_division.py    (about three minutes)
Needs numpy for the exhaustive int32 sweeps (float64 there is IEEE binary64 with
one rounding per operation, the same arithmetic the JVM's DoubleVector does).
"""

import math
import random
import sys

try:
    import numpy as np
except ImportError:  # pragma: no cover - the message is the point
    sys.stderr.write("numpy is required for the exhaustive sweeps: pip install numpy\n")
    raise SystemExit(2)

TWO52 = 1 << 52
TWO53 = 1 << 53

# --- the int32 divisions, with the dividend range each one actually sees ------------------
# From VarkaChrono (task 26's constants and their comments). `hi` is exclusive; a signed range
# is given as (lo, hi). The narrowed era step divides a biased day below 2^24; the total form
# task 88 would delete the narrowing for divides the whole biased int32 range.
INT32_DIVISIONS = [
    (146097, 0, 1 << 24, "era, narrowed: /146097 over w < 2^24"),
    (146097, 0, (1 << 32) + 719468, "era, total: /146097 over the biased int32 range"),
    (36524, 0, 146097, "century in era: /36524"),
    (365, 0, 36525, "year of century: /365"),
    (1461, 0, 4 * 146096 + 4, "Julian year: /1461 over the scaled day of era"),
    (153, 0, 5 * 365 + 3, "month: /153"),
    (5, 0, 153 * 11 + 3, "day: /5"),
    (3, 0, 15, "quarter: /3"),
    (7, 0, 685, "week: /7"),
    (2141, 0, 1 << 16, "day of month: /2141"),
    (100, 0, 1 << 24, "year century: /100 over the biased year"),
    (400, 0, 1 << 24, "year quatercentennial: /400 over the biased year"),
    (12, -(1 << 31), 1 << 31, "year-month YEAR extract: /12 over all int32 (signed)"),
    (3, -(1 << 31), 1 << 31, "year-month quarter: /3 over all int32 (signed)"),
]

# --- the long-lane divisions -----------------------------------------------------------------
NANOS_PER_DAY = 86_400_000_000_000
TIME_DIVISIONS = [
    (3_600_000_000_000, "hour(t): /3.6e12"),
    (60_000_000_000, "minute(t): /6e10"),
    (1_000_000_000, "second(t): /1e9"),
    (1_000_000, "time_trunc to milliseconds: /1e6"),
    (1_000, "time_trunc to microseconds: /1e3"),
]
INTERVAL_DIVISIONS = [
    (86_400_000_000, "extract(DAY FROM dt): /8.64e10"),
    (3_600_000_000, "extract(HOUR FROM dt): /3.6e9"),
    (60_000_000, "extract(MINUTE FROM dt): /6e7"),
    (1_000_000, "extract(SECOND FROM dt): /1e6"),
]


def recip_np(v, d):
    """trunc(v * fl(1/d)) over a float64 array, as DoubleVector MUL then D2I/D2L would."""
    return np.trunc(v * np.float64(1.0 / d))


def div_np(v, d):
    """trunc(v / d) over a float64 array, as DoubleVector DIV then D2I/D2L would."""
    return np.trunc(v / np.float64(d))


def java_quotient_np(v, d):
    """Java's `/`: truncation toward zero, on int64 arrays (numpy's // floors)."""
    q = np.abs(v) // d
    return np.where(v < 0, -q, q)


def sweep(d, lo, hi, chunk=1 << 24):
    """Exhaustive over [lo, hi): the first dividend each lowering gets wrong, or None."""
    first = {"RECIP": None, "DIV": None}
    v = lo
    while v < hi:
        stop = min(v + chunk, hi)
        ints = np.arange(v, stop, dtype=np.int64)
        expect = java_quotient_np(ints, d).astype(np.float64)
        f = ints.astype(np.float64)
        for name, fn in (("RECIP", recip_np), ("DIV", div_np)):
            if first[name] is None:
                bad = np.nonzero(fn(f, d) != expect)[0]
                if bad.size:
                    first[name] = int(ints[bad[0]])
        if all(x is not None for x in first.values()):
            break
        v = stop
    return first


def near_multiples(d, lo, hi, max_k=2_000_000, seed=88):
    """The dividends where a failure must first appear, when the range is too wide to sweep.

    For every multiple k*d in range (all of them when there are at most max_k, otherwise a
    dense prefix, a dense suffix and a random sample of the rest) the three dividends
    k*d - 1, k*d, k*d + 1: the exact multiple, where RECIP's reciprocal error can round the
    product below k, and its two neighbours, where the quotient is nearest an integer from
    either side. Plus a million random dividends as a control.
    """
    k_lo = -((-lo) // d) if lo < 0 else (lo + d - 1) // d
    k_hi = (hi - 1) // d
    count = k_hi - k_lo + 1
    if count <= max_k:
        ks = np.arange(k_lo, k_hi + 1, dtype=np.int64)
    else:
        rng = random.Random(seed)
        edge = max_k // 4
        middle = np.array(
            sorted(rng.sample(range(k_lo + edge, k_hi - edge), max_k // 2)), dtype=np.int64
        )
        ks = np.concatenate(
            [
                np.arange(k_lo, k_lo + edge, dtype=np.int64),
                middle,
                np.arange(k_hi - edge + 1, k_hi + 1, dtype=np.int64),
            ]
        )
    mult = ks * d
    probes = np.concatenate([mult - 1, mult, mult + 1])
    rng = np.random.default_rng(seed)
    probes = np.concatenate([probes, rng.integers(lo, hi, size=1_000_000, dtype=np.int64)])
    probes = probes[(probes >= lo) & (probes < hi)]
    return probes, count, count <= max_k


def probe(d, lo, hi):
    """Check both lowerings at near_multiples(d, lo, hi); returns (first_bad, checked, all)."""
    probes, count, exhaustive_k = near_multiples(d, lo, hi)
    expect = java_quotient_np(probes, d).astype(np.float64)
    f = probes.astype(np.float64)
    first = {}
    for name, fn in (("RECIP", recip_np), ("DIV", div_np)):
        bad = np.nonzero(fn(f, d) != expect)[0]
        first[name] = int(probes[bad[0]]) if bad.size else None
    return first, probes.size, exhaustive_k


def first_failure_above(d, start, limit):
    """The first multiple-neighbourhood dividend past `start` where RECIP fails, below limit."""
    k = start // d
    while k * d < limit:
        ks = np.arange(k, min(k + 1_000_000, limit // d + 1), dtype=np.int64)
        mult = ks * d
        probes = np.concatenate([mult - 1, mult, mult + 1])
        probes = probes[(probes >= start) & (probes < limit)]
        if probes.size:
            expect = java_quotient_np(probes, d).astype(np.float64)
            bad = np.nonzero(recip_np(probes.astype(np.float64), d) != expect)[0]
            if bad.size:
                return int(probes[bad[0]])
        k = int(ks[-1]) + 1
    return None


def verdict(first):
    return ", ".join(
        f"{n} {'exact' if v is None else 'WRONG at ' + str(v)}" for n, v in first.items()
    )


def main():
    ok = True
    print("The error constant the argument rests on:")
    print("  RECIP: two roundings, relative error <= 2^-52 (+2^-106); DIV: one, <= 2^-53.")
    print("  A non-multiple's quotient is >= 1/d from every integer, so truncation is safe")
    print("  while |v| < 2^52 (RECIP) or |v| < 2^53 (DIV). The bound is on the dividend.")
    print("  At an exact multiple DIV is exact; RECIP is exact only if fl(1/d)'s rounding")
    print("  error never pushes k*fl(1/d) below k - a property of d, checked below.")
    print()

    print("The control - the check can fail: d = 49, v = 49")
    r = math.trunc(49 * (1.0 / 49))
    print(
        f"  49 * fl(1/49) = {49 * (1.0 / 49)!r}, trunc = {r}  "
        f"({'RECIP WRONG, as the literature says' if r != 1 else 'unexpectedly exact'})"
    )
    if r == 1:
        ok = False
    print()

    print("int32 divisions, exhaustive over the dividend range each lowering sees:")
    for d, lo, hi, label in INT32_DIVISIONS:
        first = sweep(d, lo, hi)
        print(f"  {label:58s} {verdict(first)}")
        if first["DIV"] is not None:
            ok = False
    print()

    print(f"TIME, nanoseconds of day in [0, {NANOS_PER_DAY}) - below 2^47 - at every")
    print("multiple's neighbourhood (all multiples where there are at most two million, else")
    print("a dense prefix, a dense suffix and a random half) plus a million random dividends:")
    for d, label in TIME_DIVISIONS:
        first, checked, exhaustive_k = probe(d, 0, NANOS_PER_DAY)
        how = "every multiple" if exhaustive_k else "sampled multiples"
        print(f"  {label:44s} {verdict(first)}  [{checked} dividends, {how}]")
        if first["DIV"] is not None:
            ok = False
    print()

    print("Day-time intervals, microseconds in [-2^52, 2^52] - the bound 2.19 states:")
    for d, label in INTERVAL_DIVISIONS:
        first, checked, exhaustive_k = probe(d, -TWO52, TWO52 + 1)
        how = "every multiple" if exhaustive_k else "sampled multiples"
        print(f"  {label:44s} {verdict(first)}  [{checked} dividends, {how}]")
        if first["DIV"] is not None:
            ok = False
    print()

    print("The bound binds: past 2^52 the RECIP form's first wrong quotient, per divisor")
    print("(searched from 2^52 up to 2^53 at the multiples' neighbourhoods):")
    for d, label in INTERVAL_DIVISIONS:
        bad = first_failure_above(d, TWO52, TWO53)
        where = f"v = {bad} = 2^52 + {bad - TWO52}" if bad is not None else "none below 2^53"
        print(f"  {label:44s} {where}")
    print()

    if ok:
        print("PASS: DIV is exact for every division over its range, and the table above says")
        print("      per divisor whether the cheaper RECIP form may be used in its place.")
    else:
        print("FAIL: a DIV lowering is wrong inside its stated range; section 2.19's argument")
        print("      does not hold as written.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
