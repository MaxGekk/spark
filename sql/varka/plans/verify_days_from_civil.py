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

"""The arithmetic behind task 40, checked before it was handed over.

Two things, both of which PLAN_TASK_40.md depends on being true:

  1. days_from_civil, the inverse of the task 26 decomposition, with every
     division emitted as an *exact* magic multiply, round-trips to the identity
     over every day from year 1 to year 9999.
  2. add_months, built from that inverse, matches LocalDate.plusMonths
     semantics - month arithmetic, then clamp the day to the new month - over a
     broad sample of days and offsets.

It is committed for the same reason verify_chrono_tails.py is: the recipe
claims its arithmetic is checked, and the reader should be able to re-run the
check rather than take that on trust.

    python3 sql/varka/plans/verify_days_from_civil.py

It earned its keep on the first run. The natural formulation of the month
arithmetic, folding the year into a total month count and dividing by 12, puts
the dividend near 400,000: far past the ~46341 bound an exact magic multiply
needs, and past the ~160,000 that round-down-plus-one-correction reaches. The
fix is in the code below and is the single most important thing in the recipe.
"""

import datetime
ERA, CEN, BIAS = 146097, 36524, 5394572
def decompose(d):                      # VarkaChrono.narrowed, as shipped
    w = d + BIAS
    era = (w * 114) >> 24
    r = w - era * ERA
    if r >= ERA: era += 1; r -= ERA
    c = (r * 7349) >> 28
    doc = r - c * CEN
    if doc >= CEN: c += 1; doc -= CEN
    if c == 4: c = 3; doc += CEN
    yoc = (doc * 45966) >> 24
    doy = doc - (365 * yoc + (yoc >> 2))
    if doy < 0:
        doy += 365 + (1 if (yoc & 3) == 0 else 0); yoc -= 1
    mp = ((5 * doy + 2) * 877241) >> 27
    dom = doy - (((153 * mp + 2) * 838861) >> 22) + 1
    month = mp + 3 if mp < 10 else mp - 9
    year = 400 * (era - 32) + 100 * c + yoc + (1 if mp >= 10 else 0)
    return year, month, dom

# Hinnant's days_from_civil, with every division done as a magic multiply on a
# biased (non-negative) operand, the way the emitter would have to.
YBIAS = 13200                                    # multiple of 400
def days_from_civil(y, m, d):
    yy = y - (1 if m <= 2 else 0)
    b = yy + YBIAS                               # 0 <= b <= ~46400 over the range
    era = (b * 167773) >> 26                     # b / 400, exact magic
    yoe = b - era * 400                          # 0..399
    mp = m + (9 if m <= 2 else -3)               # March-based month, 0..11
    doy = (((153 * mp + 2) * 838861) >> 22) + d - 1
    doe = yoe * 365 + (yoe >> 2) - ((yoe * 167773) >> 24) + doy
    return (era - YBIAS // 400) * ERA + doe - 719468

# LocalDate.plusMonths semantics: month arithmetic then clamp the day.
LEN = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
def leap(y):
    b = y + YBIAS
    return 1 if ((b & 3) == 0 and ((b - ((b * 167773) >> 24) * 100) != 0
                                   or (b - ((b * 167773) >> 26) * 400) == 0)) else 0
# Do NOT fold the year into a total month count: y * 12 is ~400k, past the
# ~46341 bound an exact magic multiply needs. Keep the dividend small instead -
# it is only the month index plus the offset, so for a literal interval it is
# bounded by the literal.
MBIAS_UNITS = 2048                               # whole years of bias, a constant
def add_months(d, months):
    y, m, dom = decompose(d)
    k = (m - 1) + months + 12 * MBIAS_UNITS      # small and non-negative
    q = (k * 43691) >> 19                        # k / 12, exact magic (e = 1)
    nm = k - q * 12                              # 0..11
    ny = y + q - MBIAS_UNITS
    length = LEN[nm] + (leap(ny) if nm == 1 else 0)
    return days_from_civil(ny, nm + 1, min(dom, length))

# 1. Round trip: decompose then recompose must be the identity, everywhere.
bad_rt = 0
for d in range(-719162, 2932897):
    y, m, dom = decompose(d)
    if days_from_civil(y, m, dom) != d: bad_rt += 1
print("round trip over", 2932897 + 719162, "days -> mismatches:", bad_rt)

# 2. add_months against LocalDate.plusMonths semantics, on a broad sample.
def ref_add(dt, months):
    total = dt.year * 12 + (dt.month - 1) + months
    ny, nm = divmod(total, 12)
    ln = LEN[nm] + (1 if nm == 1 and (ny % 4 == 0 and (ny % 100 != 0 or ny % 400 == 0)) else 0)
    return datetime.date(ny, nm + 1, min(dt.day, ln))
bad_am = 0; n = 0
for d in range(-719162, 2932897, 37):
    dt = datetime.date.fromordinal(d + 719162 + 1)
    for months in (0, 1, -1, 12, -12, 13, -13, 100, -100, 1200, -1200):
        total = dt.year * 12 + (dt.month - 1) + months
        if not (1 <= total // 12 <= 9999):
            continue                              # outside datetime's own range
        want = ref_add(dt, months)
        got = add_months(d, months)
        n += 1
        if got != want.toordinal() - 719162 - 1:
            bad_am += 1
            if bad_am < 4: print("MISMATCH", dt, months, got, want)
print("add_months over", n, "cases -> mismatches:", bad_am)
