# Task 53: the Neri-Schneider month block

## 1. Where this came from

Not from the milestone. Velox was read side by side with Varka's calendar
lowering during task 48, and its scalar date decomposition
(`velox/type/FastDate.h`, reached from `year(date)` through
`Timestamp::epochToCalendarUtc`) turns out not to be Hinnant's
`civil_from_days` - the algorithm Varka's task 26 lowering follows - but
**Neri-Schneider 2022**, "Euclidean affine functions and their application to
calendar algorithms". That paper's decomposition is a strictly better shape in
two ways, and exactly one of them is expressible on `IntVector` lanes. This
task takes that one.

The half worth taking is the month block. Neri-Schneider computes the month
index and the day of month from a **single affine numerator**, taking the
quotient with a shift and the day of month out of the remainder, where Varka
computes the month with a magic multiply and then recovers the day of month by
running a second magic multiply forwards (`emitMonthStart`) and subtracting.
Same answers, fewer lane ops, and the intermediate that survives the prefix is
one the day-of-month tail can use directly rather than one it has to invert.

Velox's own file carries an MIT header for the Neri-Schneider code it adapts.
The identities are from the published paper and free to implement; **this task
derives the lowering from the paper's form and from the verification below, and
does not transliterate that file**, which is also why section 2 re-derives every
constant against Varka's own domain rather than importing one.

An interaction with task 48, stated before it is discovered: after this task a
`year`-only kernel elides the *numerator* step rather than the magic-multiply
month step, so task 48's elision shrinks from four ops to two. It stays correct
and stays a win; the number in `PLAN_TASK_48.md` 9.1 just becomes a number about
code that no longer exists, and section 5's op-count test moves with it.

**Corrected once PR #64 was integrated against task 48**: the elision covers
*two* nodes, not one. `dayofyear`'s tail blends `doy - 305` against
`doy + 60 + L` and reads no month index at all, so it too skips the month step
and its `tailReadsMarchMonth` arm is `false`. Everything this section says about
`year` therefore applies to `dayofyear` as well, and the arithmetic below counts
both. Nothing about the design moves - what moves is a count this plan
registered in advance, which is the kind of thing worth fixing on the record
rather than discovering mid-task.

## 2. The admission check, done

Same gate task 26 applied before any emitter work, and it splits the paper
cleanly in half.

### 2.1 What is expressible: the month block

Over the March-based day of year the prefix leaves in `rem`, domain `[0, 365]`:

```
num  = 2141 * doy + 197913        // max 979378, comfortably inside int32
m3   = num >>> 16                 // the month, March = 3 .. February = 14
dom0 = (num & 0xFFFF) / 2141      // the zero-based day of month
```

Verified over the whole domain, against the forms Varka ships today: `m3 - 3`
equals `(5 * doy + 2) / 153` and `dom0` equals
`doy - ((153 * mp + 2) / 5)` for **all 366 values, zero mismatches**. The
divisor in the third line is the only thing that needs a magic, and it has an
exact one well inside a lane: `/2141` over `[0, 65535]` is
`(x * 31345) >>> 26`, exact at every one of the 65536 values, with a maximum
product of 2054194575 - under `2^31 - 1` with room to spare.

The paper derives the same two constants and states their range itself, which is
a stronger check than the sweep above because it covers the reasoning and not
only the values: Example 10 applies its Theorem 3 with `k = 16` to
`m(N_Y) = (5 * N_Y + 461) / 153`, yielding `a' = 2141`, `b' = 197913` and
`U = 734`, and Equation (20) gives the remainder form over the same range,
`for all N_Y in [0, 734[`. Varka's domain is `[0, 365]`, comfortably inside it.
The transcription is in `sql/varka/papers/`; **read the PDF for the theorem
statements themselves**, for the reason that directory's README records.

The inverse direction's month-start map is expressible too, and is also a
shift rather than a magic:

```
monthStart(m3) = (979 * m3 - 2919) >>> 5      // m3 in [3, 14]
```

equal to Varka's `(153 * mp + 2) / 5` at all twelve months, with the numerator
running from 18 to 10787 - no overflow, and never negative, so the shift may be
arithmetic or logical.

### 2.2 What is not expressible: the era and year steps

The paper's real headline is that its century and year steps need **no
correction carries at all**, because it divides a `4x + 3` numerator and reads
the remainder out of the same operation. Neither reaches an int lane:

* `century = (4 * shiftedDay + 3) / 146097` has a dividend up to `2^26`. The
  largest multiplier whose product still fits a signed 32-bit lane at that
  dividend is **32**, and an exact or round-down magic for `/146097` needs
  `M ~ 2^k / 146097`, so `k <= 22` and `M = 29` - a divisor error near 1%,
  which is not a carry away from correct, it is a different algorithm.
  Searched rather than argued: no `(M, k)` is exact over `[0, 2^26)` with the
  product inside a lane.
* the year step is `(2939745 * n) >> 32`, that is the **high half** of a 64-bit
  product. `VectorOperators` has no multiply-high on any lane type - the fact
  task 26's admission check turned on, and still true.

Both would fit `LongVector` lanes at half the lane count, which is task 49's
question (`PLAN_MILESTONE_4.md` row 49), not this one. **This task changes
nothing above `rem`.** The era, century and year-of-century steps, their
round-down magics and their carries stay exactly as they are, and the year tail
- which since task 48 reads only `rem` - is untouched.

## 3. The design

### 3.1 The prefix stores a numerator, not a month

`emitChronoPrefix`'s last step becomes the two-op numerator, stored in the slot
`marchMonth` uses today (`t[5]`, renamed `monthNumerator`). Task 48's
`tailReadsMarchMonth` becomes `tailReadsMonthNumerator` with the same
membership - `year` and `dayofyear` are the two tails that read neither - and
its elision, its per-lane-group consumer set and its `VarkaEmitOptions` switch
all carry over unchanged in shape.

The switch is exhaustive with a throwing `default`, and that is load-bearing
rather than decorative: it is what made PR #64 and PR #78 fail immediately on
merging task 48 instead of computing a wrong month quietly. By the time this
task starts it carries six arms - `Year` and `DayOfYear` false, `Month`,
`DayOfMonth`, `Quarter`, `AddMonths` and `LastDay` true - and the rename must
carry all of them.

### 3.2 The month index becomes 3-based, everywhere

Varka's `mp` counts March as 0; Neri-Schneider counts March as 3, which is what
makes the reported month a subtraction with no addition in front of it:

| | today, from `mp` | with `m3` |
|---|---|---|
| month (1-12) | `mp + 3`, minus 12 where `mp >= 10` | `m3`, minus 12 where `m3 >= 13` |

That is one lane op less, and it is why the convention changes rather than
being converted back with a `- 3`: converting back would spend the op this
saves. The consequence is that every reader of the month index moves at once -
`emitChronoMonth`, `emitChronoDayOfMonth`, `Quarter`, `emitMonthStart`,
`emitAddMonths`, `emitChronoLastDay` (PR #78) and task 35's `TruncDate` - along
with `VarkaChrono.MARCH_YEAR_JANUARY`, which becomes 13 on the new axis, and the
scalar twin's `fromEra`, which must keep mirroring the emitted arithmetic.

`VarkaChrono.MARCH_TO_JANUARY_DAYS` (task 48) does **not** move: it lives on the
day-of-year axis, which this task does not touch. That is a small piece of
evidence that task 48 put the year's January test on the right axis.

### 3.3 The tails

```
month        = m3 blended with m3 - 12 where m3 >= 13
dayOfMonth   = ((num & 0xFFFF) * 31345 >>> 26) + 1
monthStart   = (979 * m3 - 2919) >>> 5
truncToMonth = d - dom0                      // no monthStart at all
```

The last line is worth calling out for task 35: `trunc(d, 'MONTH')` is today
`d - doy + monthStart(mp)`, and becomes a single subtraction of the zero-based
day of month, which this block already has.

### 3.4 Registered op counts, from `rem`

Deterministic, and section 5 asserts them off the class file rather than from
this table:

| tail | today | this task | delta |
|---|---|---|---|
| `month` | 7 | 5 | -2 |
| `dayofmonth` | 10 | 6 | -4 |
| `quarter` | 10 | 8 | -2 |
| `trunc(d, 'MONTH')` | 10 | 7 | -3 |
| `emitMonthStart`, per call | 4 | 3 | -1 |
| `year` | unchanged | unchanged | 0 |
| `dayofyear` | unchanged | unchanged | 0 |

`add_months` and `last_day` call `emitMonthStart` twice each and go through the
month and day-of-month tails, so they take the largest absolute saving; they
are also the two nodes where a mistake is least likely to be caught by a
bounded test, which is what section 5's sweep is for.

`year` and `dayofyear` are zero for the same reason: both elide, so neither
emits the step this task replaces, in either form. What does move for them is
the *kept* side of the A/B, from four ops to two, which is section 6.1's
prediction 6 and not a shipped number. `DAY_OF_YEAR_WEIGHT` (51 since PR #64
was integrated against task 48) therefore stays 51 - stated because a weight
that does not move is as easy to get wrong by adjusting it as one that does.

## 4. Files

| file | what |
|---|---|
| `VarkaChrono.java` | `MONTH_NUM_M`/`MONTH_NUM_ADD`/`MONTH_NUM_K`, `DOM_M`/`DOM_K`, `MONTH_START_M`/`MONTH_START_SUB`/`MONTH_START_K`, each with its own domain and its verification in the javadoc; `MARCH_YEAR_JANUARY` to 13; `fromEra`'s month block |
| `VarkaLoopEmitter.java` | `emitMonthNumerator`; `emitChronoMonth`, `emitChronoDayOfMonth`, `emitMonthStart`, the `Quarter` arm, `emitAddMonths`, `emitChronoLastDay` on the new axis; `tailReadsMarchMonth` renamed with its javadoc; the `CHRONO_WEIGHT`/`ADD_MONTHS_TMP_COUNT`/`LAST_DAY_TMP_COUNT` accounting re-derived, not adjusted by eye (`DAY_OF_YEAR_WEIGHT` is expected to stay 51; re-count it rather than assume it) |
| `VarkaEmitOptions.java` | `neriSchneiderMonth`, so the old block stays a live reference variant the differential checks against, per `FloorMod7`'s precedent |
| `VarkaChronoSuite.scala` | the three identities over their exact domains (366, 65536 and 12 cases), and the existing exhaustive sweep, unchanged and still green |
| `VarkaLoopEmitterSuite.scala` | section 5 |
| `VarkaEmitterParityBenchmark.scala` + committed results | section 6 |
| `PLAN_TASK_48.md` 9.1, `PLAN_TASK_35.md` 7.3, `PLAN_MILESTONE_4.md` | the elision's new size; `trunc(d, 'MONTH')`'s new form; the row and the catalogue entry |

## 5. Tests

1. **The three identities**, each over its whole domain through the shipped
   constants: the month and day-of-month pair over `[0, 365]`, the `/2141`
   magic over `[0, 65535]`, and the month-start map over the twelve months.
2. **Op counts** off the class file (`VarkaEmitterTestSupport.invocationCount`,
   added by task 48), one assertion per row of the 3.4 table.
3. **Both variants agree** on every shape the emitter suite drives, at both
   widths - the `FloorMod7` reference-variant pattern, which is what makes the
   old block worth keeping rather than deleting.
4. **The exhaustive sweep** over all 16777216 covered days, five fields, both
   variants, both sharing modes, both widths. This is the gate that matters:
   `add_months` and `last_day` recompose through these constants, and a
   month-axis mistake in either is exactly the kind of thing a boundary set can
   miss and a sweep cannot.
5. **The pinned fixtures**, expected to move: every calendar node's emitted
   bytes change, so the line map and the ladder both re-pin, and the ladder's
   unshared boundary moves for a **fourth** time.

## 6. The measurement

The same instrument and the same discipline as task 48, for the same reason its
section 9.2 records: the parity file must be regenerated by the task that
changes the bytes.

Adjacent A/B cases in the `year` section for `dayofmonth` and `month`, plus the
four-field shape, null-free and mixed-null each, at both widths, five
regenerations compared by minimums, with `per-row LocalDate year` as the in-run
control that says whether the machine moved.

### 6.1 Predictions, registered before the run

1. The op counts in 3.4 land exactly; a miss there is a bug, not a surprise.
2. `dayofmonth`, null-free, gains 4-9% at AVX-512 - four ops off a body of
   about forty, on the same latency-bound dependent chain task 48's four came
   off, where the measured answer was 1.01x. **Inside noise is the expected
   outcome again**, and the default ships on the op count.
3. `year` does not move at either width, in either direction. It shares only
   `rem` with this block.
4. The four-field shared kernel gains more than any single-field one, because
   it pays the month block once and the tails three times.
5. The unshared `HugeMethodLimit` crossing moves out again, to 21 or 22.
6. Task 48's elision shrinks from four ops to two, for **both** nodes that take
   it - `year` and `dayofyear` - and `PLAN_TASK_48.md` 9.1's number is
   superseded rather than contradicted. The elided bodies do not move at all;
   only the kept side of the switch does.

## 7. Risks

1. **The 3-based axis touching seven call sites at once.** The month index is
   read in more places than any other prefix value. Mitigated by the rename:
   `marchMonth` becomes `monthIndex3`, so a call site left on the old
   convention does not compile rather than computing a month three off.

   This plan originally named two in-flight call sites, PR #64 and PR #78.
   **Only PR #78 is one**, and the correction is worth keeping rather than
   quietly deleting, because it was an assumption about another task's lowering
   made without reading it: `dayofyear` was assumed to read the month index and
   does not. `last_day` is the real exposure and the worst of the seven - two
   `emitMonthStart` calls plus both the month and day-of-month tails - so this
   task should start after PR #78 merges, and edit that call site directly
   rather than hand-resolve it in a merge.
2. **`MARCH_YEAR_JANUARY` changing value.** A constant whose meaning is
   unchanged but whose number moves is the classic silent-merge hazard. It gets
   a javadoc sentence naming the axis, and the identity test in section 5 fails
   loudly if the two axes are ever mixed.
3. **A month-axis error surviving the boundary set.** See test 4; `add_months`
   and `last_day` are the exposure.
4. **Nothing to gain being mistaken for something to lose.** If the measurement
   comes back flat, the op-count reduction still ships - the same argument task
   48 made and measured.
5. **Scope creep into the era and year steps.** Section 2.2 is the boundary,
   and it is a hard one until int64 lanes exist. A reviewer noticing that the
   paper's headline result is not implemented here should read 2.2, not open a
   follow-up.

## 8. Sequencing

Four commits, each green on its own. Commit 1 landed with PR #80; the
corrections in sections 1, 3.1, 3.4, 6.1 and 7 are a follow-up to it, made
before commit 2 rather than folded into the work, so the plan a later reader
diffs against is the one the work actually started from.

Commit 2 waits on PR #78. Section 7's risk 1 says why: `emitChronoLastDay` is
the worst of the seven month-axis call sites, and editing it directly on master
is a different job from hand-resolving it inside a merge.

1. **This plan**, with section 2's admission check. **DONE** (PR #80).
2. **The constants and the scalar twin**: `VarkaChrono`'s new constants, the
   3-based axis, `fromEra`'s month block, and the three identity tests. No
   emitter change, so the case is on record before the code that rests on it.
3. **The emitter**, behind `neriSchneiderMonth`: the numerator, the four tails,
   the two recomposing nodes, tests 2 through 5, the fixtures re-pinned.
4. **The measurement**: the A/B cases, five regenerations at each width, the
   results file, the numbers recorded in section 9 with the default confirmed.

## 9. Outcome

Filled in when the work lands: the op counts against 3.4, the A/B by minimums
at both widths, the ladder's fourth boundary, and which of 6.1's predictions
held.
