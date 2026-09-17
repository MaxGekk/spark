# Task 88: an exact division through double lanes

*Milestone 5, section 2.19. Planned 17 September 2026, with the admission check
run rather than argued; the emitter work waits for #240 (task 29) to merge.*

## 1. Where this came from

Milestone row 88, opened on 9 September 2026 from task 68's admission check, and
the spine section 1.1 sorts to: `... 85 -> 29 -> 88 -> 102, 103, 104 -> 105`.
Every division Varka does today is a range-narrowed magic multiply, because the
Vector API has no integer divide and no multiply-high on any lane. That is
task 26's machinery: round-down constants, correction carries, `VarkaChrono`'s
range guards, and the bound that made task 68 defer `extract(YEAR FROM ym)` - the
`/12` magic is exact over 0..49,151, one forty-thousandth of the type's range.

Section 2.19's observation is that there is a second route nobody had
considered: widen to *double* lanes. `(double) v` is exact for every int32, IEEE
arithmetic is correctly rounded, and `D2I` truncates toward zero, which is
Java's `/`. At the int32 lane this is an A/B against the shipped magic. At the
long lane it is the only route there is - no 64-bit magic exists without a
128-bit product - and 1.1 names it as "the way `hour(t)`, `minute(t)`,
`time_trunc` and the interval extracts are computed at all". Tasks 102 and 103
both wait on it, which is why it is next after 29.

## 2. The admission check, done

Section 2.19 said the check "owes the constant" and that its exactness claim was
sampled. Both are now settled by `verify_double_division.py` beside
`verify_long_lane_magic.py`, run on 17 September 2026 (about three minutes). It
checks two lowerings separately, because they turn out not to be equally exact:

    RECIP   trunc(v * fl(1/d))   two roundings, a multiply
    DIV     trunc(v / d)         one rounding, a divide

**The constant.** RECIP's relative error is at most `2^-52` (two roundings of
`2^-53` each, plus a `2^-106` cross term); DIV's is `2^-53`. A non-multiple's
true quotient lies at least `1/d` from every integer, so truncation cannot cross
one while `|v| * 2^-52 < 1/d * d`, that is while `|v| < 2^52` for RECIP and
`|v| < 2^53` for DIV. The bound is on the *dividend* and does not depend on the
divisor - which is what 2.19's "divisor below 2^21" shorthand obscured, and
what the earlier draft's "fits a double" shorthand did not state at all.

**Where the argument has a hole, and the check found it.** At an exact multiple
`v = k*d`, DIV returns `k` exactly. RECIP computes `fl(k * fl(1/d))`, and when
`fl(1/d)` rounded *down* the product can land on the double just below `k`, so
truncation returns `k - 1`. Whether that happens depends on the divisor's own
reciprocal against the binade of every `k` in range - no argument settles it.
The textbook case is `49 * fl(1/49) = 0.9999999999999999`, which the script
runs first as the control that the check can fail.

**The results, exhaustive over every int32 range and at every multiple's
neighbourhood where a range is too wide to sweep:**

| division | dividend range | RECIP | DIV |
|---|---|---|---|
| era `/146097`, narrowed | `w < 2^24` | **wrong at 146097** | exact |
| era `/146097`, total | the biased int32 range | **wrong at 146097** | exact |
| century `/36524`, year `/365`, Julian `/1461` | as `VarkaChrono` states | exact | exact |
| month `/153`, day `/5`, quarter `/3`, week `/7`, day of month `/2141` | as stated | exact | exact |
| year century `/100`, `/400` | the biased year | exact | exact |
| year-month `/12`, `/3` | **all of signed int32** | exact | exact |
| `hour`, `minute`, `second` of `TIME` | `[0, 8.64e13)`, every multiple | exact | exact |
| `time_trunc` to ms, us | the same, sampled multiples | exact | exact |
| interval extracts `/8.64e10 .. /1e6` | `[-2^52, 2^52]` | exact | exact |

Three things follow, and the first is a correction to 2.19.

1. **The reciprocal form is out for the era step.** `146097 * fl(1/146097)`
   truncates to 0. Section 2.19's sampled check tried 12, 3, 7 and 100 and
   generalised; the one divisor the calendar most wanted to free from its
   narrowing is the one the cheap form gets wrong, at the first multiple. The
   era step, if it moves to doubles at all, moves to the divide.
2. **`/12` and `/3` are exact over the whole signed int32 range in both forms.**
   `extract(YEAR FROM ym)`, `extract(QUARTER FROM ym)` and `CAST(ym AS INTERVAL
   YEAR)` need no bound and no guard - task 68's deferral and task 89's blocker
   are removed by either lowering.
3. **The `2^52` bound is real, not slack.** Searched upward from `2^52`, RECIP's
   first wrong quotient for `/6e7` is at `v = 2^52 + 3549464112629503`, below
   `2^53`; the other three interval divisors fail nowhere below `2^53`. So the
   bound is per-divisor in practice and `2^52` in the argument, and the plan
   keeps the argument's number: a day-time interval beyond `2^52` microseconds
   (about 52,000 days) is the recorded decline 2.38 already anticipates.

**What the check would have rejected.** A DIV failure anywhere inside a stated
range - there was none - or a divisor at or above `2^21`, which Varka has none
of. What it did reject is narrower and more useful: one form for one divisor.

**Op counts, from the shapes rather than the tool** - `dev/varka_emit.sh
--table` prints counts for lowerings that exist, and this one does not yet, so
these are the registered expectation and the first emitter commit owes the
measured ones. At sixteen int lanes one division is: the shipped narrowed magic,
a multiply, a shift, a compare and a blend for the correction, four ops plus the
guard's compare-and-OR; the double route, two `convertShape(I2D)` for the two
eight-lane halves, two multiplies (RECIP) or two divides (DIV), two
`convertShape(D2I)` and a join of the halves, seven ops and no guard. At eight
long lanes the halves disappear: one `L2D`, one multiply or divide, one `D2L`,
three ops. Under `-XX:UseAVX=2` the conversions are the magic-number form of
section 6 (or, reinterpret, subtract; add, reinterpret, mask), which is four ops
in place of each convert.

## 3. The design

### 3.1 The mechanism

**A third lane the emitter can convert into, not a third lane it computes on.**
`VarkaLoopEmitter.Lane` gains `DOUBLE` as a *conversion target* only: no IR node
is on the double lane, no column is loaded into it, and nothing is stored from
it. A division node - the calendar prefix's steps today, the year-month extracts
and the long-lane `TIME` and interval divisions once 102 and 103 exist - emits
convert-in, one arithmetic op, convert-out, all inside the node's own lowering,
so the descriptor's `laneType()` of every IR node stays what it is. That is what
keeps task 85's oracle honest: the int32 bytes of every shape that keeps the
magic do not move.

**Two forms behind one switch, the magic kept as the reference variant.**
`VarkaEmitOptions` gains `division`, an enum `{ MAGIC, DOUBLE_RECIP, DOUBLE_DIV
}` with the default `MAGIC`, on the `FloorMod7` precedent: the shipped lowering
stays live under the same tests and the same A/B, and the default flips only in
the last commit if the numbers say so. Per divisor the emitter refuses
`DOUBLE_RECIP` where section 2 found it inexact - the era step - by a table the
verification script's output is pasted into, not by a formula; a divisor absent
from the table is emitted with `DOUBLE_DIV`.

**The conversions.** At the int lane, `IntVector.convertShape(I2D,
DoubleVector.SPECIES_<2*bits>, part)` for `part` 0 and 1 splits sixteen ints
into two eight-lane doubles; `D2I` back with the same parts and a join. At the
long lane `L2D`/`D2L` are same-width lanewise converts. Under `-XX:UseAVX=2`
the long converts do not intrinsify (section 6 of the milestone plan, measured)
and the emitter selects the magic-number form: `(v | 0x4330000000000000)`
reinterpreted and less `2^52` in, `+2^52`, reinterpret and mask out, with the
compare-and-blend that turns round-to-nearest into floor. The selection is by
the JVM's `UseAVX` at emission, one lowering per class, which the shape cache
already keys on the emit options; whether it becomes one lowering everywhere is
the timing task 121 owes.

**Truncation is not floor.** The double route truncates toward zero, which is
Java's `/` and every calendar division's need over their non-negative
dividends, and `extract(YEAR FROM ym)`'s over both signs. It is *not* what the
`floorMod7` shapes need, whose dividend is signed and whose result is a
remainder; those keep their own lowering, which already has a `DIV` reference
variant priced in the parity file at roughly an eighth of the magic. A remainder
at full width is 2.19's stated rejection case and stays one.

### 3.2 What is deliberately unchanged

* **The calendar prefix's magics stay the default.** Prediction 6.1.1 says the
  divide loses to them by a wide margin on the shapes that matter; this task
  makes the alternative real and measured, and does not flip a default on the
  strength of a simplification.
* **The `floorMod7` lowerings**, and any shape that needs the remainder.
* **The IR.** No new node: a division is a property of the nodes that already
  divide, and their lowering changes behind the switch.
* **The compiler's year-month extract arms** - admitting `extract(YEAR FROM
  ym)` and `CAST(ym AS INTERVAL YEAR)` is task 89, which this task unblocks.
* **`TIME` and interval expressions** - 102 and 103 consume the lowering; this
  task ships it and proves it, over the dividend ranges they will feed it.
* **The `2^52` bound for intervals.** A dividend that may exceed it is the
  recorded decline of 2.38; this task does not attempt a wider exact form.

### 3.3 Registered op counts

Per section 2's last paragraph: at sixteen int lanes, magic four ops plus the
guard, double route seven and no guard; at eight long lanes three, or nine under
the AVX2 conversion. The first emitter commit replaces these with
`dev/varka_emit.sh --table` for `year(d)` and `extract(YEAR FROM ym)` under all
three settings and asserts them in `VarkaLoopEmitterSuite`.

## 4. Files

| file | what |
|---|---|
| `sql/varka/plans/verify_double_division.py` | the admission check (this PR) |
| `VarkaEmitOptions.java` | the `division` switch and its enum |
| `VarkaLoopEmitter.java` | `Lane.DOUBLE` as a conversion target, the convert helpers, the AVX2 form, the per-divisor table, the division emission behind the switch |
| `VarkaLoopEmitterSuite.scala` | the parity matrix under each setting at both widths, the op counts, the refusal of `DOUBLE_RECIP` for the era |
| `VarkaEmittedBytesSuite` / `emitted_bytes.json` | unchanged under `MAGIC`; a regenerated companion only if the default flips |
| `VarkaEmitterParityBenchmark.scala` and its results | the three-arm A/B rows |
| `sql/varka/plans/PLAN_MILESTONE_5.md` | 2.19's correction, row 88 |

## 5. Tests, and what each is for

1. **The verification script**, committed and run once here; its table is
   what the emitter's per-divisor refusal is transcribed from.
2. **The parity matrix under each `division` setting**, the existing calendar
   shapes at both vector widths against the reference evaluator: catches a
   conversion that saturates, a wrong `part`, a join that drops a half.
3. **The era step under `DOUBLE_RECIP` is refused**, with a reason naming the
   divisor: pins section 2's finding in code, so a later edit that "simplifies"
   the table to a formula fails here.
4. **The AVX2 form's parity**, run under `-XX:UseAVX=2` in the same suite
   (the gate's narrow width is `MaxVectorSize`, which is a different knob, so
   this is a third invocation), over the `TIME` divisors at their whole range's
   multiples.
5. **The op counts** of 3.3 asserted, so the seven-op expectation is a test.
6. **The oracle**: `VarkaEmittedBytesSuite` green with no regeneration while
   the default is `MAGIC` - the same admission rule task 85 held itself to.

## 6. The measurement

The three-arm A/B 2.19 asks for, on `VarkaEmitterParityBenchmark`'s calendar
shapes at both widths: `MAGIC` (shipped), `DOUBLE_DIV`, and - where exact -
`DOUBLE_RECIP`, with task 65's int64 widening as a fourth arm only if 65
re-enters from milestone 6. Plus one row that only the double route can serve at
all: `extract(YEAR FROM ym)` over a full-range month count, whose baseline is
the row engine because the magic declines it. The long-lane rows arrive with 102
and 103's benchmarks, not here.

### 6.1 Predictions, registered before the run

1. **The divide loses to the magic on the calendar prefix by at least 3x** on
   the division alone, at AVX-512: `vdivpd` on a 512-bit vector has a
   throughput of one per several cycles where the magic's multiply and shift
   pipeline every cycle, and the parity file already prices the `floorMod7`
   `DIV` variant at an eighth of its magic. The default stays `MAGIC` for the
   prefix.
2. **RECIP is within 0.7x to 1.0x of the magic where it is exact** - the
   converts cost what the guard and correction cost - so `DOUBLE_RECIP` is the
   form that could displace a magic, and only where the table admits it.
3. **`extract(YEAR FROM ym)` fuses at more than 5x the row engine** under
   either double form, because its baseline is a decline: the number is the
   lane against Spark, not one lowering against another.

## 7. Risks

1. **The half-join.** `D2I` of an eight-lane double back into a sixteen-lane int
   species leaves half the lanes to fill from the second part; if the Vector
   API's `convertShape` with parts does not compose into one vector without a
   `rearrange`, the seven-op count is optimistic and prediction 2 fails on the
   join, not the arithmetic. Test 5 measures it; the op count is what says.
3. **Saturation and negatives.** `D2I` saturates out of range and `D2L`
   likewise; every dividend here is inside range by construction, and the
   signed cases (`/12`, `/3`) truncate toward zero as Java does. Test 2's
   matrix carries `Int.MinValue` and the extremes, as task 63's did.
4. **A divisor the table does not know.** A future division emitted with
   `DOUBLE_RECIP` and no entry silently takes `DOUBLE_DIV` by 3.1's rule; the
   cost is speed, never correctness. Recorded so nobody reads the fallback as
   a bug.
5. **Two conversions the JIT may not fuse.** If C2 keeps the two halves' `I2D`
   in registers well, the double route's cost is the divide; if it spills, the
   converts dominate. `dev/varka_emit.sh --asm` on the dense loop, before any
   timing, as the project's rule is.
6. **This plan assumes #240.** The long-lane half needs 29's leaves to have
   anything to divide; the int32 half - the switch, the converts, the era's
   refusal, the year-month extracts - does not, and can start first.

## 8. Sequencing

1. **This PR:** the script and its output in section 2, 2.19 corrected, the
   row planned. No code.
2. The `division` switch, `Lane.DOUBLE` as a conversion target, the int32
   converts and the per-divisor table, with the calendar prefix's steps emitted
   under `DOUBLE_DIV` and `DOUBLE_RECIP` behind it; the parity matrix under each
   setting; the op counts measured and asserted. Oracle green, default `MAGIC`.
3. The long-lane converts and the AVX2 magic-number form, selected by `UseAVX`
   at emission, with the `TIME` divisors' parity at both AVX levels - after
   #240 merges.
4. The A/B on the parity benchmark, results committed, 6.1 scored, and the
   default per divisor decided from the numbers; task 89 opens on it.

## 9. Outcome

*To be written when the work lands, section by section as the plan's own rule
asks. Nothing above is to be rewritten to look prescient; a correction is added
and says what it corrects.*
