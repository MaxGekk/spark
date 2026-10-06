# VARKA-289: the wide fuzz tests' adequacy by construction

## 1. Where this came from

Row 289 of `m7/PLAN.md`, found by the idle-window fuzz campaign of 6 October 2026 on master
`4be7aeabf60`. Of 162 composition-fuzzer seeds, 4 (2026100605003, ...05049, ...05103,
...05125) failed the wide test's adequacy assertion with 0 kernels compared, and 1 of the IR
fuzzer's 92 seeds (2026100602011) failed the wide test's reach assertion for "a group halved on
bytes". None was a wrong answer: about 9.2 million IR trees agreed with the reference evaluator.

## 2. The admission check, done

**Why the composition seeds compared nothing.** `VarkaCoverageCompositionFuzzSuite.checkKernel`
returned before emitting any kernel that is not on the int lane, so no long-lane kernel of a wide
projection was ever compared, under any seed. And an int-lane batch a guard declined was counted,
not compared: master's default seed compared 11 of its 48 kernels (40 int, 8 long). The four seeds
drew projections whose int-lane kernels all declined, and the assertion's exemption covered only
runs below 20 projections, 20 being the default.

A debugging print of the kernels that still declined after a first redraw at a smaller scale
showed the remaining cause: an input with a bound (`VarkaInputBound`, for `CAST(i AS INTERVAL
DAY)`, plus or minus 106,751,991 days) was drawn across the whole bound at every attempt, and
added to a date it passes the date guard every time.

**Why the IR seed missed a mechanism.** The wide test's 10 compositions cycle through 6 option
variants, so the 2000-byte-budget variant, which halves groups on bytes, gets one or two of
them; a seed whose composition there needs no halving reaches the mechanism nowhere.

## 3. The design

### 3.1 The mechanism

- `VarkaKernelCheck` gains `runAndCompareLong` and `LongBatch`, the long-lane check the IR fuzzer
  carried inline in `runOneLong`: 64-bit buffers, the eight-argument `run`, `evalLong`, a narrowing
  root read at four bytes. The IR fuzzer now calls it, so both suites share one long-lane check.
- `checkKernel` compares both lanes. A long-lane input is drawn by its column's type: a `TIME`
  inside the day in nanoseconds, a day-time interval within 2^45 and a `BIGINT` within 2^40 of
  zero. A declined batch is drawn twice more, nearer zero each time, a bounded input clamped into
  its bound, the last attempt within one to three. The test asserts that every kernel of every lane
  was compared, and reports the counts per lane and the batches drawn again.
- The IR fuzzer's wide test, when its compositions missed a mechanism, draws further compositions,
  cycling the variants and checking each row by row, up to six more cycles, and fails only if a
  mechanism is still unreached.

### 3.2 What is deliberately unchanged

The emitter, the compiler and every other test. The int-lane draws of an attempt that compares
are as before, so a seed's first attempt is unchanged.

### 3.3 Registered op counts

None: no emitter change.

## 4. Files

| file | what |
|---|---|
| `VarkaKernelCheck.scala` | `runAndCompareLong`, `LongBatch` |
| `VarkaIrFuzzSuite.scala` | `runOneLong` through the shared check; the wide test draws for a missed mechanism |
| `VarkaCoverageCompositionFuzzSuite.scala` | both lanes compared, declined batches drawn again, the per-lane assertion |
| `m7/PLAN.md`, `m7/VARKA-289.md` | row 289 done, this plan |

## 5. Tests, and what each is for

The two suites themselves, run as plain JVMs on 6 October 2026:

- the five seeds of section 1, each now passing;
- the default seeds and seeds 1 to 8 of the composition fuzzer, at the default width and the
  default seed at `MaxVectorSize=16`;
- the IR fuzzer at its default seed and at 2026100602011, both widths.

## 6. The measurement

Kernels compared on the composition fuzzer's default seed: 11 of 48 on master, 48 of 48 here (40
int, 8 long), with 56 batches drawn again after a decline. Seed 2026100605003: 0 of 50 on master,
50 of 50 here. The IR fuzzer's seed 2026100602011 reaches "a group halved on bytes" after 2
further compositions; the default seed needs none.

## 7. Risks

1. **A drawn-again batch tests less.** The later attempts draw values near zero, which reach fewer
   guards. Only declined batches are drawn again, and the first attempt is the one master ran.
2. **The cap hides an unreachable mechanism for longer.** Six more cycles are about 36
   compositions, a few seconds; a mechanism that is gone still fails the run.

## 8. Sequencing

One pull request: the shared check, both suites, the plan and the row.

## 9. Outcome

Done on 6 October 2026, as section 6 records: every kernel of the wide compositions is compared,
on both lanes, and the five seeds pass.
