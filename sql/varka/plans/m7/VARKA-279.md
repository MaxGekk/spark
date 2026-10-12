# VARKA-279: Declines as specified behaviour

## 1. Where this came from

Row 279 of `m7/PLAN.md` (described under "Declines as specified behaviour" in its section on the
test rows), from `m7/READING.md` 11's reading of Korat (a precondition for every behaviour),
Alive2 (refinement: decline wherever Spark raises, equal answers elsewhere) and TLP (a predicate,
its negation and IS NULL partition the rows). The wave table orders it after row 138, whose
coverage-row harness it extends.

A Varka kernel may refuse a batch - set a status bit, and the batch falls back to Spark's row
engine - and the guards that refuse are where a kernel's correctness has a boundary: inside the
range a guard admits the lowering is exact, outside it the batch must go to Spark. Today that
boundary is tested by hand, guard by guard:

* **The fuzzers stay away from it.** `VarkaReferenceEvaluator` has no notion of a declined batch:
  `GuardedDay` and `GuardedRange` pass their child through, `make_date`'s year limit is "the
  kernel's business, not the oracle's", and a failing overflow has no expected value
  (`VarkaReferenceEvaluator.scala`, around lines 51, 143, 169 and 242). `VarkaKernelCheck` takes a
  boolean `declineAllowed`, so a check can say "a decline is fine here", never "a decline must
  happen here, with this bit".
* **The coverage table says nothing about domains.** `sql/varka/coverage.json` has a free-text
  `note` per row ("a column offset, under a per-batch bound on its range"), and
  `VarkaCoverageDifferentialSuite` keeps its inputs inside the guards by hand.
* **The edges are pinned per test.** `VarkaEmitterChronoSuite`'s month-count test is the clearest
  case - the bound and one past it, on both sides, in a masked lane and an epilogue lane - but no
  table drives such tests, and no coverage row is ever run at its domain's edge.

So a guard whose bound is off by one, or a guard missing from a shape, is found only if someone
wrote that shape's edge test.

## 2. The admission check, done

Run on 11 October 2026 with a throwaway probe (`VarkaGuardProbeSuite`, not committed), which
compiles every coverage row under the default options and reads the guards its kernel carries:
the `Analysis` refusals the emitter places (`VarkaLoopEmitter.refusalsForTest`) and the per-batch
input bounds the compiler asks for (`inputBounds`).

* **The guard inventory is seven, as listed.** The six `Analysis.Refusal` reasons (day producer,
  month count, `make_date`, int overflow, day range, value range) and the input bound on a
  day-interval column (`DeclineSink.bound`, status `1 << 8`). Nothing else in the emitter or the
  compiler declines a batch.
* **Which rows carry which guard.** Of the 92 rows, 15 carry a guard: int overflow 8 (`i + 1`,
  `i - 1`, `-i`, `year(d) + i`, `abs(ym)`, `ym - ymm`, `extract(YEAR FROM ym) - 1`, and `d - ym`
  with month count as well), value range 3 (the two `time_from_*` round trips and `t + INTERVAL`),
  month count 3 (`add_months(d, i)`, `d + ym`, `d - ym`), `make_date` 1 (`make_date(2021, i, 1)`)
  and the input bound 1 (`d + CAST(i AS INTERVAL DAY)`). No row carries the day-producer guard,
  which the default options leave off, or the day-range guard, which the compiler inserts only
  where a day producer feeds a calendar node; those two are reached by composed shapes, so 3.2
  drives them on IR, not through the table.
* **The notes do not say.** Only 4 of the 15 guarded rows' notes mention their guard.
* **One open question.** `date_add(d, i)`'s note says "a column offset, under a per-batch bound
  on its range", yet the row's kernel carries neither a refusal nor an input bound. Either the
  note is stale or the bound is held elsewhere; the domain work of 3.3 answers it first, and if
  Spark can raise where the kernel answers, it becomes its own row with a reproducer.
* **Each guard's edge, from the tests that pin it.** Month count both sides of both bounds in a
  masked and an epilogue lane (`VarkaEmitterChronoSuite`, around line 1403); `make_date` one past
  each year limit (`VarkaChronoSuite`, around 284); int overflow under FAIL
  (`VarkaEmitterArithmeticSuite`, around 504); value range (`VarkaEmitterLongLaneSuite`, around
  278); day range on the composed day (`VarkaEmitterChronoSuite`, around 100); day producer one
  past each narrow bound (around 1281); the input bound one past its limit end to end
  (`VarkaDifferentialSuite`, around 901). They pass in the gate, so the edge table of 3.2 starts
  from bounds known right.

## 3. The design

### 3.1 A verdict per row

The verdict of a kernel on one row of input, decided from the inputs and the guard bounds before
the kernel runs:

| verdict | when | what the kernel must do |
|---|---|---|
| **answer** | every guard on the row's path admits its value | status 0 for a batch of such rows, and the reference's answer |
| **must decline** | a guard on the taken path rejects its value, or Spark raises (ANSI) | the guard's exact status bit (VARKA-304's exact refusal status) |
| **may decline** | a guard rejects a value on an arm the row does not take, and the guard is not qualified by its arm | either status 0 with the reference's answer on the taken arm, or that guard's bit |
| **outside the contract** | an input outside what the compiler admits (a date outside 0001-01-01 to 9999-12-31, the column contract) | nothing checked; such rows are never generated as a test of the kernel |

A batch's verdict is the strongest of its rows': one must-decline row makes the batch
must-decline. A null lane is never a reason to decline: a guard reads only valid lanes.

The reference evaluator learns the verdict rather than the kernel's answer: a function
`VarkaGuardSpec.verdict(roots, row)` beside it, walking the tree with the same values the
reference computes and applying each guard's bound where the IR carries the guard. Its bounds are
the constants the emitter and compiler read (`VarkaChrono`'s ranges, the month-arithmetic limits,
the `make_date` years, the value ranges), not copies, so the verdict cannot drift from the code by
someone editing one side.

### 3.2 Both sides of every bound

For each guard, inputs at its bound and one past it on both sides: `lo - 1`, `lo`, `hi`, `hi + 1`,
in a full vector lane and in the epilogue lane, valid and null, through both bodies. The kernel's
status is checked against the verdict, and where the verdict is answer, the rows against the
reference. This replaces nothing: the existing hand-written edge tests stay as the readable
examples, and this table is what makes every guard have them.

### 3.3 Each guarded coverage row names its domain

A coverage row that can fire a guard gets a `domain`, a SQL predicate over its columns
(`date_add(d, i)`: `i BETWEEN <lo> AND <hi>`), carried into `coverage.json` and the docs table
beside the note. `VarkaCoverageSuite` fails a row that fires a guard and has no domain, as it
fails an undocumented expression today. Then, as TLP partitions rows, each such row runs end to
end under its domain predicate, its negation and `IS NULL`:

* under the domain: fused, and the residual and fallback metrics say nothing declined;
* outside it: Spark's answer, whether Varka declined or Spark raised the same error;
* under `IS NULL`: fused, nothing declined.

That is the test-side counterpart of row 245, which proves guard completeness; this checks it on
the code as built.

### 3.4 What is deliberately unchanged

* The guards and their bounds. A bound found wrong is its own row with a reproducer.
* `VarkaKernelCheck.runAndCompare`'s signature: the boolean stays for its callers, and the verdict
  is a new entry point beside it.
* Row 138's three test forms, which this extends rather than repeats.

### 3.5 Registered op counts

None; no emitted class changes.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/test/java/.../codegen/varka/VarkaGuardSpec.java` | the verdict, from the guards' own constants |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaKernelCheck.scala` | a check against a verdict, with the expected bit |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaGuardEdgeSuite.scala` | 3.2: every guard, both sides of its bound |
| `sql/catalyst/src/test/scala/.../codegen/varka/VarkaCoverageSuite.scala` | the `domain` field, required for a guarded row |
| `sql/core/src/test/scala/.../execution/VarkaCoverageDifferentialSuite.scala` | 3.3's three partitions per guarded row |
| `sql/varka/coverage.json`, `docs/sql-varka.md` | the generated domain column |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-279.md` | the row and this plan |

## 5. Tests, and what each is for

* **The edge suite** (3.2): a guard whose bound is off by one in either direction fails it, for
  every guard, not only those someone wrote a test for.
* **A planted wrong bound**: a test that moves one guard's bound by one through a test hook and
  requires the edge suite to fail, so the suite cannot pass vacuously.
* **The domain requirement**: a coverage row that fires a guard without a domain fails
  `VarkaCoverageSuite`; checked by a planted row.
* **The partitions** (3.3): a guard missing from a shape shows as a wrong answer outside the domain;
  a guard firing where it should not shows as a decline inside it.
* **The verdict itself**: a test that `VarkaGuardSpec` agrees with the existing hand-written edge
  tests on every case they pin.

## 6. The measurement

No benchmark. The suites' times are recorded in 9; the edge suite is small (a few inputs per
guard), the partitions are three runs per guarded row.

### 6.1 Predictions, registered before the run

1. **Every guard's edges hold.** The guards' bounds were derived and swept when they were written
   (`VarkaChronoSuite` pins the `make_date` limits and the era identity's edge).
2. **The domain requirement finds rows whose note does not say they are guarded**: at least one
   coverage row fires a guard its note does not mention, since the notes are written by hand.
3. **The may-decline verdict is rare in the coverage rows**: arm qualification covers most guards
   under a conditional; it shows in the composition fuzzer's trees, not the table's.

## 7. Risks

1. **A verdict that copies the bug.** If the verdict computed a guard's value the way the kernel
   does, a wrong lowering would agree with itself. The verdict computes values with the reference
   evaluator, which shares no lowering with the emitter; only the bound constants are shared, and
   those are what the edge suite tests.
2. **Rows outside the contract leaking in.** Edge inputs for one guard can push another input
   outside the contract; such rows are dropped by the verdict, and the suite counts them.
3. **Order with row 138.** This extends 138's harness; if 138 is not merged when this starts, 3.3
   waits for it and 3.1 and 3.2 go first.

## 8. Sequencing

1. This plan, with 2 done, and the row marked Planned.
2. The verdict and the edge suite (3.1, 3.2), with the planted wrong bound.
3. The domain column and the partitions (3.3), after row 138.
4. Section 9.

## 9. Outcome

### 9.1 The verdict and the edges (8.2), 12 October 2026

Steps 3.1 and 3.2 landed; 3.3, the coverage rows' domains and their three partitions, waits for
row 138 as 7.3 and 8 said, so the row stays Planned.

**What was built.** `VarkaGuardSpec` gives the verdict on a batch from its rows and the kernel's
own guards: `VarkaLoopEmitter.refusalsForTest` for which nodes refuse, and a new
`refusalArmsForTest` for the arm chain `Analysis.armChainOf` qualifies each by, so the verdict
applies the emitter's own rule rather than a reconstruction of it. A fired guard qualified by an
arm chain must decline where the chain selects the row and must not elsewhere; an unqualified one
must decline where the row reaches the node through arms it takes, and may where it reaches it
only through arms it does not. Its bounds are `VarkaChrono`'s constants, the lane's range and a
`GuardedRange`'s own `lo` and `hi`; its values the reference evaluator's.
`VarkaGuardEdgeSuite` holds sixteen small kernels, which carry the six guards between them, to it.

**The edges.** 248 batches: every guard at `lo - 1`, `lo`, `hi` and `hi + 1` (for `make_date`'s
date, either side of February's last day in a leap year and not, and month 12 against 13), at row 0
of 17 (a full vector) and row 16 (the epilogue), the edge row valid and, in a second batch, null in
the column that carries it, each through both bodies. 116 declined; every status agreed with the
verdict for its body under `exactRefusalStatus`, and every answered batch matched the reference
evaluator row for row. Moving each end of each guard's bound by one in either direction on the
verdict's side turned some edge against it, all 24 shifts. Under an arm no row takes, the guard
qualified by it stayed silent, and with `guardUnderArm` off it declined, which the verdict allows.
The input bound admitted `-INTERVAL_DAY_LIMIT_DAYS` to `INTERVAL_DAY_LIMIT_DAYS` and refused one
past, never on a null, and its own shifted bound was caught. The verdict agreed with the sixteen
cases the hand-written edge tests pin. The suite runs in about half a second, and the gate passed
(lint on a rerun, after one scalastyle fix in the verdict).

**Prediction scored.** 1, **held**: every guard's edges hold, at both lanes. Predictions 2 and 3
are about the coverage rows and wait for 3.3, as does 2's open question: `date_add(d, i)`'s note
still promises a bound its kernel does not carry.

**Corrections to the plan.**

* **The planted wrong bound moves the verdict's bound, not the emitter's.** The emitter reads its
  bounds as `static final` constants, which javac inlines into its bytecode, so a test hook there
  would be a change to the emitter the plan did not price. Moving the verdict's bound proves the
  same thing: the edge inputs land exactly on each bound and one past it, so an emitter off by one
  would disagree with the verdict on the same rows.
* **`VarkaGuardSpec` is Scala**, beside the reference evaluator it drives, as `VarkaKernelCheck`
  is; 4's table had it Java.
* **`VarkaKernelCheck` needed no verdict entry point.** VARKA-278 added `runAndCompareStatus`,
  which returns the status; the suite holds it to the verdict itself.
* **The fourth verdict, outside the contract, is not computed.** The IR does not say which columns
  are dates, so the edge suite keeps every input inside the contract by construction instead.
* **The input bound is tested at its check.** It has no IR node: the evaluator declines the batch
  through `IntRangeOps.allWithin` before the kernel runs, and the suite drives that.
* **2 was wrong that the defaults leave the producer guard off.** `guardDayProducers` is on by
  default; the coverage rows carry none because no row puts a column-offset producer under a
  calendar node. The suite turns both optional guards on over whatever the option matrix sets, so
  a configuration without them still tests the guard.
* **A guard's count is by guard, not by kernel**: `make_date`'s NULL form answers an invalid day
  with a null and never declines.
