# VARKA-293: Reject Long.MIN_VALUE microseconds in a literal interval's one-day guard

## 1. Where this came from

Row 293 of `m7/PLAN.md`, found by the review of #663 (the TIME family's port): the guard on a
literal interval in `timeAddInterval` was `Math.abs(micros) > MICROS_PER_DAY`, and the absolute
value of `Long.MIN_VALUE` is `Long.MIN_VALUE`, negative, so that one interval passed the guard. It
was in the Scala before the port.

## 2. The admission check, done

Reproduced end to end on `master` (`363a1280790`) before the fix, with the two tests below
written first and failing: `SELECT t + INTERVAL '-106751991 04:00:54.775808' DAY TO SECOND`, the
smallest day-time interval (`Long.MIN_VALUE` microseconds), raises `DATETIME_OVERFLOW` on the row
engine and returned an answer under Varka. The mechanism: `micros * 1000` with `micros = -2^63`
is `-2^66 * 125`, which is 0 modulo 2^64, so the kernel added nothing and answered every time
unchanged, and the sum guard (the time must stay in the day) had nothing to catch. Spark's
`multiplyExact` throws on every row.

The other `Math.abs` in the compilers were read for the same edge and none is exposed: the range
analysis takes `abs` of an int-lane slot's value held in a long (never `Long.MIN_VALUE`), the
division lowering of a constant divisor (the constructor rules the extreme out), and the
decomposition bound check of guard bounds the compilers write themselves.

## 3. The design

The guard is a range: `micros < -MICROS_PER_DAY || micros > MICROS_PER_DAY`. A day either way stays
admitted, as before (the sum is then checked against the day by the kernel's guard).

### 3.2 What is deliberately unchanged

The column path (`GuardedRange` on the interval column) and the emitted bytes: no shape the
compiler admitted before is declined except the one interval, and `emitted_bytes.json` and
`coverage.json` are unchanged.

### 3.3 Registered op counts

None.

## 4. Files

| file | what |
|---|---|
| `VarkaTimeCompiler.java` | the guard as a range |
| `VarkaExpressionCompilerSuite.scala` | the compiler declines `Long.MIN_VALUE` and the intervals beyond a day, and admits a day either way |
| `VarkaTimeArithmeticSuite.scala` | the end-to-end test: Varka raises Spark's error |

## 5. Tests, and what each is for

* **The compiler test** pins the decline for `Long.MIN_VALUE`, `Long.MIN_VALUE + 1`, a microsecond
  past a day either way and `Long.MAX_VALUE`, and the admission of exactly a day either way. It
  fails on the old guard at `Long.MIN_VALUE`, and only there.
* **The end-to-end test** is the one that would have caught it in production: the same query on
  both engines, the same error. It fails on the old code with "no exception was thrown" from the
  Varka session while the row engine raised.

## 6. The measurement

None; a guard on a compile-time constant.

### 6.1 Predictions, registered before the run

1. Both new tests fail before the fix and pass after; nothing else in the TIME, coverage or
   emitted-bytes suites changes.

## 7. Risks

1. **A shape that fused is declined now.** Only `Long.MIN_VALUE`; no real query writes that
   interval as a literal.

## 8. Sequencing

The tests first, failing; the fix; the suites.

## 9. Outcome

Done on 9 October 2026. Prediction 1 held: the compiler test failed on the old guard with
`-9223372036854775808: None` (no decline), the end-to-end test with "Expected exception
SparkArithmeticException to be thrown, but no exception was thrown" from the Varka session; both
pass after, with 146 compiler, emitted-bytes, coverage and family-chain tests and 105 TIME and
coverage-differential tests passing, scalastyle and checkstyle clean.
