# VARKA-321: The censuses and probes in Java

## 1. Where this came from

Row 321 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 320 and 322 (`PLAN.md`
2.3, "The rest of the test code to Java"). Five standalone programs are Scala for no reason but
when they were written: they extend no ScalaTest base they need, drive no Scala oracle, and their
callers are scripts and a suite or two.

| program | lines | run by | writes |
|---|---:|---|---|
| `VarkaWordCensus` (catalyst) | 403 | `dev/varka_word_census.sh` | a report of each value root's validity-word work over grammar shapes |
| `VarkaCodegenGiveUpCensus` (core) | 363 | a suite, read by `dev/varka_codegen_report.py` | `sql/core/benchmarks/VarkaCodegenGiveUps-jdk25-results.txt` |
| `VarkaMethodSizeCensus` (core) | 254 | a suite, beside the one above | `sql/core/benchmarks/VarkaCodegenMethodSizes-jdk25-results.txt` |
| `VarkaInliningCliffProbe` (catalyst) | 230 | `dev/varka_inlining_cliff.sh`, `VarkaInliningCliffSuite` | `sql/catalyst/benchmarks/VarkaInliningCliff-jdk25-probe.txt` |
| `VarkaKernelWarmupProbe` (catalyst) | 196 | `VarkaKernelWarmupSuite` | `sql/catalyst/benchmarks/VarkaKernelWarmupProbe-jdk25-probe.txt` |

The row is done when the five are Java, every script and suite that runs them runs them unchanged,
and each has shown it prints what the Scala form printed.

## 2. The admission check, done

**Two of the five are suites.** `VarkaCodegenGiveUpCensus` and `VarkaMethodSizeCensus` are each a
`SparkFunSuite` - the suite runs Spark's own SQL golden-file suites and counts what it sees - with
a companion object holding the counting and the report. A suite stays Scala until row 312 lets a
suite be JUnit, so for these two the object moves to Java and the suite becomes a thin wrapper
over it, as row 268 did.

**Their proofs differ by what they print.**

* `VarkaWordCensus` and the two codegen censuses are deterministic: the same input prints the same
  bytes. The word census is cheap (20,000 grammar shapes by default); the codegen censuses run
  Spark's golden-file suites, hours for the whole corpus.
* The two probes print timings, which no two runs share; what they establish is a verdict - a fork
  fast or slow, a warm-up's method compiled or not - and the suites behind them pass or fail on it.

## 3. The design

Each program keeps its fully qualified name, its arguments and its output format, so
`dev/varka_word_census.sh`, `dev/varka_inlining_cliff.sh`, `dev/varka_codegen_report.py` and the
suites take it unchanged; a script that looks a class up by its `.scala` file learns the `.java`
one, as row 224 found for the benchmark scripts. Records for what they carry, an exhaustive
`switch` where they branch on the IR, and no `default` on a sealed switch, as the rules ask.

* **`VarkaWordCensus`, `VarkaInliningCliffProbe`, `VarkaKernelWarmupProbe`**: Java classes with a
  `main`, in catalyst's test tree, in place of the Scala objects.
* **The two codegen censuses**: their objects' work as Java classes in core's test tree; each
  suite keeps its name and becomes a wrapper that calls it.

## 4. Files

| file | what |
|---|---|
| the five programs' Java classes | the ports |
| `VarkaCodegenGiveUpCensus.scala`, `VarkaMethodSizeCensus.scala` | the thin suites |
| the scripts that resolve a class by its source file | taught the `.java` file, where one does |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-321.md` | the row and this plan |

## 5. Tests, and what each is for

* **The word census, byte for byte.** `dev/varka_word_census.sh` at its defaults and at a second
  seed, on the Scala form and on the Java one the same hour: the same report, byte for byte.
* **The codegen censuses, byte for byte on a slice.** Both censuses over the same slice of Spark's
  golden-file suites - a few hundred plans - on the Scala form and on the Java one: the same
  results file, byte for byte. The whole corpus is not rerun; its committed files stand.
* **The probes, by verdict.** `VarkaInliningCliffSuite` and `VarkaKernelWarmupSuite` pass on the
  Java forms, and one run of `dev/varka_inlining_cliff.sh` on a fast arm and a slow arm gives the
  committed file's verdicts for them.
* **The gate**, at both widths.

## 6. The measurement

None beyond 5's comparisons: the row changes no kernel and no timing a benchmark reads.

### 6.1 Predictions, registered before the run

1. **The word census and the codegen censuses print the same bytes** as the Scala forms.
2. **The probes give the same verdicts**, and their suites pass.
3. **The Java comes to about 1,700 lines** against the Scala's 1,446, as the earlier ports grew by
   a fifth to a half.

## 7. Risks

1. **A census that counts differently in translation** - an iteration order over a hash map, a
   number format - changes a report silently. 5's byte comparison catches it; an order that
   depended on a Scala map's iteration is made explicit (sorted), and the Scala form's order is
   what the Java one must reproduce.
2. **A probe that measures differently** - an allocation added to its timed loop - moves its
   verdict near a boundary. 5's fast and slow arms sit far from it; the timed loops are ported
   statement for statement.

## 8. Sequencing

1. This plan, and the row marked Planned.
2. The three standalone programs, then the two censuses with their suites, each with its
   comparison.
3. Section 9.
