# VARKA-320: The fuzz infrastructure in Java

## 1. Where this came from

Row 320 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 321 and 322 (`PLAN.md`
2.3, "The rest of the test code to Java"). Row 311 moved the checks the fuzzers call to Java; what
still ties the fuzzers to Scala is their own infrastructure:

| file | lines | what it is | used by |
|---|---:|---|---|
| `VarkaIrGrammar` | 707 | the random IR grammar, int and long lanes, and the value bounds | the IR fuzzer, the bytes oracle, the series, eight emitter suites, two Java classes |
| `VarkaIrSeries` | 415 | every tree up to a size, in a fixed order | the small-tree and variant suites, the nightly's slices |
| `VarkaShrinker` | 306 | ddmin and the tree, batch and option reductions | the IR and Spark fuzzers, the shrinker suite |
| `VarkaCompositionShrinker` | 138 | the composition fuzzer's shrinker | the composition fuzzer |
| `VarkaFuzzCase` | 78 | one drawn case: roots, columns, batch, options, label | every fuzz suite, the variant check |
| `VarkaFailureSignature` | 70 | a failure's kind and normalised text | the fuzzers, the shrinkers, the known failures |
| `VarkaKnownFailures` | 69 | `sql/varka/fuzz/known_failures.tsv` | the fuzzers |

The row is done when the seven are Java, the Scala objects that callers reach by name are
forwarders, and the fuzzers draw, enumerate, shrink and classify exactly as before.

## 2. The admission check, done

**Two Java classes already call the Scala grammar.** `VarkaEmitCostCorpus` and `VarkaProofFiles`
reach `VarkaIrGrammar` through its static forwarders; after the port they call Java.

**The grammar must draw the same trees from the same seed, call for call.** `VarkaEmittedBytesSuite`
pins the bytes of the shapes `drawShape` and `drawLongShape` draw, so a faithful port leaves
`sql/varka/emitted_bytes.json` byte-identical with no regeneration - the strongest proof any port
in this milestone has. The grammar draws from `scala.util.Random`, which wraps a
`java.util.Random` (`rnd.self`), so the same calls give the same values; and its callers draw
literals, lengths and null patterns from the same generator after the shape, so the forwarder
hands the Java grammar the wrapped `java.util.Random` and the stream continues unbroken.

**The series' order is load-bearing.** The nightly runs it in slices by index, and the variant
suite walks it in order, so the Java series must yield every tree in the same order.

**`VarkaFuzzCase` is the suites' currency, and a Scala case class.** Forty-one `copy(...)` calls
read and rebuild it, most of them in the shrinker. The shrinker cannot be Java while the case is
Scala, and a Scala case class cannot forward to a Java record, so the case becomes a Java record
with a `with` method per field and the forty-one calls become `withX(...)`, a mechanical edit.
`VarkaCompositionCase` (49 lines), which the composition shrinker rebuilds the same way, moves
with it.

**What stays Scala.** The suites, and `VarkaSparkFuzz` in `sql/core`, which calls the shrinker and
drives a Spark session.

## 3. The design

Java classes in catalyst's test tree, records for the data, sealed interfaces and exhaustive
switches where they branch on the IR:

* **`VarkaIrGrammar`**: the int and long grammars over a `java.util.Random`, `Drawn` and
  `DrawnLong` as records, the bounds as they are. A Scala `VarkaIrGrammar` object forwards, taking
  a `scala.util.Random` and passing its `self`, and returns the Scala `Seq`s its callers read.
* **`VarkaIrSeries`**: the two series as classes over iterators, in the same order; a forwarder
  where a caller needs Scala iterators.
* **`VarkaShrinker`, `VarkaCompositionShrinker`**: Java, over the Java cases; ddmin generic over
  a `List`.
* **`VarkaFuzzCase`, `VarkaCompositionCase`, `VarkaFailureSignature`**: records; the forty-one
  `copy` calls become `with` calls, and the case's `label` and option delta keep their text.
* **`VarkaKnownFailures`**: Java, reading the same TSV.

## 4. Files

| file | what |
|---|---|
| the seven files' Java classes, and `VarkaCompositionCase` | the ports |
| `VarkaIrGrammar.scala`, `VarkaIrSeries.scala` | forwarders where callers need Scala types |
| the suites and `VarkaSparkFuzz` | `copy` to `with`, and imports |
| `VarkaEmitCostCorpus.java`, `VarkaProofFiles.java` | the Java grammar |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-320.md` | the row and this plan |

## 5. Tests, and what each is for

* **The grammar, byte for byte.** `VarkaEmittedBytesSuite` passes with no regeneration: every
  drawn int and long shape emits the bytes the oracle pins.
* **The draws continue unbroken.** The IR fuzz suite's random-tree tests at their seeds compare
  the same outputs before and after, read with 311's counter of compared outputs added for the
  measurement, as 311 read it.
* **The series, in order.** Every tree the int and long series yield up to size 5, as canonical
  text, before and after: the same sequence, compared by a throwaway check and recorded in 9.
* **The shrinker, by its outcomes.** The shrinker suite passes, the IR fuzzer's planted bugs shrink
  to the same cases as before, and a planted wrong answer in the reference shrinks to the same
  smallest case and signature on both forms.
* **The known failures** replay as before, and the gate at both widths.

## 6. The measurement

None beyond 5: the row changes no kernel and no benchmark.

### 6.1 Predictions, registered before the run

1. **`emitted_bytes.json` is byte-identical** with no regeneration.
2. **The series yield the same trees in the same order**, and the fuzz suites compare the same
   outputs.
3. **Every planted bug shrinks to the same case**, with the same signature.
4. **The Java is about a third longer** than the Scala, as 311 and 321's were.

## 7. Risks

1. **A draw out of order.** A Scala expression that evaluates its arguments in an order Java does
   not - a `Seq.fill`, a by-name argument, a `match` that draws in its guard - shifts every draw
   after it and changes every shape. The bytes oracle catches it on the first shape it moves.
2. **A collection's order.** `distinct` keeps the first occurrence in both; a `Set` or `Map` whose
   iteration order the Scala form relied on is made explicit. The series check catches an order
   change in the series; the bytes oracle in the grammar.
3. **The forty-one `copy` calls.** A mechanical edit, but each names the fields it changes; a field
   missed in a `with` chain changes a shrink step silently. The planted bugs' shrunk cases are
   the check.
4. **Size.** If the port outgrows one pull request, it lands as 320a (the grammar, the series,
   the case and the signature) and 320b (the shrinkers and the known failures).

## 8. Sequencing

1. This plan, and the row marked Planned.
2. `VarkaFuzzCase`, `VarkaFailureSignature` and the `copy` calls; then the grammar and the series
   with their checks; then the shrinkers and the known failures.
3. Section 9.
