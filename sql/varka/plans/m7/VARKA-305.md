# VARKA-305: The facade's classifier in Java (300b)

## 1. Where this came from

Row 305 of `m7/PLAN.md`, filed by VARKA-300 when it cut row 300 in two (`VARKA-300.md` 2). 300a
made the data model Java records. This row ports the rest of the facade: the classifier
(`compilePartial` and the kernel rounds, the size admission, `compilePredicate` and the split),
with a result record for its triples, a table type for its Scala `LinkedHashMap`s and their
hand-written `truncate`, the size admission in its own class, and the families and `DeclineSink`
off `scala.Option`. It is the last Scala in the facade, which scope item 74.4 describes as
classification, size admission, bisection and several kernels "with triples for results and
shared tables rolled back by hand".

## 2. The admission check, done

Surveyed on 11 October 2026 at `bc8c96fc871`.

| part | size | Scala at the boundary |
|---|---:|---|
| the classifier, `VarkaExpressionCompiler.scala` | 803 lines | its own tables, `Option` results, three tuples and a function-valued case class |
| the five Java families, with `DeclineSink` and `VarkaFamilyArm` | 3,293 lines in the families | 105 `Option<` and 61 `Option.` sites, 137 `LinkedHashMap<` sites |
| the callers of the five entry points | 288 call sites in 39 files | `Option` results and default arguments |

How the families use the two Scala types is narrow. On `Option` they call `get`, `isEmpty`,
`empty`, `apply`, `isDefined`, and `map` four times; on the tables `intern`, `truncate`, `size`,
the keys in order, and a slot's key by position (`literalAt`, which walks the keys). The tables
are both int to int, keyed by a child ordinal (or a derived input's negative key) or by a literal
value, with dense slots in first-occurrence order.

**The cut.** 300 2 put the families' port in this row and gave the reason: doing the families
first "would have a Scala classifier consume Java tables through a bridge 300b deletes". The
survey says that bridge is call-site syntax, not a type: about twenty `.toScala` calls and table
constructions in one Scala file. So one pull request in two commits, each green and each with the
fusion dump identical to the base:

1. **The families off the Scala types**: a Java slot table, `Optional` results, `DeclineSink` on
   both, and the Scala classifier adapted at its family call sites.
2. **The classifier to Java**, deleting those adaptations.

If the second commit alone touches more files than 300a's 35, it becomes its own pull request,
and this section gets a correction saying so.

**The oracle.** `VarkaCoverageSuite`'s opt-in fusion dump (VARKA-300), taken fresh at
`bc8c96fc871`. It calls every entry point: `compilePartial`, `compile`, `declines`,
`compilePredicate` and `explainPredicate`, over every coverage row alone and together, with
forwarded columns, a nondeterministic entry and every row twice, the several-kernel ladders and
a 70-column projection, under seventeen option sets. It prints the input and literal tables
(`in=`, `lit=`), so a table that interned in another order would show. The dump test itself has
to change with the facade's result types; its rendered text must not.

What the check would have rejected: a third pull request for the families alone, and a Java
classifier reading Scala tables.

## 3. The design

### 3.1 The slot table

`VarkaSlotTable`, one small final class for both tables: `intern(int key)` returns the key's
slot, appending it if new; `slot(int key)` returns it or empty; `size()`; `keyAt(int slot)`;
`keys()` in slot order; `truncate(int mark)`, which drops the slots from `mark` on. A map from key
to slot beside a list of keys in slot order, so `keyAt` and `truncate` no longer walk the table.
It replaces `VarkaNodeCompiler.table`, its `truncate` and `intern`, `literalAt`'s walk,
`DeclineSink.inputBounds`' parameter, and the classifier's `Tables` construction. Not a
primitive-specialised map: the tables hold tens of entries.

### 3.2 The families on `Optional`

`compileNode`, `compileCond`, `compileTime`, the chrono and interval families, every arm and
`VarkaFamilyArm.compile` return `Optional<VarkaVectorIR>` (or `Optional<Cond>`), and
`DeclineSink.decline` and `take` return `Optional`. `Option.apply(x)` becomes
`Optional.ofNullable(x)`, its exact counterpart, where a site cannot see that `x` is non-null.

### 3.3 The classifier

A Java `final class VarkaExpressionCompiler` in the same package, so the call sites keep their
text; the Scala file goes in the same commit.

* **Inputs stay Catalyst's**: a projection list, a child output and a condition are Scala trees,
  and the entry points take them as they are. Results are Java: `Optional`, the records, `List`,
  `Map`.
* **The entry points**: `compile`, `compilePartial`, `declines`, `compilePredicate`,
  `explainPredicate`, each with an overload without options for the Scala defaults.
* **`Classified`**, a record for `classify`'s and `classifyOnce`'s triple: the partial, the
  declines, and the entries set aside for another kernel.
* **`VarkaSizeAdmission`**, the size admission in its own class: it asks the shape cache and
  returns `Optional<Decline>` (the outputs to demote, the reason, the plan's cut), and keeps the
  once-per-JVM log of emitter failures.
* **`PredicatePass`**, a record of the specs, the fused conditions and the tables, with a method
  that builds the predicate of a layout, where the Scala case class held a function.

### 3.4 What is deliberately unchanged

Every decision: the order entries and conjuncts are classified in, the rollback, the bisection,
the planned cuts, the split's search, every decline reason. The operand helpers row 86 merges
stay as they are; the families' logic is not touched, only their result and table types.

### 3.5 Registered op counts

None move: nothing in the emitter changes.

## 4. Files

| file | what |
|---|---|
| `VarkaSlotTable.java` | new |
| the five families, `VarkaFamilyArm.java`, `DeclineSink.java` | the result and table types |
| `VarkaExpressionCompiler.java` | the classifier; the Scala file deleted |
| `VarkaKernelEvaluator`, `VarkaFilterEvaluator`, `VarkaFusionReport` | call the class directly, without `MODULE$` |
| `VarkaColumnarRule`, `VarkaColumnarToRowExec`, `VarkaProjectExec` | `Optional` results |
| `VarkaCoverageSuite` | the source scan reads the facade as Java; the dump reads the new results |
| the tests and tools that call the facade or a family | the new types |

## 5. Tests, and what each is for

* **The fusion dump of 2**, byte-identical to the base after each commit.
* **`coverage.json` unchanged**: its admitted-class list is scanned from the facade's source,
  which moves from the Scala patterns to the Java ones.
* **`emitted_bytes.json` unchanged.**
* **`VarkaFamilyChainSuite`**, the golden corpus and the differential suite, unedited in their
  assertions.
* **The gate**, both widths, after each commit.

## 6. The measurement

`VarkaCompileBenchmark`, as 217 did: the pair against the merge base with
`dev/varka_bench_pair.sh`, and the committed results regenerated with `dev/varka_bench_regen.sh`,
on the idle laptop.

### 6.1 Predictions, registered before the run

1. The dump is byte-identical to the base's after each commit.
2. Compile time moves within noise on every shape (within 5%), and the 60-output projection is no
   slower: the slot table's `keyAt` and `truncate` stop walking, which only the wide shape has
   enough entries to show, and nothing else in the classifier changes its work.
3. The Java classifier, `VarkaSizeAdmission` and `VarkaSlotTable` together are under 1.7 times
   the Scala classifier's 803 lines, 267's and 300a's ratio without license headers.

## 7. Risks

1. **`Option.apply(null)` is `None`; `Optional.of(null)` throws.** Each `apply` site becomes
   `ofNullable` unless the value is visibly non-null; the dump's declines would show a change.
2. **A shared map aliased in the bisection.** Scala's `demoted ++ ...` copies; the Java must copy
   too, or a probe's demotions leak into the outer loop. The dump's 47 budget declines and the
   ladders exercise it.
3. **`NonFatal`.** The size admission catches `RuntimeException`, so an `Error` propagates as
   before; a checked exception cannot reach it.
4. **The coverage scan.** A scan that silently found fewer names in the Java facade would drop
   classes from `coverage.json`; it is unchanged, which shows the scan still finds them.

## 8. Sequencing

1. This plan; row 305 marked Planned.
2. The families off the Scala types.
3. The classifier to Java, with the measurement and the outcome.

## 9. Outcome

Done on 11 October 2026, in the two commits of 2. The second touches 31 files, under 300a's 35,
so the pull request was not cut again.

**The oracle.** The fusion dump is byte-identical to the base's (4,436 lines) after each commit,
and `coverage.json` and `emitted_bytes.json` do not move.

**Compile time.** `dev/varka_bench_pair.sh catalyst VarkaCompileBenchmark` against
`bc8c96fc871`, five interleaved rounds a side, medians; the change is in time per compile, less
is faster:

| shape (one compile) | change |
|---|---:|
| a wide projection of 60 mixed outputs | -4.3% |
| conditionals: IF, coalesce, greatest, CASE WHEN of 6 branches | -7.6% |
| TIME: hour, minute, second | -10.8% |
| calendar: year, month, make_date, add_months, last_day, weekofyear | -12.7% |
| predicate: a set of three date ranges | -13.8% |
| predicate: NOT, OR and IS NULL over two dates | -14.6% |
| predicate: IN over eight dates | -15.0% |
| predicate: a range, IS NOT NULL and an int comparison | -25.4% |

The committed files are regenerated from the port (`VarkaCompileBenchmark-jdk25-results.txt`
and its 128-bit companion). Against the files they replace, which 217's port wrote, the single
runs agree at 128 bits, where every shape takes 4.7 to 22.0% less. At full width six shapes take
1.0 to 17.3% less, and calendar and TIME read 4.6 and 3.8% slower (8763 to 9162 ns, 9019 to
9366 ns): single-run differences against an older build, which the interleaved pair, at the
same moment on the same machine, reads as 12.7 and 10.8% faster. The pair is the measurement.

Why the small shapes gain more than the wide one was not measured. The port removes the
classifier's `Option`, tuple and closure allocations per entry and the tables' walks, and a small
shape spends a larger share of its compile in the classifier than in the emitter; that is the
likely reading, not a verified one.

**Predictions scored.**

1. **Held.** The dump is byte-identical after each commit.
2. **Wrong, in the favourable direction.** The 60-output projection is no slower (4.3% faster),
   but "within 5% on every shape" does not hold: six of the eight take 10 to 25% less. The
   prediction located the gain in the slot table's walks, which only the wide shape has enough
   entries to show; the largest gains are on the smallest shapes instead.
3. **Held.** The Java classifier, `VarkaSizeAdmission` and `VarkaSlotTable` are 1,131 lines
   without license headers against the Scala classifier's 787 at `bc8c96fc871` (803 with it),
   1.44 times.

**What the port is.** The facade has no Scala left: every Varka compile entry point is a static
method of a Java class, and its results are `Optional`, records, `List` and `Map`.

**Review of the PR** (`/code-review xhigh`) found the port faithful, with no wrong answer, and
these, fixed in the PR:

* **A wide projection compiled in quadratic time.** The entry points wrapped the projection list
  without copying it, and the parser and the optimizer build it as a Scala `List`, whose wrapped
  `get(i)` walks from the head; every pass over the entries was quadratic in their number, where
  the Scala's `zipWithIndex` was linear. The entries are now copied once into a Java list.
* **An admitted kernel was classified twice**: once to ask the size admission and again to build
  it. The Scala did the same; the classification now comes back with the answer. Likewise the
  verbose EXPLAIN of a filter compiled its predicate twice, once for the conjunct lines and once
  for the split line; `explainAndCompilePredicate` returns both from one classification.
* **A binding per entry built its own lookup** over the child's columns. One binder per pass now
  builds it once.
* **`explainPredicate` returned a mutable list**, where the Scala `Seq` was immutable; it is
  copied.
* The four-table rollback, written out at four sites, is a `Marks` record at three; the fourth is
  the fault injector's (`misdescribeRollback`), which has to restore the tables wrongly and so
  stays written out.
* Stale descriptions of the classifier as Scala and as the one file that knows Spark's expression
  classes: the README's tour, the `varka` package overview and step 3 of
  `ADDING_AN_EXPRESSION.md`, which now name the families. A Javadoc link to a `LinkedHashMap`
  overload that no longer exists.
* `VarkaSlotTable` is package-private: its only test caller is in its package. `foldPick` takes a
  `BinaryOperator` rather than a Scala `Function2`. The size admission's once-per-JVM warning logs
  under the classifier's logger name, as it did, so a logging configuration naming it still
  applies. `dev/varka_emit.sh`'s "nothing fused" lines ask for the declines under the options
  given, not the defaults.

The fusion dump is byte-identical to the base's after these fixes as well.

**Corrections to this plan, which is a record and is not rewritten:** risk 3 says the size
admission catches `RuntimeException`; it catches every `Throwable` that `NonFatal` accepts and
rethrows the rest, as the Scala did, so an `AssertionError` from the emitter is logged and the
shape admitted, while a `VirtualMachineError` or `LinkageError` propagates.
