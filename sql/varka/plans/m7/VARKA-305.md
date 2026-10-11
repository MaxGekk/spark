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
