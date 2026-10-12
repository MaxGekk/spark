# A large CASE WHEN, split only where it has to be

---

[The 8000-byte cliff in Spark SQL](https://vecbricks.github.io/the-8000-byte-cliff/) ended on an
open thread. A large `CASE WHEN` inside a whole-stage codegen stage was never split into methods,
so at about a hundred branches the stage's one method passed the 8000 bytes HotSpot will
JIT-compile, and the query got several times slower than the same query without whole-stage
codegen. The fix,
[SPARK-33301](https://issues.apache.org/jira/browse/SPARK-33301), was in review.
It has merged, as [apache/spark#59225](https://github.com/apache/spark/pull/59225), and ships in
Spark 4.4.0. This post is about what it does, what it costs, and why it does not simply split
every `CASE WHEN` it can. It is written for people who work on Spark's code generation, or read
its output.

![A CASE WHEN of n branches inside a stage, five ways](figures/svg/fig31-split-as-needed.svg)

*Figure 1. Milliseconds for two million rows against the number of branches, both axes log.
Not split, the stage steps off between 64 and 96 branches and never comes back. Split as needed
follows not split below the line and split always above it. The cliff post's workaround,
`hugeMethodLimit=8000`, runs as fast as turning whole-stage codegen off, because that is what it
does.*

## 1. Why it was never split

Spark has split long expressions into methods for years, through
`CodegenContext.splitExpressionsWithCurrentInputs`. Inside a stage, that function did nothing:
its body began with a comment that whole-stage codegen was still to be supported, and returned
the code unsplit. Outside a stage the split methods take
the input row as their parameter, and every value they need can be read from it. Inside a stage
there is no row. The inputs are local variables of the operator's method, the value and null flag
of every column evaluated so far, and which of them a piece of generated code reads cannot be told
from the code string.

The fix finds out from the expressions instead. For each block of branches it walks the
expressions the block was generated from and collects the locals they read: the evaluated inputs
of the stage, the input row where the code reads it, the value of a common subexpression without
descending below it. Those become the method's parameters. A block that reads a column the
operator has not evaluated yet, or a value no parameter can carry, stays inline, in its place
between the calls. The calls are grouped eight to a method, so that a projection with several
large `CASE WHEN`s does not rebuild the problem one level up.

Here is the 96-branch stage of Figure 1 as the fix generates it (`df.explain("codegen")`, trimmed).
Its largest method is 280 bytes; unsplit, it was 9057.

```java
private void project_doConsume_0(long project_expr_0_0, boolean project_exprIsNull_0_0) {
  byte project_caseWhenResultState_0 = -1;
  do {
    project_caseWhenResultState_0 =
        project_caseWhen_0_group0_0(project_expr_0_0, project_exprIsNull_0_0);
    if (project_caseWhenResultState_0 != -1) {
      continue;
    }
    // ... groups 1 to 5 ...
    project_caseWhenResultState_0 = project_caseWhen_0_group0_6();
    // ...
  } while (false);
  final boolean project_isNull_5 = (project_caseWhenResultState_0 != 0);
  // ... write the row ...
}

private byte project_caseWhen_0_32(long project_expr_0_0, boolean project_exprIsNull_0_0) {
  // WHEN v = 65 THEN v * 65, WHEN v = 66 THEN v * 66
}
```

The column `v` reaches every method as `project_expr_0_0` and its null flag: the two locals the
walk found. Each group method calls eight block methods, and each block method holds two branches.

## 2. Splitting is not free

The obvious fix is to split every large `CASE WHEN` into methods like these. That fixes the cliff:
past it, the split stage is thirty to fifty times faster than the unsplit one. But below the line,
where the stage was fine, splitting always made it slower: 5 to 15% at 64 branches on the runners
the pull request measured, 5% in the run behind this post (97 ms against 92).

The JIT's own log says why. At 64 branches the stage's code is 6049 bytes, under the line. Kept in
one piece, the row loop calls one method that holds the whole `CASE WHEN`, and C2 will not inline
something that large, so it compiles it on its own, a straight run of comparisons with no call
inside it:

```
@ 143   ...GeneratedIteratorForCodegenStage1::project_doConsume_0$ (6049 bytes)   failed to inline: hot method too big
```

Split always, the loop calls a small method that calls five group methods. C2 inlines the first
two groups, with their block methods, and stops:

```
@ 40   ...GeneratedIteratorForCodegenStage1::project_caseWhen_0_group0_2$ (146 bytes)   failed to inline: size > DesiredMethodLimit
@ 57   ...GeneratedIteratorForCodegenStage1::project_caseWhen_0_group0_3$ (146 bytes)   failed to inline: size > DesiredMethodLimit
@ 72   ...GeneratedIteratorForCodegenStage1::project_caseWhen_0_group0_4$ (21 bytes)   failed to inline: size > DesiredMethodLimit
```

`DesiredMethodLimit` is C2's budget for one compilation: it stops inlining once the bytecode it has
inlined passes it. Its value is 8000, the same as `HugeMethodLimit`, the line past which HotSpot
does not compile a method at all. Both are develop flags, fixed in a release JVM. So splitting does
not escape the 8000 bytes; it moves them. The unsplit method is refused whole by one limit, and
the split one is put back together by the inliner until it meets the other, and every row then pays
three calls into separately compiled code instead of one.

![What C2 makes of the 64-branch stage, kept whole and split always](figures/svg/fig32-what-c2-inlines.svg)

*Figure 2. The 64-branch stage as C2 compiles it, from `-XX:+LogCompilation` on JDK 25. Kept whole,
one call a row into one method. Split always, two of the five groups inlined into the loop and
three called.*

## 3. Measure, don't predict

So the fix splits only where the stage needs it, and it finds out by asking the compiler rather
than guessing. A stage generates its code once in one piece, with every expression a split could
take marked. It compiles that code as a trial, which reports the size of every method in
bytecode. Only the methods past 8000 bytes that run once per row are ones a split can help, so it
generates the code again with just the expressions in those methods split. It keeps the split
code if that lowers the bytes past the line and compiles; otherwise it runs the code in one piece,
as Spark did before. A stage under the line keeps exactly the code it had.

The parts a reviewer asks about:

- **The decision is remembered**, keyed by a digest of the marked code with the class loader, the
  compiler, the limit and `methodSplitThreshold`. Every execution of a query generates its code
  again, and the key lets it skip the trial compiles. Tasks that meet the same stage at once wait
  on a per-key lock for one decision.
- **The variant not kept leaves the compile cache**, so the cache holds only code that runs, and a
  later compile of that variant reports what it finds. A split that fails to compile is warned
  about, remembered, and the code kept in one piece; under testing it is thrown.
- **The trial holds back its own noise**: the huge-method warnings, the "Code generated in" line
  and the codegen metrics are reported for the code kept, not for the variant discarded.
- **Below the line the check is cheap**: about 0.04 ms per code generation on a 64-branch stage
  (1.60 ms against 1.56 without it), because recording the marks does not pack blocks.

That is the most a generator that writes Java source can do. It cannot know how many bytes a method
will be until the compiler has made it, so it compiles twice and chooses. An engine that emits
bytecode directly can count the bytes as it writes them and split before anything is compiled; the
second post of the cliff pair, [Under 8000 bytes by
construction](https://vecbricks.github.io/under-8000-bytes-by-construction/), is about one that
does.

## 4. The numbers

Best times for two million rows on one GitHub runner, an AMD EPYC 7763 with JDK 25:

| shape | split as needed | split always | not split | whole-stage off | `hugeMethodLimit=8000` |
|---|---|---|---|---|---|
| one `CASE WHEN`, 64 branches | 92 ms | 97 ms | 92 ms | 212 ms | 90 ms |
| one `CASE WHEN`, 96 branches | 121 ms | 114 ms | 3815 ms | 315 ms | 322 ms |
| one `CASE WHEN`, 300 branches | 217 ms | 200 ms | 11570 ms | 963 ms | 982 ms |
| in a keyed aggregate, 300 branches | 255 ms | 231 ms | 11858 ms | 2274 ms | 2297 ms |

Read the table by column. Below the line, at 64 branches, split as needed is not split: the same
code, the same time. Past it, the unsplit stage is never compiled and takes seconds; split as
needed takes a thirtieth to a fiftieth of that, and two and a half to nine times less than running
without whole-stage codegen. The last column is the cliff post's advice for a release without the
fix: it makes a stage past 8000 bytes run without whole-stage codegen, and so it times the same as
that column, within 2%, everywhere the method is past the line, and changes nothing below it.

## 5. What it still costs

Past the line, split as needed is 5 to 13% slower than split always on one `CASE WHEN` (217 ms
against 200 here), and 8 to 15% in the keyed aggregate. That is the second code generation: a
stage that splits generates its code twice on every execution, once in one piece and once split.
The decision itself is remembered, so running the query again skips the trial compiles.

The key is also the one sharp edge. A literal that changes on every run, such as the timestamp
`current_timestamp()` becomes, changes the generated code, so such a query decides afresh each
time and pays two generations and two compiles where Spark before the fix paid one of each. And
where the trial's class comes from the compile cache, its method sizes are not known, so nothing
is remembered.

## 6. What to do

On Spark 4.4.0, nothing: a large `CASE WHEN` inside a stage is split where its method is past the
line, and left alone where it is not.

Before it, the cliff post's advice stands: set `spark.sql.codegen.hugeMethodLimit` to 8000, so
that a stage the JIT would never compile runs without whole-stage codegen instead. It is twelve
times faster than the unsplit stage at 300 branches. What it gives up is the rest: the fix is
another four and a half times faster than the workaround there, nine times in the keyed
aggregate.

## 7. Where to help

The fix splits `CASE WHEN` only. Every other caller of `splitExpressionsWithCurrentInputs` still
meets the same unsplit path inside a stage, and four follow-ups are filed, each a self-contained
change for someone who knows the code generator:

- [SPARK-60047](https://issues.apache.org/jira/browse/SPARK-60047): the other callers - `Coalesce`,
  `In`, `Greatest` and `Least`, `CreateNamedStruct`, the hashes - passing the expressions behind
  their code, so that a wide `COALESCE` or `IN` inside a stage splits the same way.
- [SPARK-60048](https://issues.apache.org/jira/browse/SPARK-60048): a decision key that leaves
  literal values out, which removes both costs of section 5's sharp edge.
- [SPARK-60049](https://issues.apache.org/jira/browse/SPARK-60049): each method's size kept in the
  compile cache, so that a cache hit needs no second trial compile.
- [SPARK-60021](https://issues.apache.org/jira/browse/SPARK-60021): `ExpandExec`'s outputs
  labelled as the fields they are, which retires a workaround the fix needed for them.

---

*How this was measured.* Every timing is from one run of Spark's `CaseWhenCodegenBenchmark`, with
an arm added for the workaround, on a GitHub-hosted runner's AMD EPYC 7763 with JDK 25, on a build
of Spark master with #59225 merged
([results](https://github.com/vecbricks/varka/blob/master/sql/core/benchmarks/CaseWhenCodegenBenchmark-jdk25-288-results.txt)
and their provenance beside them). The ranges for what splitting costs, 5 to 15% below the line
and 5 to 13% above it, and the cost of the check, are the pull request's own, over three JDKs on
three runner CPUs. The inlining decisions are from `-XX:+LogCompilation` and `-XX:+PrintInlining`
on JDK 25 on a laptop, an AMD Ryzen AI 9 HX PRO 370; they do not depend on the machine's speed
([decisions](https://github.com/vecbricks/varka/blob/master/sql/core/benchmarks/CaseWhenInlining-jdk25-288-results.txt)).
The flag values are from a fastdebug build of JDK 25, which lists develop flags. The prose rounds;
the files carry the exact values.
