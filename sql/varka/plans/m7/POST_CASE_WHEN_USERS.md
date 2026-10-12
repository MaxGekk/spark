# A large CASE WHEN, split only where it has to be

---

[The 8000-byte cliff in Spark SQL](https://vecbricks.github.io/the-8000-byte-cliff/) ended on an
open thread. A large `CASE WHEN` inside a whole-stage codegen stage was never split into methods,
so at about a hundred branches the stage's one method passed the 8000 bytes HotSpot will
JIT-compile, and the query got several times slower than the same query without whole-stage
codegen. The fix,
[SPARK-33301](https://issues.apache.org/jira/browse/SPARK-33301), was in review.
It has merged, as [apache/spark#59225](https://github.com/apache/spark/pull/59225), and ships in
Spark 4.4.0. This post is about what changes for you: what it does, how to see it, the settings it
adds, what it still costs, and what to do before you can upgrade.

![A CASE WHEN of n branches inside a stage, five ways](figures/svg/fig31-split-as-needed.svg)

*Figure 1. Milliseconds for two million rows against the number of branches, both axes log.
Not split, the stage steps off between 64 and 96 branches and never comes back. Split as needed
follows not split below the line and split always above it. The cliff post's workaround,
`hugeMethodLimit=8000`, runs as fast as turning whole-stage codegen off, because that is what it
does.*

> **In short**
>
> 1. On Spark 4.4.0 a large `CASE WHEN` inside a stage is split into methods where the stage's
>    method would pass 8000 bytes, and left alone where it would not. Nothing to set.
> 2. Before 4.4.0, `spark.sql.codegen.hugeMethodLimit=8000` is still the remedy. It is twelve times
>    faster than the unsplit stage at 300 branches; the fix is another four and a half times
>    faster than it.
> 3. On 4.4.0, if the log still says a generated method is too long to be JIT compiled, the large
>    method is something else - a wide `COALESCE` or `IN`, or a wide projection - and
>    `hugeMethodLimit=8000` is still the remedy for it.

## 1. Splitting is not free

The obvious fix is to split every large `CASE WHEN` into methods, a few branches each, and call
them from the stage. That fixes the cliff: past it, the split stage is thirty to fifty times faster
than the unsplit one. But below the line, where the stage was fine, splitting always made it
slower: 5 to 15% at 64 branches on the runners the pull request measured, 5% in the run behind
this post (97 ms against 92).

The JIT's own log says why. At 64 branches the stage's code is 6049 bytes, under the line. Kept in
one piece, the stage's row loop calls one method that holds the whole `CASE WHEN`; C2 will not
inline something that large, so it compiles it on its own, a straight run of comparisons with no
call inside it. Split always, the same loop calls a small method that calls five group methods,
each calling up to eight small methods that hold the branches. C2 inlines the first two groups
into the loop and refuses the other three, because its inlining budget for one compilation is
also 8000 bytes, so every row pays three calls into separately compiled code instead of one.

![What C2 makes of the 64-branch stage, kept whole and split always](figures/svg/fig32-what-c2-inlines.svg)

*Figure 2. The 64-branch stage as C2 compiles it, from `-XX:+LogCompilation` on JDK 25. Kept whole,
one call a row into one method. Split always, two of the five groups inlined into the loop and
three called.*

## 2. Measure, don't predict

So the fix splits only where the stage needs it, and it finds out by asking the compiler rather
than guessing. A stage generates its code once in one piece, with every expression a split could
take marked. It compiles that code as a trial, which reports the size of every method in
bytecode. Only the methods past 8000 bytes that run once per row are ones a split can help, so it
generates the code again with just the expressions in those methods split. It keeps the split
code if that lowers the bytes past the line and compiles; otherwise it runs the code in one piece,
as Spark did before. A stage under the line keeps exactly the code it had. The decision is
remembered for the stage, so running the query again skips the trial.

## 3. How to see it

`df.explain("codegen")` prints each stage with its largest method:

```
== Subtree 1 / 1 (maxMethodCodeSize:280; maxConstantPoolSize:564(0.86% used); numInnerClasses:0) ==
```

That is the 96-branch query of Figure 1 on a build with the fix. Before it, the same stage
printed 9057, past the line, and ran interpreted. A `maxMethodCodeSize` under 8000 is a stage the
JIT compiles; one over it, with whole-stage codegen on, is a stage on the cliff.

The log says it too. On 4.4.0 the first stage that stays past the line logs one WARN, naming the
remedy; further ones go to INFO:

```
Generated method too long to be JIT compiled: ...GeneratedIteratorForCodegenStage1.project_doConsume_0
is 9057 bytes, so it runs interpreted on every row. Setting spark.sql.codegen.hugeMethodLimit to
8000 runs such stages without whole-stage codegen instead. Further methods over the limit are
logged at INFO.
```

With the split, a large `CASE WHEN` no longer produces that line. If you see it on 4.4.0, the
large method is something the split does not cover yet: `COALESCE`, `IN`, `GREATEST` and `LEAST`,
a wide `named_struct` or hash, a projection with many columns, or a `CASE WHEN` whose branches read
a column the operator has not computed yet, which has to stay in place. For those,
`hugeMethodLimit=8000` remains the remedy.

## 4. The settings

Two settings come with the fix, both internal (they do not appear in the configuration docs), both
in 4.4.0:

- `spark.sql.codegen.wholeStage.splitExpressions`, default `true`: split as needed. `false`
  generates exactly the code Spark generated before the fix.
- `spark.sql.codegen.wholeStage.splitExpressions.methodLimit`, default `8000`: the largest method
  a stage keeps unsplit. `0` splits always.

Leave both alone. The first is the escape hatch if a query regresses on 4.4.0 and you want the old
code back while you report it. The second is there so the line can follow HotSpot's; setting it to
0 is the "split always" arm of the table below, a little faster above the line and slower below
it. `spark.sql.codegen.hugeMethodLimit` keeps its default, 65535: the fix does not need it, and the
two together decide what happens to a stage whose method is still past 8000 after the split.

## 5. The numbers

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

## 6. What it still costs

Past the line, split as needed is 5 to 13% slower than split always on one `CASE WHEN` (217 ms
against 200 here), and 8 to 15% in the keyed aggregate. That is the second code generation: a
stage that splits generates its code twice on every execution, once in one piece and once split.
It is a fixed cost per execution, about 17 ms on this stage, so it shows on a short query and
vanishes on a long one.

The one sharp edge is a literal that changes on every run. The decision is remembered by the
generated code, and `current_timestamp()`, `current_date()` or a date filled in by a scheduler
becomes a literal in that code, so a query that carries one decides afresh each time: two code
generations and two compiles where Spark before the fix paid one of each. That matters for a
query that is small and frequent, such as a dashboard tile or a short micro-batch; a key that
leaves literal values out is filed as
[SPARK-60048](https://issues.apache.org/jira/browse/SPARK-60048).

## 7. What to do

On Spark 4.4.0, nothing: a large `CASE WHEN` inside a stage is split where its method is past the
line, and left alone where it is not. Check `explain("codegen")` once on the queries you know were
on the cliff, and drop any `hugeMethodLimit` setting you added only for them; keep it where the
log still names a method too long to compile.

Before it, the cliff post's advice stands: set `spark.sql.codegen.hugeMethodLimit` to 8000, so
that a stage the JIT would never compile runs without whole-stage codegen instead. It is twelve
times faster than the unsplit stage at 300 branches. What it gives up is the rest: the fix is
another four and a half times faster than the workaround there, nine times in the keyed
aggregate.

---

*How this was measured.* Every timing is from one run of Spark's `CaseWhenCodegenBenchmark`, with
an arm added for the workaround, on a GitHub-hosted runner's AMD EPYC 7763 with JDK 25, on a build
of Spark master with #59225 merged
([results](https://github.com/vecbricks/varka/blob/master/sql/core/benchmarks/CaseWhenCodegenBenchmark-jdk25-288-results.txt)
and their provenance beside them). The ranges for what splitting costs, 5 to 15% below the line
and 5 to 13% above it, are the pull request's own, over three JDKs on three runner CPUs. The
inlining decisions are from `-XX:+LogCompilation` on JDK 25 on a laptop, an AMD Ryzen AI 9 HX PRO
370; they do not depend on the machine's speed
([decisions](https://github.com/vecbricks/varka/blob/master/sql/core/benchmarks/CaseWhenInlining-jdk25-288-results.txt)).
The `explain` header is from the same build, and the log line is the 4.4.0 code's. The prose
rounds; the files carry the exact values.
