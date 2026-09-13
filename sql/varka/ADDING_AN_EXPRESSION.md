# Adding an expression to Varka

Teaching Varka a new expression is the most common change this engine takes, and
it follows the same path every time. This is that path, in the order the code
runs, with the files named.

Read [the architecture guide](../../docs/sql-varka.md) first, at least its glossary - *lane*,
*word*, *morsel* and *epilogue* all mean something specific here and this page
uses them without further explanation.

## The shape of the change

The checklist below is not a proposal; it is what the last ten expression
additions actually touched, counted from the history. Four files change every
time, and the rest are conditional on what the expression needs.

| | Always | |
| :--- | :--- | :--- |
| `VarkaVectorIR.java` | 10/10 | the node |
| `VarkaLoopEmitter.java` | 10/10 | the code it emits |
| `VarkaLoopEmitterSuite.scala` | 10/10 | proof the emitted loop matches the reference |
| `PLAN_MILESTONE_4.md` | 10/10 | the task row |

| | Nearly always | |
| :--- | :--- | :--- |
| `VarkaExpressionCompiler.scala` | 9/10 | the Catalyst translation |
| `VarkaExpressionCompilerSuite.scala` | 9/10 | what compiles, and what declines and why |
| `VarkaIrFuzzSuite.scala` | 9/10 | the generator must be able to produce the node |
| `VarkaDifferentialSuite.scala` | 9/10 | end-to-end against the row engine |
| `docs/sql-varka.md` | 9/10 | the supported-expression tables |

| | When it applies | |
| :--- | :--- | :--- |
| `VarkaReferenceEvaluator.scala` | 7/10 | scalar semantics the fuzzer checks against |
| `VarkaEmitterParityBenchmark` + its three results files | 8/10 | a committed op-count and throughput case |
| `VarkaThroughputBenchmark` + its three results files | 6/10 | end-to-end throughput |
| `VarkaChrono.java` | | a scalar twin or a new calendar constant |
| `VarkaDerivedKind.java` and a `*Leaf.java` | | a value the evaluator derives per batch, such as `next_day`'s weekday column |
| `VarkaEmitOptions.java` | | a variant worth measuring both ways before choosing |
| `SKILLS.md` | 6/10 | a lesson worth keeping when the work taught one |

## The path

**1. Decide the lowering before writing anything.** Every Varka node computes
over int32 lanes with no branches: the arithmetic must be expressible as adds,
subtracts, multiplies, shifts, compares and blends. If the natural formula needs
a division, look for a magic-multiply form - the calendar code is full of them
and `emitFloorMod7` is the worked example. If it needs a branch, express it as a
mask and a blend. If it can genuinely do neither, the right answer may be to
decline the expression, which is a normal outcome and not a failure.

**2. Add the IR node** to `VarkaVectorIR`. It is a sealed interface of records,
so a new node is a record plus a `permits` entry, and the compiler will then
point at every switch that has to learn about it - which is the intended way to
find them all. Carry no literal *values* in the node: a folded constant becomes
a literal slot index, because that is what lets two queries differing only in
their constants share one emitted class.

**3. Teach the compiler to build it** in `VarkaExpressionCompiler`. This is the
only file that knows Spark's expression classes. Match the Catalyst tree, compile
the children, and return the node - or decline with a reason, which the driver
surfaces and the tests assert on. Take the decline path seriously: a wrong
decline costs performance, but a wrong *acceptance* costs correctness.

**4. Emit it** in `VarkaLoopEmitter`. Add the arm to `emitValue`, and to whichever
of the analysis passes the node participates in - the compiler's exhaustiveness
errors will list them. If the node is a value over one date, check whether it can
share the `civil-from-days` prefix rather than recomputing it.

**5. Say what its nulls do.** A node that is null when any input is null needs
nothing: that is the default. Anything else - a node that can produce a null from
valid inputs, or that is valid where an input is not - has to say so, or the dense
body will be wrong in a way no test of null-free data can catch.

**6. Say what its range does**, if it touches the calendar. The decomposition is
exact only inside a bounded day range; a node that can move a date outside it must
either be bounded at compile time or carry a runtime guard on its own result.

## Proving it

In increasing order of cost, and all of them expected:

- **`VarkaExpressionCompilerSuite`** - the expression compiles, and the shapes that
  should decline do decline. Assert the reason, not just the refusal.
- **`VarkaLoopEmitterSuite`** - the emitted kernel agrees with the reference
  evaluator across lengths, null patterns and both vector widths.
- **`VarkaIrFuzzSuite`** - extend the generator so it can produce the new node. The
  suite asserts that it reaches every node type, so a node the generator cannot
  build is a gap the fuzzer silently would not cover.
- **`VarkaDifferentialSuite`** - the whole query, against stock Spark's row engine,
  through both a columnar and a row consumer.
- **`dev/varka_emit.sh "<your expression>"`** - read what it actually compiled to.
  `--table` gives the op count a plan or a benchmark entry should register.

Then `dev/varka_gate.sh`, which runs what a task's verification section lists.

## Two things that catch people

**A benchmark case is not optional if the expression is meant to be fast.** The
project's rule is that a performance claim traces to a committed results file, so
an expression with no case has no claim. `dev/varka_bench_ids.sh` gives the next
free case id - the ids must be unique per file, and choosing one by eye gets it
wrong.

**Declining is a feature.** Every stage may refuse: the compiler on an expression
it cannot translate, the emitter on a shape it will not build, the kernel on a
batch whose values leave the range it proved. Each falls back to stock Spark for
that batch and the query still returns the right answer. Write the refusal as
deliberately as the acceptance, and test it.
