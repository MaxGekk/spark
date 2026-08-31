# Task 41: `unix_date` and `date_from_unix_date`

The smallest task in the milestone, and the only one that adds **no IR node, no
emitter code and no lane arithmetic at all**. Two compiler arms.

Read `PLAN_TASK_33.md` section 3 for general mechanics, but most of it will not
apply.

**Depends on task 38** for half of the work - see section 3. The other half can
land today.

## 1. What you are building

```
unix_date(d)             DateType    -> IntegerType
date_from_unix_date(i)   IntegerType -> DateType
```

Both are the identity on the value. Spark's own implementations are, in full:

```scala
override def nullSafeEval(input: Any): Any = input.asInstanceOf[Int]
```

A date *is* a day count; these two expressions only relabel the type. There is
nothing to compute, and this task should emit nothing.

## 2. The design: unwrap, do not add a node

**Do not add `UnixDate` and `DateFromUnixDate` IR nodes.** The IR has one lane
type, the value is unchanged, and `CompiledVarkaProjection.outputTypes` already
takes its type from the *entry* expression rather than from the IR. So the
whole lowering is:

```scala
case UnixDate(child) => compileNode(child, inputs, literals, sink)
case DateFromUnixDate(child) => compileNode(child, inputs, literals, sink)
```

The IR that comes out is whatever the child compiled to, and the entry's output
type is `IntegerType` or `DateType` because that is what the Catalyst
expression says. Both are null-intolerant identities, so validity is the
child's by construction - there is nothing to alias.

Consequences worth understanding before you start, because they are what makes
this safe:

* **Neither pinned value moves**, because no rendering changes.
* **`SELECT unix_date(d)` and `SELECT d` produce the same IR and the same shape
  hash**, and therefore share an emitted class. That is correct: kernel
  identity is about lane math, and their lane math is identical. The evaluator
  allocates the output vector from `outputTypes`, which still differs.
* The interesting shape this creates is an output whose IR root is a **bare
  `ColumnRef`** - a loop that only loads and stores. That is legal and the
  emitter handles it, but it may not have been exercised before; section 4 asks
  for a test of exactly that.

If you find yourself writing an `emitValue` arm, stop: you have misread the
task.

## 3. What lands when

* `unix_date(d)` reads a **date** column, which the engine already accepts, so
  it works as soon as the arm exists.
* `date_from_unix_date(i)` reads an **integer** column, which nothing can read
  until **task 38** lands. Write both arms anyway - the second one simply
  declines through the existing "non-date column" path until 38 opens it, and
  then starts working with no further change.

Say in the pull request which of the two you were able to test end to end.

## 4. The tests

1. **Compiler suite**: `unix_date(d)` compiles to a bare `ColumnRef(0)` with
   `outputTypes === Seq(IntegerType)`; `date_add(date_from_unix_date(i), 3)`
   compiles to `AddDays(ColumnRef, LiteralSlot)` - the point being that the
   relabel vanishes rather than appearing in the IR.
2. **Emitter suite**: a matrix case whose output root is a bare `ColumnRef`,
   over every length and null pattern. This is the shape section 2 says may be
   new; if it fails, that failure is the finding and belongs in the outcome
   section rather than being worked around.
3. **Differential**: `SELECT unix_date(d)` and, once task 38 has landed,
   `SELECT date_from_unix_date(i)` and a mixed chain like
   `date_add(date_from_unix_date(i), 3)`.
4. **The blocking case, which is the actual argument for this task**: a
   projection like `SELECT date_add(d, 1) AS a, unix_date(d) AS b` must fuse
   **both** entries. Before this task the second entry declines and takes the
   whole projection to the row path; that is what the task is worth.

Then task 33's section 4 command block, unchanged. Acceptance is the strongest
of any task here: **no pinned value moves, no committed number moves, and no
emitted bytes change for any shape that existed before.**

## 5. Explicitly out of task 41

* **An IR node for either expression.** Section 2.
* **Any other identity or reinterpreting cast.** `cast(date AS int)` is not the
  same expression and is not covered by this task; if it turns out to reach the
  compiler as something else, note it rather than adding an arm.
* **Making a bare column projection fuse in general.** If `SELECT d` does not
  fuse today, this task does not change that; it only ensures the relabelled
  form is not a blocker inside a projection that has other work.

## 6. Outcome

Filled in when the work lands. Two things worth recording: whether the bare
`ColumnRef` output shape worked first time, and whether the no-IR-node design
survived contact - if it did not, what forced a node.
