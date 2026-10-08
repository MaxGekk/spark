# VARKA-299: Varka's row fallback converts a NullType column

## 1. Where this came from

Row 299 of `m7/PLAN.md`, found by VARKA-262's first long run (`VARKA-262.md` 9): the random
differential against Spark shrank iteration 93 to `SELECT greatest(l, l2) AS c0, date_add(d, i)
AS c1 FROM fz WHERE NOT (i = 5)` over four rows, ANSI off, two columns `VOID` and one `i` of
100000. Spark answers; Varka raised `UNSUPPORTED_DATATYPE: Unsupported data type "VOID"`. The
reproducer is `sql/varka/fuzz/spark/void-column-fallback.sql`, filed `status: known VARKA-299`.

## 2. The admission check, done

Is the cause where the stack trace says, and is the fix possible without touching what the kernels
do? A scratch test on 8 October 2026 asked Spark's vectors three questions:

| question | answer |
|---|---|
| can `OnHeapColumnVector` and `OffHeapColumnVector` be allocated for a schema with `NullType`? | yes, both |
| do they take `appendNull`, report `isNullAt` and read back through a `ColumnarBatch` row? | yes, both |
| does `new RowToColumnConverter(schema)` accept `NullType`? | **no**: `UNSUPPORTED_DATATYPE`, from `getConverterForType` |
| does the converter over the schema with `NullType` replaced by a nullable `BooleanType` write a null cell into the real vectors? | yes |

So the cause is exactly the converter: the vectors and the kernels' pass-through are fine, which
is why the batches the kernel serves worked, and only the row fallback, which writes the
survivors with the converter, failed. Four places build one (`VarkaFilterExec`'s fallback,
`VarkaFilterEvaluator`'s generic path, `VarkaKernelEvaluator`'s residual and
`VarkaVectorProjection`), and the fix is one helper for all four.

What the check would have rejected: declining the fusion of any plan whose child carries a
`NullType`, which would have lost the kernels' pass-through for a bug in the fallback.

## 3. The design

### 3.1 The mechanism

`VarkaRowToColumn(schema)` returns a `RowToColumnConverter` built over `schema` with every
`NullType`, at any depth, replaced by a nullable `BooleanType` (arrays, maps and structs
recursively, `containsNull` and `valueContainsNull` set where the element was `NullType`). A
`NullType` cell is always null, and the converter checks null first and appends a null, so the
output is exactly what a `NullType` cell is. The vectors are allocated with the real schema and
keep it. The four call sites replace `new RowToColumnConverter(x)` with `VarkaRowToColumn(x)`.

### 3.2 What is deliberately unchanged

* The kernels, the emitter and the compiler: this is the fallback only.
* Spark's own `RowToColumnConverter`, which is Spark's and lacks `NullType` for its own reasons.

### 3.3 Registered op counts

None move.

## 4. Files

| file | what |
|---|---|
| `sql/core/src/main/scala/.../execution/VarkaRowToColumn.scala` | the helper |
| `VarkaFilterExec.scala`, `VarkaFilterEvaluator.scala`, `VarkaKernelEvaluator.scala`, `VarkaVectorProjection.scala` | the four call sites |
| `sql/core/src/test/scala/.../execution/VarkaNullTypeFallbackSuite.scala` | the regression tests |
| `sql/varka/fuzz/spark/void-column-fallback.sql` | `known` becomes `regression` |
| `sql/varka/plans/m7/PLAN.md`, `VARKA-299.md` | the row and this plan |

## 5. Tests, and what each is for

| test | what it catches that no other would |
|---|---|
| a filter whose batch is declined keeps a `NullType` column | the reported crash, end to end on the Arrow cache |
| a filter that keeps nothing of a declined batch | the converter with zero surviving rows |
| a projection of a `NullType` column beside a declined output; a `NullType` literal beside a fused one | the other fallbacks, so a change to them cannot reintroduce it (they did not fail before the fix) |
| the converter writes a `NullType` cell as a null, on and off heap, flat and in an array | the helper itself, including the nested case |
| `VarkaSparkReproducerSuite` on the saved file | the random differential's own shrunk case, now a regression |

## 6. The measurement

None: the fallback is the slow path by design and nothing on the kernels' path changed.

### 6.1 Predictions, registered before the run

1. The filter tests and the converter test fail before the fix and pass after it.
2. The projection tests pass before the fix as well, since their fallbacks did not reach a
   `NullType` in the shapes tried.
3. The 20,000-composition run of VARKA-262 stays clean, and the saved reproducer replays as a
   regression.

## 7. Risks

1. **Another path reads a `NullType` vector as a boolean.** The substituted schema is only the
   converter's; the vectors and the batch keep `NullType`. The test reads the cells back through
   the real vectors and a `ColumnarBatch` row.
2. **The three call sites the tests did not fail on** may carry the same bug on a shape not tried;
   they use the helper too, so they are fixed in any case, but that is untested by a failing test.

## 8. Sequencing

1. The regression tests against the unfixed behaviour (three fail).
2. The helper and the four call sites.
3. The reproducer becomes a regression; the neighbouring suites and the random differential.

## 9. Outcome

Done on 8 October 2026.

**The fix held.** Before it, the two filter tests and the converter test failed; after it the
suite passes, and so do the 263 tests of the neighbouring suites (`VarkaFilterExecSuite`,
`VarkaProjectExecSuite`, `VarkaKernelEvaluatorSuite`, `VarkaDifferentialSuite`,
`VarkaCoverageDifferentialSuite`, the random differential and the reproducer replay).
`sql/varka/fuzz/spark/void-column-fallback.sql` is now `status: regression` and agrees.

**Predictions scored.** 1 held. 2 held: the projection and residual shapes passed before the fix,
so only the filter fallback is shown to have been broken; the other three call sites use the
helper, which is the right place for them, but no test of mine failed on them. 3 held for the
reproducer; the 20,000-composition run was not repeated, since the fix touches no path that run
exercises beyond the one reproducer.

**Found on the way.** `VarkaSparkReproducerSuite`'s regression branch had never run: its
assertion message evaluated `now.get` eagerly, so a regression file that agreed raised
`None.get`. Only the `known` branch had been exercised, because the directory held one `known`
file. Fixed on VARKA-262's branch with a test of both statuses on a query that always agrees.
