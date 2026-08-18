# Varka MVP Implementation Plan

This is the implementation plan for the Varka MVP (Date Arithmetic Over
ArrowColumnarBatch). It supersedes the sketch in `Varka_MVP.md` where the
two disagree; `VISION.md` remains the architectural source of truth.

Per-task detail lives in separate files:

- `PLAN_TASK_1.md` - standalone engine module + `VarkaMorsel` (completed).
- `PLAN_TASK_2.md` - `DateVectorOps` SIMD kernels (in progress).

## 1. Corrections to the design docs (ground truth in this repo)

This plan is grounded in the actual `vecbricks-varka` tree (Spark
5.0.0-SNAPSHOT, branch `master`, JDK 25 installed). The two design docs
contain details that do not match this codebase; they are corrected here:

| Doc says | Reality | Action |
| :--- | :--- | :--- |
| `ArrowColumnarBatch` / `ArrowVector` | `ColumnarBatch` / `ArrowColumnVector` (Java, `sql/catalyst/src/main/java/org/apache/spark/sql/vectorized/`) | Use `ArrowColumnVector.getValueVector()` (ArrowColumnVector.java:47) to reach the Arrow `ValueVector`. |
| Spark `DateType` -> Arrow type | `DateDayVector` (DATE32, int32 days since epoch) | `VarkaMorsel` targets `DateDayVector`. |
| `VectorMask.fromMemorySegment(SPECIES, validity, i, JAVA_BYTE)` | Arrow validity buffers are **bit-packed (1 bit/row)**; a byte-per-lane read is a correctness bug | Load a `long` from the validity segment and build the mask with `VectorMask.fromLong(SPECIES, bits)` (verified present in JDK 25). |
| "64-byte alignment guaranteed by Arrow's allocator" | Not a guarantee | Treat alignment as a **diagnostic** in tests, never an assertion. Vector-API masked loads/stores work unaligned. |
| `CodeGenerator.doCompile` / `currentContext` | `doCompile` was lifted into the pluggable `CodeCompiler` backend (CodeCompiler.scala:63); no `currentContext` ThreadLocal exists | The Ghost-fallback hook lives in `CodeCompiler`/`CodeGenerator.compile` (CodeGenerator.scala:1567). |
| Repo builds at Java 25 | Repo builds at **Java 17** (`--release 17`, enforcer pins bytecode version) | The engine is a **standalone module outside the Spark reactor**, built on JDK 25. Spark stays on 17. |
| `spark.sql.varka.enabled` (testing section) vs `spark.sql.codegen.varka.enabled` | Inconsistent | Standardize on `spark.sql.codegen.varka.*`. |
| `arrow-vector` version | `19.0.0` (pom.xml:240) | Engine depends on `org.apache.arrow:arrow-vector:19.0.0`. |

## 2. MVP architecture overview

```
sql/varka/
  VISION.md                      (unchanged)
  Varka_MVP.md                   (unchanged)
  IMPLEMENTATION_PLAN.md         <- this file (high-level)
  PLAN_TASK_1.md                 <- Task 1 detail (completed)
  PLAN_TASK_2.md                 <- Task 2 detail
  engine/                        <- STANDALONE Java 25 module (Task 1-2). NOT in Spark reactor.
    pom.xml                      (--release 25, --add-modules jdk.incubator.vector)
    src/main/java/org/apache/spark/sql/varka/
      memory/VarkaMorsel.java    (Task 1)
      vector/DateVectorOps.java  (Task 2)
    src/test/java/...            (Task 1/2 unit tests)
  spark/                         <- FUTURE Spark-side integration module (Tasks 3+); integration
                                    strategy TBD
```

Design rules carried from VISION: zero string generation on the happy path;
constants passed as runtime args (never inlined into a plan hash); SIMD with
strict scalar tail; masked loads/stores so null lanes never read garbage;
ghost Janino fallback only in the Spark-side compile hook (Task 5).

## 3. MVP task breakdown

| # | Task | Deliverable | Validation | Plan |
| :--- | :--- | :--- | :--- | :--- |
| 1 | **Standalone module + `VarkaMorsel`** | `sql/varka/engine/` Maven module; Arrow `DateDayVector` -> `MemorySegment` mapping | `VarkaMorselTest` | DONE (`PLAN_TASK_1.md`) |
| 2 | `DateVectorOps` SIMD kernels | `vectorAddDays` / `vectorSubDays` / `vectorDateDiff` (IntVector + bit-packed mask + scalar tail) | Differential unit test vs scalar reference; JMH vs scalar loop (follow-up) | `PLAN_TASK_2.md` |
| 3 | `VarkaClassLoader` + `TaskContext` lifecycle | Scala loader with `release()` via `TaskCompletionListener` | Metaspace plateau under 10k-query loop | TBD |
| 4 | Catalyst hooks | `ClassFileCodegenSupport` trait; `DateAdd`/`DateSub`/`DateDiff` emit `invokestatic` to `DateVectorOps` | Bytecode disassembly matches expected stack order | TBD |
| 5 | Class assembly + Ghost fallback | `JavaClassFileEngine` (Class-File API); hook in `CodeCompiler`/`CodeGenerator.compile`; lazy Janino string + cache on failure | Compile-failure injection test hits Janino path, no crash | TBD |
| 6 | Execution-path integration | Intercept in `ColumnarToRowExec` (Columnar.scala:134) when batch is Arrow-backed and projection is Varka-eligible | `SELECT DATE_ADD(...)` matches Janino result | TBD |
| 7 | Differential + perf testing | `QueryTest` suite (Varka on/off), JMH integrated, Metaspace stress | `checkAnswer` equality; throughput/Gen-time metrics | TBD |
| 8 | Config flags + docs | `spark.sql.codegen.varka.enabled/.patch.threshold/.fallback.ghost.enabled` in `SQLConf` | flag toggling tests | TBD |

**Open decision (deferred):** Tasks 3+ need a Spark-side home. Options:
(a) in-reactor optional module `sql/varka/spark/` (touches root `pom.xml`
module list + enforcer), or (b) standalone build against published Spark
jars. Revisit at the end of Task 2.
