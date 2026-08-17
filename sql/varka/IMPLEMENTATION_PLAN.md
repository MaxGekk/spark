# Varka MVP Implementation Plan

This is the implementation plan for the Varka MVP (Date Arithmetic Over
ArrowColumnarBatch). It supersedes the sketch in `Varka_MVP_Plan.md` where the
two disagree; `VISION.md` remains the architectural source of truth.

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
  Varka_MVP_Plan.md              (unchanged)
  IMPLEMENTATION_PLAN.md         <- this file
  engine/                        <- STANDALONE Java 25 module (Task 1-2). NOT in Spark reactor.
    pom.xml                      (--release 25, --add-modules jdk.incubator.vector)
    src/main/java/org/apache/spark/sql/varka/
      memory/VarkaMorsel.java    (Task 1)
      vector/DateVectorOps.java  (Task 2)
    src/test/java/...            (Task 1/2 unit tests)
  spark/                         <- FUTURE Spark-side integration module (Tasks 3+); integration
                                    strategy TBD when Task 2 lands
```

Design rules carried from VISION: zero string generation on the happy path;
constants passed as runtime args (never inlined into a plan hash); SIMD with
strict scalar tail; masked loads/stores so null lanes never read garbage;
ghost Janino fallback only in the Spark-side compile hook (Task 5).

## 3. MVP task breakdown (Tasks 2-8 outlined, Task 1 detailed in section 4)

| # | Task | Deliverable | Validation |
| :--- | :--- | :--- | :--- |
| 1 | **Standalone module + `VarkaMorsel`** | `sql/varka/engine/` Maven module; Arrow `DateDayVector` -> `MemorySegment` mapping | `VarkaMorselTest` - full detail below |
| 2 | `DateVectorOps` SIMD kernels | `vectorAddDays` / `vectorSubDays` / `vectorDateDiff` (IntVector + bit-packed mask + scalar tail) | Differential unit test vs scalar reference; JMH vs scalar loop |
| 3 | `VarkaClassLoader` + `TaskContext` lifecycle | Scala loader with `release()` via `TaskCompletionListener` | Metaspace plateau under 10k-query loop |
| 4 | Catalyst hooks | `ClassFileCodegenSupport` trait; `DateAdd`/`DateSub`/`DateDiff` emit `invokestatic` to `DateVectorOps` | Bytecode disassembly matches expected stack order |
| 5 | Class assembly + Ghost fallback | `JavaClassFileEngine` (Class-File API); hook in `CodeCompiler`/`CodeGenerator.compile`; lazy Janino string + cache on failure | Compile-failure injection test hits Janino path, no crash |
| 6 | Execution-path integration | Intercept in `ColumnarToRowExec` (Columnar.scala:134) when batch is Arrow-backed and projection is Varka-eligible | `SELECT DATE_ADD(...)` matches Janino result |
| 7 | Differential + perf testing | `QueryTest` suite (Varka on/off), JMH integrated, Metaspace stress | `checkAnswer` equality; throughput/Gen-time metrics |
| 8 | Config flags + docs | `spark.sql.codegen.varka.enabled/.patch.threshold/.fallback.ghost.enabled` in `SQLConf` | flag toggling tests |

**Open decision (deferred):** Tasks 3+ need a Spark-side home. Options:
(a) in-reactor optional module `sql/varka/spark/` (touches root `pom.xml`
module list + enforcer), or (b) standalone build against published Spark
jars. Revisit at the end of Task 2.

## 4. Task 1 - Detailed plan: standalone module + `VarkaMorsel`

### 4.1 Goal

A self-contained Java 25 module that maps an Arrow `DateDayVector`'s data
buffer and bit-packed validity buffer into `java.lang.foreign.MemorySegment`s,
verified by unit tests against Arrow's own accessors. No Spark, no SIMD ops,
no codegen in this task.

### 4.2 Files to create

```
sql/varka/engine/pom.xml
sql/varka/engine/.gitignore                          (target/)
sql/varka/engine/src/main/java/org/apache/spark/sql/varka/memory/VarkaMorsel.java
sql/varka/engine/src/test/java/org/apache/spark/sql/varka/memory/VarkaMorselTest.java
```

### 4.3 `pom.xml` (key config)

- Standalone project (NO Spark parent - the parent pins Java 17 and enforces
  bytecode version). `groupId=org.apache.spark.varka`,
  `artifactId=varka-engine`.
- `maven.compiler.release=25`
- Deps: `org.apache.arrow:arrow-vector:19.0.0`,
  `org.apache.arrow:arrow-memory-netty:19.0.0` (netty allocation manager for
  `RootAllocator`; add `arrow-memory-unsafe` if the manager fails to resolve),
  test scope `org.junit.jupiter:junit-jupiter` (5.10+).
- `maven-surefire-plugin` 3.2.5 with
  `<argLine>--add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED</argLine>`.
- Build with the repo wrapper on JDK 25:
  `build/mvn -f sql/varka/engine/pom.xml test` (first run downloads Maven). If
  the wrapper misbehaves on a standalone pom, install Maven and use `mvn`
  directly.

### 4.4 `VarkaMorsel.java` - public API

```java
package org.apache.spark.sql.varka.memory;

public final class VarkaMorsel {
  private VarkaMorsel() {}

  /** int32 days-since-epoch column + bit-packed validity, mapped to MemorySegments. */
  public record DateMorsel(
      MemorySegment data,      // int32 days; byteSize = dataBuf.capacity() (>= rowCount*4)
      MemorySegment validity,  // bit-packed, 1 bit/row; byteSize = validityBuf.capacity()
                               //   (>= (rowCount+7)/8); null when the vector is all-null
      int rowCount,
      long nullCount) {
    public boolean allNull() { return nullCount == rowCount; }
    public boolean noNulls() { return nullCount == 0; }
  }

  public static DateMorsel extractDate(ValueVector vector, int rowCount);
  public static void reportAlignment(DateMorsel m);  // diagnostic only
}
```

`extractDate` steps:

1. Validate `vector instanceof DateDayVector`; `rowCount >= 0`;
   `rowCount <= vector.getValueCount()` (else `IllegalArgumentException`).
2. If `vector.getNullCount() == rowCount` -> `validity = null`, skip validity
   mapping.
3. `ArrowBuf dataBuf = vector.getDataBuffer();` ->
   `data = MemorySegment.ofAddress(dataBuf.memoryAddress()).reinterpret(dataBuf.capacity());`
4. `ArrowBuf validityBuf = vector.getValidityBuffer();` ->
   `validity = MemorySegment.ofAddress(validityBuf.memoryAddress()).reinterpret(validityBuf.capacity());`
5. Return record. Segments are sized to **buffer capacity**, not
   `rowCount*4`/`(rowCount+7)/8` - this is what makes the Task 2 masked reads
   safe at the tail (see pitfalls).

### 4.5 Pitfalls baked into the design

- **Validity is bit-packed.** Task 2 will read a `long` at validity byte
  offset `i/8` (chunk start `i` is always a multiple of 8) and build the mask
  via `VectorMask.fromLong(SPECIES, bits)`; `bits` bit *j* corresponds to row
  *i+j*. The doc's byte-per-lane mask is wrong and must not be reproduced.
- **Tail long read can exceed the nominal `(rowCount+7)/8` bytes** (e.g.
  `rowCount=8` -> 1 byte; a `JAVA_LONG` read at offset 0 needs 8). Sizing
  segments to buffer `capacity()` covers the common case, but Task 2 must
  still guard `i/8 + 8 <= validity.byteSize()` before the vector chunk and
  push the remainder to the scalar tail. `VarkaMorselTest` adds
  non-multiple-of-8 row counts (13, 17) precisely to exercise this.
- **Zero-capacity buffers**: all-null or 0-row vectors may return capacity-0
  buffers; `reinterpret(0)` is valid, and `nullCount`/`rowCount` fields let
  Task 2 short-circuit.
- **Alignment is diagnostic, not contractual.** `reportAlignment` logs each
  buffer address and its 64-byte alignment; nothing asserts it.
- **Native access**: `MemorySegment.ofAddress` is a restricted method - the
  `--enable-native-access=ALL-UNNAMED` argLine and executor JVM flag (VISION
  section 10) are mandatory.
- **Endianness**: reads use `ByteOrder.LITTLE_ENDIAN` (Arrow's in-memory
  layout) in Task 2, not native order.

### 4.6 `VarkaMorselTest.java` - test matrix

All cases allocate a `DateDayVector` under an Arrow `RootAllocator`, populate
it, then **assert the segment read-back against Arrow's own `vector.get(i)` /
`vector.isNull(i)` as the oracle**:

1. `N=1000`, alternating valid/null -> data ints and validity bits match;
   `nullCount` matches `vector.getNullCount()`.
2. No-null vector -> `nullCount == 0`, `noNulls()`, all validity bits set.
3. All-null vector -> `validity == null`, `allNull()`.
4. Empty vector (`rowCount=0`) -> both segments valid objects, zero reads.
5. Boundary rows: `N = 1, 7, 8, 9` (validity byte boundaries), `N = 13, 17`
   (non-multiple-of-8 tail), `N = 64, 100, 1000`.
6. `data.byteSize() >= 4*rowCount` and `validity.byteSize() >= (rowCount+7)/8`.
7. `rowCount > vector.getValueCount()` -> `IllegalArgumentException`.
8. `reportAlignment` prints addresses/alignment (log-only, not asserted).

### 4.7 Definition of done (Task 1)

- Module compiles at `--release 25` with both JVM flags; all `VarkaMorselTest`
  cases pass via `build/mvn -f sql/varka/engine/pom.xml test`.
- No changes anywhere in the Spark reactor (root `pom.xml` untouched; Spark
  still builds at Java 17).
- Alignment diagnostics visible in test output.

### 4.8 Out of scope for Task 1

`DateVectorOps` SIMD kernels, JMH, `VarkaClassLoader`, Catalyst hooks,
`CodeCompiler` integration, Ghost fallback, `SQLConf` flags - Tasks 2+.
