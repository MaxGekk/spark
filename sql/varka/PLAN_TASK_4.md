# Varka Task 4 - Catalyst hooks (`ClassFileCodegenSupport`)

**Status: PLAN SAVED - implementation pending.** See
`IMPLEMENTATION_PLAN.md` for the high-level MVP plan. Task 4 is the declarative
Catalyst hook: a marker trait + emission contract that marks `DateAdd` /
`DateSub` / `DateDiff` as Varka-eligible and pins the exact `invokestatic`
stack order for the `DateVectorOps` batch kernels. Runtime routing (the
`JavaClassFileEngine` assembler + ghost fallback) is Task 5; execution
interception is Task 6.

## 1. Goal

- A `ClassFileCodegenSupport` trait (catalyst, Scala) that `DateAdd` /
  `DateSub` / `DateDiff` mix in, exposing a pure-data emission spec
  (`ClassFileGenOp`) plus an eligibility rule.
- A `CodegenContext` registry (`classFileGenExpressions` /
  `isClassFileGenEligible`) - the hook Task 5's router consumes.
- An engine-side (Java 25) probe test that assembles the `invokestatic`
  sequence for each kernel, defines it via `VarkaClassLoader`, runs it
  functionally, and **disassembles** it to prove the stack order.
- **Zero-risk:** the runtime string codegen of the three expressions is
  unchanged in Task 4. Existing behavior is preserved; routing is deferred.

## 2. Investigated areas (findings)

### 2.1 The expressions (datetimeExpressions.scala)

- `DateAdd(startDate: Expression, days: Expression)` (line 524) and `DateSub`
  (line 571) - `days` is an **Expression** (typically a `Literal`), typed
  `IntegerType`/`ShortType`/`ByteType` via `inputTypes`.
- `DateDiff(endDate: Expression, startDate: Expression)` (line 3531), both
  children `DateType`.
- All three override `doGenCode` (`nullSafeCodeGen` / `defineCodeGen`) and are
  `nullIntolerant`. They are not `CodegenFallback`.

### 2.2 The interception point

- `Expression.genCode` (Expression.scala:216-238) is the public entry;
  `doGenCode` (line 280) is the string hook. VISION directs the trait to
  intercept `genCode`, register, and not generate strings on the happy path.
- `CodegenFallback` (CodegenFallback.scala:26-65) is the existing precedent of
  diverting codegen through registration (it appends `this` to
  `ctx.references`).
- There is no `doCompile`/`currentContext` in this tree; compile is
  `CodeGenerator.compile` -> `CodeCompiler.active(code)` (CodeGenerator.scala:
  1567-1614).

### 2.3 The kernels (engine, Java 25)

`DateVectorOps` (Task 2) is batch/columnar, all `void`, primitives only
(DateVectorOps.java:67-184). Argument order IS the JVM stack order:

- `vectorAddDays(long srcData, long srcValidity, int srcNullCount,
  long dstData, long dstValidity, int length, int daysOffset)` ->
  descriptor `(JJIJJII)V`.
- `vectorSubDays` -> same descriptor, `(JJIJJII)V`.
- `vectorDateDiff(long dataA, long validityA, int nullCountA,
  long dataB, long validityB, int nullCountB,
  long dstData, long dstValidity, int length)` ->
  descriptor `(JJIJJIJJI)V`.

### 2.4 Module constraint (the "Spark-side home" resolution)

- Catalyst compiles in-reactor at `--release 17`; it CANNOT use
  `java.lang.classfile` (JDK 22+).
- The engine is a standalone module at `--release 25`
  (`org.apache.spark.varka:varka-engine:0.1.0-SNAPSHOT`).
- Consequence: the emission contract must be **plain strings** in catalyst;
  real assembly/disassembly lives in the engine. The descriptor strings are
  the compile-time linkage between the modules.
- **No new Spark-side module is needed for Task 4**: additive source in the
  existing `sql/catalyst` module plus the existing engine module. No root pom,
  no enforcer, no engine dependency in catalyst. Runtime linkage of the engine
  (Task 5) will be by name/reflection.

### 2.5 Disassembly surface (verified)

`ClassFile.of().parse` -> `ClassModel` -> method `CodeModel.elements()`
yields instruction views: `InvokeInstruction` (`owner()`, `name()`,
`typeSymbol()` -> `MethodTypeDesc`) and `LoadInstruction` (`slot()`,
`typeKind()`). This is exactly what the stack-order assertion needs.

## 3. Design

### 3.1 `ClassFileGenOp` (catalyst, pure data)

```scala
case class ClassFileGenOp(
    ownerClassName: String,    // "org.apache.spark.sql.varka.vector.DateVectorOps"
    methodName: String,        // "vectorAddDays" | "vectorSubDays" | "vectorDateDiff"
    methodDescriptor: String,  // "(JJIJJII)V" | "(JJIJJIJJI)V"
    kind: ClassFileGenOpKind)  // DateAdd | DateSub | DateDiff
```

### 3.2 `trait ClassFileCodegenSupport` (catalyst)

```scala
trait ClassFileCodegenSupport {
  self: Expression =>
  def classFileGenOp: ClassFileGenOp
  def isClassFileGenEligible: Boolean
  def daysOffsetConstant: Option[Int]
}
```

The trait also overrides `genCode` to register `this` into the ctx registry
and then delegate to `super.genCode(ctx)` - runtime behavior unchanged, the
registry populated for Task 5's router.

### 3.3 Patched expressions

- `DateAdd`/`DateSub`: `classFileGenOp` = the add/sub kernel spec;
  `isClassFileGenEligible` = `startDate` is a plain `Attribute` of `DateType`
  and `days` is a foldable integral literal (`daysOffsetConstant` folds it via
  `Number.intValue()`; null or non-foldable days -> not eligible).
- `DateDiff`: `classFileGenOp` = the diff kernel spec; eligible when both
  children are plain `Attribute`s of `DateType`.
- Nested Varka expressions (e.g. `DateDiff(DateAdd(a, 1), b)`) are out of MVP
  eligibility (the batch kernel needs concrete column buffers).

### 3.4 `CodegenContext` registry (additive)

- `classFileGenExpressions: mutable.ArrayBuffer[ClassFileCodegenSupport]`
- `registerClassFileGenExpression(e)`
- `isClassFileGenEligible: Boolean`

### 3.5 Plan-level collector `VarkaClassFileGen` (catalyst)

`def eligibleOps(projectList: Seq[Expression]): Seq[ClassFileGenOp]` - used by
Task 6 (ColumnarToRowExec interception) and Task 5 (assembler input). Unit
tested in Task 4.

## 4. Validation - "bytecode disassembly matches expected stack order"

- **Engine-side** `DateVectorOpsEmissionTest` (Java 25): for each kernel,
  assemble a probe class via the Class-File API with a
  `public void run(<prims>)` method whose body loads the args in stack order
  and `invokestatic`s the kernel; define it via `VarkaClassLoader`; run it on a
  small native buffer (functional check vs a scalar reference); then
  `ClassFile.of().parse(bytes)` and assert exactly one `InvokeInstruction`
  with the right owner/name/`typeSymbol`, preceded by the `LoadInstruction`
  sequence in the exact argument order.
- **Catalyst-side** `ClassFileCodegenSupportSuite`: asserts the emitted spec
  strings equal the engine's actual descriptor constants; the eligibility
  matrix (literal vs non-literal days; byte/short/int literals; non-date
  children; DateDiff nested-child exclusion); the `CodegenContext` registry.

## 5. File layout

```
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/
  ClassFileCodegenSupport.scala    (trait + ClassFileGenOp + VarkaClassFileGen)
  CodeGenerator.scala              (CodegenContext registry, additive)
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/
  datetimeExpressions.scala        (patch DateAdd/DateSub/DateDiff)
sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/codegen/
  ClassFileCodegenSupportSuite.scala
sql/varka/engine/src/test/java/org/apache/spark/sql/varka/vector/
  DateVectorOpsEmissionTest.java
sql/varka/PLAN_TASK_4.md
sql/varka/IMPLEMENTATION_PLAN.md   (task table update)
```

No pom changes: `sql/catalyst` and the engine are existing modules.

## 6. Definition of done (Task 4)

- Catalyst suite green (`ClassFileCodegenSupportSuite`), no regressions in the
  existing codegen suites.
- Engine suite green (`build/mvn -f sql/varka/engine/pom.xml test`, all prior
  + new emission test).
- Stack order proven by disassembly on the engine side; the catalyst test
  asserts the exact descriptor strings.
- Only `sql/varka/` + additive catalyst sources; ASCII, <=100-char lines.

## 7. Explicitly deferred

- Task 5: `JavaClassFileEngine` (full `GeneratedClass` assembly), routing at
  `CodeGenerator.compile`/`CodeCompiler` (skip string generation when
  eligible), ghost Janino fallback + cache, Spark-side `VarkaClassLoader`
  wiring.
- Task 6: `ColumnarToRowExec` interception, destination buffer allocation,
  non-eligible column handling, end-to-end `SELECT DATE_ADD(...)`.
- Task 7: differential + Metaspace stress. Task 8: config flags.

## 8. Follow-ups / risks

- Nested Varka expressions and non-foldable `days` are excluded from MVP
  eligibility; they keep the existing string path.
- The registry + `genCode` registration must not change the runtime codegen
  path in Task 4 (additive only).
