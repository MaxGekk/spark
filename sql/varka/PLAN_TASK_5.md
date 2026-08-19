# Varka Task 5 - Class assembly + Ghost fallback

**Status: PLAN** (decisions recorded). See `IMPLEMENTATION_PLAN.md` for the
high-level MVP plan. Task 5 builds on the Task 4 hooks: a Class-File API
engine that assembles the full `GeneratedClass` shape, routing in the single
`CodeGenerator.compile` funnel, a lazy Janino ghost fallback, and a
catalyst-side class loader mirroring the engine's `VarkaClassLoader`.
Execution-path interception (the real batch dispatch) is Task 6.

## 1. Goal

- `JavaClassFileEngine` (catalyst, `java.lang.classfile`): assemble the full
  Janino-equivalent `GeneratedClass` shape for a Varka-eligible codegen
  context, driven by the Task 4 registry (`ctx.classFileGenExpressions`).
- Routing hook in the single `CodeGenerator.compile` funnel, driven by the
  Varka ops attached to `CodeAndComment`.
- Ghost fallback: on assembly/load failure, lazily route to the Janino
  backend; the winning path is cached under the same key so a failed
  assembly is never retried and the user job never crashes.
- Catalyst-side `VarkaGeneratedClassLoader` mirroring the engine's
  `VarkaClassLoader` contract (define + registry + `release()`); per-task
  `TaskCompletionListener` wiring is deferred to Task 6.
- Validation: compile-failure injection test hits the Janino path, results
  correct, no crash.

## 2. Investigated areas (findings)

### 2.1 The compile funnel

- `CodeGenerator.compile(CodeAndComment)` (CodeGenerator.scala:1581) is the
  single funnel: it routes to the cache `loadFunc` (1612) which calls
  `backend.compile(code)` (`CodeCompiler.active(code)`).
- `CodeCompiler.active` routes to the Janino or JDK backend; both implement
  `compile(code): (GeneratedClass, ByteCodeStats)`.
- There is **no** `doCompile`/`currentContext` ThreadLocal in this tree
  (VISION/MVP sketches are outdated); the `CodegenContext` is local to each
  generator and alive exactly where the `CodeAndComment` is constructed.

### 2.2 `CodeAndComment` call sites

Nine `CodeGenerator.compile` call sites; the MVP-relevant ones are:

- `GenerateUnsafeProjection.create` (GenerateUnsafeProjection.scala:436).
- `GenerateMutableProjection` (:149).
- `WholeStageCodegenExec.doCodeGen/doExecute` (WholeStageCodegenExec.scala:742).
- `WholeStageCodegenEvaluatorFactory` (:45).

`WholeStageCodegenExec.doExecute` already wraps compile in a try/catch that
falls back to interpreted execution when `conf.codegenFallback` is set - the
final safety net below the ghost fallback.

### 2.3 The registry (Task 4)

- `ctx.classFileGenExpressions: mutable.ArrayBuffer[ClassFileCodegenSupport]`
  (CodeGenerator.scala:156) is public, populated by
  `ClassFileCodegenSupport.genCode` registration.
- `VarkaClassFileGen.eligibleOps(projectList: Seq[Expression])` computes the
  ops. Note: `ArrayBuffer[ClassFileCodegenSupport]` is invariant, so the
  call site must widen (e.g. `ctx.classFileGenExpressions.map(e => e:
  Expression)`) or `eligibleOps` gains an overload for
  `Seq[ClassFileCodegenSupport]`.

### 2.4 `GeneratedClass` contract

- `abstract class GeneratedClass { def generate(references: Array[Any]): Any }`
  (CodeGenerator.scala:1440).
- `compile` returns a `(GeneratedClass, ByteCodeStats)`; callers invoke
  `clazz.generate(ctx.references.toArray)` (e.g.
  GenerateUnsafeProjection.scala:437). The assembled class must therefore:
  extend `GeneratedClass`, have a public no-arg constructor, and its
  `generate(Object[])` must return the evaluator instance.

### 2.5 Bytecode disassembly constraint

- Scala 2.13 hits a cyclic-reference bug when it reads the sealed Class-File
  instruction hierarchy (`UnboundRetInstruction`); disassembly assertions
  must live in a Java helper (precedent: `ClassFileGenOpVerifier`, Task 4).

### 2.6 Engine linkage

- The engine module (`sql/varka/engine`) is standalone, not in the Spark
  reactor; catalyst references `DateVectorOps` only by name
  (`ClassDesc.of`). Runtime resolution of the kernel class is a classpath
  concern (Task 6 deployment). Task 5 tests use a test-only stub kernel with
  the same FQCN.

## 3. Design

### 3.1 `CodeAndComment` carries the Varka ops

- Add `val classFileGenOps: Seq[ClassFileGenOp] = Nil`; excluded from
  `equals`/`hashCode` (like `comment`) since it is a function of the body.
- Generators attach the ops when building the `CodeAndComment`:
  `GenerateUnsafeProjection`, `GenerateMutableProjection`,
  `WholeStageCodegenExec`, `WholeStageCodegenEvaluatorFactory`. Other call
  sites (predicate/ordering/etc.) leave the list empty and keep Janino.

### 3.2 Routing + ghost fallback in `CodeGenerator.compile`

- Inside the cache `loadFunc`: if `code.classFileGenOps.nonEmpty`, try
  `JavaClassFileEngine.assembleGeneratedClass` -> define via
  `VarkaGeneratedClassLoader` -> instantiate -> return
  `(GeneratedClass, ByteCodeStats)`. On `NonFatal` -> `logWarning` and fall
  through to `backend.compile(code)` (Janino).
- Because both paths run inside `loadFunc`, the `NonFateSharingCache` caches
  whichever path won under the same key `(classLoaderRef, backend, code)`; a
  failed assembly is never retried.
- `ByteCodeStats`: compute from the assembled model (max method code size /
  const pool size) via a Java helper, or return `ByteCodeStats.UNAVAILABLE`
  initially. Minor decision, resolved during implementation.

### 3.3 `JavaClassFileEngine` (catalyst, Class-File API)

- `assembleGeneratedClass(className, ctx, ops, schema): Array[Byte]`
  producing the Janino-equivalent shape:
  - public wrapper class extending `GeneratedClass` with
    `generate(Object[])` returning `new SpecificVarkaProjection(references)`;
  - `SpecificVarkaProjection` extends `UnsafeProjection`: references field,
    no-arg constructor, `initialize(int)` no-op, and `apply(InternalRow)`
    **stub** throwing `UnsupportedOperationException` ("Varka batch
    execution wired in Task 6").
- The stub is honest about Task 5 scope: the batch kernels cannot run per-row;
  real dispatch lands in Task 6's `ColumnarToRowExec` interception.
- Test hook: `@volatile var failAssemblyForTesting: Boolean` in the companion
  (`private[expressions]`), checked at the top of assembly, forcing the ghost
  path in tests.

### 3.4 `VarkaGeneratedClassLoader` (catalyst mirror)

- Extends `ClassLoader`; `defineGeneratedClass(name, bytes)` + registry +
  `findClass` + idempotent `release()` - mirrors the engine
  `VarkaClassLoader` contract. The engine loader remains for engine tests;
  the mirror is the runtime one (catalyst cannot depend on the engine).

### 3.5 Test stub kernel

- Test-only `org.apache.spark.sql.varka.vector.DateVectorOps` with the same
  FQCN on the catalyst test classpath, so any happy-path class linkage
  resolves in tests. Real engine jar on the Spark runtime classpath = Task 6.

## 4. Validation

- **`JavaClassFileEngineSuite`** (catalyst):
  - full-class shape by disassembly (Java helper): wrapper `generate` bridge,
    wrapper->evaluator wiring, references field, `apply` present;
  - `generate(references)` returns an `UnsafeProjection` instance;
  - loader define/release lifecycle (define, load, release -> cleared,
    load-after-release throws).
- **Ghost fallback injection test**: an eligible `DateAdd(startAttr,
  Literal(3))` projection through `GenerateUnsafeProjection` with
  `failAssemblyForTesting=true` -> projection works, results equal the Janino
  path, no crash; an assembly-attempt counter proves the fallback is cached
  (assembled exactly once across repeated `generate` calls).
- **Regression**: existing codegen suites green (`GeneratedProjectionSuite`,
  `GenerateUnsafeProjectionSuite`, `CodegenSubexpressionEliminationSuite`,
  `CodeCompilerSuite`); engine suite untouched.
- **Style**: ASCII, <=100-char lines, scalastyle clean.

## 5. File layout

```
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/
  JavaClassFileEngine.scala            (new)
  VarkaGeneratedClassLoader.scala      (new)
  CodeGenerator.scala                  (CodeAndComment.classFileGenOps + loadFunc routing)
  GenerateUnsafeProjection.scala       (attach ops)
  GenerateMutableProjection.scala      (attach ops)
sql/core/src/main/scala/org/apache/spark/sql/execution/
  WholeStageCodegenExec.scala          (attach ops)
  WholeStageCodegenEvaluatorFactory.scala (attach ops)
sql/catalyst/src/test/java/org/apache/spark/sql/catalyst/expressions/codegen/
  ClassFileShapeVerifier.java          (new Java disassembly helper)
sql/catalyst/src/test/java/org/apache/spark/sql/varka/vector/
  DateVectorOps.java                   (test stub, same FQCN as engine kernel)
sql/catalyst/src/test/scala/org/apache/spark/sql/catalyst/expressions/codegen/
  JavaClassFileEngineSuite.scala       (new)
sql/varka/PLAN_TASK_5.md               (this file)
sql/varka/IMPLEMENTATION_PLAN.md       (task table update)
```

## 6. Definition of done (Task 5)

- `JavaClassFileEngineSuite` green; ghost fallback injection test hits the
  Janino path with correct results and no crash; assembly not retried after
  the fallback (cached).
- Assembled class shape proven by disassembly (generate bridge + evaluator +
  references wiring + apply present).
- Existing codegen suites green; engine suite untouched.
- ASCII, <=100-char lines; only catalyst/sql-core additive source + docs.

## 7. Explicitly deferred (Task 6)

- `ColumnarToRowExec` interception when the batch is Arrow-backed and the
  projection is Varka-eligible.
- Real batch dispatch (replaces the `apply` stub), destination buffer
  allocation, Arrow buffer -> `MemorySegment` mapping.
- `TaskCompletionListener` release wiring for `VarkaGeneratedClassLoader`.
- Engine jar on the Spark runtime classpath (kernel resolution).
- End-to-end `SELECT DATE_ADD(...)` matching Janino results.

## 8. Follow-ups / risks

- The funnel routes on `classFileGenOps` attached at `CodeAndComment`
  construction, so the Janino string is still built on the happy path (cheap
  concatenation; Janino parsing/compilation is what is skipped). VISION's
  strict "zero strings" would require early routing before string build - a
  follow-up refinement to revisit when Task 6 touches the generators.
- The `apply` stub is a landmine if anything invokes it before Task 6; tests
  must never call it (the ghost test forces assembly failure, so the stub is
  never reached).
- ArrayBuffer invariance at the `eligibleOps` call site (see 2.3).