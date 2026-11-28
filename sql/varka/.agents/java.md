# Java code: modern Java 25, on the code a change touches

Set by the owner on 4 October 2026. They apply to the Java a change writes or touches and to
every Scala-to-Java port; existing Java that a change does not touch is not swept to match.

**The language.**

* **A record for every data carrier.** A type that holds values and has no mutable identity is a
  `record`, with its invariants checked in the compact constructor. A class only where mutable
  state or inheritance is the point.
* **Closed sets are sealed, and matched exhaustively.** A closed set of cases is a sealed
  interface of records, consumed by a pattern-matching `switch` with record patterns where they
  destructure, never by an `instanceof`-and-cast ladder. A switch over a sealed type or an enum
  has no `default`, so that a new case is a compile error at every consumer instead of a silent
  fallthrough. The IR (`VarkaVectorIR`) is the model.
* **`var` where the right-hand side names the type** (`var groups = new ArrayList<Integer>()`),
  the type written out where it does not (`int widest = widestDriver(driver)`).
* **Text blocks for multi-line strings; the diamond, method references, and `String.replace`
  over `replaceAll` for a literal.**
* **`List.of`, `Set.of`, `Map.of` for collections nothing modifies** - they reject nulls, which
  matters where a value arrives from Scala.
* **Statements before `super(...)`** (flexible constructor bodies, final in 25) for validating
  arguments in the few class hierarchies there are.
* **`ThreadLocal` for per-thread mutable buffers**, such as the fallback scratch; `ScopedValue`
  only for immutable context passed down a call, which Varka has not needed so far.

**What it runs on.**

* **The JDK, not a library**, for anything the JDK covers: no new Guava or Commons use in Varka's
  Java (and see the shading rule below for the Guava already in Spark).
* **`jdk.incubator.vector` is the one incubator module, and the reason the engine exists**;
  preview features stay off - no `StableValue`, no `StructuredTaskScope` - and so does anything
  that would need `--enable-preview` in Spark's build.
* **No allocation, streams, lambdas or boxing on a path that runs per batch or per row**: the
  runtime helpers an emitted kernel calls and the evaluators' batch loops. Streams, lambdas and
  records are welcome in the compiler, which runs once per shape; even there a plain loop beats a
  stream that is harder to read.
* **Explicit imports.** No `import module`: it hides what a file depends on, and Spark's
  checkstyle expects imports by name.

**The suites.**

* **A new catalyst suite whose helpers are Java is a JUnit 5 class under `@VarkaSuite`**
  (VARKA-312), each `@Test` carrying its sentence as a `@DisplayName`, the way a ScalaTest test is
  named: `@DisplayName("a reduction that fails differently is not smaller")`, not a method name
  standing in for one. The sentence is the test's name in sbt's output, the matrix's report, the
  JUnit XML and `sql/varka/matrix/skips.tsv`. `@Tag(VarkaSuite.PINS_DEFAULTS)` is `PinsDefaults`,
  and the annotation's `watchdogMinutes` and `ownJvm` are the ScalaTest traits' parameters;
  `dev/varka_matrix.sh` finds a `Varka...Suite` under it beside the Scala suites, so the gate and
  CI run it with no change. `sql/varka/skills/testing-and-debugging.md` says how to write and run
  one.
* **An existing ScalaTest suite moves only when a task touches it**, as for other code, and the
  sql/core suites stay ScalaTest: their base is Spark's `SharedSparkSession`, which no JUnit
  extension provides yet.

**What stays as it is.** Spark's interop surface stays Scala - the `SparkPlan` subclasses, the
Catalyst rules, the ScalaTest suites over Spark's sessions - as the migration rule in
`sql/varka/AGENTS.md` says. Test helpers are Java, and an existing ScalaTest suite is a thin Scala
layer over them. A new benchmark is Java on `VarkaBenchmarkBase`, the catalyst test tree's adapter
over Spark's harness (VARKA-224), and a Scala benchmark is ported when a task touches it, the port
proving itself by reproducing its committed results; the engine module's tests are JUnit 5 as they
are, and no new assertion library comes in. A refactor is proven by `emitted_bytes.json`
byte-identical, a port by the coverage and family-chain oracles. Lines stay within 100 columns, and
`dev/lint-java` passes, and so does `dev/varka_errorprone.sh`: Error Prone over Varka's Java, and
NullAway over every class `dev/varka_nullaway_pending.txt` does not list (VARKA-266).

## Build traps of Java and Scala in Spark's build

* Java in a non-core module cannot pass a Guava type to a `core` API. Maven shades
  `core`, relocating `com.google.common` to `org.sparkproject.guava`, so a signature
  like `NonFateSharingCache(Cache)` arrives at javac with the relocated parameter
  type. Scala is unaffected - scalac reads the Scala pickle, which shading does not
  rewrite - and SBT does not shade at all, so only the Maven CI job catches it. Keep
  Guava inside the module that owns it and reimplement the few lines locally when a
  `core` utility cannot be reached without one; `SKILLS.md` records the mechanism and
  the seconds-long local check. The same applies in reverse to Scala: a Scala call
  site passing a Guava type compiles but names a method the shaded artifact does not
  have.
* A Java class holding an incubator-module type (`jdk.incubator.vector`) in a field
  needs `--add-modules` in **two** places under Maven: `scala-maven-plugin`'s
  `javacArgs`, which compiles it, and that plugin's `jvmArgs`, because zinc extracts
  the class's API afterwards by calling `Class.getDeclaredFields()` reflectively in the
  compiler's own JVM. Missing the second gives `NoClassDefFoundError` *after* a
  successful compile, and only the Maven CI job sees it - the sbt launcher already runs
  with the module resolved. `SKILLS.md` records the mechanism and a seconds-long local
  check. The engine module needs neither block because it has no parent pom and uses
  `maven-compiler-plugin` directly.
* Scala cannot see `java.lang.classfile` types: Scala 2.13's typechecker reports
  "illegal cyclic reference" when completing them, and the Maven build's scaladoc
  pass fails on it. Emitter-adjacent code that touches the Class-File API stays in
  Java, with no class-file type in any signature and fully-qualified names inside
  method bodies; Scala reaches it through plain `byte[] -> String` shims.
  `build/sbt catalyst/doc` is the local gate that reproduces the CI failure.
