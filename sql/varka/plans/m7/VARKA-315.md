# VARKA-315: A reproduction file when a kernel fails

*Row 315 of `m7/PLAN.md`, added by the owner on 12 October 2026 with rows 313, 314 and 316 to 319
(`m7/PLAN.md` 2.4, "Errors and troubleshooting"). Planned on 14 October 2026, after VARKA-314a
(#805) landed the context record this row writes out. Section 2 corrects the row in five places.*

## 1. Where this came from

The row: a generated class that fails to load or verify, a memory-sanitizer violation, or a
run-time exception inside a kernel writes one self-contained file - the canonical IR, the options
that differ from the defaults, the plan fragment and shape key the emitter takes, and the failing
batch where there is one - which `dev/varka_emit.sh` replays to the same failure. The file is
VARKA-314's context record written out, headed by the failure's `VarkaFailureSignature` kind so
that a replay is compared with it mechanically. The models are Velox's `persistDataAndSql` and
`ExpressionRunner` (`m7/READING.md` 4: a failing expression's input vectors and SQL saved, and a
runner that replays them) and the JVM's `hs_err` report (written by default, on the failure, with
what was running). Done when a planted failure of each kind writes a file that replays to the same
signature.

## 2. The admission check, done

Read on 14 October 2026 in a worktree at master `f37b52026ba` (after #805 and #807).

**Where each kind surfaces today.**

| kind | production | under test (`spark.testing`) | what is left behind |
| --- | --- | --- | --- |
| class fails to verify or define | `VarkaShapeCacheImpl.emit`: `defineGeneratedClass`, then `getConstructor()` links the class, so a `VerifyError` or `ClassFormatError` leaves `getOrEmit` (which unwraps Guava's wrapper) into `VarkaEvaluatorBase.fusedRunner`'s emission catch; `isCatchable` admits a `LinkageError`, so it is an `emission-failure` fallback | earlier: `VarkaSizeAdmission.admit` asks `VarkaShapeCache.admit(key)` at planning with `verify = Utils.isTesting()`, `ClassFile.verify` fails, the `VerifyError` is not `NonFatal` so the admission rethrows it, and the compiler's planning catch (`catch (RuntimeException)`, VARKA-314a) lets it pass: the query fails on the driver with a raw `VerifyError` | a warning with 314's record (production); nothing but the stack trace (test). The class dump never has the class: `VarkaClassDump.dump` runs in `newRunner` after `getOrEmit` returned |
| class fails to link at first execution (`NoSuchMethodError`) | the call site resolves when the kernel first runs, inside `VarkaKernelRunner.invoke`, which wraps it as `VarkaKernelFailure`: a `kernel-failure` fallback with the batch | the same, then `refuseUndeclaredFallback` fails the task | the record with the batch's row and null counts, not its values |
| memory-sanitizer violation | the sanitizer is off in production (`-Dvarka.sanitizeMemory`, a `static final`) | a `VarkaMemoryViolation` from a mapping in the kernel (`VarkaVectorSupport.ofAddress`) or from `verifyCanaries` after it; `isCatchable` excludes it, so `invoke` rethrows it unwrapped and `serveBatch`'s `catch (Throwable)` rethrows it before any record is built: the task fails | the violation's message alone |
| run-time exception inside a kernel | `invoke` wraps it as `VarkaKernelFailure`: `kernel-failure` | the same, then refused | as for the link failure |

**What the class dump needs.** `spark.sql.codegen.varka.classDumpDirectory`, set before the
failure; it writes the class bytes of every kernel that loads, and none of a kernel that fails to.

**What `dev/varka_emit.sh` takes.** SQL expression text, resolved against `--columns name:type`,
and `--options name=value,...` (`VarkaEmitDump`); it emits, and with `--rounds` runs the kernel
over synthetic int32 data. It takes no IR, no batch and no file, and nothing parses
`VarkaVectorIR.canonical` back: there is no reader for the canonical form.

**What the emitter takes, and why the canonical IR is enough to replay.** The class is
`VarkaLoopEmitter.emit(classNameFor(hash), outputs, numInputs, numLiterals, sourceFileFor(hash),
"shape " + hash, options)` (`VarkaShapeCacheImpl.build`), every argument a function of the
`VarkaShapeKey` (outputs, input count, literal count, options, warmed). The shape hash is SHA-256
over the outputs' canonical renderings, the two counts and `options.canonical()`, and the cache
serves one class per hash, so two trees with one canonical rendering already get one class: the
canonical form carries everything the bytes depend on. Two components are absent from it on
purpose and need a rule in a parser: `ConstDivide.dividendBound`, which "changes no emitted byte"
(its javadoc), and `BoundedDivide`'s multiplier and shift, which `BoundedDivide.of` derives from
the divisor and bound. The literal values are not in the key at all; they travel as the kernel's
`scalarArgs` and `longArgs`, so a file must carry them beside the key.

**What the row's "plan fragment" is today.** The emitter's `planFragment` argument has been the
string `shape <hash>` since the class cache (`VarkaDebugInfo`'s class comment); the Catalyst
entries are in the evaluator's identity, which 314's record carries. So the file has the record
for a reader and the shape key for a replay, and no separate fragment.

**What `VarkaFailureSignature` classifies.** It is in catalyst's *test* tree (VARKA-277), a record
of a kind and the message's first line with its numbers replaced by `#`. Kinds: `memory violation`
for a `VarkaMemoryViolation`; `generated class: <simple name>` for any `LinkageError` (so a
`VerifyError` and a `NoSuchMethodError` are both "generated class" kinds, told apart by the name);
four mismatch kinds the kernel checks raise; `declined batch`; `emitter rejection`; otherwise the
exception's simple class name. Production code cannot call it from the test tree, so it moves to
main, unchanged.

**Which failures can be planted so that a file replays them.** A plant has to ride the shape key,
or the replay has nothing to reproduce it from: the test hooks (`setFailKernelForTesting`, which
throws a `NoClassDefFoundError` in the runner and not in the kernel, and
`setFailEmissionForTesting`) are not in any file. Fault-injector options ride the key:

* `misdescribeAdd` exists: an `AddDays` against a wrong descriptor, which verifies and then fails
  at first execution with `NoSuchMethodError` naming `IntVector.add`. A link failure inside the
  kernel, through the kernel-failure path.
* Nothing produces a class the verifier rejects: `misdescribeWordLiveness` makes the emitter throw
  rather than build a class.
* Nothing maps outside a buffer, and nothing makes an emitted kernel throw at run time.

So two fault injectors are added (3.4), neither reachable from a configuration, both excluded from
the fuzzer's draws and the option matrix by their `FAULT_INJECTOR` reason, as the five existing
ones are (`VarkaIrFuzzSuite`, `VarkaMatrixRuns.configurations`, `VarkaMatrixSuite`).

**Where the failing batch can be read.** A kernel's inputs are not always the batch's columns: a
derived input (a weekday name or a trunc level parsed by the row engine's parser) is an int32
column `fill` writes into the task's scratch. Both kinds of kernel failure that carry a batch
(the kernel-failure and the violation) come out of `invoke`, after `fill` completed for the batch,
so the runner's `srcData`, `srcValidity` and `srcNullCount` are that batch's - 314a's correction 6
found them stale only after a failure inside `fill`, which is not a kernel failure and writes no
file. The file records the kernel's inputs as the kernel read them: what a replay needs, and what
no Catalyst-level batch would give for a derived input.

**What the admission would have rejected.** A replay from SQL text through `VarkaEmitDump`'s
existing path: the row's own wording suggested it, but the evaluator holds bound Catalyst
expressions that need not print back as SQL that resolves alike (ANSI flags, time zones, analyzer
rewrites), and it would replay the compiler as well as the kernel. And a replay that ran the class
bytes the file carries: it would reproduce the failure for ever, after the emitter was fixed.

### 2.1 Corrections to the row

1. *"The plan fragment and shape key the emitter already takes"*: the emitter takes the shape key;
   its plan fragment is `shape <hash>`. The file carries the key, and the Catalyst entries in the
   record's identity.
2. *"Which `dev/varka_emit.sh` replays"*: the tool takes SQL text. It gains `--replay <file>`, and
   the canonical IR gains a parser, held to `shapeHash(parse(canonical(t))) == shapeHash(t)`.
3. *"A generated class that fails to load or verify"*: in production it fails at the emission, with
   no batch; under test it fails at planning, in the admission's verification, with no evaluator.
   Both write the file (3.2).
4. *"The failing batch"*: the kernel's inputs as the kernel read them, a derived input included,
   not the Catalyst batch.
5. *"Planted failure of each kind"*: only the link failure has a plant today; two fault injectors
   are added for the other kinds.

## 3. The design

### 3.1 The file

One text file, `key: value` lines, so a reader can open it and a parser can read it. In order:

1. `# Varka reproduction file (format 1); replay with dev/varka_emit.sh --replay <this file>`
2. `signature kind:` and `signature message:` - `VarkaFailureSignature.of(failure, "")`, where the
   failure is what failed: the `VarkaKernelFailure`'s cause, the violation, or what the emission,
   the definition or the verification threw.
3. The 314 record, as `VarkaErrorContext.render` writes it (indented, for the reader; the parser
   skips it).
4. `written at`, `java`, `sanitizer: on|off`.
5. The shape key: `shape hash`, `lane`, `inputs`, `literals`, `warmed`, `options` (the
   `name=value` label 314 renders and `VarkaMatrixRuns.parse` reads), one `output:` line per
   output's canonical IR.
6. The literal values: `int literals`, `long literals`.
7. The kernel call, where there is one: `rows`, `scratch bytes per row`, `output widths`, and one
   `input:` line per kernel input - its width, the capacity its data and validity buffers had, its
   null count, whether it was derived, and its data and validity bytes in Base64. A batch over
   `maxBatchBytes` keeps every line but the bytes, and says so (`batch: omitted ...`).
8. `class:` the failing class's bytes in Base64, where a class was defined and is under 4 MiB:
   the replay compares the class it emits with it, so a file whose failure the emitter no longer
   reproduces says whether the bytes changed.

### 3.2 Where it is written

Each site already holds the record's fields and calls one writer, `VarkaReproduction.write`
(catalyst, `codegen/varka/`), in a `catch`, then rethrows or falls back exactly as before:

* **The kernel's two failures**, in `VarkaEvaluatorBase.runKernel`: a `catch (VarkaKernelFailure |
  VarkaMemoryViolation e)` around the existing body, which asks the runner for the call
  (`VarkaKernelRunner.capture`), writes, and rethrows `e` unchanged. `runKernel` is the method whose
  sanitizer window the violation came from, and a further kernel of a projection runs it as its own
  evaluator (`VarkaKernelPart`), so its file names it. `serveBatch`'s catches are unchanged.
* **The emission**, in `fusedRunner`'s emission catch: the shape key and literals, no batch, no
  class.
* **The planning verification**, in `VarkaSizeAdmission.admit`: on a `LinkageError` from the
  admission, before rethrowing it, with stage `planning`, so a replay verifies the bytes as the
  admission did instead of loading them.

A record built for the file has the kind `memory-violation` for a violation (a new constant beside
`PLANNING`); the others keep 314's tags.

### 3.3 Enabled, where, and how big

* **On by default.** The row's complaint is that the class dump has to be configured before the
  failure, so a file that has to be enabled first would not answer it. `hs_err` is on by default for
  the same reason. `spark.sql.codegen.varka.reproduction.enabled` (internal, default true) turns it
  off.
* **Where.** `spark.sql.codegen.varka.reproduction.directory`, unset by default, meaning
  `<java.io.tmpdir>/varka-reproductions` on the JVM that failed - an executor's for a kernel, the
  driver's for the planning verification. The directory is created owner-only (`rwx------`) and
  each file owner-only (`rw-------`) where the file system has POSIX permissions, since a batch is
  user data. The file is written to a temporary name and moved into place, so a reader never sees
  half of one. Its name is `varka-repro-<shape hash>-<kind>-pid<pid>.txt`.
* **The writer's warning names the file** and the command that replays it, which is how a user
  finds it: `Wrote a reproduction of this Varka failure to <path>; replay it with
  dev/varka_emit.sh --replay <path>`.
* **Bounds.** One file per shape and signature kind per JVM, so a kernel that fails on every batch
  writes once; at most 32 files per JVM; the batch's bytes only when the kernel's inputs total
  `spark.sql.codegen.varka.reproduction.maxBatchBytes` or less (internal, default 16 MiB, 0 never
  writes values); the class only under 4 MiB. A batch over the cap still records its row count,
  null counts and capacities, and a replay then runs a synthetic batch of that shape and says so: a
  failure that does not depend on the values - a link failure, a wrong mapping - still replays.
* **The original exception is never touched.** The writer is called from a `catch` and returns; it
  catches every throwable of its own except a `VirtualMachineError` and logs it as a warning, so a
  write failure cannot replace the failure or fail a query that would have fallen back. The
  settings are read through `SQLConf.get()` inside the writer, on the failure path only, which also
  keeps them out of the exec nodes' constructors.

### 3.4 The fault injectors

* **`dropStackMaps`** (flag, `FAULT_INJECTOR`): the class is built with the Class-File API's
  `StackMapsOption.DROP_STACK_MAPS`, so every method with a branch fails verification with
  "Expecting a stackmap frame at branch target" - a verifier failure of the kind a stack-map bug in
  the emitter would give.
* **`misdescribeDataBytes`** (count, `FAULT_INJECTOR`, zero in production): bytes added to the
  kernel's `dataBytes`, the nominal size every input, output and scratch segment is mapped at.
  Positive maps past a buffer, which the sanitizer reports as a violation (and which nothing reads
  without it); negative maps short, and the vector loop's last load fails with
  `IndexOutOfBoundsException` inside the kernel - the run-time exception.

At zero and false both emit exactly today's bytes, so `emitted_bytes.json` does not move.

### 3.5 The replay

`dev/varka_emit.sh --replay <file>` runs `VarkaEmitDump --replay <file>`, in a JVM started with
`-Dvarka.sanitizeMemory=true` when the file says the sanitizer was on. The replay
(`VarkaReplay`, catalyst test tree):

1. parses each output's canonical IR (`VarkaCanonicalIr`), rebuilds the shape key with the options
   parsed from the label, and refuses when its hash is not the file's - the proof that nothing was
   lost on the way;
2. emits the class as the cache builds it, and says whether its bytes are the file's;
3. walks the failing path as far as the file's kind says: verifies the bytes (`planning`), or
   defines and links the class (`emission-failure`), or runs it over the recorded call
   (`kernel-failure`, `memory-violation`) with the sanitizer's window opened and every buffer
   registered or guarded as `runKernel` and the runner do;
4. classifies what it threw with `VarkaFailureSignature` and prints it beside the file's: the same
   signature exits 0, anything else - another signature, or no failure - exits 1.

### 3.6 What is deliberately unchanged

* The ghost fallback and every catch's decision: a file is written beside them.
* 314b's sites and its docs section (`VARKA-314.md` 8).
* The warm-up's background run of a new kernel (`VarkaKernelWarmup`), which runs it on a copy of a
  batch on its own thread: a failure there is not one of the row's three paths today and writes no
  file.
* 313's metrics and `VarkaFallbackAccounting`, which this row does not edit.

### 3.7 Registered op counts

None: no default emission changes; `emitted_bytes.json` must be unchanged.

## 4. Files

| file | what |
| --- | --- |
| `VarkaFailureSignature.java` | moved from catalyst's test tree to main, unchanged |
| `codegen/varka/VarkaReproduction.java` (new) | the file: its record, rendering, parsing and the bounded writer |
| `codegen/varka/VarkaErrorContext.java` | the `memory-violation` kind |
| `codegen/varka/VarkaEmitOptions.java`, `VarkaEmitOption.java` | the two fault injectors |
| `codegen/varka/VarkaLoopEmitter.java`, `VarkaBodyEmitter.java` | what they do |
| `codegen/VarkaSizeAdmission.java` | the planning verification's write |
| `internal/SQLConf.scala` | the three settings |
| `execution/varka/VarkaKernelRunner.java` | `capture`, the kernel call read at a failure |
| `execution/VarkaEvaluatorBase.java` | the `runKernel` catch and the emission catch's write |
| catalyst test: `VarkaCanonicalIr.java`, `VarkaReplay.java` (new), `VarkaEmitDump.java` | the parser, the replay and `--replay` |
| catalyst test: `VarkaReproductionSuite.java` (new, JUnit) | sections 5.1 to 5.4 |
| catalyst test: `VarkaEmitOptionSuite.scala` | the two options' reason |
| core test: `VarkaReproductionEndToEndSuite.scala` (new) | sections 5.5 to 5.8 |
| `dev/varka_emit.sh` | `--replay` |
| `docs/sql-varka.md`, `sql/varka/skills/`, `SKILLS.md` | the file, the settings, the lesson |

Row 313 (in flight beside this one) edits the metrics and `VarkaFallbackAccounting`; this row
touches `VarkaEvaluatorBase` in two existing catches only.

## 5. Tests, and what each is for

1. **The parser round-trips every node type and every drawn tree**: for the every-node-type key and
   for trees of the IR fuzz grammar at both lanes, `canonical(parse(canonical(t)))` equals
   `canonical(t)` and the shape hash agrees. Catches a node the parser does not know, or a
   component it fills wrongly.
2. **The file round-trips**: written and read back, every field equal, the batch's bytes included;
   a batch over the cap reads back as omitted with its shape.
3. **Each plant replays at the emitter level**: `dropStackMaps` fails `ClassFile.verify` and the
   link; `misdescribeAdd` and `misdescribeDataBytes < 0` fail the run; each written as a file and
   replayed to the same signature. And a file whose options no longer produce the failure replays to
   "not reproduced", exit 1: the comparison can fail.
4. **A file whose IR does not hash to its shape hash is refused**.
5. **End to end, the run-time exception**: a query under `misdescribeDataBytes=-4` falls back, the
   file is written with the batch's values, and replays to `IndexOutOfBoundsException`'s signature.
6. **End to end, the link failure**: `misdescribeAdd`, the same, `generated class:
   NoSuchMethodError`.
7. **End to end, the violation** (with `-Dvarka.sanitizeMemory=true`, cancelled without, as
   `VarkaMemorySanitizerEndToEndSuite` is): `misdescribeDataBytes=+4096` fails the task with the
   violation, unchanged; the file replays to `memory violation`.
8. **End to end, the verification**: `dropStackMaps` fails planning with the `VerifyError`,
   unchanged; the file replays to `generated class: VerifyError`.
9. **The writer never interferes**: with the directory a file, the planted kernel failure still
   falls back with the right answer and the same warning, and the write failure is a warning; with
   `enabled=false` nothing is written; a second failure of the same shape writes no second file.
10. **The bytes oracle and the option suites**: `emitted_bytes.json` unchanged,
    `VarkaEmitOptionSuite` with the two new reasons, the binding-policy audit.

## 6. The measurement

None on this laptop: other jobs share it, and no committed number would hold. The change adds no
code to a path a batch takes unless it fails; prediction 1 is checked by the existing allocation
test instead of a benchmark.

### 6.1 Predictions, registered before the run

1. No per-batch change: the `VarkaErrorContextSuite` steady-state allocation test and
   `VarkaProjectExecSuite`'s sampler test pass unchanged.
2. `emitted_bytes.json` is unchanged.
3. The parser round-trips every tree the grammar draws on the first try, apart from
   `ConstDivide`'s bound, which needs the long-lane rule of 2.
4. The end-to-end suite adds under 30 s to the sql step.

## 7. Risks

1. **A batch is user data on disk.** Owner-only permissions, a byte cap, a switch, and the
   documentation say so; 0 for `maxBatchBytes` keeps values out entirely.
2. **The replay's buffers differ from production's** - an Arrow capacity rounded otherwise, a canary
   not placed - and a violation names a different buffer. The file records each input buffer's
   capacity and the replay registers exactly that; test 7 checks the signature, which names the
   buffer's role.
3. **A signature that carries a run's address**: `VarkaFailureSignature` replaces every number, a
   hexadecimal address included, so an address in a violation's message reads `#` on both sides.
4. **A writer that throws while a failure is handled** would replace it: test 9.

## 8. Sequencing

1. This plan, and the row corrected.
2. `VarkaFailureSignature` to main.
3. The canonical-IR parser and its round-trip test.
4. The fault injectors.
5. The file and its writer, the settings, the write sites.
6. The replay, `--replay`, and the end-to-end suite.
7. Docs, the lesson, section 9 and the row.

## 9. Outcome

*14 October 2026.*

**What was built.** As section 3 planned: `VarkaFailureSignature` moved to main; the two fault
injectors; `VarkaReproduction` (the file, its text form, its parser and its bounded writer);
the three settings; the four write sites (`runKernel` for a kernel's exception and for a
violation, the emission catch, the admission's verification); `VarkaKernelRunner.capture`;
`VarkaCanonicalIr`, `VarkaReplay` and `VarkaEmitDump --replay` behind `dev/varka_emit.sh
--replay`.

**Done-when, checked.** Each kind planted through a fault injector writes a file that replays to
the same signature, in `VarkaReproductionEndToEndSuite` (sql) through the production write
sites: `IndexOutOfBoundsException` (`misdescribeDataBytes=-4`), `generated class:
NoSuchMethodError` (`misdescribeAdd`), `memory violation` (`misdescribeDataBytes=4096`, with
the sanitizer), `generated class: VerifyError` (`dropStackMaps`, at planning under test) and an
emitter's own `IllegalStateException` (`misdescribeWordLiveness`, through the emission catch),
and a filter kernel's `IndexOutOfBoundsException` through the filter's own write site.
Two files were replayed by hand through `dev/varka_emit.sh --replay` as well (the violation and
the verify failure, both `REPRODUCED`, exit 0), and a file with its signature edited replayed to
`NOT REPRODUCED`, exit 1. A file another suite's planted fault wrote under the default settings
(`VarkaSparkFuzzSuite`'s `misdescribeRollback=3`, an `IllegalArgumentException` from the
emitter) replayed to its signature too, with no change to that suite.

**Where the code corrected the plan.**

1. **The writer swallows a `VirtualMachineError` too.** 3.3 said it catches everything else; an
   `OutOfMemoryError` there is most likely its own copy of the batch, and rethrowing it would
   replace the failure it records. The write sites also build their arguments (the record, the
   shape key) inside a catch of their own, since those are evaluated before the writer is entered.
2. **The file has no `lane` line.** The replay derives the lane from the IR
   (`VarkaVectorIR.emissionLane`), so a file cannot disagree with itself about it.
3. **The record is written as `# ` comment lines**, since a cause's message can span lines (a
   `VerifyError`'s does) and would otherwise break the `key: value` form.
4. **Test 5.3 moved to the sql suite.** Planting each kind at the emitter level and writing its
   file there would have produced the file's signature with the replay's own harness, which proves
   nothing; the end-to-end suite plants through the real write sites. The catalyst suite keeps the
   verify failure at both stages that meet it and the not-reproduced case, and adds a check that
   every option's label reads back through `VarkaMatrixRuns.parse` - the file's `options` line
   depends on it.
5. **A production verify failure at the emission is not reached by a query under test**, since the
   admission verifies first (2). The emission catch's write is covered by the emitter-throw plant,
   and the definition step of the replay by `VarkaReproductionSuite`.
6. **The review's corrections** (`/code-review high` on the branch, ten findings):
   - the filter's kernel runs outside `runKernel` (`VarkaFilterEvaluator.filterMask`), so 3.2's
     one site missed every predicate kernel: the filter's mask call now writes too, and the call
     records that its outputs were the filter's selection bitmaps, which the replay lays out in
     one buffer as the filter does;
   - the emission catch's shape hash could read `unavailable (...)` (314's `safely`), which named
     the file after it and made every replay refuse it: the writer uses a caller's hash only when
     it is one, and hashes the key otherwise;
   - the admission wrote any `LinkageError` as `planning`, whose replay only verifies: it writes
     only a `VerifyError` now;
   - 3.3's per-JVM bounds do not bound a shared directory over executors that restart, and the
     pid alone does not tell containerised executors apart: a directory holds at most 256 files
     from all JVMs, and each name carries a per-JVM random token after the pid;
   - the record and the call were built on every failing batch before the writer's checks: the
     writer now takes the record as a supplier and tests its memo before reading the settings, and
     a failed write keeps its claim, so a directory that cannot be written warns once;
   - the replay leaked its class loader when definition failed, and registered a zero validity
     capacity as a whole bitmap; `dev/varka_emit.sh` expanded an empty array that bash before 4.4
     calls unbound.
   One finding is not changed: the replay's output buffers always have room for a canary, where
   production's capacity is Arrow's. Under the sanitizer `runKernel` allocates `CANARY_ROWS`
   extra rows for exactly that room, so production's outputs have it too.
7. **Files written by the suites land in each test JVM's own tmpdir** (`target/.../tmp`, which
   Spark's build sets), not `/tmp`: the default location needed no test-only rule.

**Predictions (6.1), scored.**

1. **Held.** `VarkaErrorContextSuite` (its steady-state allocation test included) and
   `VarkaProjectExecSuite` pass in the scoped sql run; no code runs on a batch that does not fail.
2. **Held.** `VarkaEmittedBytesSuite` passes; `emitted_bytes.json` is unchanged.
3. **Held.** The parser round-tripped every node type and 800 drawn trees on the first run, and
   60 long-lane shapes emitted byte-identical classes from the parsed trees.
4. **Held, pessimistic.** The end-to-end suite's eight tests take 4.5 s (38 s with sbt's start)
   against 30 s predicted; the line is in the calibration table.

**Evidence**, on the final commit. `dev/varka_scoped_suites.sh catalyst`: 646 passed, 26
canceled, 0 failed, and the quiet step 7 passed, 14 canceled. `dev/varka_scoped_suites.sh sql`
(sanitizer and both-bodies check on): 691 passed, 7 canceled, 0 failed, and the quiet step 5
passed. The end-to-end suite
alone, after the review's fixes (a ninth test, a filter kernel's failure): 9 passed with the
sanitizer on; before them, 7 passed and 1 canceled without it.
`SparkConfigBindingPolicySuite` 3 passed; `catalyst/doc` passes.

**Left for later.**

- A kernel failure on the warm-up's background thread writes no file (3.6).
- Under the both-bodies check (`-Dvarka.checkBothBodies`), a violation in the body the batch did
  not take is written with the batch, but the replay runs the body the batch's null counts choose.
- The default directory under a shared `/tmp` can be created first by another user, which leaves
  the files unwritten (a warning) rather than readable: the file is created owner-only either way.

**Found by the option matrix, 15 October 2026.** On #816's job, `groupBudget=64` failed "a
memory-sanitizer violation fails the task unchanged and writes a file that replays to it" with
"not a Varka reproduction file of format 1; its first line is" (empty), while the matrix's own
rerun on the merge base passed: #816 changes no code, and the failure was a race the test lost.
`VarkaReproduction.write` claims a `(shape hash, kind)` pair and then writes through a temporary
file and a move. A query's tasks run side by side: the first to fail claims the pair, a sibling
failing on the same shape finds it claimed, rethrows at once, and the job's failure reaches the
test while the first task is still creating its temporary file (an empty, dot-named file, which
the test then listed as "the" file and parsed) or has not created it yet. The test already used
its own directory (`spark.sql.codegen.varka.reproduction.directory`), so another suite's file
was never the cause. Reproduced locally as the matrix runs it, the test body repeated 300 times
in one JVM pinned to two cores beside three busy loops: 17 of 300 iterations failed before the
change, with the empty-first-line error and with "expected one reproduction file, got:" (none
yet), and 1 of 300 after waiting for a finished file, an iteration that found no file at all
for 30 s. Spacing the iterations by 300 ms removed that last one in 300 more (and in 300 under
the defaults), which reads as a task of the previous iteration claiming the pair after the
test's `resetForTesting`; one test per failure kind does not repeat a kind back to back. The
test's `reproduced` now waits up to 30 s for a finished file (a name without the leading dot)
before counting. The product is left as it is: the error reaching the user before the file is
written is by design (the writer never delays a failure), and a file that is only ever renamed
into place is never half written.

The sweep of the suite over every configuration found a second, older failure of the same
family, in the three `arrowBatchSize` configurations (1, 3, 17; a batch-size axis, not an emit
option): the cached view's null every 97th row left the failing batch of a small-batch run
without nulls, and the tests read that batch's null count and validity. The view now has a null
every second row, so any batch of more than one row holds both; for one row a batch no data can
(a one-row batch is without nulls or all null, which has no validity buffer), and the two tests
that read it are skip-list lines under `arrowBatchSize=1`.

Review of the branch: the null-every-second-row view no longer gives these tests a null-free batch
(the replay's no-nulls body is reached by `VarkaReproductionSuite`, not here), and the leftover
task described above is still possible in a back-to-back loop and is not closed by the poll.
