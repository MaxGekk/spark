# VARKA-370: can a running JVM reset the counts that turn the Vector API's class-check traps into calls?

*Row 370 of `m7/PLAN.md`. Opened on 15 October 2026 from the reading of `VARKA-295.md` 9.6, which
is in pull request #825 (branch `varka-295e`) and not yet on master. The
admission check of section 2 is mostly done in the opening hour, because every mechanism it asks
about is a few lines of HotSpot source and a command that takes seconds; what it leaves open is
named in 2.4.*

## 1. Where this came from

`VARKA-295.md` 9.6 names the cause of the warm-up flake: C2 stops putting an uncommon trap on the
class-check miss path of the Vector API's inlined template calls (the hit path stays inlined) when
two sums over the whole compile pass a limit. Class-check traps are summed
against `PerMethodTrapLimit` (100) and decompiles against `PerMethodRecompilationCutoff` / 2 + 1
(201), each shared method's recorded MethodData count added once per inlined copy
(`Compile::too_many_traps` and `Compile::too_many_recompiles`, `opto/compile.cpp`). The loop of a
warmed kernel inlines `VectorOperators.opKind` 170 times and it holds 4 to 5 traps, recorded by
earlier standalone C2 compiles of the templates that speculated on one operator class. Past the
limit a class-check miss path becomes a real virtual call, the `binaryOp` intrinsic refuses the
vector, and one `int[16]` array survives per loop iteration. Setting `PerMethodTrapLimit` and
`PerMethodRecompilationCutoff` to 100,000 on the command line removed every release in 1,000
warm-ups, but a JVM option cannot be set by a Spark executor's own code. (Step 1's run `t6`, which
cycled, set `PerMethodTrapLimit` and `PerBytecodeTrapLimit`, not the cutoff: `VARKA-295.md` 9.5.)

Another task is testing a remedy inside Varka (pre-warming the shared templates, branch
`varka-295f`). This row asks the other question first, so that the remedy is chosen knowing what
the platform allows: **can the counts be reset, or the limits raised, from inside a running JVM,
and if so by what, with which startup option, and at what scope (the whole JVM or one method)?**
The answer decides whether Varka can fix this from its own code or only by documenting an
operator's option.

## 2. The admission check, done

The sources are `/home/max/proj/openjdk-build/jdk25` (commit `6c48f4ed7`, August 2025, a
JDK 25 GA-era tree); the experiments ran on the system JDK 25.0.4.1 (the distribution build). The
only difference met is that the tree's `WhiteBox.java` declares six natives the runtime does not
register, which prints six warnings and changes nothing below.
The experiments are not committed; each is described with its exact calls and results, so it
can be rewritten in a few lines: `ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)`
with `getVMOption` and `setVMOption`; `ManagementFactory.getPlatformMBeanServer().invoke` on
`com.sun.management:type=DiagnosticCommand` with `vmSetFlag` and `compilerDirectivesAdd`, each
taking a `String[]`; and `jdk.test.whitebox.WhiteBox.getWhiteBox()` with `setIntxVMFlag`,
`getIntxVMFlag` and `clearMethodState`.

### 2.1 The flags: none of the three is manageable

`java -XX:+UnlockDiagnosticVMOptions -XX:+PrintFlagsFinal -version` lists 14 `manageable` flags
(the heap-dump flags, the heap-free ratios, `SoftMaxHeapSize`, the periodic-G1 pair,
`PrintClassHistogram`, `PrintConcurrentLocks`, `ShowCodeDetailsInExceptionMessages`). `PerMethodTrapLimit` (100),
`PerBytecodeTrapLimit` (4), `PerMethodRecompilationCutoff` (400) and `PerBytecodeRecompilationCutoff`
(200) are plain `product` flags (`runtime/globals.hpp` 1353 to 1372), and `PerMethodSpecTrapLimit`
(5000) is `EXPERIMENTAL`; none carries `MANAGEABLE`. The consequence is the same through every
management door, because they share one check, `WriteableFlags::set_flag` (`services/writeableFlags.cpp`
237: "only writeable flags are allowed to be set"):

| door | result for `PerMethodTrapLimit`, `PerBytecodeTrapLimit`, `PerMethodRecompilationCutoff` |
|---|---|
| `HotSpotDiagnosticMXBean.setVMOption`, default JVM and with `-XX:+UnlockDiagnosticVMOptions` | `IllegalArgumentException: VM Option "..." is not writeable` for each; `getVMOption` reports `writeable=false`; `MaxHeapFreeRatio` as a control sets |
| `DiagnosticCommand` MBean, `vmSetFlag` | `only 'writeable' flags can be set` |
| `jcmd <pid> VM.set_flag <flag> 100000` | `only 'writeable' flags can be set` for all three; the control reaches its range check |

Verdict: **not available**, with or without the diagnostic unlock. Making a flag manageable is a
source change in `globals.hpp`, i.e. a JDK patch.

### 2.2 Compiler directives and `CompileCommand`: not available for these limits

A directive's options are the lists `compilerdirectives_common_flags` and
`compilerdirectives_c2_flags` in `compiler/compilerDirectives.hpp` (40 to 100), and the option
table behind `-XX:CompileCommand` and `Compiler.directives_add` is the same set
(`compiler/compilerOracle.hpp`). They hold `Enable`, `Exclude`, `Inline` and `DontInline`
patterns, `BackgroundCompilation`, `MaxNodeLimit`, `DisableIntrinsic`, `ControlIntrinsic`,
`RepeatCompilation`, the print and log switches, `CompileThresholdScaling` (a command only) and a
few more. `grep -i 'trap\|recompil\|MethodData\|TypeProfile'` over both files finds only
`CompilerDirectivesIgnoreCompileCommands`. A one-line experiment through the `DiagnosticCommand`
MBean's `compilerDirectivesAdd`, which a Java program can call on itself with no startup option:
a file `[ { match: "*.*", c2: { PerMethodTrapLimit: 100000 } } ]` is refused ("Parsing of
compiler directives failed"), and one setting `BackgroundCompilation` and an `inline` pattern for
`jdk.incubator.vector.VectorOperators::opKind` is accepted ("1 compiler directives added").

Verdict: the mechanism is **available** (at run time, from Java, with no startup option) but
**cannot reach the trap and decompile limits**. It can steer which methods inline
(`inline: ["+jdk.incubator.vector.*::*"]`), but that is the inlining decision of `InlineTree`;
the gate of the reading is not an inlining decision at all: the hit path stays inlined, and the
gate sits on the miss path of a class check
(`too_many_traps_or_recompiles` at `opto/doCall.cpp` 236 and 289), which a forced inline does not
consult. So a forced-inline directive is not a way to restore the trap. Whether a forced-inline directive changes the released loops is not tested here (2.4).

### 2.3 WhiteBox: reaches both, needs a startup option and a class the JDK does not ship

`prims/whitebox.cpp` registers its natives only when `WhiteBoxAPI` is true
(`JVM_RegisterWhiteBoxMethods`, 3029), a `DIAGNOSTIC` flag (`runtime/globals.hpp` 1866), and only
for a class loaded by the null class loader. The class (`jdk.test.whitebox.WhiteBox`) is part of
the JDK's test library, not of any JDK image, so it must be compiled from
`test/lib/jdk/test/whitebox/WhiteBox.java` and put on `-Xbootclasspath/a`. Two things it does that
the management beans do not:

* **It sets any flag.** `WB_SetIntxVMFlag` calls `JVMFlagAccess::set` directly
  (`SetVMFlag`, `whitebox.cpp` 1297), with no writeability check. Experiment: with
  `-XX:+UnlockDiagnosticVMOptions -XX:+WhiteBoxAPI` and the class on the boot class path,
  `setIntxVMFlag("PerMethodTrapLimit", 100000)`, the same for `PerMethodRecompilationCutoff`, and
  `setIntxVMFlag("PerBytecodeTrapLimit", 100)` (set only to show the third flag settable; it is not
  part of the remedy) read back `100000 100000 100`. Without
  `-XX:+WhiteBoxAPI`, and with only the unlock, the first call throws
  `UnsatisfiedLinkError` (the natives are never registered).
* **It clears one method's counts.** `clearMethodState(Method)` (`whitebox.cpp` 1255) calls
  `MethodData::init()`, whose first line resets the `CompilerCounters` (`_nof_decompiles` and the
  trap history, `oops/methodData.cpp` 1406 and `methodData.hpp` 2041), then cleans the profile
  and clears the not-compilable bits and the method counters. Experiment: it ran on
  `VectorOperators.opKind` without error. Per-bytecode trap records inside the profile are not
  cleared: `init()` resets the method-level counters, and `clean_method_data(true)` cleans weak
  class links and extra-data entries, while the per-bytecode trap states that
  `Compile::too_many_traps(method, bci, reason)` consults sit in the profile's data layouts. So the
  call resets the sums' inputs but not the per-bytecode route that `VARKA-295.md` 9.5 (`t5`) names
  as the other way to the virtual call. It also zeroes the method's invocation and backedge
  counters and cleans its profile, which can set off recompilation of the shared templates.

Both are read live: `Deoptimization::per_method_trap_limit` reads the flag on every call
(`runtime/deoptimization.hpp` 444) and `too_many_recompiles` reads
`PerMethodRecompilationCutoff` on every call (`opto/compile.cpp` 4192), so a value set at run time
governs the next compile, not the ones already installed.

Verdict: **needs a startup option** (`-XX:+UnlockDiagnosticVMOptions -XX:+WhiteBoxAPI` and
`-Xbootclasspath/a:<jar>`), and the jar is a build of the JDK's test library. This is no better
than the two startup options of 9.6 for an executor (both are JVM options set before launch), and
it is worse as a product dependency. It is useful in tests and in the replay: it would allow a
per-method reset in the fuzz suite's JVM without a restart.

### 2.4 What the check leaves open

* **Instrumentation.** A Java agent (`-javaagent`, or a late attach, which the JVM refuses for its
  own process unless `-Djdk.attach.allowAttachSelf=true` is given) can `retransformClasses` or
  `redefineClasses` the Vector API's template classes. Whether redefining a class with
  identical bytes gives its methods fresh MethodData (HotSpot keeps "equivalent modulo constant
  pool" methods and their profiles) was not read. Even if it did, an agent is a startup option
  (`-javaagent`) or an attach permission, so it only moves the question.
* **Whether the runtime change fixes the flake.** 2.3 shows the flags change; the claim that
  raising them mid-run removes the released loops is a prediction (6.1), tested by the fuzz
  suite's warm-up replay of `VARKA-295.md` 9.6 with the flags set from a WhiteBox call before the
  first warm-up.
* **A forced-inline directive.** Registered as prediction 3.
* **A different JDK.** The verdicts are for JDK 25; other versions were not read.

## 3. The design

There is no code change: the deliverable is the verdict table and the answer to the row's
question. The experiments of 2 are small enough to commit as a probe in the 295 follow-up if a
later row needs them again; this row does not commit one.

### 3.1 The verdicts so far

| mechanism | verdict | evidence |
|---|---|---|
| `HotSpotDiagnosticMXBean.setVMOption`, `jcmd VM.set_flag`, `DiagnosticCommand` `vmSetFlag` on the three flags | **not available** | 2.1: none is `manageable`; the three doors refuse them, unlocked or not |
| Making one manageable | needs a JDK patch | `globals.hpp` 1353 to 1372 |
| `Compiler.directives_add` / `DiagnosticCommand` `compilerDirectivesAdd` | **available at run time, cannot reach the limits** | 2.2: the trap-limit key is refused, a valid directive is added |
| `-XX:CompileCommand` | **needs a startup option**, same option set as directives | `compilerOracle.hpp` |
| WhiteBox `setIntxVMFlag` on the three flags | **needs a startup option** (`WhiteBoxAPI`, unlock, boot class path jar); then available | 2.3: read back `100000 100000 100` |
| WhiteBox `clearMethodState` | **needs a startup option**; then a per-method reset of the decompile count and method-level trap history, not of per-bytecode trap states | 2.3 |
| Instrumentation redefinition | **open** | 2.4 |

### 3.2 What is deliberately unchanged

Row 295's cell and `VARKA-295.md` (the other task owns the remedy and the warm-up code). No
Varka code, no JVM option in the build or the CI workflows.

### 3.3 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `sql/varka/plans/m7/VARKA-370.md` | this plan |
| `sql/varka/plans/m7/PLAN.md` | row 370, its entries in 2.4 and the ordering |

## 5. Tests, and what each is for

None yet. If the open items of 2.4 are closed by an experiment worth keeping, it becomes a probe
under the 295 follow-up, not a test of this row.

## 6. The measurement

The remaining step is one replay, in the design of `VARKA-295.md` 9.6: the warm-up fuzz arm over
400 compositions (500 warm-ups) a run, with `PerMethodTrapLimit` and `PerMethodRecompilationCutoff`
set by WhiteBox before the first warm-up (two runs, 1,000 warm-ups), and a third arm calling
`clearMethodState` on the shared templates (three runs), counting deadline releases. The
control is the 9.6 table (no option: 18, 10, 39 releases; both limits at 100,000 on the command
line: 0 and 0).

### 6.1 Predictions, registered before the run

1. Setting `PerMethodTrapLimit` and `PerMethodRecompilationCutoff` to 100,000 through WhiteBox
   before the first warm-up gives 0 releases in the two runs' 1,000 warm-ups, as the startup options did,
   since both are read at every compile.
2. Calling `clearMethodState` on `VectorOperators.opKind` and the other two shared templates of
   9.6 (`Int512Vector::lanewise`, `ImplCache::find`) after the warm-up of the first composition
   removes fewer releases than the flags do: it resets the method-level counts but not the
   per-bytecode trap states, and the next composition's standalone compiles can set the counts
   again. The replay says how many it removes; none would show the per-bytecode route matters.
3. A forced-inline directive for `jdk.incubator.vector.*` leaves the releases unchanged, since
   the gate is on the miss path and not on the inlining decision.

## 7. Risks

1. **The runtime JDK differs from the source tree** (25.0.4.1 against an August 2025 commit). The
   experiments cover behaviour; the line numbers are the tree's. A later reader checks the
   function names, not the numbers.
2. **WhiteBox is a test API.** A result obtained with it says what the platform can do, not what
   Varka may ship; the row's answer keeps the two apart.
3. **A per-method reset may not hold.** The standalone compiles of the shared templates can raise
   the counts again, the per-bytecode states stay, and `init()` zeroes the method's counters and
   cleans its profile, which can cause recompilation of the template; 6.1 prediction 2 tests the
   first two, and the replay's log shows the third.

## 8. Sequencing

1. This plan with the findings of 2, and the row marked planned. No code.
2. The replay of section 6, after `varka-295f` shows what the in-Varka remedy does, so the two
   results are read together.
3. Section 9, and the answer to the row: whether a per-JVM or per-method reset is possible from
   Varka's own code, or only by an operator's option.

## 9. Outcome

Open. What the admission check already says: from Varka's own code with no startup option, the
limits cannot be raised and the counts cannot be reset. Run-time directives are available but do
not reach them. Only WhiteBox does, and it needs two JVM options and a boot class path jar, so a
Spark executor gets it no more cheaply than the two options of `VARKA-295.md` 9.6. The remedy
that needs no option is to avoid creating the counts, which is what `varka-295f` tests.
