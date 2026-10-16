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

## 9. Outcome (16 October 2026): one no-option route resets the counts, at the price of a deoptimization

The placeholder this section held said that from Varka's own code, with no startup option, "the
limits cannot be raised and the counts cannot be reset". The first half stands; the second is
wrong. A running JDK 25 can load a Java agent into itself through its own `DiagnosticCommand`
MBean, and that agent can redefine the Vector API's classes with their own bytes, which gives
every method a new, empty MethodData (9.1). In the warm arm, one such reset a minute into the
run took the deadline releases from 58 and 68 to 1 and 0 (9.3). It is not a fix to ship (9.4).

### 9.1 The instrumentation route, read and run

**Loading an agent with no startup option.** `JVMTI.agent_load` is a diagnostic command exported
to every source, the MBean among them (`DCmdFactoryImpl<JVMTIAgentLoadDCmd>(full_export, ...)`,
`services/diagnosticCommand.cpp` 123). A library path ending in `.jar` is loaded as a Java agent
through the `instrument` library (299 to 330). The only gate is `EnableDynamicAgentLoading`
(`prims/jvmtiAgent.cpp` 494), a product flag whose default is `true` (`runtime/globals.hpp`
1765). If the flag is not set on the command line, the Java side prints a four-line warning
(`InstrumentationImpl.java` 107: "A Java agent has been loaded dynamically ... will be disallowed
by default in a future release", JEP 451). A program calls it on itself in one line, with no
attach and no `-Djdk.attach.allowAttachSelf`:

    ManagementFactory.getPlatformMBeanServer().invoke(
        new ObjectName("com.sun.management:type=DiagnosticCommand"), "jvmtiAgentLoad",
        new Object[] {new String[] {"/path/agent.jar"}}, new String[] {String[].class.getName()});

What it needs: the `jdk.management` and `java.instrument` modules in the runtime image, an agent
jar on disk whose manifest names an `Agent-Class` with `agentmain` and `Can-Redefine-Classes:
true`, and the flag not turned off. Experiment: on the system JDK 25.0.4.1, with no option at all
beyond `--add-modules jdk.incubator.vector`, the call returned `return code: 0`, printed the
warning, and `agentmain` received the `Instrumentation`. (Spark's test JVM options already carry
`-XX:+EnableDynamicAgentLoading`, from `project/SparkBuild.scala`'s `javaOptions` as
`jvm.opts` lists it, so in the replay the warning does not print.) The agent class is on the
application class path too, so the application sees the same class and its static
`Instrumentation` field.

**What redefinition does to MethodData.** `VM_RedefineClasses` parses the new bytes into a
scratch class and installs the scratch class's methods: `the_class->set_methods(_new_methods)`
(`prims/jvmtiRedefineClasses.cpp` 4269). Those are new `Method` objects with no MethodData; the
old ones are kept only for frames still running them, and the only MethodData code touches is
`clean_weak_method_links` on the new methods (3725 to 3731). jmethodIDs and reflected methods
resolve to the new methods afterwards. So redefining a class with its own bytes (read here from
`jrt:/modules/jdk.incubator.vector/...`) resets every count of every method of the class: the
trap history, the decompile count, the per-bytecode trap bits and the receiver profiles, all of
it. The class has to be modifiable: not hidden, not an array (`is_modifiable_class`, 337), so the
Vector API's lambda forms and hidden classes are out of reach, and the 115 to 189 loaded
`jdk.incubator.vector` classes that are not hidden are in. The cost is `flush_dependent_code`
(4085): `AlwaysRecordEvolDependencies` is `true` by default (`globals.hpp` 1988), so every
nmethod with a dependency on a redefined class, which is every compiled kernel, is deoptimized;
not the whole code cache.

`RetransformClasses` runs the same VM operation, so it is the same route. A native JVMTI agent
(the same diagnostic command with a `.so`) has the same two functions and no function that resets
MethodData. JVMCI's `CompilerToVM.reprofile` (`jvmci/jvmciCompilerToVM.cpp` 1375) does reset one
method: it makes its code not entrant and calls `MethodData::reinitialize()`, which zeroes every
cell. But it needs `-XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI` (`jvmci_globals.hpp` 47)
and `--add-exports` of `jdk.internal.vm.ci`, so it is a startup option like WhiteBox (not run).

**The probe.** A standalone program (`Probe370`, in the scratch directory named at the end) feeds
one compiled caller of `IntVector.add` receivers of four species, which records 22 to 30 traps in
3 or 4 vector methods, among them 8 on `IntVector.add`. A reduction loop through that caller then
allocates 11,360 to 12,384 bytes a call (64 iterations of 16 lanes), where it allocated 0 before
the pollution; each measurement deoptimizes the loop first so it is compiled again.

| probe (2 runs of 3 cycles each, on cores 0 to 2) | counts after the step | loop allocation after the step |
|---|---|---|
| nothing (1 run) | unchanged | 12,384 B in 3 of 3 cycles |
| `Agent370.redefine` of the 189 loaded vector classes, 68 to 94 ms | 0 in every method | 0 B in 4 of 6 cycles |
| WhiteBox `clearMethodState` on all 6,568 vector methods, 2 to 4 ms | 0 in every method | 12,384 B in 6 of 6 cycles |

So redefinition from inside a JVM started with no option resets the counts, and in this toy it
usually gives the clean compile back. The clear zeroes the counts and does not, here; one
candidate reason is that `init()` also zeroes the MethodData's own invocation and backedge
counters, and C2 reads a call site's receiver profile only from a mature MethodData
(`ciMethod::call_profile_at_bci`, `ci/ciMethod.cpp` 468; maturity is computed from those counters,
`CompilationPolicy::is_mature`). That was not shown, and the replay below disagrees with the toy.

### 9.2 Two corrections to section 2

1. **The clear does reach the per-bytecode route.** 2.3 says `clearMethodState` resets the sums'
   inputs "but not the per-bytecode route", and 6.1 prediction 2 rests on it. The per-bytecode
   bits do survive `init()`, but C2 never reads them alone: `ciMethodData::has_trap_at`
   (`ci/ciMethodData.cpp` 505) returns 0 when the method-level `trap_count(reason)` is 0,
   "regardless of trap_state", and `init()` zeroes those counts (`CompilerCounters()`). So after
   the clear the gate at `IntVector.add` bci 5 is open until the next trap of the same reason
   anywhere in the method, when the stale bit counts again. The 4-lane replay (9.3) agrees.
2. **`PerBytecodeTrapLimit` is not the gate C2 consults.** `VARKA-373.md` 2 reads the log's
   `count='4'` as the site having reached `PerBytecodeTrapLimit`'s default. In the source,
   `Compile::too_many_traps(method, bci, reason)` (`opto/compile.cpp` 4142) treats any recorded
   trap at the bci as too many ("Assume PerBytecodeTrapLimit==0"), and prints the method-level
   count; `PerBytecodeTrapLimit` is read only by the runtime, which makes the trapping code not
   entrant once the method's count reaches it at a bci that trapped before
   (`runtime/deoptimization.cpp` 2403). So the 4 in the log is the count at which the runtime
   forced the recompile, and raising the flag delays that recompile without opening the gate:
   the trapping code runs longer, which fits run `t6` of `VARKA-295.md` 9.5 (both trap limits at
   100,000, 151 releases and a deoptimization cycle).

### 9.3 The replay, with the flags and resets made at run time

The warm arm of `VarkaSparkFuzzSuite` ("both sides of the warm-up handover", seed 20261008) with
the template warm-up off (`-Dvarka.warmup.vectorTemplates=false`, since with it on the preferred
species already releases nothing, `VARKA-295.md` 9.7 and 9.8), in a JVM started by hand from
`dev/varka_matrix.sh --module sql --list`'s classpath and `jvm.opts` at master 7cc402b8f34,
pinned with `taskset -c 0-2`, one JVM at a time. Every arm, the control included, had
`-XX:+UnlockDiagnosticVMOptions -XX:+WhiteBoxAPI -Xbootclasspath/a:<WhiteBox jar>`, loaded the
agent through the MBean before the suite started, and read the WhiteBox counts over every
`jdk.incubator.vector` method every 30 s; the arms differ only in the one action:

* **flags**: WhiteBox `setIntxVMFlag` of `PerMethodTrapLimit` and `PerMethodRecompilationCutoff`
  to 100,000 before the suite starts (read back `100000 100000`);
* **redefine once**: at 60 s, `redefineClasses` of every loaded, non-hidden vector class with its
  own bytes, through the agent (115 to 117 classes, 124 to 409 ms);
* **clear once**: at 60 s, WhiteBox `clearMethodState` on every method of those classes (6,568
  methods, 2 to 7 ms).

60 s is after the first 18 to 76 class-check traps are recorded and before most releases (the
first in the control is iteration 53 of 400; the arm reports verdicts only at its end). Every run printed `Tests: succeeded 1`; every release was at the
deadline (10 s in this arm), none before it. The load is the 1-minute average before and after
each run, most of it other agents' work on cores 3 to 23.

| arm | lanes, compositions | deadline releases | COMPILED | seconds | load before / after |
|---|---|---|---|---|---|
| control | preferred (16), 400 | 58 | 476 | 905 | 22 / 48 |
| control | preferred, 400 | 68 | 466 | 894 | 22 / 13 |
| flags | preferred, 400 | 0 | 534 | 336 | 47 / 38 |
| flags | preferred, 400 | 0 | 534 | 458 | 39 / 34 |
| redefine once | preferred, 400 | 1 | 533 | 323 | 38 / 30 |
| redefine once | preferred, 400 | 0 | 534 | 186 | 12 / 9 |
| clear once | preferred, 400 | 2 | 532 | 293 | 27 / 23 |
| clear once | preferred, 400 | 3 | 531 | 824 | 35 / 39 |
| control | 4 (`lanesOverride=4`), 100 | 29 | 113 | 466 | 32 / 29 |
| control | 4, 100 | 30 | 112 | 391 | 21 / 24 |
| redefine once | 4, 100 | 17 | 125 | 319 | 26 / 36 |
| redefine once | 4, 100 | 16 | 126 | 260 | 36 / 18 |
| clear once | 4, 100 | 1 | 141 | 163 | 28 / 26 |
| clear once | 4, 100 | 3 | 139 | 178 | 18 / 22 |

The controls agree with the record (9.6: 18, 10, 39 on 12 cores; 9.7: 53, 78, 33 on 6;
`VARKA-373.md` 9, 4 lanes off: 31, 18).

What the counts did after the reset:

* **redefine**: the counts are back within 30 s (at the preferred species 83 to 106 traps, 21 to
  25 of them class checks, against 103 to 110 and 18 to 23 just before; at 4 lanes 143 to 182),
  presumably because every kernel's code was thrown away and recompiled from empty profiles. At the
  preferred species the releases stay away anyway, and the methods that hold class-check traps
  afterwards are others: the per-method list (added to the wrapper after the first
  control, flags and redefine runs) shows `VectorOperators.opKind`, 9.6's culprit, in the one
  control and the one flags run that carried it (the flags run also `ImplCache.find`), and neither
  appears in any of the three reset runs at the preferred species that carried it.
  So what helps is the template profiles filled again by a JVM that has already run many
  operator classes, as the template warm-up of 9.7 fills them by design, not the count being
  zero; that is a reading, not shown. At 4 lanes it halves the releases (16, 17 against 29, 30).
* **clear**: the counts stay low (7 to 25 traps at 90 s, 12 to 59 at the end, against 117 to 303
  at the end of the controls), since no code was thrown away and the zeroed method counts
  reopen the gates (9.2). It removes nearly all releases at both lane counts, the 4-lane
  per-bytecode case included, which the toy of 9.1 did not predict.

### 9.4 The answer to the row

| mechanism | verdict | evidence |
|---|---|---|
| raise the limits: `setVMOption`, `jcmd VM.set_flag`, `DiagnosticCommand` `vmSetFlag` | **not available** | 2.1 |
| compiler directives, run-time or `-XX:CompileCommand` | **available, cannot reach the limits or the counts** | 2.2 |
| WhiteBox `setIntxVMFlag`, `clearMethodState` | **needs a startup option** (`-XX:+WhiteBoxAPI`, the unlock, a boot class path jar); then the flags gave 0 and 0 releases and one clear 2 and 3 (1 and 3 at 4 lanes) | 2.3, 9.3 |
| JVMCI `reprofile` | **needs a startup option** (`EnableJVMCI` and an export); resets one method fully | 9.1, source only |
| a Java agent loaded in-process through `JVMTI.agent_load`, redefining the vector classes with their own bytes | **available with no startup option on JDK 25**, with a warning and on JEP 451's path to being refused by default; resets every count of every non-hidden vector class, and deoptimizes every kernel | 9.1, 9.3: 1 and 0 releases at the preferred species, 16 and 17 at 4 lanes |

So a per-method reset from Varka's own code is not possible without an option, a per-class reset
is: redefinition is per class, not per method, and the JVM's own code that inlined those
classes goes with it. Raising the limits stays an operator's option. For an executor this is a
worse remedy than the template warm-up (`VARKA-295.md` 9.7), which avoids creating the counts at
no deoptimization: redefinition throws away the compiled kernels of every query in the JVM, the
counts come back within seconds, it depends on a dynamic-agent default that the JDK has announced
it will change, and it brings a JVMTI capability into the product. It is worth knowing as a
diagnostic, and as the one in-process way to undo a pollution that has already happened (for
instance by query-path vector code before any warm-up, 9.7). Nothing in the product changes here.

### 9.5 Predictions of 6.1, scored

1. **Right.** The flags set through WhiteBox before the first warm-up gave 0 and 0 releases in
   1,068 warm-ups, as the startup options did.
2. **Wrong in its reason, right by a hair in its letter.** The clear removed fewer releases than
   the flags (2 and 3 left against 0 and 0), but it removed 56 of 58 and 65 of 68, and at 4 lanes,
   where the cause is the per-bytecode route the prediction said it would miss, 26 to 29 of 29 and
   30. The per-bytecode bits are not what decides (9.2). It was also run once at 60 s, not "after
   the warm-up of the first composition", which the arm does not report as it happens.
3. **Not run.** The forced-inline directive was not measured; the replays above answered the
   row without it.

### 9.6 Not done

A periodic reset (the redefinition every 30 s): the source and the once-arm's 30-second return
of the counts already say what it would cost, a deoptimization of every kernel each time. The
forced-inline directive (prediction 3). The 128-bit preferred species (`-XX:MaxVectorSize=16`),
whose releases `VARKA-373.md` 9 says are a different cause. The flags at 4 lanes: the per-method
flags do not touch the per-bytecode gate by the source (9.2), so they were not run there.

The probe, the agent, the replay wrapper (`Replay370`, which runs ScalaTest's `Runner` after the
agent load and the arm's action), the run script and every run's log are in
`/tmp/claude-1000/-home-max-proj-vecbricks-varka/53e2e690-d028-4b57-a163-fcdc477c97cf/scratchpad/370/`,
not in the repository; the WhiteBox jar there is built from the source tree's
`test/lib/jdk/test/whitebox`.
