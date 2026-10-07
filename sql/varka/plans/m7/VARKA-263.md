# VARKA-263: A sanitizer for kernel memory access

*Scoped 7 October 2026 (milestone 7 section 2.2, row 263); opened 7 October 2026.*

## 1. The question

A kernel gets raw addresses. `VarkaVectorSupport.ofAddress` is
`MemorySegment.ofAddress(addr).reinterpret(bytes)`, where `bytes` is computed inside the kernel from
the batch length and checked against nothing: the segment's bounds check enforces the size the
kernel claimed, not the size the buffer has. Item 58 found the emitter's plumbing to be where the
record's bugs were (a byte-per-lane read of a bit-packed validity buffer among them), and the
word-at-a-time validity writes are safe only because the buffers Varka allocates itself are padded:
Arrow only recommends padding, and its IPC reader slices a buffer to its exact length
(`m7/READING.md` 5). So nothing today fails by name when a kernel reads or writes past a buffer;
it reads what lies beyond, or writes into a neighbour, or takes the JVM down.

Row 263 asks for a test-only check of every mapped segment against the buffers the evaluator
really handed the kernel, a canary past each buffer Varka owns, a record of where each segment came
from, and every byte back at close, with the suites and the fuzzers run under it.

What the code looks like today (7 October 2026, master `72c0fbf2fbf`):

* The addresses are produced in three places: `VarkaKernelRunner.fill` (the sources, and the
  derived inputs in the scratch), `VarkaEvaluatorBase.scala:288` (the destinations) and
  `VarkaFilterEvaluator.scala` (the filter's own two paths), and consumed by the emitted kernels
  through three `ofAddress` call sites in `VarkaBodyEmitter` and by the engine's ops.
* Nine production sites map an address without going through `ofAddress` at all:
  `SelectionVectorOps:196`, `IntRangeOps:69,72`, `TruncLevelLeaf:71,72`, `WeekdayLeaf:91,92`,
  `VarkaKernelWarmup:624,639` and `VarkaFilterEvaluator.scala:151` (`VarkaMorsel:137` maps a whole
  `ArrowBuf` at its real capacity, which is already the right size).
* **The modules do not see each other.** The engine has no dependency on catalyst, and both
  catalyst and `sql/core` have `varka-engine` at *test* scope only, so neither can name
  `VarkaVectorSupport` in main code; the emitted kernels reach it by name at run time. A registry
  that the runner fills and `ofAddress` reads therefore cannot live in the engine: `core` could not
  call it. It can live in catalyst, which `core` sees, with the engine's `ofAddress` reaching it by
  a method handle resolved only when the flag is on.

## 2. The change

Done in steps, one commit each, so that each can be reverted alone:

1. *Route every raw mapping through one function a module,* with no behaviour change: the engine's
   `ofAddress`, and in catalyst a small `VarkaSegments.map(addr, bytes)` that the nine sites use;
   a test (and a pre-commit rule) that fails on `.reinterpret(` anywhere else, so that a tenth
   site cannot appear. Proof: `emitted_bytes.json` byte-identical (the emitter is untouched).
2. *The registry,* `VarkaMemorySanitizer` in catalyst, enabled by `-Dvarka.sanitizeMemory=true`,
   read once into a `static final`. A thread-local window, armed on the task thread for one batch:
   buffers register into it (label, address, real capacity) as the runner and the evaluators
   obtain them, and a mapping outside every registered range fails naming its label. A thread that
   never armed a window (the warm-up's thread, a unit test calling a kernel directly) is not
   checked. Address 0 is allowed only for a segment of 0 bytes (the all-null validity). The check
   in `ofAddress` is `if (ENABLED) CHECK.invokeExact(addr, bytes)` with the body elsewhere, so the
   method stays under the inlining threshold.
3. *Canaries,* only past bytes Varka owns: under the flag the evaluator allocates its output
   vectors, and the scratch, with a small tail, registers the range without it, writes a pattern in
   the tail, and checks it when the kernel returns. An input buffer is never written past its
   capacity: that memory is Arrow's.
4. *Every byte back at close:* the Varka test base asserts, after each suite, that the root
   allocator is back to its baseline and that the warm-up arena and scratch are closed.
5. *On in the suites and the fuzzers,* off in the benchmarks, the bytes oracle and the JIT-measuring
   phase (the sanitizer's branches change what C2 compiles), with a test that fails if the flag is
   not set in a suite JVM, so that "runs with it on" is checked and not claimed.

Alternatives considered: *pass `MemorySegment`s to the leaves instead of addresses*, so that the
runner maps them through the checked function: rejected, since it changes the public signature of
five methods and their tests for what the thin `VarkaSegments` does without touching them; *a
SQLConf instead of a system property*: rejected, a test-only switch should not enter the config
audit; *checking inside the emitted kernel*: rejected, the off path must cost nothing and the
emitter's bytes must not move.

## 3. Predictions, registered before the run

1. `emitted_bytes.json` is unchanged by every step (the emitter is untouched).
2. With the flag off, `ofAddress` stays under 35 bytecode bytes (HotSpot's `MaxInlineSize`) and is
   still inlined into a kernel, which `-XX:+PrintInlining` shows on one kernel.
3. With the flag on, the Varka suites and the fuzzers find no out-of-range access on master: item
   58's bugs were fixed. The first runs will find false positives (the all-null validity address,
   buffers registered late), and each of those is a bug in the sanitizer, fixed in it.
4. The full Varka run with the flag on takes less than 1.3 times the run with it off, measured as
   row 246 measures its before and after.
5. Each of four seeded violations fails by name: a mapping one byte past a buffer, a mapping at an
   unregistered address, an overwritten canary, a leak at close.
6. After step 1 no `.reinterpret(` is left outside the two functions.

## 4. Verification

*Written as the work happened, 7 October 2026.*

**Step 1, routing.** `VarkaEmittedBytesSuite` and the suites of the touched classes
(`IntRangeOpsSuite`, `TruncLevelLeafSuite`, `WeekdayLeafSuite`, `VarkaSelectionVectorOpsSuite`,
`VarkaKernelWarmupSuite`) passed: 28 succeeded, 1 canceled, none failed, and `emitted_bytes.json`
did not move. The pre-commit rule rejects a raw `.reinterpret(` in a probe file and passes the
three files that may have one.

**Steps 2 to 4, the sanitizer.** `VarkaMemorySanitizerSuite` (catalyst, ten tests) drives the
window directly: a mapping inside a buffer from its first to its last byte, one byte past, before,
between and spanning two buffers, the null address, a negative size, a wrap-around, an empty
window, a canary intact and overwritten. `VarkaMemorySanitizerEndToEndSuite` (core, seven tests)
runs only with `-Dvarka.sanitizeMemory=true`: a real projection's kernel increases the count of
mappings checked, so the sanitizer is on and reached it; a mapping one byte past a registered
buffer fails through the engine's `ofAddress` and through catalyst's `VarkaSegments`, naming the
buffer; an overwritten canary fails the check made when the kernel returns; a nested suite that
leaks a buffer is aborted by name and bytes, and one that closes it passes; and a violation is not
a catchable kernel failure. `ofAddress` is 22 bytecode bytes (`javap`), under HotSpot's 35.

**Five broad runs of every Varka suite with the flag on** (`dev/varka_matrix.sh --defaults --split 3
-j 6 --jvm-arg -Dvarka.sanitizeMemory=true`, the gate's wide step without the two JIT-measuring
suites), each after one more step:

| run | after | ok | failed | canceled | violations |
|---|---|---|---|---|---|
| 1 | the engine's `ofAddress` and the registry | 954 | 6 | 27 | one pattern, 36 |
| 2 | `VarkaSegments.map` checked too | 955 | 6 | 27 | the same |
| 3 | canaries | 958 | 6 | 27 | the same, with exact sizes |
| 4 | the leak check as `beforeAll`/`afterAll` | - | - | - | did not compile: `TPCBase` widens those to public |
| 5 | the leak check wrapping `run` | 958 | 6 | 27 | the same; no suite aborted for a leak |

All six failures are one pattern: a mapping of 80000 bytes where the nearest registered buffer,
output data 0, holds 40000, and one of 40 bytes over 20, in `VarkaTimeArithmeticSuite` and
`VarkaCoverageDifferentialSuite` (`hour(t)`, `minute(t2)`, `second(t)`, the `emit.useAVX` switch,
the second-of-the-day sweep, an extract under another expression). The cause, read in
`VarkaBodyEmitter.emitSizes`: `dataBytes = length * lane.byteStride` is the size of every data
segment, and a `NarrowLane` root's output is an int32 stored at `i * 4`, so it is mapped at twice
its size. The answers are right. That is row 292.

**Run 6, the same suites and classpath without the flag** (later the same day): 960 ok, none failed,
33 canceled, in 325 seconds against run 5's 321. The six extra cancellations are the six
flag-dependent tests of `VarkaMemorySanitizerEndToEndSuite`, which cancel without the property, so
the 27 canceled of the flagged runs are the same 27 the suites cancel anyway; this corrects the
sentence above, written before this run, which said they had not been compared. The six TIME tests
that fail under the sanitizer pass without it.

**`-XX:+PrintInlining` on `VarkaWarmupEndToEndSuite`** (the suite that drives kernels to C2), flag
off: `VarkaVectorSupport::ofAddress (22 bytes)` is inlined at 61 call sites as "inline (hot)" and
at 10 more as "inline"; the one refusal is a cold site, "low call site frequency".

**Step 5, the sanitizer on (7 October 2026, after row 292).** `dev/varka_matrix.sh`, which CI, the
weekly option matrix and the gate all run their suites through, passes `-Dvarka.sanitizeMemory=true`
to every test JVM by default, and `--no-sanitizer` turns it off: the gate's quiet phase, the two
suites that measure the JIT, uses it, since the sanitizer's branches change what C2 compiles; no
benchmark runs through the runner. The runner also proves the flag arrived: when it is on and
`VarkaMemorySanitizerEndToEndSuite` ran, the run fails if that suite cancelled a test, because
every test in it that needs the flag cancels without it (six of them did, in the run without it).
The catalyst fuzzers, which run kernels through `VarkaKernelCheck` and have no evaluator to open a
window, now run them inside one over the buffers the harness allocated; the harness allocated each
validity bitmap at the nominal `(length + 7) / 8` bytes, which is not what Arrow gives a kernel (whole
64-bit words), so it allocates the words now. A test runs a small kernel through that harness and
asserts the count of mappings checked rises. The gate's wide step with the sanitizer on, every suite
of both modules: 966 ok, none failed, none aborted, 27 canceled (the suites' own), in 321 seconds,
no violation.

## 5. Outcome

*Done, 7 October 2026: steps 1 to 4 in #658, row 292 in #660, step 5 in the pull request after.*

1. *The bytes oracle is unchanged by every step.* Held for step 1, the only step that touches main
   code the emitter's classes share; steps 2 to 4 changed nothing the emitter reads. To be run once
   more at the end.
2. *`ofAddress` stays small and inlined with the flag off.* Held: 22 bytes against 35, and inlined
   hot into the kernels of the warm-up suite, as above.
3. *No out-of-range access on master.* Failed, in the way the task exists to fail: one finding,
   an over-wide mapping of a narrowed output, row 292. No canary was overwritten and no mapping
   left an input, a derived input, the scratch or the selection bitmaps. No false positive was
   met: the all-null column's address 0 is never mapped with a size in these suites.
4. *Under 1.3 times the time with the flag on.* Held: the whole Varka run over three JVMs a module
   took 321 seconds with the flag on and 325 without (0.99 times). That is a wall-clock of a suite
   run, not a kernel benchmark; the benchmarks run with the flag off by design.
5. *Each seeded violation fails by name.* Held, in both suites, for a byte past a buffer, an
   unregistered mapping, an overwritten canary and a leak.
6. *No raw `.reinterpret(` outside the two functions.* Held, and kept so by the pre-commit rule.

*The done-when, "the suites and fuzzers run with it on".* Held: every Varka suite of both modules
runs with it, in CI and in the gate, and the runner fails a run that says so and does not. The
limits, which are the design's and not oversights: a kernel run that has no window is not checked
(the warm-up's thread, a unit test that calls a kernel directly); the canaries are past Arrow
buffers Varka allocates, not past the `Arena` segments the catalyst harness allocates, which are
registered for the mapping check and have no tail; and the warm-up's own `Arena` has no accounting
for "every byte back". The sanitizer found one thing in the code, row 292, and one in the
harness, the validity bitmaps; nothing else.

## 6. Explicitly out of this task

* A sanitizer in production, or a mode that continues after a violation: it is a test tool, and
  a violation fails the test.
* A native sanitizer (AddressSanitizer or valgrind) under the JVM, which row 272's platform arms
  and the milestone's later nightly could take up.
* Memory that is not Arrow's: the warm-up's `Arena` has no allocation accounting to compare, so
  "every byte back" covers the Arrow root allocator, which holds every buffer an evaluator hands a
  kernel, and the warm-up's arena is closed by its try-with-resources and not measured.
* Anything the sanitizer finds beyond the sanitizer itself becomes its own row, as the plan asks
  of rows 262, 263 and 269.
