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

*Filled in as the work happens.*

## 5. Outcome

*Filled in as the work happens.*

## 6. Explicitly out of this task

* A sanitizer in production, or a mode that continues after a violation: it is a test tool, and
  a violation fails the test.
* A native sanitizer (AddressSanitizer or valgrind) under the JVM, which row 272's platform arms
  and the milestone's later nightly could take up.
* Anything the sanitizer finds beyond the sanitizer itself becomes its own row, as the plan asks
  of rows 262, 263 and 269.
