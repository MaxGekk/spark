# The emitter and its kernels

## Asking the emitter

For an emitter question, `dev/varka_emit.sh "year(d)" "month(d)"` prints what
a projection compiles to, its shape hash, and per emitted method the bytecode
size and the `IntVector`/`VectorMask` invocation counts on the scale the
emitter suite's op-count tests use - so a plan's registered op counts come
from the tool, not from reading the emitter. `--asm` adds C2's assembly for the
dense loop and its mnemonic frequencies; `--options k=v` selects any
`VarkaEmitOptions` variant by name. `--table --variant k=v` prints the
op-count table a plan registers instead - one row per expression, one column
per variant, deltas against the defaults - ready to paste.

`-XX:+PrintAssembly`, and so `--asm` and the assembly suite, need the hsdis
disassembler plugin, which no JDK ships. `dev/varka_hsdis_build.sh` builds it
in seconds from the JDK's one source file and the distribution's capstone
library, without a JDK build, and checks HotSpot loads it.

## Rules

* Every hot loop method stays small by construction (`GROUP_BUDGET`): C2's compile
  time grows steeply with vector-op count, and a wide method is slower to compile
  *and* slower to run. Sibling methods, not longer methods. Two recorded exceptions:
  the budget partitions *between* outputs and never splits inside one, so a single
  output wider than the budget still forms one method - the capped `IN` chain
  (VARKA-20, up to 33 ops in the benchmarked shape) is the known case, its ~30 ms
  one-time C2 compile per fresh shape accepted because the task-18 class cache
  amortizes it and the measured win at the cap is 4.0x; and calendar outputs over
  one date share a method up to `FUSED_CEILING` (400 ops), because the
  shared civil-from-days prefix makes the wider method less work rather than more
  (VARKA-32, 2.15x on four fields). Beyond that, the emit cost model's prediction bounds a group
  in `GROUP_BUDGET`'s place where it can (VARKA-254: with the plan's margins, a call-site budget
  and a group it prices), and the budget binds everywhere else; a group the prediction widens is
  checked on call sites at every later join and split after the build however narrow. Whether two
  plain chains over a shared subchain should share a method under the budget's rule is VARKA-43's
  open question (VARKA-17 measured the merge as a loss; the parity file has read it as a win since
  VARKA-46 moved the validity OR ahead of the vector work).
* **A new bounded lowering comes with its proof file** (VARKA-242). A lowering exact only over a
  bound - a magic multiply over a range, a fold, a perfect hash, a form that rounds - ships with a
  file under `sql/varka/proofs/` that proves it over that bound and finds where it first fails,
  rendered from the code's own constants by `VarkaProofFiles`, as `sql/varka/proofs/README.md`
  describes; a calendar division joins `ChronoDivide`'s table, whose rows are proven as they are
  added. A sweep of the whole decomposition is not the same check: it cannot see a constant whose
  site has stopped being exact while a later step still absorbs the error
  (`sql/varka/plans/m7/VARKA-242.md` 9).
* **The suites run with the memory sanitizer on** (VARKA-263): `dev/varka_matrix.sh` passes
  `-Dvarka.sanitizeMemory=true`, so every mapping a kernel makes is checked against the buffers the
  evaluator handed it, and a suite that leaves Arrow memory allocated is aborted. A buffer given to
  a kernel is registered where it is obtained (`VarkaMemorySanitizer.register` or `guard`, which
  also puts a canary past a buffer Varka allocates), an address becomes a segment only through
  `VarkaSegments.map` or the engine's `VarkaVectorSupport.ofAddress` (the pre-commit hook rejects
  a raw `.reinterpret(`), and a buffer Varka allocates under the flag leaves room for the canary.
  Benchmarks and the JIT-measuring suites run with it off (`--no-sanitizer`), since its branches
  change what C2 compiles. The kernel warm-up (`VarkaKernelWarmup`) runs each job in a window of
  its own over its own arena, registered at the rows a call can reach and not at the allocation's
  padding, so the code C2 compiles from it keeps the branch a batch in a window takes
  (`VarkaSanitizedWarmupSuite`). A violation is not a kernel failure and is never a fallback: in a
  warm-up it ends the job and is handed to the next `VarkaKernelWarmup.awaitIdle`, which throws it.
* The ghost fallback is a correctness contract: a Varka failure degrades to the
  row engine and never fails a query. Anything that can return a *wrong* answer
  rather than fail - a cache key, for one - gets its own differential coverage,
  because the fallback cannot catch it.
