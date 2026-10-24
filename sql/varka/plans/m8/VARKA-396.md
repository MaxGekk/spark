# VARKA-396: wrong answers from C2-compiled kernels on AVX2

## 0. Why this row exists

`m8/PLAN.md` row 396: `VarkaIrFuzzSuite`'s compiled-history test (row 274a) gives wrong answers on
GitHub-hosted runners - three of four JVMs on 19 October 2026, and every night from 20 to 24
October, one to three JVMs a night. Every failure is in a C2-compiled history (`released`,
`warm-same`, `recompiled`, `quiet-guard`, `quiet-remainder`); none in the interpreter. The row
asks for the cause, a fix or a guard, and a regression test per case.

## 1. Findings (24 October 2026)

**The runner is AVX2-only.** The fuzz workflow now logs the CPU: AMD EPYC 7763 (Zen 3), `avx2`,
no AVX-512, `UseAVX=2`, `MaxVectorSize=32`, Zulu 25.0.4+7 and 25.0.5+8. The laptop (Zen 5,
AVX-512) had replayed every case without failing: its JVM always runs `UseAVX=3`.

**Reproduced on the laptop by the instruction set.** Replaying the 24 October long-lane case
(seed 202610241, iteration 10, `warm-same`) under OpenJDK 25.0.4.1:

| JVM flags | Result |
| :--- | :--- |
| none (AVX-512 instructions, 512-bit vectors) | passes |
| `-XX:MaxVectorSize=32` (AVX-512 instructions, 256-bit vectors) | passes |
| `-XX:UseAVX=2` | fails |
| `-XX:UseAVX=2 -XX:MaxVectorSize=32` (the runner's) | fails |

So the defect follows the AVX2 instruction set, not the vector width. Under the runner's flags
five of six recent failures reproduce: 202610240/7 (int lane, `released`), 202610241/10 (long,
`warm-same`), 202610243/4 (int, `warm-same`, a selection mismatch), 202610231/1 (long,
`recompiled`), 202610221/8 (long, `quiet-guard`), 202610222/1 (long, `warm-same`); 202610190/17
(19 October, `quiet-remainder`) does not, on today's master. A fastdebug build of JDK 25 GA
(`jdk-25-ga`) fails the same case with its warm-up given two minutes to settle, and no HotSpot
assertion fires: the defect is in JDK 25 since GA, not a regression of an update.

**The shrunk case.** `if(c0 > c1, c0, try_add(c1, c2))` over longs, the masked body, seven rows
(one lane group of four and a three-row masked epilogue), with eleven options off; unstable (it
does not fail the same way three times). Its masked loop, read with `javap`: three loads, `compare
(GT)` to `toLong`, the known-true word `m & v0 & v1`, the add and its overflow mask
`((s ^ c1) & (s ^ c2)) < 0` to `toLong`, `VectorMask.fromLong(kT)` and `blend`, the store and the
validity word.

**What it depends on.** Each switch below, alone, makes the replay pass (one run each):

| Switch | Replay |
| :--- | :--- |
| `-XX:DisableIntrinsic=_VectorCompare`, `_VectorMaskOp` (`toLong`), `_VectorFromBitsCoerced` (`fromLong`), `_VectorBlend`, `_VectorLoadOp` | passes |
| `_VectorLoadMaskedOp`, `_VectorStoreMaskedOp`, `_VectorBinaryOp` | fails |
| `-XX:-DoEscapeAnalysis`, `-XX:-EliminateAllocations`, `-XX:-IncrementalInline` | passes |
| `-XX:-UseLoopPredicate`, `-XX:-UseProfiledLoopPredicate`, `-XX:LoopUnrollLimit=0`, `-XX:-UseSuperWord`, `-XX:-PartialPeelLoop`, `-XX:-SplitIfBlocks` | fails |
| `-XX:PerMethodTrapLimit=0 -XX:PerBytecodeTrapLimit=0` | passes |

The intrinsics are the chain of every predicated select Varka emits: compare, mask to long, long
to mask, blend. The replay deoptimizes kernels' `loopMasked0` hundreds of times, mostly at
`profile_predicate` traps. Each passing switch changes the graph C2 builds, so the table does not
yet say whether a runtime deoptimization produces the wrong value or the compiled code is wrong
without one; the harness's per-case trap verdicts decide that (2.1).

**Not reproduced outside Varka yet.** Three standalone programs (`target/repro/standalone/` on the
investigation branch) do not fail under AVX2: the masked loop transcribed over native memory with
its epilogue; the same after the chain run over other species and element types; 48 generated
copies of the body, each deoptimized in its middle with vectors and masks live. A JBS search
(24 October) found no report that matches: JDK-8386154 is a wrong mask result on AVX-512 only.

## 2. Next

1. **Trap or compile.** The shrunk case alone through `VarkaCompiledCheck.run`, no earlier draws
   in the JVM, printing the raw failure and the history's verdicts: whether the kernel trapped in
   the compared call, and whether the case needs the earlier draws' profiles.
2. **The wrong value's shape.** The failing row's want and got against the other branch's value,
   a neighbour lane's, an earlier batch's: which names the link of the chain.
3. **The emitted class itself** in a bare warm-then-compare loop, then cut it down: segments made
   in the method (`VarkaVectorSupport.ofAddress`, which escape analysis eliminates), the validity
   helpers, until it is a JDK-only program.
4. **The cause, and a report to OpenJDK** with that program if it is C2's.

**Mitigation is the owner's decision.** The shape is every predicated select, so the emitter
cannot avoid it. The options, once the cause is named: decline fusion on hosts without AVX-512
(the row engine answers; no speedup there); keep fusing and document a JVM flag that fixes every
case, after measuring its cost (each candidate above is JVM-wide, and escape analysis off undoes
the Vector API's box elimination); or wait for a JDK fix with one of the two in place.

## 3. Done when

As row 396 says: the cases reproduced and their cause named; each fixed, or kept from the
emitter with the JDK bug recorded; each shrunk case a regression test under the history that
failed; a runner fuzz night passing the compiled test.
