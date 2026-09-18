# Task 124: the no-fallback proof, `PrintIntrinsics` in CI

*Written 18 September 2026. Section 2.59 of `PLAN_MILESTONE_5.md` opened this
task on 16 September 2026, from the September surveys (`SCOPE_MILESTONE_6.md`
items 16 to 29, #223).*

## 1. What this is for

A Vector API call ends in one of three places: one instruction, a C2 sequence,
or a silent Java fallback. Only the JVM says which. The datapath probe measures
ratios and infers; `-XX:+PrintIntrinsics` is the compiler's own statement, and it
is the only thing that surfaces a fallback on a new runner or a new lane type
*before* a benchmark does.

It matters more now than when it was scoped. The long lane arrived with task 29,
task 88 added double-lane conversions, task 28 will add int-to-long ones, and
task 121 is about to publish numbers under `-XX:UseAVX=2` - which is exactly the
configuration where `dev/varka_canary/L2DProbe.java` already records converts
failing to inline.

## 2. What the flag actually prints, measured before planning the check

Section 2.59's "How" was written from the shape such a check usually takes. Run
against this repository's own probes on 18 September 2026, three of its
assumptions are wrong, and two of them would produce a check that fails on
correct code. All four findings below come from `L2DProbe` and `MagicProbe`,
which are committed and which the reader can re-run.

### 2.1 There is no `** Rejected`, and there is a third category

2.59 says to grep for `** not supported` and `** Rejected`. The vocabulary this
JDK emits is `** not supported` and **`** missing constant`**; `** Rejected`
never appears.

### 2.2 Only one of the two categories discriminates

`L2DProbe`, the same probe at two AVX levels:

| | `** missing constant` | `** not supported` |
|---|---|---|
| default AVX (converts intrinsify) | 6 | **0** |
| `-XX:UseAVX=2` (converts do not) | 6 | **22** |

`** not supported` is a clean signal: nothing in the healthy run, twenty-two in
the unhealthy one, naming exactly the casts the probe's javadoc predicts would
fail.

`** missing constant` is **background**. It is identically present in both runs,
and its lines show why: `vclass_from=LoadP etype_from=Phi` is a species argument
that had not yet been constant-folded at that compilation attempt, against the
`vclass=ConP etype=ConP` form of a folded one. It reflects an early compilation,
not a final lowering, and a check that failed on it would fail on every healthy
run. **The check reads `** not supported` only**, and this section is why.

A caveat to record rather than discover later: a *persistent* missing constant
would be a real fallback, and counting attempts cannot tell a transient one from
a persistent one. Separating them needs the last compilation of a method rather
than every attempt, which is out of scope here and belongs with a task that has
a reason to want it.

### 2.3 The line carries no method, and the log is interleaved

2.59 says to fail on lines "whose method is under a Varka package". The line is:

    ** not supported: arity=1 op=cast#510/3 vlen2=4 etype2=double ismask=0

There is no method on it. The method is on a preceding line of the inlining
tree, and the log is written concurrently by the compiler threads, so lines
arrive spliced - one real example from these runs:

    @ 4   java.lang.invoke.LambdaForm$Holder::constant_L (8 bytes)   force inline
    by annotation  ** missing constant: opr=RShiftI vclass=ConP ...

Two consequences. **Attribution has to be process-level**: the check runs a
workload that is Varka's kernels and nothing else, and every `** not supported`
in that process is Varka's by construction. And **the matcher must search within
a line rather than anchor at its start**, or the spliced ones are missed.

### 2.4 "Any unsupported op fails" would fail the AVX2 reference kernel

`MagicProbe` is the kernel written *because* the converts do not work under
AVX2 - the magic-number form is the AVX2 answer. Under `-XX:UseAVX=2` it still
emits:

    ** not supported: arity=1 op=store vlen=4 etype=double atype=byte ismask=no
    ** not supported: arity=1 op=store vlen=4 etype=long  atype=byte ismask=no

These have not been attributed to Varka code, and at process level they cannot
be. Whatever their origin, a rule of "any such line fails" turns the AVX2 arm red
on its own reference kernel - and a CI check that fails on correct code is
switched off, which is a worse outcome than not having it.

So the check needs a **baseline per AVX level**, not a blanket rule: an
allowlist keyed on the op shape (`op=`, `etype=`, `atype=`), committed with the
reason each entry is on it, and a failure when anything outside it appears. Task
121 should also know these store lines exist before it quotes AVX2 numbers.

## 3. The design

**The workload.** A forked JVM running Varka kernels and nothing else, so that
process-level attribution is sound. `VarkaEmitDump --rounds N` already loads an
emitted class and runs it hot enough to reach C2 - it is what `dev/varka_emit.sh
--asm` uses to get a compilation printed - so the check reuses it over a fixed
list of expressions rather than inventing a second driver.

**The expressions.** One per lowering family the emitter has, chosen so that a
new family without a row is visible: the calendar prefix, a comparison, the
arithmetic and its overflow test, the validity helpers, and - the reason this
task is timely - the long lane and task 88's double-lane division.

**The verdict.** Fail when a `** not supported` line appears whose op shape is
not in the committed baseline for this AVX level. Print the offending lines and
the expression being run, because "a fallback happened somewhere" is not
actionable and the process gives us the expression for free.

**The second half of 2.59**, `dev/varka_datapath.sh` printing the
`EnableVectorSupport` line from `-Xlog:compilation` beside `MaxVectorSize`, is
independent and small, and lands with the same change.

## 4. Tests, and the positive control that already exists

The control 2.59 asks for - "a kernel given an operation the match rules refuse
fails it" - **needs no fixture**: `L2DProbe` under `-XX:UseAVX=2` is exactly
that, committed, and reads 0 lines clean against 22 refused. The check's own test
runs it both ways and asserts the verdict flips.

That matters beyond convenience. A grep-based check whose pattern goes stale
passes vacuously and looks identical to a check that found nothing - the same
failure mode task 125 had to handle in the surface driver, where a comparison
with nothing to compare had to say so out loud. Here the positive control is what
keeps the pattern honest, so it runs on every invocation rather than being a test
of the test.

## 5. What this does not do

* **Attribute a fallback to a source line.** Process-level is the scope; the
  expression under test is the resolution, and that is enough to act on.
* **Separate a transient missing constant from a persistent one**, 2.2.
* **Run under every AVX level in CI.** The baseline is per level, and the AVX2
  arm is task 121's; this lands the mechanism and the default-level baseline, and
  121 adds its own.

## 6. Sequencing

1. **This PR:** the plan, and 2.59 corrected - it names a message that does not
   exist, omits one that does, and proposes an attribution the log cannot
   support.
2. The runner and the baseline at the host's own AVX level, with `L2DProbe` as
   the positive control.
3. The `EnableVectorSupport` line in `dev/varka_datapath.sh`.
4. The CI job, once the check has run green locally at both AVX levels.

## 7. Outcome

*To be written when the work lands.*
