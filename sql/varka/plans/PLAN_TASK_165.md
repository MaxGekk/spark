# Task 165: a forced 128-bit species compiles scalar on the runners and packed on the laptop

*Scoped and planned 21 September 2026 (milestone 5 section 2.101, row 165), from
the first assembly gate run that named its machine (`PLAN_TASK_150.md` section
6). The first step is already in flight in the change that opened the row.*

## 1. The question

`VarkaAssemblySuite` forks a probe under `-XX:MaxVectorSize=16` and asserts
that C2 compiles the probe's own gather to a packed gather on an `xmm`
register. On this laptop it does. On the CI runners it does not, and the
failure is not noise: the body is a scalar loop of 248 instructions every time,
on the Intel Xeon runners in the week's earlier failures and, in the first run
that named its machine, on an AMD EPYC 9V45 with four processors - the same
Zen 5 family as this laptop, at the same `UseAVX=3` and `MaxVectorSize=64`.
The synchronous compile from task 150 was in place on that run, and the two
failures it was built for were gone; this one survived it. At the machine's own
width the same probe's gather is packed on the same VM, so the machine has the
instructions; something refuses them at the forced species.

What is known to differ between the two environments, in the order this plan
suspects them: the JDK build (Zulu 25 on the runners, Ubuntu's OpenJDK 25 here);
what the hypervisor exposes to a four-processor VM, which the JVM reads at
start-up into the feature set C2's matcher consults; and the processor count
itself, which sets JIT ergonomics. What is known not to differ: the CPU
family, the two flags, and the compile having finished.

The question matters beyond the gate. A JVM that refuses the Vector API at a
forced 128-bit species on the machines most readers have is a fact about where
Varka's 128-bit numbers can be reproduced, and the coverage table's 128-bit
column is this laptop's census at that species.

## 2. The change

1. **Let the JVM say why.** Done in #294: the probe that decides the 128-bit
   precondition runs under `-XX:+PrintIntrinsics`, and where its gather comes
   out scalar the cancel message quotes C2's own `**` refusal lines - the same
   lines the width census reads - with the host. The next gate run that lands
   on a refusing runner supplies the reason with no dispatch and no new job.
2. **Read it.** A `not supported` line names the operation, the lane count and
   the element type the matcher refused, which is a decision of the JVM's back
   end for that machine's feature set; a `missing constant` line is a
   compile-order effect and would point back at the probe; no line at all
   means the intrinsic was never attempted, which points at class loading or
   at the probe's own code path under that flag.
3. **Reproduce the condition on the laptop, one command each,** in the order
   of section 1: the probe under Zulu 25 downloaded beside the system JDK;
   the probe under `-XX:ActiveProcessorCount=4`; and, if the runner's refusal
   names a feature, the laptop's feature set against the VM's, read from
   `-XX:+PrintFlagsFinal` and the `CPU:` line the JVM logs at start-up, which
   the gate's machine report step already prints.
4. **Decide from what reproduces.** A JDK build difference is recorded and the
   gate pins the build it asserts on; a processor-count effect is a flag on the
   probe child, as `-Xbatch` was; a feature the VM does not expose is a machine
   property, in which case the cancel stays, narrowed to that reason, and
   `HARDWARE.md` says which machines it applies to. A Varka assumption, if the
   reason turns out to be one, is corrected in the suite.

## 3. Predictions, registered before the first refusing run under the new message

1. **The refusal is the matcher's.** The message quotes a `not supported` line
   for the gather at four int lanes, not a `missing constant` line and not
   nothing, since the body is the same 248 instructions on every refusing run
   and a compile-order effect would vary.
2. **The processor count is not it.** The laptop under
   `-XX:ActiveProcessorCount=4` still packs the gather at the forced species.
3. **The JDK build is not it either.** Zulu 25 on the laptop packs it too, so
   the difference is what the VM exposes: a feature the 128-bit encoding of
   the gather needs under `UseAVX=3` that the four-processor VMs of the pool
   do not report, and the JVM's `CPU:` line on the runner differs from the
   laptop's in that feature.
4. **The 512-bit path is unaffected on those machines**, as the same runs
   already show, so the outcome narrows the gate's cancel to the forced
   species and changes no default-width assertion.

If 3 fails and the laptop's Zulu 25 also refuses, the cause is the build and
the plan's decision is the pin; if 2 fails, it is ergonomics and the decision
is a flag.

## 4. Verification

The gate's own cancel message on the runner pool is the instrument, read from
the first run that cancels after #294 merges; a controlled run, if one is
needed, goes through the width-audit workflow's x86 job, which already runs a
census on the pool and can carry the probe. Each laptop experiment is recorded
here with its command line and the JVM's `CPU:` line. The assembly suite passes
unchanged on the laptop throughout; the suite changes only when the decision of
section 2 step 4 is taken.

## 5. Outcome

*Written from the first refusing run's message.*

## 6. Explicitly out of this task

The NEON question: the aarch64 runner refuses nothing at 128 bits
(`PLAN_TASK_153.md` section 6) and is not part of this. Any change to the
gate's default-width assertions. Making the 128-bit numbers reproducible on
the pool if the cause is a machine property, which is a documentation matter
for task 118's README and not a fix.
