# Why a warmed Vector API kernel can still allocate

*Draft for row 371, 16 October 2026. Every number is from `VARKA-295.md` 9.5 to 9.8,
`VARKA-370.md` 9, `VARKA-373.md` and `sql/varka/demo/trap_sums-laptop-jdk25-output.txt`; measured
on JDK 25.0.4 on an AMD Zen 5 laptop at 512-bit vectors.*

Varka compiles Spark SQL expressions into Java Vector API kernels. A kernel is warmed before it
serves a query: compiled by C2, checked to run without allocating, and only then handed real
batches. A warm-up that does not reach that state within its deadline is released, and the
batches go back to Spark's row engine. In a few warm-ups out of every hundred, the kernel was
compiled at the top tier, in use, and still allocated: one `int[16]`, 80 bytes, on every loop
iteration. The same shape compiled clean on the next run.

This post is about why, and it is a story about counters that are not where you would look.

## Ruling things out

We ran the warm-up test seven times with the JIT's own record switched on
(`-XX:+LogCompilation`, deoptimization logging, tiered events) and classified every release, 171
of them, from the record of its own kernel class. There are three usual suspects, and each leaves
a different trace:

* **A deoptimization cycle**: the compiled method is thrown away and recompiled again and again.
  The log shows repeated not-entrant marks. None of the 171 had one.
* **A C2 bailout**: the compile gives up, and the method runs in a lower tier. None had one.
* **A compile nobody asked for**: the tiered policy never queued the hot method. Every loop method
  was compiled at tier 4 and in use.

So the allocation was in C2's own output.

## The two sums

The compiled loop of a released kernel had real virtual calls on class-check miss paths, where a
clean compile of the same shape had uncommon traps. When a call through an inlined Vector API
method cannot prove its receiver's class, C2 can either leave a trap on the unlikely path or
compile a real call there. A real call means the vector flowing out of it is a plain `IntVector`
of unknown shape, the `binaryOp` intrinsic refuses it, and the box it lives in becomes an array.
Escape analysis removes the box object but not that array.

Which of the two C2 chooses is decided in `doCall.cpp`, and it is not decided per call site. It
adds up counts over the whole compile:

* class-check traps against `PerMethodTrapLimit`, 100 by default, and
* decompiles against `PerMethodRecompilationCutoff` / 2 + 1, 201 by default,

where each shared method contributes its recorded count **once per inlined copy**. In our loop,
`VectorOperators.opKind` is inlined 170 times. It held 4 or 5 traps. In a clean compile the sum
stays at 4; in a released one it passes 100 at parse 427 of 3,309, and from there on every
class-check miss in the method is compiled as a real call.

The log says so in one line, if you know to look for it:

```
<observe trap='class_check' count='0' mcount='0' ccount='254'/>
```

## Who leaves the traps

Not the kernel. The traps sit in the Vector API's own shared templates (`Int512Vector::lanewise`,
`opKind`, `ImplCache::find`), left there by earlier standalone C2 compiles of those templates. A
template compiled while it had seen one operator class speculates on that class; the first time
another class arrives, it traps. One earlier composition is enough: `abs` over a year-month
interval, run before the kernel in question, released 5 times in 6, and 55 of 220 compositions did
it alone. It is a race between a template's compile and the first operator class it had not seen.

## What removes it

Raise both limits: with `-XX:PerMethodTrapLimit=100000 -XX:PerMethodRecompilationCutoff=100000`,
1,000 warm-ups released none, against 10 to 39 in 500 without them. Which options matters: the
cutoff alone left 3 to 11, and raising the per-method and per-bytecode trap limits together caused
a deoptimization cycle instead.

Raising the limits is an operator's choice: none of the three is a manageable flag, compiler
directives have no option for them, and WhiteBox needs two startup options and a jar. Throwing the
counts away is within a program's reach: on JDK 25 a JVM can load a Java agent into itself
through its own `DiagnosticCommand` MBean, and redefining the Vector API classes with their own
bytes gives every method an empty profile. One such reset a minute into the run took the releases
from 58 and 68 to 1 and 0. It is not a fix: it deoptimizes every method that inlined a vector
class, the counts are back within half a minute, it prints a warning that the JDK says will become
a refusal, and at 4 lanes it only halved the releases. Warming the shared templates first remains
the remedy.

## What a library can do

Make sure the templates are compiled before they can be compiled wrong. Varka now runs, once per
JVM and before its first kernel, a short warm-up on its background compile thread that calls every
Vector API method the kernels use with every operator class, interleaved, for 300 rounds. The
templates then reach C2 having seen all the classes and record no class-check trap. At volume, on
the seed that had shown 442 releases in 2,000 compositions without it, there were none, and none
on five more seeds; steady-state throughput showed no difference we could see.

It is not the whole answer. At a 4-lane override the releases come from a different counter, a
per-bytecode trap count at one call site of `IntVector.add`, which warming the narrower vector
shape did not clear, and warming both shapes made worse. And where the preferred vector is 128 bits
wide, as on most Arm cores, the
warm-up does not help at all; that case is still open.

If you use the Vector API in a long-lived JVM: run your vector code once, with every operator
you will use, before the code you care about is compiled. If you can set JVM options, the two
limits above remove the problem outright.

## Reproducing it

The replay is the warm-up arm of Varka's composition fuzzer over one prefix and one composition:
`abs` over a year-month interval, then the kernel above, each replay in a fresh JVM. From a Varka
checkout, `sql/varka/demo/trap_sums.sh --runs 100` runs it 100 times with Varka's template warm-up
off, 100 with the two options added, and 100 as Varka ships, and counts the releases. On our
laptop that took 2 hours 40 minutes on 3 cores and released 77, 0 and 0, every one the same
kernel; `--compositions 400` gave 29, 0 and 0 in the 400-composition arm the counts above come
from. `--keep` replays until one releases, keeps its `-XX:+LogCompilation` log, and prints the
`<observe ... ccount=.../>` line of each of the kernel's compiles; a clean replay's log has none.
The run is in `sql/varka/demo/trap_sums-laptop-jdk25-output.txt`.
