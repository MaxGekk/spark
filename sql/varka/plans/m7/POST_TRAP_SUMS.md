# Why a warmed Vector API kernel can still allocate

A Java Vector API kernel, compiled by C2 at the top tier and in use, allocated one `int[16]`, 80
bytes, on every loop iteration. The next run compiled the same code clean. This happened in a few
warm-ups in every hundred in Varka, which compiles Spark SQL expressions into such kernels and
hands one real batches only once C2 has compiled it and it runs without allocating. A warm-up
that misses its deadline is released, and its batches go back to Spark's row engine.

> **In short**
>
> 1. C2 decides whether a class-check miss is a cheap trap or a real call from trap counts summed
>    over the whole compile, where a shared method counts once per inlined copy. One method
>    inlined 170 times pushed the sum past its limit, and the real calls cost a vector array per
>    loop iteration.
> 2. The traps were left by earlier compiles of the Vector API's shared code that had seen one
>    operator class.
> 3. Two JVM options remove it. Running every operator through the vector code first avoids it;
>    Varka now does, and its releases went from about 440 in 2,000 kernels to none.

## Ruling things out

We ran the warm-up test with the JIT's own log on (`-XX:+LogCompilation`, deoptimization logging,
tiered events) and classified every release, 171 in six runs, from the record of its own kernel
class. Each usual suspect leaves a different trace:

* **A deoptimization cycle**: the compiled method is thrown away and recompiled again and again.
  The log shows repeated not-entrant marks. None of the 171 had one.
* **A C2 bailout**: the compile gives up, and the method runs in a lower tier. None had one.
* **A compile nobody asked for**: the tiered policy never queued the hot method. Every loop method
  was compiled at tier 4 and in use.

So the allocation was in C2's own output.

## The two sums

The compiled loop of a released kernel had real virtual calls on class-check miss paths, where a
clean compile of the same shape had uncommon traps. When an inlined Vector API call cannot prove its
receiver's class, C2 leaves either a trap on the unlikely path or a real call. A real call returns a
plain `IntVector` of unknown shape, the `binaryOp` intrinsic refuses it, and the box it lives in
becomes an array that escape analysis does not remove.

`doCall.cpp` makes that choice not per call site but from two sums over the whole compile:
class-check traps against `PerMethodTrapLimit`, 100 by default, and decompiles against
`PerMethodRecompilationCutoff` / 2 + 1, 201. Each shared method contributes its recorded count
**once per inlined copy**, and `VectorOperators.opKind`, inlined 170 times in our loop, held 4 or
5 traps.

![One sum for the whole compile](figures/svg/fig41-one-sum-for-the-whole-compile.svg)

*Figure 1. One kernel's loop. In a clean compile the sum stays at 4; in a released one it passes
100 at parse 427 of 3,309, and every later miss becomes a real call.*

The log shows it in one line:

```
<observe trap='class_check' count='0' mcount='0' ccount='254'/>
```

## Who leaves the traps

Not the kernel. The traps sit in the Vector API's shared templates (`Int512Vector::lanewise`,
`opKind`, `ImplCache::find`), left by earlier standalone C2 compiles of them. A template compiled
after seeing one operator class speculates on it; the first other class traps. One earlier kernel
is enough: `abs` over a year-month interval, run first, made the kernel release 5 times in 6, and
55 of 220 kernels did it alone.

![Who leaves the traps](figures/svg/fig42-who-leaves-the-traps.svg)

*Figure 2. A race between a template's compile and the first operator class it had not seen.*

## What removes it

Raise both limits: with `-XX:PerMethodTrapLimit=100000 -XX:PerMethodRecompilationCutoff=100000`,
1,000 warm-ups released none, against 10 to 39 in 500 without them. Which options matters: the
cutoff alone left 3 to 11, and raising the per-method and per-bytecode trap limits together caused
a deoptimization cycle instead.

That is an operator's choice: none of the three is a manageable flag, compiler directives cannot
set them, and WhiteBox needs startup options. A program can instead throw the counts away: on JDK
25 a JVM can load a Java agent into itself through its own `DiagnosticCommand` MBean, and
redefining the Vector API classes with their own bytes empties every method's profile. One such
reset a minute in took the releases from about 60 to nearly none. It is not a fix: it deoptimizes
every method that inlined a vector class, the counts return within half a minute, the JDK warns
that it will refuse such agents, and at 4 lanes it only halved the releases.

![What removes the releases](figures/svg/fig43-what-removes-the-releases.svg)

*Figure 3. The template warm-up off, with both limits raised, and as Varka ships; below,
separate runs with the limits raised through WhiteBox or the counts reset once at 60 s.*

## What a library can do

Make sure the templates are compiled before they can be compiled wrong. Varka now runs, once per
JVM before its first kernel, a short warm-up that calls every
Vector API method the kernels use with every operator class, interleaved, for 300 rounds. The
templates then reach C2 having seen all the classes and record no class-check trap. On the seed
that had shown about 440 releases in 2,000 kernels without it there were none, and none on five
more seeds, with no steady-state cost we could measure.

It is not the whole answer. At a 4-lane override the releases come from a per-bytecode trap at
one call site of `IntVector.add`, which warming the narrower shape did not clear and warming both
shapes made worse. On x86 capped at 128-bit vectors the warm-up does not help: C2 there has
no 2-lane mask, so a kernel over longs with a mask runs the Vector API's allocating Java fallback.

![Where the template warm-up helps](figures/svg/fig44-where-the-template-warm-up-helps.svg)

*Figure 4. The first 100 kernels of the warm-up test, template warm-up on and off.*

If you use the Vector API in a long-lived JVM: run your vector code once, with every operator
you will use, before the code you care about is compiled. If you can set JVM options, the two
limits above remove it outright.

## How these were measured

On JDK 25.0.4, an AMD Zen 5 laptop with 512-bit preferred vectors, in the warm-up arm of Varka's
composition fuzzer, which warms one kernel per random composition of expressions. The record is
`VARKA-295.md` 9.5 to 9.8 (Figures 1 and 2), `VARKA-370.md` 9 with its spike's `results/`
(Figure 3, lower block), `VARKA-373.md` 9 (Figure 4) and `VARKA-374.md`.

To reproduce on Linux: `sql/varka/demo/trap_sums.sh --runs 100` replays `abs` over a year-month
interval and then the allocating kernel in a fresh JVM, 100 times each with the template warm-up
off, with the two options, and as Varka ships. On our laptop, 2 hours 40 minutes on 3 cores, they
released in 77, 0 and 0 JVMs. `--compositions 400` runs one JVM over 400 compositions instead.
`--keep` replays until one releases and prints the `<observe ... ccount=.../>` line of each of the
kernel's compiles; a clean replay has none. Our run is
`sql/varka/demo/trap_sums-laptop-jdk25-output.txt`.