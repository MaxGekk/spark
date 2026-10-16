# VARKA-374: At a 128-bit preferred species the warm-up does nothing, and 42 of 100 compositions release

## 1. Where this came from

`VARKA-373.md` 9 (16 October 2026). With `-XX:MaxVectorSize=16`, the gate's setting and the
preferred species of a 128-bit host such as Apple Silicon, the template warm-up of #827 feeds
exactly the species the kernels use and still does nothing: 43, 41, 41 and 43 of the first 100
compositions of the warm arm released at the deadline with it, 46 and 45 without it. At 512 bits
the same warm-up takes the rate to 0 of 100. The mechanism at 128 bits is not the one of
`VARKA-295.md` 9.6 or of 373's 4-lane case: the virtual call at `IntVector.add` is absent, and
the JFR allocation samples name `Long128Vector.maskFromArray`, `Long128Mask.prepare`,
`Int128Mask.bOp` and `LongVector.broadcastBits` inside kernel loops.

## 2. The admission check

To be done first, in the manner of `VARKA-373.md` 2: the 128-bit warm arm (400 compositions, the
JIT's own log, every release classified) with the mask sites named above, to learn whether the
cause is a compile-time trap count at a mask method, a boxing that escape analysis does not
remove in the mask path, or something the log has not shown. It would reject any fix that is
chosen before the sites are classified.

## 3. The design

Open until section 2 names the cause. The candidates, none chosen: warm the mask paths (the
warm-up's rounds use the arithmetic operators and not `maskFromArray` or `Mask.prepare`), avoid
the mask method in the emitted loop, or accept the rate on a 128-bit host and record it.

## 4. Files, tests, measurement, risks

Written when section 2 is done. The measurement is deadline releases and COMPILED counts of the
400-composition warm arm at `-XX:MaxVectorSize=16`, three runs with and without the change, and
the 512-bit arm unchanged at 0.

## 9. Outcome

Not started.
