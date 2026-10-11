# VARKA-307: The bytes suite in parallel

## 1. Where this came from

Row 307 of `m7/PLAN.md`, filed by VARKA-306 (`VARKA-306.md` 9.1) when it added two pinned arms to
`VarkaEmittedBytesSuite`. The suite emits every shape it holds once per pinned arm and width, one
pass after another, and draws the twenty thousand fuzz shapes again for every pass. The row asks
for the shapes drawn once and the passes run on the laptop's cores, once the emitter is shown safe
to call from several threads.

## 2. The admission check, done

Measured on 11 October 2026 at `fc056778159`.

**What the suite costs.** Alone on the idle laptop the pinning test takes 2 minutes, the suite 2
minutes 12 seconds. In the gate it took 325 seconds at the wide width and 316 at the narrow (row
308's gate, its JUnit times), in the gate's catalyst-1 JVM, whose suites total 791 seconds against
254 and 170 for the other two: that JVM is the wide step's critical path (795 seconds), so a second
off the suite there is a second off the gate.

**What it does.** `render()` emits, at each of the two widths, the coverage rows, the 10,000 int
fuzz shapes and the 10,000 long ones under the defaults, then, for each of the nine pinned arms
(five `useAVX` levels, the two `validityOrFirst` subject arms, the two reference forms), every one
of those shapes again: twenty passes over about twenty thousand shapes, each a fresh draw from the
grammar. Every shape is independent: its hashes are a function of the shape and the options.

**Whether the emitter is safe to call from several threads.** The emit path
(`VarkaLoopEmitter.emit`, `Analysis`, `Slots`, the body emitters, the lowerings) holds no mutable
static state: no static field that is not final, and no static collection filled after class
initialization, in the package. The mutable statics there belong to the runtime, not the emit
path: the shape cache, the compile directive and the warmup worker. The options record is
immutable; the price table is a `Map.of` read only; the Class-File API's `ClassFile` is an
immutable value and each emission builds its own. That is a reading, not a proof, and it matters
beyond this suite: the compiler emits at planning on the Spark driver, where concurrent queries
plan on several threads. So the design adds a test that would see a race.

What the check would have rejected: running the passes in parallel without the test.

## 3. The design

### 3.1 Drawn once

The two fuzz sequences become held `IndexedSeq`s, drawn once per JVM; `fuzzShape(k)` and
`longFuzzShape(k)` read them. The coverage rows already are (a `lazy val`).

### 3.2 Hashed in parallel, joined in order

One helper maps a function over indices on a fixed pool of threads and returns the results in
index order, so every digest is built from the same text in the same order as before. The pinning
test's passes hash their shapes through it: the per-width coverage hashes, each fuzz block, and
each arm's digest, whose per-shape lines are computed in parallel and concatenated in order. The
pool has one thread per available processor, or `-Dvarka.bytes.threads`; at 1 it is the serial
suite.

### 3.3 The test that would see a race

The first two thousand int fuzz shapes and the coverage rows, emitted on every thread of the pool
at once, each emission's bytes compared with the same shape emitted alone on the test's thread:
byte for byte, at both widths. An emitter that shared a mutable structure between emissions would
fail it intermittently and then reliably at a large enough corpus.

### 3.4 What is deliberately unchanged

What is hashed and how: `emitted_bytes.json` does not move. The option audit (opt-in) and the
`validityOrFirst` test stay serial; they are seconds.

### 3.5 Registered op counts

None: no emitter change.

## 4. Files

| file | what |
|---|---|
| `VarkaEmittedBytesSuite.scala` | the held shapes, the ordered parallel map, the race test |
| `sql/varka/skills/testing-and-debugging.md` | the emitter's thread safety, and how it is checked |

## 5. Tests, and what each is for

* **The pinning test**, unchanged in what it asserts: `emitted_bytes.json` byte-identical, and
  again with `-Dvarka.bytes.threads=1`, so the parallel and serial suites agree.
* **The race test** of 3.3.
* **The gate**, both widths, with the suite's JUnit time read from it.

## 6. The measurement

The suite's time alone on the idle laptop and in the gate, before (2) and after.

### 6.1 Predictions, registered before the run

1. `emitted_bytes.json` is unchanged, at any thread count.
2. Alone, the pinning test takes under 40 seconds (from 2 minutes): twenty independent passes on 24
   hardware threads, less the serial coverage parse and the JSON rendering.
3. In the gate the suite takes under 120 seconds (from 325), and the wide step falls by at least
   two minutes unless the gate's split rebalances its JVMs.

## 7. Risks

1. **A race the test does not reach.** The race test covers the shapes the oracle holds; a shared
   structure touched only by a rare shape could escape it. The static reading of 2 is the other
   half of the evidence, and the oracle itself, regenerated in parallel, is a third.
2. **Oversubscription in the gate.** Six test JVMs at a time on 24 hardware threads, one of them
   now using all of them: the suite's gain could come partly out of its neighbours. The gate's
   step times, not only the suite's, are what 6.1's third prediction is scored on.

## 8. Sequencing

1. This plan; row 307 marked Planned.
2. The held shapes, the parallel map, the race test and the skills note.
3. The outcome.
