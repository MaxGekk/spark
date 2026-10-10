# VARKA-302: The fuzzer's planted-failure test finds its failing draw under every seed

## 1. Where this came from

The night of 10 October 2026 ran `VarkaIrFuzzSuite` at fourteen seeds, 1010001 to 1010014, 10,000
iterations a lane each. Every random tree matched the reference evaluator, on both lanes, at every
seed. But eight of the fourteen runs (1010001, 1010002, 1010006, 1010007, 1010008, 1010011,
1010012 and 1010013) failed one test, "a failure of the generated class fails its test with the
smaller case in the message": `Expected exception TestFailedException to be thrown, but no
exception was thrown`. The test came with VARKA-277. It passes at the suite's default seed, which
CI uses, and passed at the nightly's seed that day (20261010). Across the fourteen it fails more
often than it passes, so a nightly that varies the seed fails it on most days.

The cause is in the test. It turns on the planted bug `misdescribeWordLiveness` for draw 0 and
expects `runChecked` to fail. A planted bug fails only a case whose shape reaches the code it
breaks, and what draw 0 is depends on the seed (`-Dvarka.fuzz.seed`, which the nightly and a
multi-seed night vary). The two shrinking tests beside it search the first 200 draws for one that
fails, and they passed at all fourteen seeds.

The owner's decision on the morning of 10 October: log it as a row and fix it, together with the
first prediction of VARKA-240, which was to be scored from that pull request's CI.

## 2. The admission check, done

The first draw that fails with `misdescribeWordLiveness` on, at each seed, found by the fixed
search (a temporary print, removed before the commit), with `misdescribeAdd`'s for the other
shrinking test:

| seed | `misdescribeWordLiveness` | `misdescribeAdd` |
| ---: | ---: | ---: |
| default | 0 | 0 |
| 1010001 | 2 | 1 |
| 1010002 | 2 | 8 |
| 1010003 | 0 | 11 |
| 1010004 | 0 | 6 |
| 1010005 | 0 | 9 |
| 1010006 | 6 | 0 |
| 1010007 | 1 | 0 |
| 1010008 | 4 | 8 |
| 1010009 | 0 | 0 |
| 1010010 | 0 | 1 |
| 1010011 | 1 | 0 |
| 1010012 | 3 | 2 |
| 1010013 | 1 | 8 |
| 1010014 | 0 | 10 |

The seeds whose first failing draw is not 0 are exactly the eight that failed in the night, so draw
0 is the whole cause. The furthest is draw 11 of the 200 searched, so the search has room.

## 3. The design

### 3.1 The first failing draw

`shrunkPlanted`'s search for the first failing draw becomes its own helper, `firstPlantedFailure`,
which also names the seed when no draw in 200 fails. The test calls it instead of
`planted(drawInt(0), ...)`. The search runs `outcomeOf`, which catches the failure, and the test
then runs the case it found through `runChecked`, which is what it checks, so the case fails twice:
once in the search and once under test. A case's run is milliseconds.

### 3.2 What is deliberately unchanged

The test's two assertions: the message names the shrunk case and the signature `emitter rejection`.
The first shrinking test already asserts that the first failing draw under this option is an
emitter rejection, at every seed. The fuzzer, the shrinker and the planted options. The
composition fuzzer's planted tests (`VarkaCoverageCompositionFuzzSuite`), which passed in every
nightly and multi-seed run.

### 3.3 Registered op counts

None; no emitted code changes.

## 4. Files

| file | what |
|---|---|
| `VarkaIrFuzzSuite.scala` | `firstPlantedFailure`; the test uses it |
| `m7/VARKA-240.md` | 9.4: prediction 1 scored from #696's CI |
| `m7/PLAN.md` | row 302 |

## 5. Tests, and what each is for

`VarkaIrFuzzSuite` at the default seed and at each of the fourteen seeds of the night, with 10
iterations a lane (the planted tests do not depend on the iteration count): the test passes at
every seed, and the first failing draw at each is recorded in section 2.

## 6. The measurement

None. The proof is the fifteen seeds.

### 6.1 Predictions, registered before the run

1. **All fifteen seeds pass the test**, each with a failing draw well inside the 200 searched: the
   shrinking tests found one at every seed of the night.
2. **The default seed's first failing draw is 0**, which is why the test was written to name it.

## 7. Risks

1. A seed with no failing draw in 200 would fail the test with a message naming the seed, instead
   of a confusing "no exception was thrown". Section 5's run shows how far from that the fifteen
   seeds are.

## 8. Sequencing

One commit: the plan, the test fix, VARKA-240's prediction scored and row 302. The fix is two lines
of test code, so the plan does not get a commit of its own.

## 9. Outcome, 10 October 2026

The test takes the first failing draw. `VarkaIrFuzzSuite` passes, all nine tests, at the default
seed and at each of the night's fourteen, with 10 iterations a lane and `-Dvarka.ownJvm=true`.
Section 2 has each seed's first failing draw.

**Predictions.** 1 holds: all fifteen pass, the furthest failing draw at 11 of 200. 2 holds: the
default seed's first failing draw is 0 under both planted options.

**VARKA-240's prediction 1** is scored in its 9.4: the "Varka proofs" step took 7 seconds on the
runner, 0.8 of them solving.
