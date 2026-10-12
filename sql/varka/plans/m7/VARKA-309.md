# VARKA-309: The Spark differential's planted-rollback test depends on the run's seed

## 1. Where this came from

Row 309 of `m7/PLAN.md`, logged from the night of 11 October 2026. `VarkaSparkFuzzSuite`'s test
"a forced output finds a rollback that truncates one input too far" turns on a planted bug in the
compiler's rollback (`misdescribeRollback` 3), searches the first 300 compositions of the run's
seed, and requires the first one whose answer changes to be a mismatch under a forced decline. A
planted bug fails only a composition whose shape reaches the code it breaks, so the run's seed
decides whether the first 300 hold one: at seed 1 none did, and the night's long run had to leave
the test out. `dev/varka_nightly.sh` runs the whole suite at each day's date as the seed, so the
test would fail on some nights with nothing wrong. VARKA-302 fixed the same problem in the IR
fuzzer's planted-failure test.

## 2. The change

The test draws from a seed of its own, `-Dvarka.sparkfuzz.plantedSeed`, the suite's default
seed unless set, which the run's `-Dvarka.sparkfuzz.seed` no longer moves. A check that a planted
bug is caught needs one composition that reaches the bug, not the run's variety. The test also
reports at which composition the planted bug first changes an answer, so a survey can read it.

## 9. Outcome

Done on 12 October 2026, in one commit with this plan.

**The survey.** The planted test at 32 seeds, through `-Dvarka.sparkfuzz.plantedSeed`: the
suite's default, seed 1, the twenty seeds of the night of 11 October 2026 (2026101251 to
2026101270) and the dates 20261001 to 20261012, the seeds the nightly uses. **At 11 of the 32 no
composition of the first 300 reaches the planted bug** - 1, 20261001, 20261004, 20261007,
20261012, 2026101251, 2026101254, 2026101256, 2026101258, 2026101264 and 2026101270 - so the test
as it was would have failed at a third of the seeds, four of the twelve dates among them, today's
included. Where one does, the first is at composition 0 to 229. The default seed, 20261008, which
the test now keeps, first changes an answer at composition 82, and the change is the
forced-decline mismatch the test requires.

**Why not search further instead.** Searching more compositions at the run's seed would make a
failure rarer, not impossible, and costs up to eight minutes a seed when nothing is found; a seed
of its own costs one search to composition 82, and the run's seed still varies every other test
of the suite.
