# VARKA-373: The template warm-up feeds only the preferred width, so 4-lane kernels are not helped

## 1. Where this came from

`VARKA-295.md` 9.8 (15 October 2026): the warm-up of the shared Vector API templates (#827) removes
the deadline releases at the preferred species and at 16 lanes (0 releases in 3 runs against 19
to 59 without it), but at `lanesOverride=4` it makes no difference: 183, 141 and 144 releases with
it, 136, 139 and 148 without, 25 to 32 minutes a run in the 400-composition arm. A kernel at a
narrower species reaches the same shared templates, whose profiles the warm-up fed only with
preferred-width receivers. 9.8 lists what is not known: why 4 lanes is not fixed (no
deoptimization log was taken for a 4-lane run), whether feeding the narrower species would fix
it, and whether production ever emits at 4 lanes.

## 2. The admission check, done

To be done before the change, in the order of `VARKA-295.md` 9.5 and 9.6: (1) the 4-lane warm-up
fuzz arm run with the JIT's own log, every release classified (deoptimization cycle, C2 bailout,
policy, or a compiled kernel still allocating), to learn whether the cause is the trap and
decompile counts of 9.6 or something else; (2) whether production can emit at 4 lanes at all: a
host whose `SPECIES_PREFERRED` is the 128-bit one, or `-XX:MaxVectorSize=16`, which the test gate
already uses. If no production host emits at 4 lanes, the task is a test-configuration question,
not a runtime one, and says so. It would reject a fix that warms every species when only one is
reachable.

## 3. The design

### 3.1 Warm the species a kernel will use

If step 1 shows the same class-check sums, `VarkaVectorTemplateWarmup` runs its rounds at each
species a host can bake (the preferred one and the narrower ones the emitter's `lanesOverride`
and the host's maximum vector size allow), on the same `varka-kernel-warmup` thread, once per
JVM, behind the existing property. Otherwise the design follows from the cause found.

### 3.2 What is deliberately unchanged

The preferred-species warm-up, its 300 rounds and its disable property; the query-path vector
code (`IntRangeOps`, `SelectionVectorOps`), which `VARKA-295.md` 9.7 leaves open.

### 3.3 Registered op counts

None; no emitter change.

## 4. Files

| file | what |
|---|---|
| `sql/catalyst/src/main/java/org/apache/spark/sql/catalyst/expressions/codegen/VarkaVectorTemplateWarmup.java` | the species the rounds run at |
| `sql/varka/plans/m7/VARKA-295.md` | 9.9, the result |

## 5. Tests, and what each is for

* The warm-up unit test at each species: the shared templates compile with no class-check trap
  recorded (the compile log), which a preferred-only warm-up would fail at the narrow species.
* The 400-composition warm arm at `lanesOverride=4`, 3 runs: deadline releases.
* The same arm at the preferred species and at 16 lanes: still 0, so the new rounds do not undo
  the old fix.

## 6. The measurement

Deadline releases and COMPILED counts in the 400-composition warm arm at `lanesOverride` 4, 16 and
the preferred species, warm-up on, three runs each, arms alternating over core sets as in 9.8;
the steady-state throughput driver of 9.8 at the preferred species, to show the extra rounds cost
nothing there.

### 6.1 Predictions, registered before the run

1. At 4 lanes the releases fall from 136 to 183 to under 10 per run, if step 1 names the trap sums;
   if it names something else, the prediction is void and the cause is the finding.
2. The extra warm-up costs under 300 ms more on the worker thread, off the query's path.
3. The preferred-species and 16-lane results do not change.

## 7. Risks

1. The cause at 4 lanes is not the trap sums, and warming more species does nothing; section 2's
   log shows it before any code is written.
2. Warming species the host cannot run throws; the existing `Throwable` guard in the warm-up
   catches it, and a test checks that an unsupported species is skipped.

## 8. Sequencing

The JIT-log classification first (a record, no code); the species loop behind the property; the
measurement; the default and the plan's 9.9 last.

## 9. Outcome

Not started.
