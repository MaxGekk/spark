# Task 134: the partitions ladder

*Milestone 5, section 2.69. Opened 16 September 2026 from the September surveys;
run 16 to 17 September 2026 in an idle machine window the owner offered.*

## 1. Where this came from

Every number this engine has published is one partition on one core. A Spark
executor runs one task per core, so the question the closing write-up cannot
dodge is whether a win measured alone survives the memory system being shared.
Two papers in the record say it may not: Schmidt (`SCOPE_MILESTONE_6.md` item
25) has a 48-core socket saturating DRAM at about twelve scalar threads, with
SIMD only lowering that count, and Kersten's branch-free selection loses a fifth
of its throughput at twenty threads from bandwidth alone.

## 2. The admission check, done

**The script could not do it.** `dev/varka_bench_surface.sh` hard-coded
`--master local[1]`, so `--partitions P` only ever made P tasks queue on one
core. The ladder needs a second knob, and this task adds it: `--cores C`, the
driver's `local[C]`, recorded in the provenance beside `partitions`. The two
stay separate because they are separate - P partitions on C cores is a queue C
deep - and the ladder sets them equal.

**Two things change meaning above one core**, and the plan says so rather than
letting a reader assume otherwise. The fixed-share rule, `(wall - executor) /
wall`, goes negative once executor time is a sum over parallel tasks, so it
stops being a guard; the comparable column becomes executor time, and the
wall-time ratio between the two arms at one rung stays meaningful because both
arms pay the same scheduling cost. And the cache build is parallel too, so a
rung's absolute numbers are comparable to another rung's only through the
arms' ratio.

**The machine.** `aqua`, AMD Ryzen AI 9 HX PRO 370: 12 physical cores, 24
hardware threads, the full AVX-512 flag set, and a datapath probe that reads
about 1.14 at 512 against 256 bits - so this is a scaling measurement, not a
width one. It is both machines section 2.69 asks for at once: a Zen 5, and an
SMT machine where 12 and 24 are the physical-core and hardware-thread rungs
(Gottschlag's sibling channel).

## 3. The design

### 3.1 The mechanism

Four rungs - 1, 12, 24 and 6 cores, in that order so that the three most
informative are done first if the window ends early - each with two arms, the
engine on and the engine off, at the committed 1e9 rows and a 56g driver, which
are `PLAN_TASK_99.md`'s parameters so the one-core rung is comparable with the
committed files.

    dev/varka_bench_surface.sh --rows 1000000000 --partitions P --cores P \
      --driver-memory 56g \
      varka-pP=$PWD:$J25:varka \
      varka-off-pP=$PWD:$J25

Eight files, `DateSurface-varka-p{1,6,12,24}-results.txt` and their
`varka-off-` twins. The existing committed one-core files are left alone: this
ladder's rungs are one session and carry their own labels.

### 3.2 What is deliberately unchanged

The driver, the inventory, the fixture, the row count and the heap. The
committed `varka-jdk25` and `varka-off-jdk25` files. `--master local[1]` stays
the default, so every existing command line means what it meant.

## 4. Files

* `dev/varka_bench_surface.sh` - the `--cores` knob and what it changes.
* `sql/varka/bench/benchmarks/DateSurface-varka-p*-results.txt` - eight new.
* `sql/varka/plans/PLAN_MILESTONE_5.md` - row 134.

## 5. Tests, and what each is for

None: this is a measurement. The script's own guards run per arm - the canary,
the load check, the datapath probe, and `--expect-fused` on every Varka row, so
a rung that quietly stopped fusing fails rather than publishing a scalar number.

## 6. The measurement

The eight files.

### 6.1 Predictions, registered before the run

1. **The engine's advantage narrows as cores rise.** The lightest surface rows
   are already memory-bound at one core (`PLAN_TASK_62.md` 11.13 puts a third of
   the surface there), so they have nothing to gain and everything to lose from
   sharing; the heavy calendar rows should hold most of their ratio.
2. **12 to 24 buys little and may cost.** The second thread of a core shares its
   vector units, so on rows bound by arithmetic the sibling adds queueing, not
   throughput; on memory-bound rows it may help by hiding latency.
3. **Scaling is sublinear from 6 cores up on the light rows** and closer to
   linear on the calendar rows, which is the same split as prediction 1 seen per
   rung rather than per arm.
4. Every rung fuses every row: occupancy changes no plan.

## 7. Risks

* **The window ends before the fourth rung.** The order is 1, 12, 24, 6 for that
  reason: the first three answer the question and the fourth refines it.
* **Thermal drift over four rungs.** The canary runs per arm and its numbers are
  in each file; a rung whose canary says the machine moved is reported, not
  quietly averaged in.
* **The heap.** 1e9 rows cached is about 23 GiB; 56g leaves room at every rung,
  and the machine has 83 GiB with 77 free at the start.

## 8. Sequencing

1. The `--cores` knob. 2. This plan. 3. The build, before the window goes quiet.
4. The four rungs, serially, under `systemd-inhibit` so the laptop cannot sleep
through them. 5. Section 9, the row, and the files committed.

## 9. Outcome

*To be written when the rungs land.*
