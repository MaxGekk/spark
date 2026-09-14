# Task 97: A bandwidth-bound row lost 22.6% and nothing in its lowering changed

## 1. Where this came from

`PLAN_MILESTONE_5.md` row 97 and section 2.32, opened 14 September 2026 out of
task 78's surface regeneration. `date_add(d, 3)` reads **1811.6 M rows/s** in the
surface committed on 7 September (`DateSurface-varka-jdk25-results.txt` at
`4d6ea4ca3aa`) and **1401.4 M/s** in the one committed on 13 September - a loss of
**22.6%** - with the machine canary clean on both runs, the same host, the same
governor and the same billion rows.

The question is not whether the number moved. It is whether the *engine* moved,
or the *fixture* did, and the answer decides whether roughly a third of the
surface - every memory-bandwidth-bound entry - is currently understated.

## 2. The admission check, done

**The lowering did not change.** `date_add(d, 3)` emits four `IntVector`
operations today, from `dev/varka_emit.sh --table`; `date_add(d, i)` emits four
as well. Of the fifty-five commits between the two runs, exactly one touches the
`AddDays` neighbourhood of the emitter - `2972c9a229e`, task 93's re-arm - and
every hunk of it is a `case GuardedDay` arm, an import, or a slot allocation
behind `reachesGuardedDay`. A literal-offset `AddDays` tree contains no
`GuardedDay` node and reaches none of them. The column-offset support this shape
might otherwise be blamed on landed long before the first of the two runs.

**The fixture did change, and the check pins when.** The 7 September file carries
35 entries and none over an interval column. Today's carries 52, of which 8 read
`ymm`, `ymy` or `ym`. `git log -S "AS ymm,"` on the driver names `ef1a7dbdea7`
(9 September, "Year-month interval columns in the date lane") as the commit that
added them to `buildTable`, and that commit is an ancestor of the second run and
not of the first. So the cached table went from three int32 columns to six
between the runs, and its `cache:` line from about 12 GiB to **23.2 GiB**.

**What this check would have rejected.** If the op count had moved, or if a
commit between the runs had touched the emitted `AddDays` path for a literal
offset, the task would be a regression hunt over fifty-five commits rather than a
fixture question, and the design below would be the wrong instrument.

## 3. The design

### 3.1 A table-shape switch on the benchmark driver

`DateSurfaceBenchmark` gains `--table-columns all|dates`. `all` is today's six
columns and stays the default, so every committed file and every dispatch is
unchanged. `dates` builds only `d`, `d2` and `i` - the three the table carried
before `ef1a7dbdea7` - which is the fixture the 1811.6 M/s number was measured
over.

The switch is on the *table*, not on the entry list: the same entries run either
way, and an entry that reads a column the narrow table does not have is refused
by the driver rather than silently skipped. That refusal is the point - it makes
the pairing explicit instead of leaving two runs with different entry sets to be
compared by eye.

The shape is recorded in the provenance as `table columns:`, so a results file
says which fixture produced it and the comparison is checkable from the files
alone.

### 3.2 What is deliberately unchanged

The entry lists, the row count, the guards, and the default. This task measures;
it does not decide the fixture. If the hypothesis holds, *what the surface should
build* is a decision for its own task, because it trades reproducibility of the
committed history against measuring each row over a table shaped like the query
it represents.

### 3.3 Registered op counts

None. No expression's lowering changes, which is section 2's whole finding.

## 4. Files

* `sql/varka/bench/src/main/java/.../DateSurfaceBenchmark.java` - the switch, the
  narrow table, the provenance key, and the refusal when an entry needs a column
  the shape does not carry.
* `dev/varka_bench_surface.sh` - pass it through.
* `sql/varka/bench/src/test/java/.../SurfaceTest.java` - the refusal is asserted.
* `sql/varka/plans/PLAN_MILESTONE_5.md` - the row, marked planned.

## 5. Tests, and what each is for

* **The narrow table has exactly three columns, and the wide one six.** A test
  that reads the built schema, so a future column added to one shape and not the
  other cannot pass unnoticed - which is precisely how this task's subject
  arrived.
* **An entry reading an absent column is refused, with its name in the message.**
  The alternative is a silently shorter run whose file looks comparable and is
  not.
* **The provenance carries the shape.** Without it two files are
  indistinguishable, and the whole comparison rests on remembering which was
  which.

## 6. The measurement

Four runs on an idle development machine, the Varka arm only, at the committed
1e9 rows and the 56g driver the resident table needs, `--only` over three
entries chosen to span the regime:

* `date_add(d, 3)` - the subject, and the most bandwidth-bound entry in the file.
* `year(d)` - a mid-weight calendar extraction.
* `weekofyear(d)` - the heaviest single call in the surface, and the control: if
  the table's width moves *this* row materially, the effect is not what section
  2 says it is.

Each entry is run over the `dates` table and the `all` table, in one session per
shape so the pairing shares a JVM and a page cache state.

### 6.1 Predictions, registered before the run

1. **`date_add(d, 3)` over the three-column table reads within 5% of 1811.6 M/s,
   and over the six-column table within 5% of 1401.4.** This is the prediction
   that matters: it says the fixture reproduces both historical numbers on one
   machine on one day, which is what would make the working set the explanation
   rather than a coincidence of two runs a week apart.
2. **`weekofyear(d)`, the control, moves less than 5% between the two shapes.**
   It does about 64 emitter ops against `date_add`'s four, so it is bound by
   arithmetic and should not care what else is in the table. If it moves as much
   as the subject, the cause is not bandwidth and section 2's reasoning is wrong.
3. **The effect is sub-linear in table size.** The table doubles, 12 to 24 bytes
   per row, and the observed loss is 22.6%, not 50%: the kernel still reads only
   its own four bytes per row, and what degrades is locality across the batch
   sequence rather than the volume it must fetch. So I expect `year(d)` -
   mid-weight - to lose something between zero and the subject's 22.6%, and
   nearer zero than to it.

## 7. Risks

1. **The two historical numbers may not reproduce at all**, if something other
   than the table changed the machine between 7 and 13 September. The canary was
   clean on both, which is the evidence against, but the canary controls for
   thermal and load state rather than for kernel or firmware changes. Prediction
   1 is what detects this: if neither number reproduces, the task's answer is
   "not the fixture, and not attributable from here", and that is worth
   recording as a limit on what these files can settle.
2. **`--only` changes the run's shape**, because a full surface run builds the
   table once and amortises it over 52 entries. The measurement uses the same
   `--only` set for both arms, so the pairing is sound even if the absolute
   numbers sit slightly off a full run's.
3. **Page cache and allocation state** differ between a 12 GiB and a 23.2 GiB
   table in ways beyond the benchmark's control. Running the two shapes in one
   session each, rather than interleaved, means a shape's numbers share their
   allocation history - which is the honest arrangement, not an ideal one.

## 8. Sequencing

1. The plan, and the milestone row marked planned.
2. The switch, the narrow table and the provenance key, with the three tests -
   green before any measurement.
3. The four runs, and section 9 written from them.
4. If the hypothesis holds, a follow-up row for the fixture decision; if it does
   not, a follow-up row for the regression hunt. Either way this task ends with a
   number and a named next step, not with a fix.

## 9. Outcome

<!-- after the measurement -->
