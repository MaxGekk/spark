# Task 58: extract(YEAROFWEEK) as Year over the Thursday shift

## 1. Where this came from

`PLAN_MILESTONE_4.md` row 58 and section 2.25, from the coverage survey of 4
September 2026. The ISO week-based year - `DateTimeUtils.getWeekBasedYear`,
`LocalDate.get(IsoFields.WEEK_BASED_YEAR)` - is 2004 for 2005-01-02 and 2021
for 2020-12-31: the calendar year of the Thursday of the day's ISO week. No
registered function reaches it; `extract(YEAROFWEEK FROM d)` and
`date_part('YEAROFWEEK', d)` resolve to Spark's `YearOfWeek` expression,
which declines today with "unsupported expression". Task 37 built the
Thursday shift as a node of its own, `ThursdayOf`, precisely so this task is
`Year` over it: section 2.25 named the two ways 37 could go, and 37 took the
one that leaves this task a compiler arm and nothing in the emitter.

## 2. The admission check, done

**The identity is task 37's, already swept.** `PLAN_TASK_37.md` 2 checked,
over every one of the 3,652,059 days from 0001-01-01 to 9999-12-31, that
`year(t)` with `t = d + 3 - weekday0(d)` equals `isocalendar`'s week-based
year: zero mismatches. That is the whole of this task's arithmetic.

**The spelling.** `Extract.parseExtractField` resolves `YEAROFWEEK` to
`YearOfWeek(source)` (checked in this worktree), a `GetDateField` whose
`func` is `DateTimeUtils.getWeekBasedYear` and whose output is `IntegerType`;
`date_part` goes through the same function. There is no other spelling.

**The range.** `ThursdayOf` shifts a day by `[-3, +3]`, and task 37 gave
task 52's analysis that arm; `Year` over it is admitted at compile time
inside the calendar range by the same rule `weekofyear` is, three days short
of a bare date's last fusing shift. Nothing new to check.

**What the check would have rejected:** a spelling that reached the
compiler as something other than `YearOfWeek`, or a `getWeekBasedYear` that
was not the Thursday's year; neither is the case.

## 3. The design

### 3.1 One compiler arm

    case YearOfWeek(child) =>
      compileNode(child, inputs, literals, sink)
        .flatMap(c => admitCalendar(new ThursdayOf(c), expr, literals, sink))
        .map(new IRYear(_))

beside task 37's `WeekOfYear` arm, which it mirrors line for line: the same
`admitCalendar` over the built shift, the same `Year` node the family has
had since task 26. Under the emitter's CSE, `weekofyear(d)` and
`yearofweek(d)` in one projection are two tails over one `ThursdayOf` and
one prefix - task 37's sharing test already asserts the pair costs under
ten ops more than `weekofyear` alone - and a projection that also reads
`year(d)` decomposes the bare date separately, as row 37 says.

No new IR node, no emitter change, no new option, no pinned fixture moves:
the shape `Year(ThursdayOf(col))` is one task 37's fixtures already render.

### 3.2 What is deliberately unchanged

* The emitter: `Year`'s tail and `ThursdayOf`'s arm are task 37's; this task
  reads them.
* `weekofyear`: its arm stays; the two share by CSE, not by a combined node.
* A `yearweek` key: scope item 11 of `SCOPE_MILESTONE_6.md`, a product
  decision, not this task.

### 3.3 Registered op counts

None move. `yearofweek(col)` alone is `ThursdayOf`'s 19 plus `year`'s tail,
read off the bytes by the register in section 5 for the record: 51 dense-loop
`IntVector` calls.

## 4. Files

| file | what |
|---|---|
| `VarkaExpressionCompiler.scala` (+ suite) | the arm; imports; the compiler test through `Extract` |
| `VarkaLoopEmitterSuite.scala` | the register line for `yearofweek(col)` |
| `VarkaDifferentialSuite.scala` | the boundary rows, the three fields in one differential, the spellings |
| `VarkaEmitterParityBenchmark.scala` + files | a `yearofweek` row and the shared pair beside task 37's rows |
| `docs/sql-varka.md` | one line under task 37's bullet |
| `PLAN_MILESTONE_4.md`, this file | row 58, section 9 |

## 5. Tests, and what each is for

* Compiler: `extract(YEAROFWEEK FROM d)` through `Extract` itself compiles to
  `Year(ThursdayOf(ColumnRef(0)))` with an `IntegerType` output; beside
  `weekofyear(d)` the two outputs share the `ThursdayOf` node structurally;
  the range boundary at three days short of a bare date's, as 37's test does
  for `weekofyear`.
* Register: `yearofweek(col)`'s dense-loop count, pinned once read.
* Differential: the rows the ISO year moves on - December 28 to January 4 of
  2004/2005, 2020/2021 and 2026/2027, whose week 1 starts in the old year,
  and of 2018/2019 and 2022/2023, where it does not, plus the century years
  1900 and 2000 and the range ends 0001-01-01 and 9999-12-31 - with
  `weekofyear`, `yearofweek` and `year` over the same column in one query,
  since the three disagree on exactly those rows; both spellings; a null;
  the filter route `WHERE extract(YEAROFWEEK FROM d) <> year(d)` counted,
  which selects the boundary rows themselves.
* The emitter's oracle for `Year` is `LocalDate.getYear` and for `ThursdayOf`
  the `with(THURSDAY)` adjuster (task 37); nothing to add.

## 6. The measurement

`VarkaEmitterParityBenchmark`, the "year" section, beside task 37's rows:
`yearofweek` null-free and mixed nulls, and the pair `weekofyear +
yearofweek` in one kernel as the sharing row, both widths, one regeneration
on an idle machine after task 37's has landed. `VarkaThroughputBenchmark`
gains an `extract(YEAROFWEEK FROM d)` row.

### 6.1 Predictions, registered before the run

1. `yearofweek` runs at 0.85x to 0.95x of `year`'s rate: the shift's 19 ops
   on `year`'s roughly 30 at 256 bits.
2. The shared pair costs under 10% more than `weekofyear` alone, since the
   prefix and the shift are computed once and `year`'s tail is a few ops.
3. Nothing else moves beyond the machine-day variance already recorded.

## 7. Risks

1. **Stacking.** This task's arm calls `admitCalendar` and `ThursdayOf`,
   which exist only on task 37's branch until it merges; the branch is cut
   from 37's, or the task waits a day. Either way the diff is the same five
   lines.
2. **A future Spark changes the spelling.** The compiler test builds the tree
   through `Extract`, so it fails there first.

## 8. Sequencing

0. Task 37 merged, or this branch cut from `varka-task-37`.
1. This plan and the milestone row.
2. The arm, the compiler test, the register line, the differential, the
   docs line.
3. The benchmark rows, one regeneration, section 9, row 58.

## 9. Outcome

Filled in when the measurement lands.
