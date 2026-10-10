# VARKA-285: Coverage of the generated code

## 1. Where this came from

Row 285 of `m7/PLAN.md`, from the JaCoCo spike of 4 October 2026 (`m7/PLAN.md` 2.2): row 265
measures the emitter, and nothing measures the classes it emits, where the kernels' instructions
run. The spike ran the gate's wide-step suites under the JaCoCo agent: of 13,461 classes emitted,
1,555 ran; about 730 of those ran only their masked body and about 135 only their dense one; 3,651
of 5,596 dense loop methods were never entered. Row 284 closes that gap and needs this row's
report to show it closed. Done when `dev/varka_gen_coverage.sh` runs the Varka suites under the
agent, sums each emitted class's coverage on its own - per method kind through
`VarkaMethodNames` and per IR operation through the class's `LineNumberTable` key - and the
report is committed, listing the missed branches inside executed loops by operation.

## 2. The admission check, done

The spike is the admission check, and its findings shape the tool (`m7/PLAN.md` 2.2 and the
spike's notes):

* **The agent sees emitted classes** once `inclnolocationclasses=true`: they are defined by an
  ordinary class loader with no code source. `classdumpdir` keeps their bytes, which the analysis
  needs since they exist nowhere else.
* **JaCoCo's own report refuses the data**: tests emit one class name with different bytes, and
  one `CoverageBuilder` takes one class per name. So each dumped class is analysed with a builder
  of its own, against the execution data of its own id.
* **No cost worth naming**: the wide step's suites took 841 s against 837 s.
* **The lines are IR nodes already**: `VarkaDebugInfoReader.lineMap` gives each emitted line's
  node, so a class's line coverage sums per operation.

## 3. The design

### 3.1 The script and the analyser

* `dev/varka_gen_coverage.sh` runs `dev/varka_matrix.sh --defaults` - every Varka suite of
  `catalyst` and `sql/core`, as the gate's wide step - with the agent on every test JVM, all of
  them appending to one execution file, and the emitted classes dumped. Then it runs the
  analyser and writes the report.
* `dev/varka_gen_coverage/VarkaGenCoverage.java`, a single-file Java program run against
  `org.jacoco.core`, ASM and catalyst's classes, so that no build gains a dependency: for each
  dumped class that carries a Varka line map, its coverage alone; classes that ran are those with
  a covered instruction. Summed:
  * the classes emitted and run, and of those run, which entered both drivers, only the dense
    one or only the masked one - row 284's measure;
  * per method kind (dispatch, driver, stage, loop, epilogue, each side): methods emitted in run
    classes and entered, instructions and branches covered;
  * per IR operation: instructions and branches covered over the lines it owns;
  * the missed branches inside loop methods that ran, by operation - the gaps a test reaches
    but does not exercise.
* The report goes to `sql/varka/coverage/generated.md`, committed, its numbers traced to it.

### 3.2 What is deliberately unchanged

The suites, the emitter and every build: the agent is a JVM argument, the analyser a program run
on its own classpath. Row 284's fix, row 265's emitter coverage and its mutation run.

### 3.3 Registered op counts

None move.

## 4. Files

| file | what |
|---|---|
| `dev/varka_gen_coverage.sh` | the run |
| `dev/varka_gen_coverage/VarkaGenCoverage.java` | the analysis |
| `sql/varka/coverage/generated.md` | the report |
| `m7/PLAN.md`, the testing lessons | the records |

## 5. Tests, and what each is for

The tool is measured by its report: the counts are checked against the spike's in kind (classes
run against emitted, the two drivers), and one class is checked by hand against its methods.

## 6. The measurement

One run on the laptop at the default width.

### 6.1 Predictions, registered before the run

1. Since the spike, row 276's per-row test and row 247's subject arms run more kernels, so more
   classes run than the spike's 1,555, and the share of run classes entering both drivers is
   still under half: row 284 is not done.
2. The run takes within ten per cent of the gate's wide step.

## 7. Risks

1. Concurrent JVMs appending to one execution file: JaCoCo locks the file to append, which the
   spike relied on.

## 8. Sequencing

1. This plan. 2. The script, the analyser, the run, the report and the records.

## 9. Outcome, 10 October 2026

### 9.1 What was built

`dev/varka_gen_coverage.sh` and `VarkaGenCoverage.java` as section 3 describes, and the report,
`sql/varka/coverage/generated.md`. Three things the first run showed, each fixed before the
committed one:

* **The suites in their own JVM went unmeasured.** `VarkaOwnJvm` drops every `-javaagent` from
  the child's command line, so the ten suites it runs - both fuzzers, the validity, division and
  long-lane suites among them - emitted kernels JaCoCo never saw. The child now keeps JaCoCo's
  agent, which appends to its file under a lock. With it, 2,803 classes ran where the first run
  counted 907.
* **A wide kernel's line map is cut** to fit one constant (`VarkaDebugInfo.TRUNCATED`); the cut
  entry read as an operation of its own. The analyser drops it and counts the lines past the cut
  as `(line map cut)`: five classes, which own 844,662 instructions.
* **"Ran" meant "constructed".** The hand check of the smallest class with a dense side never
  entered found one whose only covered method was its constructor: a test that builds a kernel and
  calls the wrong overload. A class now ran when a kernel method was entered; 34 fell out.

### 9.2 The predictions scored

1. **Holds.** 2,803 classes ran, against the spike's 1,555; 733 of them entered both drivers, 399
   only the dense one, 1,607 only the masked one and 64 neither. Under half entered both, so row
   284 is not done.
2. **Not scored cleanly.** The run's suites took 390 s; the gate's wide step took 465 to 882 s
   on this laptop today with other work beside it, so the agent's cost hides inside that spread,
   as the spike found (841 s against 837 s).

### 9.3 What this leaves

* **Row 284** reads the class table; **row 265** the missed branches in executed loops, where
  `makeDate:ANSI` (528), `if` (206) and the checked arithmetic lead: largely the decline paths,
  which a loop reaches and a test rarely takes.
* **Per-operation sums are by line mark**: a node owns the instructions after its mark until the
  next, so a leaf owns the stores that follow it. Finer attribution would need the emitter to mark
  where a node's code ends.
