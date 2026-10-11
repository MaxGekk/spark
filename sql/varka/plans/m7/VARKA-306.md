# VARKA-306: The bytes suite pins the reference arms

## 1. Where this came from

Row 306 of `m7/PLAN.md`, filed by VARKA-83's review (`VARKA-83.md` 9.1). The first version of
#712 changed the scratch-local rule, and the change moved bytes in 23 of 64 multi-group guarded
cases under `groupLocalSlots=false` and a byte budget of 0. `VarkaEmittedBytesSuite` passed
anyway. Its option arms are the settings a session can select (`useAVX`, and `validityOrFirst`
over its subject), and these two are not among them. A probe written for the review found the
change, not the oracle. The row: pin the reference arms too, and show that the 9.1 regression,
reintroduced, fails the suite.

## 2. The admission check, done

Which arms, over which shapes, would have caught the regression? The row suggests a new set of
shapes with refusing nodes in some groups only. Checked on 11 October 2026 at `fd52b033767` by
reintroducing the first version's rule in `Slots.guardScratch` (a refusing node takes a scratch
local whenever its reason parks its value, whatever the body emits) and comparing digests
against master:

* **Four arms**: `groupLocalSlots=false`, a byte budget of 0, and each with a group budget of 1.
* **Four shape sets**: the oracle's coverage rows, its ten thousand int fuzz shapes, its ten
  thousand long fuzz shapes, and the review probe's eight multi-group shapes.

| arm | coverage | int fuzz | long fuzz | probe shapes |
|---|---|---|---|---|
| `groupLocalSlots=false` | same | moved | same | moved |
| byte budget 0 | same | moved | same | moved |
| either, with a group budget of 1 | same | moved | same | moved |

At both widths, every arm sees the regression through the int fuzz shapes the oracle already
holds, whose two- and three-root draws group into separate methods. The coverage rows are one
output each, and the long shapes did not move. So no new shape set is needed, and the group
budget of 1 adds nothing.

Cost: the existing `armDigest` emits every shape the oracle holds, and one arm at one width took
5 to 8 seconds on the laptop. The suite took about six minutes in the last gate. Two arms add
about half a minute. Every one of the options table's thirty or so `REFERENCE` arms would add
about six minutes, doubling the suite, so this row pins the two whose slot planning spans the
whole kernel, which is where a per-body rule can go wrong unseen.

What the check would have rejected: a new hand-written shape set, which the existing corpus
makes redundant.

## 3. The design

### 3.1 Two more pinned arms

`pinnedArms` gains `groupLocalSlots=false` and `methodByteBudget=0`, each one digest per width
over every shape, as the `useAVX` arms are. `emitted_bytes.json` gains their four digests under
`option_arms`, and its description and the suite's doc say why they are there: not because a
session can select them, but because their frames span the kernel, so a slot rule that holds
per body can still move their bytes.

### 3.2 What is deliberately unchanged

The coverage and fuzz hashes, the existing arms, and the option audit. Pinning further reference
arms is not this row: the audit already reports which options move bytes, and a regression in
another arm would need its own argument for a pin.

### 3.3 Registered op counts

None move: nothing in the emitter changes.

## 4. Files

| file | what |
|---|---|
| `VarkaEmittedBytesSuite.scala` | the two arms, and their reason in the doc and the file's description |
| `emitted_bytes.json` | regenerated: four digests added, nothing else |

## 5. Tests, and what each is for

* **The regenerated file differs from master's only by the four digests** and the description,
  so every existing pin holds.
* **The 9.1 regression, reintroduced, fails the suite**, naming the two arms; the row's proof.
  Reverted, it passes.
* **The gate**, both widths.

## 6. The measurement

The suite's time, from the gate's report before and after. No benchmark.

### 6.1 Predictions, registered before the run

1. The suite takes under a minute longer in the gate.
2. The reintroduced regression moves both new arms at both widths and no existing pin.

## 7. Risks

1. **An arm that declines or throws on a fuzz shape.** `methodHashes` records a decline as the
   arm's answer, but a byte budget of 0 rejects a shape over the legacy node cap with an
   `IllegalArgumentException`. The admission run emitted the ten thousand int fuzz shapes under
   both arms without one; the suite itself fails on any throw, so the regeneration checks the
   rest.
2. **Churn.** A task that changes either reference form now regenerates the file, as one that
   changes the defaults does. That is the point of the row.

## 8. Sequencing

1. This plan; row 306 marked Planned.
2. The arms and the regenerated file, in one commit.

## 9. Outcome

Done on 11 October 2026, as 3.1 describes, and one change the plan did not list.

* **The 9.1 regression, reintroduced, fails the suite.** With the first version's rule in
  `Slots.guardScratch`, the suite reports `option arm groupLocalSlots=false` and
  `option arm methodByteBudget=0`, each at both widths, and nothing else. Reverted, it passes.
* **The failure names a moved arm.** Not in the plan: the suite's failure report compared the
  coverage hashes and the fuzz blocks but never `option_arms`, so the first run of the proof
  failed with "no hash differs; the file's other content or its formatting changed". The report
  now walks the arms, by name and width, gone and new included.
* **`emitted_bytes.json` changed by the four digests and its description's sentence**, nothing
  else.
* **The gate passes**: every Varka suite at both widths, and lint.

**Predictions scored (6.1).**

1. **Held.** Run alone, the pinning test took 2 minutes 4 seconds with the two arms and 1 minute
   43 seconds without them, 21 seconds more. The gate's own report is no measure of it: the
   suite ran in a different shard beside different suites from the last gate's.
2. **Held**: both new arms at both widths, and no existing pin.

### 9.1 Review of #716 (`/code-review high`), 11 October 2026

No wrong answer. Fixed in the pull request:

* **A shape over the legacy op cap would have broken the oracle.** The byte budget of 0 rejects a
  shape of more than `MAX_FUSED_NODES` distinct ops with an `IllegalArgumentException`, which
  `methodHashes` did not catch, so a coverage row or a grammar change past the cap would have
  failed the suite and its regeneration alike. That rejection is now the arm's answer, as a
  decline is; any other rejection still fails the suite.
* **The failure report names any differing value.** Where no walker sees a difference, it walks
  both files and names each differing value by its path, so a section added later is not
  invisible the way `option_arms` was. Checked by editing a fuzz seed in the committed file: the
  report names `/lanes/4/fuzz/seed`.
* **The two arms come from the options table by name**, so the pin and the audit name one
  emission; the suite's doc and the file's description say what `option_arms` holds now; and
  `skills/emitter-and-ir.md` says why the two reference forms are pinned.

Not fixed here: the suite runs its eighteen full passes one after another and draws the fuzz
shapes again for each. Running them in parallel needs the emitter shown safe across threads
first, which is row 307. A marker in the options table for the arms to pin is not added: 3.2
gives the reason for pinning these two only.
