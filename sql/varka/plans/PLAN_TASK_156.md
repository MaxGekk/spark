# Task 156: the narrowed store at two 64-bit lanes

*Opened 20 September 2026 from `PLAN_MILESTONE_5.md` 2.92 and `PLAN_TASK_102.md`
8.6; the diagnosis the same afternoon, the measurement queued for the night.*

## 1. The question

Route A's narrowed store (`PLAN_TASK_102.md` 8.3) costs, against the wide store
of the same tree in `VarkaTimeBenchmark`, within 4% on every shape at 512 bits
and a quarter *less* past L3, but at 128 bits `second` reads 12% under the wide
store and the three fields together 18%, where `hour` and `minute` are within
5%. The per-group cost over two rows explained the direction; nothing explained
why the heavier shapes pay more, when the store should be a smaller share of
them. The project establishes JIT facts from the JVM's own output, so the
question was put to the assembly.

## 2. The instrument

`dev/varka_emit.sh --asm` dumps C2's disassembly of a kernel's dense loop, but
it drove int32 kernels only. It now accepts the long lane's column types
(`time`, `time(p)`, `bigint`, `interval day to second`), runs a long-lane
kernel through the eight-argument entry point with each output buffer at its
Spark type's width - so a narrowed root is driven as the evaluator drives it -
counts the `LongVector` ops beside the `IntVector` ones in its method table,
and takes `--width=N` to read a body at a species other than the host's. The
first dumps of `second(t)` produced no method table at all: the tool passed the
int literal count to the emitter where the tree's literal sits in `longArgs`,
which `fused.numLiterals` fixes.

## 3. What the assembly says

The innermost loop of the narrowed `second(t)` at 128 bits, beside the wide
two-divide shape `time_diff('MINUTE', time_trunc('SECOND', t), TIME'00:00:00')`
at the same width, both one lane group per iteration:

| body | instructions per group | divides | of which the store | in-loop bounds check |
|---|---:|---:|---|---|
| wide, plain store | 36 | 2 | 1 (`vmovdqu` folded into `(%r13,%r9,8)`) | none |
| narrowed, masked store | 45 | 2 | 1 masked, plus `vpshufd`, `vmovq` | `cmpq`, `jg` |

The nine extra instructions are not the narrowing, which is two (`vpshufd $8`
packs the two lanes' low halves, `vmovq` clears the upper half). Four are
scalar address arithmetic - `movslq`, `shlq $3`, `addq`, `shrq $1` - computing
the long byte offset and then halving it, where the wide store folds the
induction variable straight into its addressing mode. Two are a bounds check
on the destination that stays inside the loop. One is a base add. And the
masked store itself.

Two consequences follow, and both fall harder on the heavier shapes. The
address arithmetic and the check are per group whatever the shape, so they are
a larger share of `hour`'s short body than of `second`'s - which is the wrong
direction, and rules them out as the whole answer. The other consequence is
the one that fits: **no narrowed loop is unrolled.** The wide one-divide body
(`time_diff('SECOND', ...)`) runs two groups per iteration at 128 bits and
eight at 512; every narrowed body, at both widths, runs one. On two 64-bit
lanes the divider is latency-bound - each `vdivpd` on two lanes waits for the
previous one in its dependency chain - and unrolling is what lets the second
group's divide issue while the first's completes. `hour` has one divide per
group, `second` two dependent ones (`t / 10^9`, then `/ 60`), the three fields
three: the longer the chain, the more an unrolled loop hides and the more its
absence costs. At 512 bits eight lanes give the divider throughput work and the
loss is small, which is what the file showed.

## 4. Two changes, and what each did in the dump

**The offset from the row index.** The store's offset is now `(long) i * 4`,
derived from the induction variable the way the lane's own offset is, instead
of `byteOffset >>> 1`. That removed the shift arithmetic - two instructions per
group - and nothing else: the bounds branch stayed in the loop and the loop
stayed unrolled once. The masked `intoMemorySegment` reaches its intrinsic
through a branch on `offset <= byteSize - vectorByteSize`, and for a vector
twice as wide as the rows it writes, the last group of every batch fails that
test; a branch that is false on the last iteration is not one loop predication
hoists, and a loop with it is not one C2 unrolls.

**The half-width species** (`VarkaEmitOptions.narrowHalfSpecies`, off by
default). The conversion targets the int species with the long lane's own
count - `SPECIES_64` for two lanes, `SPECIES_256` for eight - so the converted
vector is exactly the group's values and the dense body stores it whole; the
epilogue stores under its remainder mask. Honoured at a baked lane count only,
since the half of the preferred species has no named constant. In the dump the
narrowed `second(t)` at 128 bits becomes 37 instructions per group against the
wide store's 36, no bounds branch, and `hour(t)` is unrolled twice again like
the wide one-divide body. That is the wide store's loop shape recovered, one
instruction over.

The cost the option carries is the one `PLAN_TASK_28.md` 2.2 recorded: a second
`IntVector` species in the JVM can make the Vector API's shared templates inline
bimorphically and box the int kernels that share them. `VarkaTimeBenchmark`
runs the int32 arms in the same JVM as this one, so their rows against the
committed file are the canary for that: if they hold within the band while the
new arm runs, the pollution did not happen there.

## 5. Predictions, registered before the run

The regeneration at 512, 256 and 128 bits is queued behind tonight's task 105
chain, in this branch, with the offset fix in the shipped form and the half
species as a new arm beside it.

1. **The shipped (masked) form moves little.** With the shift gone, `second`
   at 128 bits closes at most a third of its 12% gap to the wide store; the
   unrolling it lacks is the larger part.
2. **The half-species arm reads within 3% of the wide store on every shape at
   128 bits**, `second` and the three fields included, and within 3% at 256
   and 512, since its loop is the wide store's loop.
3. **The int32 arms do not move**: `seconds of day, int32 lanes, emitted` and
   the hand-written arm stay within 3% of the committed file, so the second
   species did not box them in this JVM.
4. Past L3 at 512 bits the half-species arm keeps the narrowed store's
   quarter over the wide one, since it writes the same bytes.

If 2 and 3 hold, the half-species form becomes the shipped one where the lane
count is baked, and the general case - the half of the preferred species as a
static final of the emitted class - is the follow-up; if 3 fails, the masked
form stays and the cost is recorded as the price of one species per type.
