# VARKA-259: bytecode without source, the companion of row 258's post

*Row 259 of `m7/PLAN.md`, `m9/SCOPE.md` item 79. Planned and drafted on 16 October 2026 in the
shape of `VARKA-371.md`: the question, the claim, the evidence the record already holds, the
outline, what a reader can reproduce, and done-when. It is the pair of row 258 ("how Spark compiles
a query, and why it compiles it again", `VARKA-258.md`), published with it or right after, each
linking the other. The draft is `POST_BYTECODE.md`; it uses only what the record holds, and section
9.1 lists what is owed before publication.*

## 1. The question

The reader is a Spark or JVM developer who has read, or will read, row 258's post: Spark writes one
Java source string per stage, Janino compiles it on the driver and on every executor, a per-JVM
cache keyed by the source text skips the compile for a repeated string, and HotSpot then runs the
class through its tiers. The reader knows what a code generator is and may have written one. The
post answers: **what changes when a query engine's generator writes bytecode instead of Java?**

## 2. The claim, and the five-minute version

Writing the class directly with the JDK's Class-File API makes generating it cheap (about 99 us
against about 6.5 ms for a Janino compile in Varka's codegen benchmark, though not yet over the
same expressions), makes every method's size a fact read off the class before it is defined
instead of a guess from source length. It does not shorten the JIT's wait: a new class
still starts interpreted, and in the cold-start benchmark Varka without its warm-up is the slowest
arm on its first runs. The saving that matters is the class that is never rebuilt, not the class
that is built fast, and that comes from keying the class by a shape with its constants as slots,
which is a property of the IR, not of bytecode: Spark already passes strings, decimals and binaries
as references (`VARKA-258.md` 2).

The five-minute version: the size guess Spark has to make and Varka does not; the shape key with
constants as slots, the direct answer to row 258's hook, kept apart from what bytecode changes,
with SPARK-60048 (the key of the split decision, as `POST_CASE_WHEN_USERS.md` describes it) as
Spark meeting the question in a narrower place; the emission cost with its caveat; the cold-start
table; what a developer gives up.

## 3. The evidence the post cites

* **Emission against Janino**: `sql/core/benchmarks/VarkaCodegenBenchmark-jdk25-results.txt`,
  "janino: GenerateUnsafeProjection.generate" 6516903.0 ns a class (best 7 ms, average 8) against
  "varka: fused emit + define + load + instantiate" 99365.0 ns, 65.6X, on the laptop's Ryzen AI 9
  HX PRO 370 with JDK 25.0.4. The arms differ: Janino compiles a one-output `date_add` unsafe
  projection with a new literal each iteration (`VarkaCodegenBenchmark.java`, the cache defeat
  row 258 describes); Varka emits the two-output DAG-CSE kernel. Item 79 asks for both over the
  same expression list; that benchmark is owed (9.1).
* **The first query**: `sql/core/benchmarks/VarkaColdStartBenchmark-jdk25-results.txt`, rung 16
  (sixteen entries of `greatest(add_months(d, k), date_add(d, k), last_day(d))` over 100,000
  Arrow-cached rows): plan only 17 ms vanilla, 18 Varka (Varka's plan holds the emission); first
  run 110, 241 without warm-up, 140 with it; second run 53, 213, 62; once compiled 27; the warm-up's
  verdicts COMPILED after 1580 to 1873 ms. Provenance in the file beside it (commit `0914c607ff8`).
* **The size read off the class, and the cap met at assembly**: `m6/POST.md` section 2 and
  `skills/emitter-and-ir.md`, "The class-file cap on a method is met before it can be measured".
* **The stack-map generator as a verifier**, and the `NoSuchMethodError` it does not catch:
  `skills/emitter-and-ir.md`, "The Class-File API's Stack-Map Generator Is a Free Verifier".
* **Line numbers as IR node indices**: `skills/emitter-and-ir.md`, "Generated Code Can Carry Its
  Own Debug Info".
* **The shape key**: `m6/POST.md` section 2 (`date_add(d, 7), year(d)` and `date_add(d, 30),
  year(d)` share a hash) and `spark.sql.codegen.varka.cache.maxEntries` in `StaticSQLConf`.
* **Spark's side**: `spark.sql.codegen.methodSplitThreshold`'s documentation and the seven
  characters a byte of `m6/POST.md` section 1; SPARK-60048 as cited by `POST_CASE_WHEN_USERS.md`.

## 4. The outline

1. **From row 258's post.** One paragraph of its pipeline, and the question.
2. **What a string costs.** The size guess, and the cache key that holds the literals.
3. **Writing the class directly.** The size read, not estimated (with the 65,535-byte cap met at
   assembly); the assembler's stack check.
4. **What the class is keyed by.** Constants as slots, a property of the IR rather than of
   bytecode; Spark's references for strings; SPARK-60048.
5. **What emission costs.** The codegen benchmark's two arms, and why they are not yet like for
   like.
6. **What it does not change.** The cold-start table and the warm-up.
7. **What you give up.** No source to read, and the tools that replace it.
8. **Reproducing it.**

## 5. What a reader can reproduce

* `dev/varka_emit.sh "date_add(d,7)" "year(d)"`: the shape, its hash, the literals in slot order
  and every method's bytes.
* `dev/varka_bench_regen.sh core VarkaCodegenBenchmark` and the same for
  `VarkaColdStartBenchmark`, with the machine stated.
* On stock Spark, `df.explain("codegen")` for the generated Java and `maxMethodCodeSize`.

## 6. Done when

A benchmark that emits and Janino-compiles the same expression list is committed with its
provenance and quoted in the post; the draft is kept short (the owner cuts length from Varka's
public writing); every number traces under `dev/varka_quote_check.py`; and the post is published
beside row 258's, each linking the other.

## 7. Sequencing

1. This plan and the draft, and the row marked drafted.
2. The matched benchmark, on an idle machine or a GitHub runner.
3. Row 258's publication, then this post's, then 9.2.

## 8. Explicitly out of this task

* Varka's correctness story (the proofs, the differential), which is the closing post's (row 270).
* The size control in detail, which `m6/POST.md` told; the post links it.
* Proposing a literal-free cache key upstream: SPARK-60048 is filed, and the post cites it.

## 9. Outcome

### 9.1 The draft, 16 October 2026

`POST_BYTECODE.md` is the draft, about 1300 words on 4's outline, written from the evidence of 3
only. Owed before publication:

1. **The matched benchmark** item 79's done-when asks for: emission against Janino's compile of the
   same kernels from the same expression list, committed with provenance. Until then the draft
   quotes `VarkaCodegenBenchmark` with the difference between its arms stated, and a bracketed note
   marks the table to replace.
2. **Row 258's published link**: its post is outlined, not drafted; the draft's first link is a
   placeholder.
3. **The owner's edit.**
