# Varka Project Lessons Learned

Working notes from implementing Varka (Spark columnar execution): reusable debugging
lessons and project-specific gotchas, especially the negative results - the questions
that were settled by measurement and should not be re-litigated. Not instructions; see
`AGENTS.md` for the workflow rules.

The lessons themselves live one topic per file under
[`sql/varka/skills/`](sql/varka/skills/), and this page is the index over them. That
split is for retrieval: the whole record is two hundred kilobytes, no reader and no
tool needs all of it to answer one question, and a topic file is small enough to open
whole. Add a lesson as a new `##` section in the topic file it belongs to, then
regenerate this index with `dev/varka_toc.py SKILLS.md --from sql/varka/skills`, which
the pre-commit hook also checks. Groups below are in file order.

<!-- BEGIN generated contents -->
#### [Benchmarking method](sql/varka/skills/benchmarking.md)
* [A benchmark that reuses `repeats` for wall-clock scaling must also scale its declared row count](sql/varka/skills/benchmarking.md#a-benchmark-that-reuses-repeats-for-wall-clock-scaling-must-also-scale-its-declared-row-count)
* [The benchmark controls are necessary and not sufficient](sql/varka/skills/benchmarking.md#the-benchmark-controls-are-necessary-and-not-sufficient)
* [A `--rounds` probe with no nulls compiles only half the kernel](sql/varka/skills/benchmarking.md#a---rounds-probe-with-no-nulls-compiles-only-half-the-kernel)
* [Flipping a default silently retires every A/B built on `DEFAULTS`](sql/varka/skills/benchmarking.md#flipping-a-default-silently-retires-every-ab-built-on-defaults)
* [Write the Prediction Down, Then Measure](sql/varka/skills/benchmarking.md#write-the-prediction-down-then-measure)
* [A benchmark guard that fails in one direction gets satisfied by the failure in the other](sql/varka/skills/benchmarking.md#a-benchmark-guard-that-fails-in-one-direction-gets-satisfied-by-the-failure-in-the-other)
* [A benchmark number is reproducible within a run and not between runs](sql/varka/skills/benchmarking.md#a-benchmark-number-is-reproducible-within-a-run-and-not-between-runs)
* [A count of calls on an owner is not a count of the work you mean](sql/varka/skills/benchmarking.md#a-count-of-calls-on-an-owner-is-not-a-count-of-the-work-you-mean)
* [An inventory made by reading is not an inventory made by counting](sql/varka/skills/benchmarking.md#an-inventory-made-by-reading-is-not-an-inventory-made-by-counting)
* [A default decided from a regeneration costs a second regeneration](sql/varka/skills/benchmarking.md#a-default-decided-from-a-regeneration-costs-a-second-regeneration)
* [A number superseded inside its own PR loses its provenance when the PR squash-merges](sql/varka/skills/benchmarking.md#a-number-superseded-inside-its-own-pr-loses-its-provenance-when-the-pr-squash-merges)
* [A band says which moves to read; an invariant says which to stop the line for](sql/varka/skills/benchmarking.md#a-band-says-which-moves-to-read-an-invariant-says-which-to-stop-the-line-for)

#### [Build and environment](sql/varka/skills/build-and-environment.md)
* [Classpath Shadowing (the stub trap)](sql/varka/skills/build-and-environment.md#classpath-shadowing-the-stub-trap)
* [Build Gotchas](sql/varka/skills/build-and-environment.md#build-gotchas)
* [Build Performance (measured, Aug 2026)](sql/varka/skills/build-and-environment.md#build-performance-measured-aug-2026)
* [Environment Facts (verified in this repo)](sql/varka/skills/build-and-environment.md#environment-facts-verified-in-this-repo)
* [Extra Sessions on the Shared Context](sql/varka/skills/build-and-environment.md#extra-sessions-on-the-shared-context)
* [Building a fastdebug JDK for HotSpot diagnostics](sql/varka/skills/build-and-environment.md#building-a-fastdebug-jdk-for-hotspot-diagnostics)

#### [Calendar algorithms](sql/varka/skills/calendar-algorithms.md)
* [Reading a paper into the repo](sql/varka/skills/calendar-algorithms.md#reading-a-paper-into-the-repo)
* [A reciprocal's top bits are a remainder, and other things a neighbouring codebase had](sql/varka/skills/calendar-algorithms.md#a-reciprocals-top-bits-are-a-remainder-and-other-things-a-neighbouring-codebase-had)
* [Validate a fixed-format string with a saturating subtraction](sql/varka/skills/calendar-algorithms.md#validate-a-fixed-format-string-with-a-saturating-subtraction)
* [Velox is a semantics reference for the calendar family, not a performance one](sql/varka/skills/calendar-algorithms.md#velox-is-a-semantics-reference-for-the-calendar-family-not-a-performance-one)
* [The Julian map: one division stage fewer in civil-from-days](sql/varka/skills/calendar-algorithms.md#the-julian-map-one-division-stage-fewer-in-civil-from-days)
* [A GPU port of Spark's date code is scalar code run per thread](sql/varka/skills/calendar-algorithms.md#a-gpu-port-of-sparks-date-code-is-scalar-code-run-per-thread)

#### [The emitter and the IR](sql/varka/skills/emitter-and-ir.md)
* [Sharing below the node level in an emitter](sql/varka/skills/emitter-and-ir.md#sharing-below-the-node-level-in-an-emitter)
* [Generated Code Can Carry Its Own Debug Info (Class-File API)](sql/varka/skills/emitter-and-ir.md#generated-code-can-carry-its-own-debug-info-class-file-api)
* [The Class-File API's Stack-Map Generator Is a Free Verifier](sql/varka/skills/emitter-and-ir.md#the-class-file-apis-stack-map-generator-is-a-free-verifier)
* [What "the masked method is the dense method's bytes" actually took](sql/varka/skills/emitter-and-ir.md#what-the-masked-method-is-the-dense-methods-bytes-actually-took)
* [A store the loop repeats per group with a constant operand is a fill the driver should do once](sql/varka/skills/emitter-and-ir.md#a-store-the-loop-repeats-per-group-with-a-constant-operand-is-a-fill-the-driver-should-do-once)
* [A refused call is refused by the caller's budget, and the caller's budget is spent in program order](sql/varka/skills/emitter-and-ir.md#a-refused-call-is-refused-by-the-callers-budget-and-the-callers-budget-is-spent-in-program-order)
* [A budget that bounds the method is not a budget that bounds the work](sql/varka/skills/emitter-and-ir.md#a-budget-that-bounds-the-method-is-not-a-budget-that-bounds-the-work)
* [A range guard belongs where the value is made, not where it is read](sql/varka/skills/emitter-and-ir.md#a-range-guard-belongs-where-the-value-is-made-not-where-it-is-read)
* [A derived input must never raise, because the row engine's null check comes first](sql/varka/skills/emitter-and-ir.md#a-derived-input-must-never-raise-because-the-row-engines-null-check-comes-first)
* [Read two fields out of one product, and put the axis where the formula wants it](sql/varka/skills/emitter-and-ir.md#read-two-fields-out-of-one-product-and-put-the-axis-where-the-formula-wants-it)
* [The shape a test picked because nothing lowered it](sql/varka/skills/emitter-and-ir.md#the-shape-a-test-picked-because-nothing-lowered-it)
* [A refusal shared by two positions carries one reason, and it can be true of only one](sql/varka/skills/emitter-and-ir.md#a-refusal-shared-by-two-positions-carries-one-reason-and-it-can-be-true-of-only-one)

#### [Testing and debugging](sql/varka/skills/testing-and-debugging.md)
* [Buffer-Reuse Aliasing (UnsafeProjection)](sql/varka/skills/testing-and-debugging.md#buffer-reuse-aliasing-unsafeprojection)
* [Alias Unwrap Is Needed at Two Layers](sql/varka/skills/testing-and-debugging.md#alias-unwrap-is-needed-at-two-layers)
* [Masked Bugs](sql/varka/skills/testing-and-debugging.md#masked-bugs)
* [Debugging Method: Progressive Isolation](sql/varka/skills/testing-and-debugging.md#debugging-method-progressive-isolation)
* [Columnar Transition Wiring (plan level)](sql/varka/skills/testing-and-debugging.md#columnar-transition-wiring-plan-level)
* [Metrics as the "did it really run" proof](sql/varka/skills/testing-and-debugging.md#metrics-as-the-did-it-really-run-proof)
* [Independent Reference Evaluators as Test Oracles](sql/varka/skills/testing-and-debugging.md#independent-reference-evaluators-as-test-oracles)
* [A fixture that fills undefined memory decides what its whole matrix can catch](sql/varka/skills/testing-and-debugging.md#a-fixture-that-fills-undefined-memory-decides-what-its-whole-matrix-can-catch)
* [Testing Under AQE](sql/varka/skills/testing-and-debugging.md#testing-under-aqe)
* [A closed `ArrowBuf` still answers `capacity()` and `memoryAddress()`](sql/varka/skills/testing-and-debugging.md#a-closed-arrowbuf-still-answers-capacity-and-memoryaddress)
* [A checklist for the next node type or mode, from what three reviews found in this one](sql/varka/skills/testing-and-debugging.md#a-checklist-for-the-next-node-type-or-mode-from-what-three-reviews-found-in-this-one)
* [Check that the place a prediction blames actually exists](sql/varka/skills/testing-and-debugging.md#check-that-the-place-a-prediction-blames-actually-exists)

#### [What C2 does with these loops](sql/varka/skills/the-jit.md)
* [C2 Compile Latency Is the Wide-Vector-Loop Cliff (root cause, proven)](sql/varka/skills/the-jit.md#c2-compile-latency-is-the-wide-vector-loop-cliff-root-cause-proven)
* [Watching what C2 compiled, at runtime, with no flags](sql/varka/skills/the-jit.md#watching-what-c2-compiled-at-runtime-with-no-flags)
* [A bimodal kernel is usually the register allocator, and here is how to prove it](sql/varka/skills/the-jit.md#a-bimodal-kernel-is-usually-the-register-allocator-and-here-is-how-to-prove-it)
* [Two things share the name `uncommon_trap`, and only one of them happened](sql/varka/skills/the-jit.md#two-things-share-the-name-uncommon_trap-and-only-one-of-them-happened)
* [Calling into an uncompilable method costs something even when it does nothing](sql/varka/skills/the-jit.md#calling-into-an-uncompilable-method-costs-something-even-when-it-does-nothing)
* [A hand-written comparison kernel needs every fast path the real one has](sql/varka/skills/the-jit.md#a-hand-written-comparison-kernel-needs-every-fast-path-the-real-one-has)

#### [The Vector API and vector width](sql/varka/skills/vector-api-and-width.md)
* [Vector API on HotSpot, Measured (JDK 25, x86-64)](sql/varka/skills/vector-api-and-width.md#vector-api-on-hotspot-measured-jdk-25-x86-64)
* [Every operator the plans rely on is one instruction; two species in one JVM is a box per iteration](sql/varka/skills/vector-api-and-width.md#every-operator-the-plans-rely-on-is-one-instruction-two-species-in-one-jvm-is-a-box-per-iteration)
* [An overflow check is nearly free in wide lanes and expensive in narrow ones](sql/varka/skills/vector-api-and-width.md#an-overflow-check-is-nearly-free-in-wide-lanes-and-expensive-in-narrow-ones)
* [A local nothing reads is not free, and its cost is width-dependent](sql/varka/skills/vector-api-and-width.md#a-local-nothing-reads-is-not-free-and-its-cost-is-width-dependent)
* [This machine's AVX-512 is 256 bits wide, and every "512-bit" number in this repo is really a 256-bit one](sql/varka/skills/vector-api-and-width.md#this-machines-avx-512-is-256-bits-wide-and-every-512-bit-number-in-this-repo-is-really-a-256-bit-one)
* [A default that is wrong at one width may be wrong for a reason the width creates](sql/varka/skills/vector-api-and-width.md#a-default-that-is-wrong-at-one-width-may-be-wrong-for-a-reason-the-width-creates)

#### [Working in this repository](sql/varka/skills/working-in-this-repo.md)
* [Repo Workflow (vecbricks/varka)](sql/varka/skills/working-in-this-repo.md#repo-workflow-vecbricksvarka)
* [A recipe for a cheap agent ages at the rate of the emitter, not of the arithmetic](sql/varka/skills/working-in-this-repo.md#a-recipe-for-a-cheap-agent-ages-at-the-rate-of-the-emitter-not-of-the-arithmetic)
* [A value that depends on repo state is queried, not chosen](sql/varka/skills/working-in-this-repo.md#a-value-that-depends-on-repo-state-is-queried-not-chosen)
* [A red Build here may be failing on code this repository does not contain](sql/varka/skills/working-in-this-repo.md#a-red-build-here-may-be-failing-on-code-this-repository-does-not-contain)

<!-- END generated contents -->
