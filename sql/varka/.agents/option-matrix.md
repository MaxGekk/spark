# The fuzzer and the option matrix

Beside the emitter suite's curated matrices,
`VarkaIrFuzzSuite` runs random IR trees over random columns, null patterns,
lengths and `VarkaEmitOptions` variants against the shared
`VarkaReferenceEvaluator`; a failure names its seed and iteration and replays
with `-Dvarka.fuzz.seed=<seed> -Dvarka.fuzz.only=<iteration>`. A new option is
fuzzed the day it lands: the suite draws from `VarkaEmitOption.TABLE`, and
`VarkaEmitOptionSuite` fails until a new record component has its table entry.
The same entry gives it its configurations in the option matrix
(`dev/varka_matrix.sh`, `sql/varka/plans/m7/VARKA-248.md` 3.2), which reruns the Varka suites
with one option changed at a time. A test starts from `VarkaMatrix.base`, not
`VarkaEmitOptions.DEFAULTS`, so the matrix reaches its kernels; a test whose
subject is the defaults' emitted structure takes the tag `PinsDefaults`; a test
whose subject is what C2 makes of a kernel takes `C2Code`, which runs it only
under a configuration that changes what C2 is given (`VarkaMatrixRuns.C2_OPTIONS`);
and an answer test a configuration breaks by design gets a line in
`sql/varka/matrix/skips.tsv` with its reason. A local `git merge` or rebase,
on a branch that has the rule, merges that file with git's union driver, so two
branches appending skips do not conflict (GitHub's own merge check may not use
it). Union also keeps a line one side removed or edited next to the other's
append, so after such a merge run `dev/varka_lint.py sql/varka/matrix/skips.tsv`,
which reads every line and flags a test named twice.
A Varka pull request's Build runs
the suites at the defaults only: its option configurations are checked by the
`varka-option-matrix` workflow, which runs all of them, dispatched on its branch
on the fork or with `pr` and `sha` on vecbricks/varka. On the fork a configuration
runs a SQL test only where the Build of the same commit recorded that it moves
something the test compiled or emitted (`VarkaTestReach`); a test the record
lacks, or one `skips.tsv` names under the configuration, still runs. A run on
vecbricks/varka reuses no Build, so it has no record and runs every test. An option
read anywhere but the compiler and the emitter - by an evaluator as it runs, say -
changes answers no record sees, so its configuration goes in
`VarkaTestReach.FULL`, with the reason, and runs every test.
