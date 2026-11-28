# The checks before a commit and a push

Before committing, `dev/varka_precommit.sh` (or install it once with
`--install-hook`) checks the staged files for the rules that slip most: a
non-ASCII byte outside a string literal, a source line over 100 columns, a
`TODO`/`FIXME` marker under a Varka directory, a comment that narrates history
(`used to`, `the old`, `since VARKA-<n>`) in Varka's code, the quote check when a
document changed, `ruff check` plus `ruff format --check` on Python files
(CI's Python linter runs both), and the rules of `dev/varka_lint.py`: a license
header on a new source file, a third-party import its module never had (Maven may
lack what sbt resolves transitively), a new file no CI module claims, the skip
list's integrity, `task N` for `VARKA-N`, repeated benchmark case ids, a Varka conf
without a binding policy, two shell traps and Guava in Varka code. The hook also
installs a commit-msg check of the attribution line and a pre-push check that
refuses a branch pushed to vecbricks/varka and re-checks what is pushed; check a
pull request's title and body with `dev/varka_msg_check.py pr --title ... --body-file
...` before opening it (VARKA-492).

The laptop is AVX-512 and CI's runners are AVX2, so before pushing a change
to what the emitter writes, run the width audit at the preferred width once more under
`JDK_JAVA_OPTIONS=-XX:UseAVX=2` (a `--jvm-arg` does not reach the audit's probe JVM):
masked `BYTE` and `SHORT` loads and stores, and masks at two 64-bit lanes, have no
lowering there, and the laptop cannot show it otherwise.
