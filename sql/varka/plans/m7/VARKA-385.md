# VARKA-385: The Varka engine's Maven plugins from the cache

## 1. Where this came from

Milestone 7's debt register (`PLAN.md` section 10, "In CI and tooling", the second entry) and row
385. CI's step "Install the Varka engine into the local Maven repository" runs
`./build/mvn -B -f sql/varka/engine/pom.xml ... install` with no local Maven repository to start
from, so Maven downloads its plugins and the engine's dependencies from Maven Central on every
run. On 16 October 2026 a transient miss of `org.apache.maven.plugins:maven-resources-plugin:3.4.0`
("could not be resolved ... Could not transfer artifact") failed it twice: in the "Linters,
licenses, and dependencies" job of #888's run and in the mllib module job of #889's run. Each cost
a rerun of a full CI run.

## 2. The admission check, done

**Which jobs install the engine.** All of them go through the composite action
`.github/actions/install-varka-engine`, so one edit there covers every caller. 18 jobs in five
workflows (listed by parsing the workflow files on master, f0ac48b6d69):

| workflow | jobs | local Maven repository before the step |
|---|---|---|
| `build_and_test.yml` | `precompile`, `build` (the module matrix), `pyspark`, `sparkr`, `lint`, `docs`, `tpcds-1g`, `docker-integration-tests`, `k8s-integration-tests` | none: they call `setup-build-env` without `cache-maven-local` |
| `build_and_test.yml` | `java25`, `varka-engine` (two architectures, `run-tests: true`) | none: they call `actions/setup-java` directly, no cache step at all |
| `python_hosted_runner_test.yml` | `precompile`, `build` | none, as above |
| `varka-fuzz.yml`, `varka-option-matrix.yml` | `fuzz`, `series`, `matrix` | none: their own cache steps cover `build/`, `~/.sbt` and Coursier |
| `maven_test.yml` | `precompile-maven`, `build` | Spark's own, `maven-<os>-java<ver>-<hash(**/pom.xml)>`, restored by `setup-build-env` with `cache-maven-local: 'true'` |

**Why the cache is skipped.** The row's wording needs a correction: the step that is skipped is
not "Cache SBT and Maven" - that one runs in every `setup-build-env` caller and caches Maven's
*launcher* (`build/apache-maven-*`), `build/*.jar` and `~/.sbt`. The `~/.m2/repository` cache is
the step "Cache Maven local repository", guarded by `if: inputs.cache-maven-local == 'true'`; the
input defaults to `'false'` and only `maven_test.yml` sets it, because Spark's sbt jobs resolve
through Coursier and never needed a Maven repository until the engine install arrived.

**Reusing Spark's key would not be warm.** The cache listings of both repositories
(`gh api repos/<repo>/actions/caches`, 16 October 2026: 36 entries on `MaxGekk/spark`, 80 on
`vecbricks/varka`) hold no `maven-Linux-*` entry at all - `maven_test.yml` does not run on either.
Worse, saving the engine's 35 MB under Spark's exact key would stop `maven_test.yml` ever saving
its full repository there, since a cache key is never overwritten. And pinning the engine's
plugins to Spark's versions shares nothing either: the warm caches in these jobs are Coursier's,
a store Maven does not read.

**What the engine build resolves.** `./build/mvn -B -f sql/varka/engine/pom.xml
-Dmaven.test.skip=true install` into a fresh empty `-Dmaven.repo.local` (Maven 3.9.16, JDK 25.0.4)
downloaded 301 files, 35 MB; with the tests run, 316 files, 36 MB - the extra 15 are surefire's
JUnit Platform provider (`surefire-junit-platform:3.2.5` and its dependencies), which is resolved
only when tests actually execute. The plugins:

| plugin | version | where the version came from |
|---|---|---|
| `maven-resources-plugin` | 3.4.0 | Maven 3.9.16's default lifecycle bindings (unpinned) |
| `maven-compiler-plugin` | 3.13.0 | the engine pom |
| `build-helper-maven-plugin` | 3.6.1 | the engine pom |
| `maven-surefire-plugin` | 3.2.5 | the engine pom (`maven.surefire.version`) |
| `maven-jar-plugin` | 3.5.0 | Maven 3.9.16's default lifecycle bindings (unpinned) |
| `maven-install-plugin` | 3.1.4 | Maven 3.9.16's default lifecycle bindings (unpinned) |

The unpinned three take their versions from the Maven that `build/mvn` downloads, which is the
root pom's `maven.version`. So the engine pom alone does not determine what the build resolves,
and a cache keyed on its hash could go stale on a Maven bump. (Spark's root pom pins jar 3.5.0 and
install 3.1.4 too, compiler 3.14.1 and surefire 3.5.3; resources it leaves to its Apache parent.)

**Offline works from a filled repository.** With the 35 MB repository from the first run, the
same install with `-o` succeeds and downloads nothing; with `-DskipTests` too. With the tests run,
`-o` over the skip-tests repository fails on `surefire-junit-platform:3.2.5`, so the two modes need
separate cache entries.

**Downloads are not in CI's log.** `actions/setup-java` sets `MAVEN_ARGS=-ntp` ("Configured
MAVEN_ARGS to include -ntp to suppress Maven transfer progress logs", lint job of run 38962144218),
so a CI log never lists Maven's downloads, cold or warm. That decides the evidence: an install that
succeeds with `-o` has downloaded nothing by construction, since offline Maven fails on any
artifact it does not have.

What the check would have rejected: the plan of reusing Spark's cache key (no such cache exists,
and sharing it would break `maven_test.yml`), and pinning to Spark's versions as a cache measure.

## 3. The design

### 3.1 A small cache of the engine's own, and an offline install on an exact hit

In `.github/actions/install-varka-engine/action.yml`, around the install:

1. **Restore** `~/.m2/repository` with `actions/cache/restore@v6` under the key
   `varka-engine-m2-v1-<os>-<build|test>-<hash(sql/varka/engine/pom.xml)>`. `build` or `test` is
   the action's `run-tests` input (section 2: the modes resolve different sets); the test mode
   falls back to the build entry of the same pom hash, which lacks only surefire's provider. There
   is no fallback to an older pom's entry: it would carry every superseded version forward into
   each new entry, and a cold install is 13 seconds. `v1` is there to discard a bad entry, since a
   cache key is never overwritten.
2. **Install**: on an exact hit, `./build/mvn -o ...`, after the log line `Local Maven repository
   restored from <key>: installing offline (-o)`. If the offline install fails to resolve an
   artifact (Maven's "in offline mode" message), the entry is stale: a `::warning::` annotation
   says so and the install goes online. Any other failure - compile, test - fails the step at once,
   so a flaky engine test is not rerun into a green job. On a miss or a partial hit the install
   runs online after the line `No exact cache hit for <key>: installing online, then saving the
   cache`, and an online attempt that fails with "Could not transfer artifact" - the 16 October
   error - is retried twice, 30 seconds apart, with a warning each time. Maven does not cache
   transfer errors, so each attempt really contacts Central again.
3. **Save** with `actions/cache/save@v6` right after the install, only when the restore was not
   an exact hit. An explicit save step rather than `actions/cache`'s post-job save, because later
   steps add to `~/.m2/repository` (lint's reactor checkstyle, `java25`'s full Maven build) and a
   job-end save would snapshot that under this key, in caches already near their 10 GB cap.

The path stays `~/.m2/repository`: the step exists so that later Maven `-pl` builds and sbt find
the engine jar in the default local repository.

A new input, `cache` (default `'true'`), is set to `'false'` by `maven_test.yml`'s two jobs, which
restore Spark's whole Maven repository into the same directory before the step; a save there would
capture it all under the engine's key. They behave exactly as before.

In `sql/varka/engine/pom.xml`, the three default-lifecycle plugins are pinned at the versions
Maven 3.9.16 already binds (resources 3.4.0, jar 3.5.0, install 3.1.4), so the engine pom alone
fixes every artifact the build resolves and the key can hash that file only. No version changes,
so the build is unchanged.

Because `precompile` saves right after its install and the module jobs `need` it, the first run of
a branch is already warm for every job that starts after `precompile`.

### 3.2 What is deliberately unchanged

* **The engine's own jar is in each entry**, the one built by the commit that saved it. Every
  install overwrites it; leaving it out would need an exclusion pattern under the cached
  directory, which `actions/cache` does not apply reliably to a directory it archives whole.
* **Seeding the default branch.** Caches are scoped to their branch and no `Build` run saves on
  the fork's master, so each new branch starts cold; the transfer retry covers that run.
* **Compiler 3.13.0 and surefire 3.2.5** stay: aligning them to Spark's 3.14.1 and 3.5.3 changes
  the build and buys no cache.
* **`java25` and `varka-engine` have no `build/` launcher cache**, and the lint container missed
  the `build-Linux-*` and `coursier-Linux-*` caches in run 38962144218 and downloaded Maven itself.
  Both are separate from this row; the launcher is fetched with `curl --retry 3` from a mirror.
* **`maven_test.yml`** keeps Spark's own cache and the online install.

### 3.3 Registered op counts

Not applicable: no emitter change. The CI equivalent is the count of downloads in the step, 301
(skip-tests) and 316 (tests) cold, 0 warm.

## 4. Files

| file | what |
|---|---|
| `.github/actions/install-varka-engine/action.yml` | the restore, the offline-first install with its transfer retry, and the save; the `cache` input |
| `.github/workflows/maven_test.yml` | `cache: 'false'` at its two install steps |
| `sql/varka/engine/pom.xml` | resources, jar and install plugins pinned at Maven 3.9.16's defaults |
| `sql/varka/plans/m7/PLAN.md` | row 385's status; section 10's entry rewritten as swept |

## 5. Tests, and what each is for

There is no CI test of a workflow; the checks are local, against the install step's script
extracted from the action file (`run:` of the install step, with `MAVEN_ARGS` pointing
`maven.repo.local` at a scratch directory and `CACHE_HIT`/`CACHE_KEY` set as the restore step
would):

1. Cold, both modes: the script prints the miss line, installs online, `BUILD SUCCESS`.
2. Warm, both modes, over the repository the cold run filled: the script prints the offline line,
   `BUILD SUCCESS`, no download line - the done-when, locally.
3. Stale exact hit: test mode over the skip-tests repository - the offline install fails on
   `surefire-junit-platform`, the warning is printed, the online install succeeds.
4. A compile failure on an exact hit (a temporary broken source file): the step fails after the
   offline attempt alone, with no online rerun.
5. Transfer failure: a settings mirror pointing at a closed local port - three online attempts,
   two warnings, then the step fails.
6. YAML parses; `actionlint` over the workflows gives the same findings as master; `shellcheck`
   on the extracted script is clean.

## 6. The measurement

The first CI run of the pull request, read from its log (section 9 states what it must show).

### 6.1 Predictions, registered before the run

1. The warm install downloads nothing, shown by `-o` succeeding: holds locally (section 5, test 2).
2. The cache entries are about 35 MB (build) and 36 MB (test), the size of the local repositories.
3. The warm step is no slower than the cold one, whose Maven part took 12.8 s in the lint job of
   run 38962144218; a 35 MB restore is a few seconds.

## 7. Risks

1. **The cache in a container job has a different version** (`actions/cache` folds the compression
   method into the version, and an image without `zstd` falls back to gzip), so a container job may
   miss the entry a host job saved. It then saves its own and is warm from its next run; the first
   CI run's log shows which.
2. **An exact hit that lacks an artifact** - only possible if something other than the engine pom
   decides what is resolved. The warning annotation shows it and the online install keeps the job
   green; the entry stays bad until the key's `v1` is bumped, which the warning says.
3. **Concurrent saves of the same key** in one run (the two `varka-engine` architectures, or lint
   and `precompile`): one wins, the others log "Unable to reserve cache" as a warning, harmless.

## 8. Sequencing

Two commits: this plan with the admission check and the row marked planned, then the action,
the pom pins, the `maven_test.yml` input and section 9. The plan and the code were written in one
sitting, after the admission check.

## 9. Outcome

Filled in when the local checks of section 5 and the first CI run land.
