# Trap-count reset spike (VARKA-370)

Can a running JVM reset or avoid the MethodData trap and decompile counts that make C2 compile the
class-check misses of the Vector API's inlined templates as virtual calls?
`sql/varka/plans/m7/VARKA-370.md` section 9 has the answer and quotes the numbers in `results/`.
None of this is in the build: no sbt or Maven source set reads this folder.

## What is here

| file | what |
|---|---|
| `src/Agent370.java` | a Java agent the JVM loads into itself through its `DiagnosticCommand` MBean (`jvmtiAgentLoad`), and the redefinition of every loaded, non-hidden `jdk.incubator.vector` class with its own bytes from `jrt:/` |
| `src/Counts370.java` | reads each method's trap and decompile counts through WhiteBox |
| `src/Probe370.java` | the standalone probe of 9.1: pollutes `IntVector.add`, then resets (`redefine`, `clear` or `none`) and measures a loop's allocation |
| `src/Replay370.java` | wraps ScalaTest's `Runner` for the replay of 9.3: loads the agent, does the arm's action (`-Dr370.mode`), prints the counts every 30 s |
| `src/MANIFEST.txt` | the agent jar's manifest (`Agent-Class`, `Can-Redefine-Classes`) |
| `build.sh` | builds the WhiteBox jar from the JDK source tree's test library and the spike's jar |
| `run.sh` | one replay run on cores 0 to 2 |
| `results/*.txt` | per arm: each run's command, exit, seconds, load before and after, verdict line, the iterations released at the deadline and every `R370` line; `probe.txt` is the probe's output |

The per-method trapped list (`R370 trapped`) was added to `Replay370` after runs `ctl1`, `flags1`
and `redef1`, so those three runs do not print it; the committed source is the later version.

## Commands

    out=/some/scratch/dir
    sql/varka/spikes/trap-reset/build.sh $out              # JDK source tree as second argument
    taskset -c 0-2 dev/varka_matrix.sh --module sql --list  # the classpath and JVM options

    # the probe
    taskset -c 0-2 java --add-modules jdk.incubator.vector -XX:+UnlockDiagnosticVMOptions \
      -XX:+WhiteBoxAPI -Xbootclasspath/a:$out/wb.jar -cp $out/probe.jar Probe370 \
      $out/probe.jar redefine        # or clear, none

    # the replay arms (ITER=100 and -Dvarka.matrix.config=lanesOverride=4 for the 4-lane rows)
    sql/varka/spikes/trap-reset/run.sh $out ctl1 none
    sql/varka/spikes/trap-reset/run.sh $out flags1 flags
    sql/varka/spikes/trap-reset/run.sh $out redef1 redefine -Dr370.once=true -Dr370.firstMs=60000
    sql/varka/spikes/trap-reset/run.sh $out clear1 clear -Dr370.once=true -Dr370.firstMs=60000
    grep 'warm-up verdicts' $out/*/run.log

Every arm, the control included, runs with `-XX:+UnlockDiagnosticVMOptions -XX:+WhiteBoxAPI` and
the WhiteBox jar on the boot class path, and loads the agent, so the arms differ only in the
action. Only the agent route needs no startup option; WhiteBox is there to set flags, clear
methods and read counts.
