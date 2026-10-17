/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.codegen.varka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaCompiledCheck.History;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaCompiledCheck.Observed;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaCompiledCheck.Tier;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTierWitness.Verdict;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;

/**
 * The pieces of VARKA-272 that a forked arm's run rests on, tested without a fork: the answers'
 * digest and file ({@link VarkaAnswerLog}), the JVM's facts held to a claimed tier
 * ({@link VarkaPlatformFacts}), and each tier's rules over hand-written verdicts
 * ({@link VarkaCompiledCheck#notReached(Tier, List)}). The rules under real JVMs are the arms'
 * runs, which {@code dev/varka_platform.sh} makes and {@code VARKA-272.md} 9 records.
 */
@VarkaSuite
public final class VarkaPlatformSuite {

  private static final Verdict C2 = new VarkaTierWitness.C2();
  private static final Verdict INTERPRETED =
      new VarkaTierWitness.Mixed(Map.of("loopDense0", 0), Map.of("loopDense0", 0));
  private static final Verdict AT_C1 =
      new VarkaTierWitness.Mixed(Map.of("loopDense0", 1), Map.of("loopDense0", 1));
  private static final Verdict AT_C2_AFTER =
      new VarkaTierWitness.Mixed(Map.of("loopDense0", 0), Map.of("loopDense0", 4));
  private static final Verdict OSR1 = new VarkaTierWitness.Osr("loopDense0", 1);
  private static final Verdict OSR4 = new VarkaTierWitness.Osr("loopDense0", 4);
  private static final Verdict TRAPPED =
      new VarkaTierWitness.Trapped("runMasked", "unloaded", "reinterpret", "K.runMasked");

  /** The facts of a default JVM with the vector flag readable. */
  private static final VarkaPlatformFacts DEFAULT_JVM = new VarkaPlatformFacts(Map.of(
      "UseCompiler", "true", "UseInterpreter", "true", "TieredCompilation", "true",
      "TieredStopAtLevel", "4", "EnableVectorSupport", "true"), "/jdk", "25");

  private static Observed observed(LaneType lane, History h, boolean confirmed,
      boolean compiled, Verdict... verdicts) {
    return new Observed(lane, h, "K", confirmed, List.of(verdicts), List.of(verdicts), compiled,
        "[]");
  }

  @Test
  @DisplayName("a batch's digest changes with one validity bit or one value, and a decline "
      + "digests its status")
  void digest() {
    long a = VarkaAnswerLog.row(VarkaAnswerLog.row(VarkaAnswerLog.START, true, 5L), false, 0L);
    long again = VarkaAnswerLog.row(VarkaAnswerLog.row(VarkaAnswerLog.START, true, 5L), false, 0L);
    assertEquals(a, again);
    long value = VarkaAnswerLog.row(VarkaAnswerLog.row(VarkaAnswerLog.START, true, 6L), false, 0L);
    long bit = VarkaAnswerLog.row(VarkaAnswerLog.row(VarkaAnswerLog.START, true, 5L), true, 0L);
    assertNotEquals(a, value);
    assertNotEquals(a, bit);
    assertNotEquals(VarkaAnswerLog.declined(1), VarkaAnswerLog.declined(2));
    assertNotEquals(a, VarkaAnswerLog.declined(1));
  }

  @Test
  @DisplayName("the answers file holds one line per compare, with the key's repeat count")
  void answersFile(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("answers.tsv");
    try (var log = new VarkaAnswerLog(file)) {
      log.write("seed=1 iteration=0", 1L);
      log.write("seed=1 iteration=1\twith a tab", 2L);
      log.write("seed=1 iteration=0", 3L);
    }
    assertEquals(List.of("1\t1\tseed=1 iteration=0", "2\t1\tseed=1 iteration=1 with a tab",
        "3\t2\tseed=1 iteration=0"), Files.readAllLines(file));
  }

  @Test
  @DisplayName("a tier's contradictions with the JVM's flags are named, and none for the JVM it "
      + "claims")
  void contradictions() {
    assertEquals(List.of(), DEFAULT_JVM.contradictions(Tier.C2, Optional.empty()));
    assertEquals(List.of(), DEFAULT_JVM.contradictions(Tier.C2, Optional.of(true)));
    assertEquals(List.of("UseCompiler=true where the claimed tier needs false"),
        DEFAULT_JVM.contradictions(Tier.INTERPRETER, Optional.empty()));
    assertEquals(List.of("TieredStopAtLevel=4 where the claimed tier needs 1"),
        DEFAULT_JVM.contradictions(Tier.C1, Optional.empty()));
    assertEquals(List.of("UseInterpreter=true where the claimed tier needs false"),
        DEFAULT_JVM.contradictions(Tier.XCOMP, Optional.empty()));
    assertEquals(List.of("EnableVectorSupport=true where the claimed tier needs false"),
        DEFAULT_JVM.contradictions(Tier.C2, Optional.of(false)));
    var locked = new VarkaPlatformFacts(Map.of("UseCompiler", "true", "UseInterpreter", "true",
        "TieredStopAtLevel", "4"), "/jdk", "25");
    List<String> unreadable = locked.contradictions(Tier.C2, Optional.of(true));
    assertEquals(1, unreadable.size());
    assertTrue(unreadable.get(0).contains("-XX:+UnlockExperimentalVMOptions"), unreadable.get(0));
  }

  @Test
  @DisplayName("the JVM at hand reads as the tier it claims, and its facts line names each flag")
  void thisJvm() {
    VarkaPlatformFacts facts = VarkaPlatformFacts.read();
    // A forked arm claims its tier by property, and a developer's JVM may carry its own flags:
    // the test holds the JVM to the claim, the default one where nothing is claimed.
    assertEquals(List.of(), facts.contradictions(Tier.fromProperty(), Optional.empty()));
    String line = facts.line();
    assertTrue(line.startsWith("platform: java.home="), line);
    assertTrue(line.contains(" UseCompiler=") && line.contains(" TieredStopAtLevel="), line);
    assertEquals(List.of("UseCompiler", "UseInterpreter", "TieredCompilation",
        "TieredStopAtLevel"), VarkaPlatformFacts.FLAGS.subList(0, 4));
  }

  @Test
  @DisplayName("a tier is named by its label, and the property's default is C2")
  void tiers() {
    assertEquals(Tier.XCOMP, Tier.named("xcomp"));
    assertEquals(Tier.INTERPRETER, Tier.named("interpreter"));
    assertThrows(IllegalArgumentException.class, () -> Tier.named("c3"));
    if (System.getProperty("varka.fuzz.tier") == null) {
      assertEquals(Tier.C2, Tier.fromProperty());
    }
  }

  @Test
  @DisplayName("the interpreter's rule: every call interpreted and no compile of a kernel class")
  void interpreterRule() {
    var fine = List.of(observed(LaneType.INT, History.COLD, false, false, INTERPRETED));
    assertEquals(List.of(), VarkaCompiledCheck.notReached(Tier.INTERPRETER, fine));
    var served = VarkaCompiledCheck.notReached(Tier.INTERPRETER,
        List.of(observed(LaneType.INT, History.COLD, false, false, C2)));
    assertEquals(1, served.size());
    assertTrue(served.get(0).contains("was not interpreted under the interpreter alone"),
        served.get(0));
    var compiled = VarkaCompiledCheck.notReached(Tier.INTERPRETER,
        List.of(observed(LaneType.LONG, History.COLD, false, true, INTERPRETED)));
    assertEquals(1, compiled.size());
    assertTrue(compiled.get(0).contains("was compiled under the interpreter alone"),
        compiled.get(0));
  }

  @Test
  @DisplayName("C1's rule: cold interpreted, released served by C1's code, no level-4 code")
  void c1Rule() {
    var fine = List.of(
        observed(LaneType.INT, History.COLD, false, false, INTERPRETED),
        observed(LaneType.INT, History.RELEASED, false, true, INTERPRETED, OSR1, AT_C1),
        observed(LaneType.LONG, History.COLD, false, false, INTERPRETED),
        observed(LaneType.LONG, History.RELEASED, false, true, AT_C1));
    assertEquals(List.of(), VarkaCompiledCheck.notReached(Tier.C1, fine));
    var never = VarkaCompiledCheck.notReached(Tier.C1, List.of(
        observed(LaneType.INT, History.RELEASED, false, false, INTERPRETED, INTERPRETED)));
    assertEquals(1, never.size());
    assertTrue(never.get(0).contains("no call of the INT lane was served by C1's code in 2"),
        never.get(0));
    // A level-4 OSR in released, and a cold call compiled at level 4 on its way out: the latter
    // breaks two rules at once, cold's and C1's.
    var level4 = VarkaCompiledCheck.notReached(Tier.C1, List.of(
        observed(LaneType.INT, History.RELEASED, false, true, OSR1, OSR4),
        observed(LaneType.LONG, History.COLD, false, false, AT_C2_AFTER)));
    assertEquals(3, level4.size(), level4.toString());
    assertEquals(2, level4.stream()
        .filter(m -> m.contains("served by level-4 code under C1 alone")).count(),
        level4.toString());
    assertEquals(1, level4.stream()
        .filter(m -> m.contains("cold, a call on K was not interpreted")).count(),
        level4.toString());
  }

  @Test
  @DisplayName("the -Xcomp rule: nothing interpreted at both edges, the warm-ups C2")
  void xcompRule() {
    var fine = List.of(
        observed(LaneType.INT, History.COLD, false, true, TRAPPED, AT_C2_AFTER),
        observed(LaneType.INT, History.WARM_SAME, true, true, C2, C2),
        observed(LaneType.INT, History.RELEASED, false, true, TRAPPED, C2));
    assertEquals(List.of(), VarkaCompiledCheck.notReached(Tier.XCOMP, fine));
    var interpreted = VarkaCompiledCheck.notReached(Tier.XCOMP,
        List.of(observed(LaneType.INT, History.COLD, false, true, INTERPRETED)));
    assertEquals(1, interpreted.size());
    assertTrue(interpreted.get(0).contains("interpreted at both edges under -Xcomp"),
        interpreted.get(0));
    var warm = VarkaCompiledCheck.notReached(Tier.XCOMP,
        List.of(observed(LaneType.LONG, History.WARM_OTHER, true, true, AT_C2_AFTER, C2)));
    assertEquals(1, warm.size());
    assertTrue(warm.get(0).contains("warm-other, no settled case of the LONG lane"), warm.get(0));
  }

  @Test
  @DisplayName("the default rule through the seam: cold interpreted, warm-ups C2, released by OSR")
  void c2Rule() {
    var fine = List.of(
        observed(LaneType.INT, History.COLD, false, false, INTERPRETED),
        observed(LaneType.INT, History.WARM_SAME, true, true, C2),
        observed(LaneType.INT, History.WARM_OTHER, true, true, TRAPPED),
        observed(LaneType.INT, History.WARM_OTHER, true, true, C2),
        observed(LaneType.INT, History.RELEASED, false, true, INTERPRETED, OSR4, C2));
    assertEquals(List.of(), VarkaCompiledCheck.notReached(Tier.C2, fine));
    var noOsr = VarkaCompiledCheck.notReached(Tier.C2, List.of(
        observed(LaneType.LONG, History.RELEASED, false, true, INTERPRETED, OSR1)));
    assertEquals(List.of("history not reached: released, no case of the LONG lane was entered by "
        + "an on-stack replacement in 2 batches"), noOsr);
    // A lane whose warm-ups never settled has not reached the history: a rule that passed for
    // want of a settled case would read an arm with the intrinsics off as proven.
    var unconfirmed = VarkaCompiledCheck.notReached(Tier.C2, List.of(
        observed(LaneType.INT, History.WARM_SAME, false, true, AT_C2_AFTER)));
    assertEquals(1, unconfirmed.size(), unconfirmed.toString());
    assertTrue(unconfirmed.get(0).contains("warm-same, no case of the INT lane settled"),
        unconfirmed.get(0));
    var oneSettled = VarkaCompiledCheck.notReached(Tier.C2, List.of(
        observed(LaneType.INT, History.WARM_SAME, false, true, AT_C2_AFTER),
        observed(LaneType.INT, History.WARM_SAME, true, true, C2)));
    assertEquals(List.of(), oneSettled, "one settled all-C2 case proves the lane");
  }
}
