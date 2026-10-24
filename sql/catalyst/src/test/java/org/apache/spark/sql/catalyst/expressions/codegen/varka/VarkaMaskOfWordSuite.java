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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.CodeElement;
import java.lang.classfile.Instruction;
import java.lang.classfile.MethodModel;
import java.lang.classfile.Opcode;
import java.lang.classfile.instruction.ConstantInstruction;
import java.lang.classfile.instruction.InvokeInstruction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import scala.jdk.javaapi.CollectionConverters;

/**
 * No emitted kernel turns a computed word into a mask with {@code VectorMask.fromLong}
 * (VARKA-491). JDK 25's C2 miscompiles {@code fromLong(s, m.toLong())} on x86 without AVX-512
 * mask registers (VARKA-396), and a masked body's known-true word is exactly that round trip when
 * its columns have no nulls; the emitter builds every such mask with {@code VarkaMaskOps}'s calls
 * instead. This suite holds the emitted bytes to that, so a new site cannot bring the
 * round trip back: over every coverage row's kernel, at the defaults and under each configuration
 * of the option matrix, each {@code fromLong} must take a constant, a bitmap read through
 * {@code VarkaVectorSupport}, or the int mask of a kernel that converts between lanes (whose lane
 * counts differ, which the rule requires to be equal). Anything else fails, naming the
 * configuration, the row and the method.
 */
@VarkaSuite
public final class VarkaMaskOfWordSuite {

  private static final String VECTOR_MASK = "jdk/incubator/vector/VectorMask";
  private static final String SUPPORT = "org/apache/spark/sql/varka/vector/VarkaVectorSupport";
  private static final AtomicInteger CLASSES = new AtomicInteger();

  /** The coverage rows' kernels as the compiler builds them, with their names. */
  private static List<VarkaWidthAuditShapes.Shape> shapes() {
    String home = System.getProperty("spark.test.home", System.getenv("SPARK_HOME"));
    assertTrue(home != null, "spark.test.home or SPARK_HOME is not set.");
    var out = new ArrayList<VarkaWidthAuditShapes.Shape>();
    for (var e : CollectionConverters.asJava(
        VarkaWidthAuditShapes.all(new File(home, "sql/varka/coverage.json")))) {
      if (e instanceof VarkaWidthAuditShapes.Shape s) {
        out.add(s);
      }
    }
    return out;
  }

  /**
   * The {@code fromLong} calls of {@code bytes} whose argument is computed, as
   * {@code method: the instruction before it}.
   */
  static List<String> computedFromLongs(byte[] bytes) {
    var found = new ArrayList<String>();
    ClassModel model = ClassFile.of().parse(bytes);
    for (MethodModel m : model.methods()) {
      var code = m.code();
      if (code.isEmpty()) {
        continue;
      }
      // The two instructions before each call: a species load sits between the argument and
      // the call only in emitToDrivingLane's shuffle (toLong, species, dup_x2, pop).
      Instruction before = null;
      Instruction beforeThat = null;
      for (CodeElement el : code.get()) {
        if (!(el instanceof Instruction ins)) {
          continue;
        }
        if (ins instanceof InvokeInstruction call && call.opcode() == Opcode.INVOKESTATIC
            && call.owner().asInternalName().equals(VECTOR_MASK)
            && call.name().stringValue().equals("fromLong")
            && !argumentIsAllowed(before, beforeThat)) {
          found.add(m.methodName().stringValue() + ": after " + before);
        }
        beforeThat = before;
        before = ins;
      }
    }
    return found;
  }

  private static boolean argumentIsAllowed(Instruction before, Instruction beforeThat) {
    if (before instanceof ConstantInstruction) {
      return true;
    }
    if (before instanceof InvokeInstruction call && call.opcode() == Opcode.INVOKESTATIC
        && call.owner().asInternalName().equals(SUPPORT)) {
      return true;
    }
    // emitToDrivingLane: [int mask] toLong, species, dup_x2, pop, fromLong at the long lane.
    return before != null && before.opcode() == Opcode.POP && beforeThat != null
        && beforeThat.opcode() == Opcode.DUP_X2;
  }

  /**
   * The computed {@code fromLong} calls of every shape's kernel under {@code options}, each as
   * {@code where: row - method: instruction}, counting the kernels emitted into {@code scanned}.
   * A shape the options refuse a plan for, or make the emitter decline, has no kernel to scan.
   */
  private static List<String> scan(List<VarkaWidthAuditShapes.Shape> shapes,
      VarkaEmitOptions options, String where, AtomicInteger scanned) {
    var wrong = new ArrayList<String>();
    for (var s : shapes) {
      var roots = CollectionConverters.asJava(s.roots());
      if (VarkaKernelPlan.of(roots, options).refusal().isPresent()) {
        continue;
      }
      byte[] bytes;
      try {
        bytes = VarkaLoopEmitter.emit("org.apache.spark.sql.varka.execution.VarkaMaskScan"
                + CLASSES.incrementAndGet(), roots, s.numInputs(), s.numLiterals(), null, null,
            options);
      } catch (VarkaEmitDeclined d) {
        continue;
      }
      scanned.incrementAndGet();
      for (String site : computedFromLongs(bytes)) {
        wrong.add(where + ": " + s.name() + " - " + site);
      }
    }
    return wrong;
  }

  private static void assertNone(List<String> wrong) {
    assertTrue(wrong.isEmpty(), "fromLong of a computed word, which JDK 25 miscompiles on AVX2 "
        + "(VARKA-396):\n  " + String.join("\n  ", wrong));
  }

  @Test
  @Tag(VarkaSuite.PINS_DEFAULTS)
  @DisplayName("no coverage row's kernel turns a computed word into a mask with fromLong")
  void noComputedFromLong() {
    var scanned = new AtomicInteger();
    var wrong = scan(shapes(), VarkaEmitOptions.DEFAULTS, "defaults", scanned);
    assertTrue(scanned.get() > 200, "only " + scanned + " coverage rows were scanned");
    assertNone(wrong);
  }

  /**
   * The same over each configuration of the option matrix, which runs its suites on AVX2 hosts
   * too: so the sites of VARKA-491's table are every site an option can reach, not only the
   * defaults'. The batch-size axis changes no emit option and is left out.
   */
  @Test
  @DisplayName("no option matrix configuration's kernel turns a computed word into a mask with "
      + "fromLong")
  void noComputedFromLongInTheMatrix() {
    var shapes = shapes();
    var scanned = new AtomicInteger();
    var wrong = new ArrayList<String>();
    for (String config : VarkaMatrixRuns.configurations()) {
      if (!config.startsWith(VarkaMatrixRuns.ARROW_BATCH_SIZE)) {
        wrong.addAll(scan(shapes, VarkaMatrixRuns.parse(config), config, scanned));
      }
    }
    assertTrue(scanned.get() > 200 * 30, "only " + scanned + " kernels were scanned");
    assertNone(wrong);
  }

  @Test
  @DisplayName("the scan finds the round trip under fromLongMasks, so it is not vacuous")
  void scanFindsTheRoundTrip() {
    var options = VarkaEmitOptions.DEFAULTS.toBuilder().fromLongMasks(true).build();
    var found = scan(shapes(), options, "fromLongMasks", new AtomicInteger());
    assertTrue(!found.isEmpty(), "no computed fromLong found even with fromLongMasks on");
  }
}
