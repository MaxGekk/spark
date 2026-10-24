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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.analysis.Analyzer;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicInterpreter;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.Interpreter;
import org.objectweb.asm.tree.analysis.Value;

import scala.jdk.javaapi.CollectionConverters;

/**
 * No emitted kernel hands {@code VectorMask.fromLong} a word computed from a mask's
 * {@code toLong} (VARKA-491). JDK 25's C2 miscompiles {@code fromLong(s, m.toLong())} on x86
 * without AVX-512 mask registers (VARKA-396), and a masked body's known-true word becomes that
 * round trip once its columns have no nulls, which C2 learns from the profile; the emitter XORs
 * every masked-body word with {@code VarkaOpaqueZero.zero}, a field C2 cannot fold, before
 * {@code fromLong}. This suite holds the emitted bytes to that, so a new site cannot bring the
 * round trip back: over every coverage row's kernel, at the defaults and under each configuration
 * of the option matrix, a data-flow pass marks every long that a {@code toLong} reaches, through
 * locals, arithmetic and branches, and clears the mark where the long is XORed with that field.
 * Each {@code fromLong} must take an unmarked long - a constant, a bitmap read, what {@code ~},
 * {@code &} and {@code |} make of them, or a word XORed with the field - or be the lane change of
 * a kernel that converts between lanes, whose lane counts differ, which the rule requires to be
 * equal. Anything else fails, naming the configuration, the row and the method.
 */
@VarkaSuite
public final class VarkaMaskOfWordSuite {

  private static final String VECTOR_MASK = "jdk/incubator/vector/VectorMask";
  private static final String OPAQUE_ZERO = Type.getInternalName(VarkaOpaqueZero.class);
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
   * A value of the data-flow pass: ASM's basic value, whether a {@code toLong} reaches it, and
   * whether it is {@code VarkaOpaqueZero.zero}.
   */
  private record Marked(BasicValue basic, boolean fromToLong, boolean opaque) implements Value {
    @Override
    public int getSize() {
      return basic.getSize();
    }

    static Marked of(BasicValue basic, boolean fromToLong) {
      return basic == null ? null : new Marked(basic, fromToLong, false);
    }
  }

  /**
   * ASM's basic interpreter, with a mark on every value a {@code VectorMask.toLong} result flows
   * into. Conservative: a unary or binary operation on a marked value is marked, and a merge of
   * two values is marked where either is - except an XOR with {@code VarkaOpaqueZero.zero}, the
   * emitter's guard, whose result C2 cannot see is the word.
   */
  private static final class ToLongFlow extends Interpreter<Marked> {
    private final BasicInterpreter basic = new BasicInterpreter();

    ToLongFlow() {
      super(Opcodes.ASM9);
    }

    @Override
    public Marked newValue(Type type) {
      return Marked.of(basic.newValue(type), false);
    }

    @Override
    public Marked newOperation(AbstractInsnNode insn) throws AnalyzerException {
      BasicValue value = basic.newOperation(insn);
      boolean opaque = insn instanceof FieldInsnNode f && f.getOpcode() == Opcodes.GETSTATIC
          && f.owner.equals(OPAQUE_ZERO) && f.name.equals("zero");
      return value == null ? null : new Marked(value, false, opaque);
    }

    @Override
    public Marked copyOperation(AbstractInsnNode insn, Marked value) throws AnalyzerException {
      BasicValue copy = basic.copyOperation(insn, value.basic());
      return copy == null ? null : new Marked(copy, value.fromToLong(), value.opaque());
    }

    @Override
    public Marked unaryOperation(AbstractInsnNode insn, Marked value) throws AnalyzerException {
      return Marked.of(basic.unaryOperation(insn, value.basic()), value.fromToLong());
    }

    @Override
    public Marked binaryOperation(AbstractInsnNode insn, Marked a, Marked b)
        throws AnalyzerException {
      boolean guarded = insn.getOpcode() == Opcodes.LXOR && (a.opaque() || b.opaque());
      return Marked.of(basic.binaryOperation(insn, a.basic(), b.basic()),
          !guarded && (a.fromToLong() || b.fromToLong()));
    }

    @Override
    public Marked ternaryOperation(AbstractInsnNode insn, Marked a, Marked b, Marked c)
        throws AnalyzerException {
      return Marked.of(basic.ternaryOperation(insn, a.basic(), b.basic(), c.basic()), false);
    }

    @Override
    public Marked naryOperation(AbstractInsnNode insn, List<? extends Marked> values)
        throws AnalyzerException {
      var basics = values.stream().map(Marked::basic).toList();
      return Marked.of(basic.naryOperation(insn, basics), isCall(insn, "toLong"));
    }

    @Override
    public void returnOperation(AbstractInsnNode insn, Marked value, Marked expected)
        throws AnalyzerException {
      basic.returnOperation(insn, value.basic(), expected.basic());
    }

    @Override
    public Marked merge(Marked a, Marked b) {
      BasicValue merged = basic.merge(a.basic(), b.basic());
      boolean fromToLong = a.fromToLong() || b.fromToLong();
      boolean opaque = a.opaque() && b.opaque();
      return merged.equals(a.basic()) && fromToLong == a.fromToLong() && opaque == a.opaque() ? a
          : new Marked(merged, fromToLong, opaque);
    }
  }

  private static boolean isCall(AbstractInsnNode insn, String name) {
    return insn instanceof MethodInsnNode call && call.owner.equals(VECTOR_MASK)
        && call.name.equals(name);
  }

  /** The instruction before {@code insn}, labels, line numbers and frames skipped. */
  private static AbstractInsnNode previous(AbstractInsnNode insn) {
    AbstractInsnNode p = insn.getPrevious();
    while (p != null && p.getOpcode() < 0) {
      p = p.getPrevious();
    }
    return p;
  }

  /**
   * Whether the {@code fromLong} at {@code insn} is emitToDrivingLane's: {@code [int mask]
   * toLong, aload species, dup_x2, pop, fromLong} at the long lane, where the int mask has twice
   * the long species' lanes.
   */
  private static boolean isLaneChange(AbstractInsnNode insn) {
    AbstractInsnNode pop = previous(insn);
    AbstractInsnNode dup = pop == null ? null : previous(pop);
    AbstractInsnNode species = dup == null ? null : previous(dup);
    AbstractInsnNode toLong = species == null ? null : previous(species);
    return pop != null && pop.getOpcode() == Opcodes.POP
        && dup != null && dup.getOpcode() == Opcodes.DUP_X2
        && species != null && species.getOpcode() == Opcodes.ALOAD && isCall(toLong, "toLong");
  }

  /**
   * The {@code fromLong} calls of {@code bytes} whose word a {@code toLong} reaches, as
   * {@code method: instruction index}.
   */
  static List<String> computedFromLongs(byte[] bytes) {
    var found = new ArrayList<String>();
    var node = new ClassNode();
    new ClassReader(bytes).accept(node, ClassReader.SKIP_FRAMES);
    for (MethodNode m : node.methods) {
      if (m.instructions.size() == 0) {
        continue;
      }
      Frame<Marked>[] frames;
      try {
        frames = new Analyzer<>(new ToLongFlow()).analyze(node.name, m);
      } catch (AnalyzerException e) {
        throw new IllegalStateException(m.name + " does not verify: " + e.getMessage(), e);
      }
      for (int i = 0; i < frames.length; i++) {
        AbstractInsnNode insn = m.instructions.get(i);
        Frame<Marked> frame = frames[i];
        if (frame != null && insn.getOpcode() == Opcodes.INVOKESTATIC
            && isCall(insn, "fromLong")
            && frame.getStack(frame.getStackSize() - 1).fromToLong() && !isLaneChange(insn)) {
          found.add(m.name + ": instruction " + i);
        }
      }
    }
    return found;
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
    assertTrue(wrong.isEmpty(), "fromLong of a word a toLong reaches, which JDK 25 miscompiles "
        + "on AVX2 (VARKA-396):\n  " + String.join("\n  ", wrong));
  }

  @Test
  @Tag(VarkaSuite.PINS_DEFAULTS)
  @DisplayName("no coverage row's kernel hands fromLong a word a toLong reaches")
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
  @DisplayName("no option matrix configuration's kernel hands fromLong a word a toLong reaches")
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
  @Tag(VarkaSuite.PINS_DEFAULTS)
  @DisplayName("the opaque zero every masked-body word is XORed with is zero")
  void opaqueZeroIsZero() {
    // Public and mutable so that no JIT folds it; a write anywhere would corrupt every mask.
    assertTrue(VarkaOpaqueZero.zero == 0L, "VarkaOpaqueZero.zero is " + VarkaOpaqueZero.zero);
  }

  @Test
  @DisplayName("the scan finds the round trip under fromLongMasks, so it is not vacuous")
  void scanFindsTheRoundTrip() {
    var options = VarkaEmitOptions.DEFAULTS.toBuilder().fromLongMasks(true).build();
    var found = scan(shapes(), options, "fromLongMasks", new AtomicInteger());
    assertTrue(!found.isEmpty(), "no computed fromLong found even with fromLongMasks on");
  }
}
