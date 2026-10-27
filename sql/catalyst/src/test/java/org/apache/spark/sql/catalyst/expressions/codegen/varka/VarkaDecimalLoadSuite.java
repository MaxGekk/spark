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
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.DecimalColumn;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntArith;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IntOp;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Overflow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;

/**
 * The slot table of the decimal load (VARKA-347 3 item 1): a {@link DecimalColumn} kernel run
 * over Arrow decimal vectors whose sixteen-byte slots are written directly, so that a slot can
 * hold what Spark's own writer never writes - a high word that is not its low word's sign, a value
 * with more digits than the precision, garbage under a null.
 *
 * <p>The oracle is Spark's column reader: the same vector wrapped in {@link ArrowColumnVector},
 * whose decimal accessor raises on a valid slot that holds no value of the type and never reads a
 * null one. The kernel must decline the batch with {@code STATUS_DECIMAL_SLOT} exactly where some
 * valid row raises there, and otherwise answer each valid row's unscaled value.
 *
 * <p>Every case runs at two, four and eight long lanes, in the dense body (no null) and the masked
 * one (nulls in every group), with the planted slot in the first lane group, a middle one and the
 * epilogue, at precision 1, 9, 10 and 18. In a JVM of its own: the widths are three species of the
 * long lane, which the shared JVM refuses (VARKA-246).
 */
@VarkaSuite(ownJvm = true)
public final class VarkaDecimalLoadSuite {

  private static final int[] WIDTHS = {2, 4, 8};
  private static final int[] PRECISIONS = {1, 9, 10, 18};
  /**
   * Batches shorter than one lane group at every width but the narrowest, so no loop runs and the
   * epilogue's second vector is all masked off; one whose last row is in the epilogue at every
   * width; and one with no epilogue.
   */
  private static final int[] LENGTHS = {1, 3, 37, 64};
  private static final int SCALE = 0;

  /** One slot as Arrow holds it: the low word, then the high word. */
  private record Slot(long lo, long hi) {
    static Slot of(long value) {
      return new Slot(value, value >> 63);
    }
  }

  /** What a batch holds: each row's slot and whether it is null. */
  private record Batch(Slot[] slots, boolean[] nulls) {
    int length() {
      return slots.length;
    }
  }

  private static long power(int precision) {
    long p = 1L;
    for (int i = 0; i < precision; i++) {
      p *= 10L;
    }
    return p;
  }

  /**
   * A batch of in-precision slots, with nulls on every third row when {@code masked}, each null
   * holding garbage: a random low word and a random or all-ones high word.
   */
  private static Batch batch(int length, int precision, boolean masked, long seed) {
    var random = new Random(seed);
    long bound = power(precision) - 1;
    var slots = new Slot[length];
    var nulls = new boolean[length];
    for (int i = 0; i < length; i++) {
      nulls[i] = masked && i % 3 == 1;
      if (nulls[i]) {
        slots[i] = new Slot(random.nextLong(), i % 2 == 0 ? -1L : random.nextLong());
      } else {
        slots[i] = Slot.of(Math.floorMod(random.nextLong(), 2 * bound + 1) - bound);
      }
    }
    return new Batch(slots, nulls);
  }

  /** The rows a planted slot is tried at: the first group, a middle one and the last row. */
  private static int[] positions(int length) {
    return java.util.stream.IntStream.of(0, length / 2, length - 1).distinct().toArray();
  }

  /** The slots each of which must decline the batch where it is valid. */
  private static List<Slot> declining(int precision) {
    long p = power(precision);
    return List.of(Slot.of(p), Slot.of(-p),
        // A wrong high word: zero under a negative low word, minus one under a positive one, one.
        new Slot(-5L, 0L), new Slot(5L, -1L), new Slot(5L, 1L), new Slot(0L, 1L),
        new Slot(Long.MIN_VALUE, 0L), new Slot(Long.MAX_VALUE, -1L));
  }

  /** The slots at the bounds, which pass. */
  private static List<Slot> passing(int precision) {
    long bound = power(precision) - 1;
    return List.of(Slot.of(bound), Slot.of(-bound), Slot.of(0L), Slot.of(-1L));
  }

  /** Spark's reading of a batch: each valid row's unscaled value, or that some row raised. */
  private record SparkReading(long[] unscaled, boolean raised) { }

  /** The vector of a batch, its slots written word by word and its nulls marked. */
  private static DecimalVector vector(Batch b, int precision, BufferAllocator allocator) {
    var v = new DecimalVector("d", allocator, precision, SCALE);
    v.allocateNew(b.length());
    for (int i = 0; i < b.length(); i++) {
      v.getDataBuffer().setLong(i * 16L, b.slots()[i].lo());
      v.getDataBuffer().setLong(i * 16L + 8, b.slots()[i].hi());
      if (b.nulls()[i]) {
        v.setNull(i);
      } else {
        v.setIndexDefined(i);
      }
    }
    v.setValueCount(b.length());
    return v;
  }

  private static SparkReading spark(DecimalVector v, int precision) {
    var column = new ArrowColumnVector(v);
    long[] unscaled = new long[v.getValueCount()];
    boolean raised = false;
    for (int i = 0; i < unscaled.length; i++) {
      if (column.isNullAt(i)) {
        continue;
      }
      try {
        Decimal d = column.getDecimal(i, precision, SCALE);
        unscaled[i] = d.toUnscaledLong();
      } catch (ArithmeticException e) {
        raised = true;
      }
    }
    return new SparkReading(unscaled, raised);
  }

  /** The kernel's answer: its status, then each row's value and validity. */
  private record KernelAnswer(int status, long[] values, boolean[] valid) { }

  private static KernelAnswer run(VarkaFusedKernel kernel, DecimalVector v, long[] literals) {
    int length = v.getValueCount();
    int nulls = v.getNullCount();
    long[] srcData = {v.getDataBuffer().memoryAddress()};
    long[] srcValidity = {nulls == length ? 0L : v.getValidityBuffer().memoryAddress()};
    int[] nullCounts = {nulls};
    try (var arena = Arena.ofConfined()) {
      MemorySegment out = arena.allocate(length * 8L, 64);
      MemorySegment outValid = arena.allocate(((length + 63) / 64) * 8L, 64);
      long[] outData = {out.address()};
      long[] outValidity = {outValid.address()};
      int status = VarkaKernelChecks.sanitized(kernel, length, new long[] {16L},
          new long[] {8L}, srcData, srcValidity, outData, outValidity,
          scratch -> kernel.run(srcData, srcValidity, nullCounts, outData, outValidity,
              new int[0], literals, length, scratch));
      long[] values = new long[length];
      boolean[] valid = new boolean[length];
      for (int i = 0; i < length; i++) {
        values[i] = out.get(ValueLayout.JAVA_LONG, i * 8L);
        valid[i] = (outValid.get(ValueLayout.JAVA_LONG, (i >>> 6) * 8L) >>> (i & 63) & 1L) != 0;
      }
      return new KernelAnswer(status, values, valid);
    }
  }

  /** {@code roots} over one decimal column at {@code lanes} long lanes, loaded. */
  private static VarkaEmitterFixtures.Loaded kernel(List<VarkaVectorIR> roots, int literals,
      int lanes) {
    var options = VarkaMatrixRuns.BASE.withLanesOverride(lanes);
    var emitted = VarkaEmitterFixtures.emit(roots, 1, literals, options);
    List<String> errors = VarkaEmitterTestSupport.verify(emitted.bytes());
    assertTrue(errors.isEmpty(), () -> "the class does not verify: " + errors);
    return VarkaEmitterFixtures.load(emitted);
  }

  /**
   * Runs the kernel of {@code root} (a function {@code f} of the unscaled value) over {@code b}
   * and holds it to Spark's reading; returns whether the batch declined.
   */
  private static boolean check(String context, VarkaFusedKernel kernel, Batch b, int precision,
      long[] literals, java.util.function.LongUnaryOperator f) {
    try (BufferAllocator allocator =
        ArrowUtils.rootAllocator().newChildAllocator("varka-decimal-load", 0, Long.MAX_VALUE);
        DecimalVector v = vector(b, precision, allocator)) {
      SparkReading expected = spark(v, precision);
      KernelAnswer got = run(kernel, v, literals);
      if (expected.raised()) {
        assertEquals(VarkaFusedKernel.STATUS_DECIMAL_SLOT, got.status(),
            () -> context + ": Spark's reader raises, so the kernel must decline");
        return true;
      }
      assertEquals(0, got.status(), () -> context + ": Spark's reader reads every row");
      for (int i = 0; i < b.length(); i++) {
        int row = i;
        assertEquals(!b.nulls()[i], got.valid()[i], () -> context + ": validity of row " + row);
        if (!b.nulls()[i]) {
          assertEquals(f.applyAsLong(expected.unscaled()[i]), got.values()[i],
              () -> context + ": row " + row);
        }
      }
      return false;
    }
  }

  /**
   * The slot table over {@code roots}: an in-precision batch, the bounds, garbage under nulls, and
   * every declining slot planted at each position, valid (declines) and null (does not).
   */
  private static void slotTable(String name, java.util.function.IntFunction<List<VarkaVectorIR>>
      roots, int literals, long[] literalValues, java.util.function.LongUnaryOperator f) {
    int declined = 0;
    for (int lanes : WIDTHS) {
      for (int precision : PRECISIONS) {
        var loaded = kernel(roots.apply(precision), literals, lanes);
        VarkaFusedKernel k = loaded.kernel();
        for (int length : LENGTHS) {
          for (boolean masked : new boolean[] {false, true}) {
            String at = name + " at " + lanes + " long lanes, precision " + precision
                + ", length " + length + (masked ? ", with nulls" : ", no null");
            long seed = 31L * lanes + 7L * precision + length + (masked ? 1 : 0);
            assertTrue(!check(at, k, batch(length, precision, masked, seed), precision,
                literalValues, f), at + ": an in-precision batch must not decline");
            for (int row : positions(length)) {
              for (Slot slot : passing(precision)) {
                Batch b = batch(length, precision, masked, seed);
                b.slots()[row] = slot;
                b.nulls()[row] = false;
                assertTrue(!check(at + ", " + slot + " at row " + row, k, b, precision,
                    literalValues, f), at + ": a slot at the bound passes");
              }
              for (Slot slot : declining(precision)) {
                Batch b = batch(length, precision, masked, seed);
                b.slots()[row] = slot;
                b.nulls()[row] = false;
                String context = at + ", " + slot + " at row " + row;
                assertTrue(check(context, k, b, precision, literalValues, f),
                    context + ": Spark's reader must raise on the planted slot");
                declined++;
                // The same slot under a null is never read, by Spark or by the kernel.
                b.nulls()[row] = true;
                assertTrue(!check(context + " under a null", k, b, precision, literalValues, f),
                    context + ": a null slot must not decline");
              }
            }
          }
        }
      }
    }
    assertTrue(declined > 0, "the table planted no declining slot");
  }

  @Test
  @DisplayName("a decimal column's slots load as their low words, and a valid slot Spark's reader "
      + "raises on declines the batch")
  void slotTableOfTheLoad() {
    slotTable("d", precision -> List.of(new DecimalColumn(0, LaneType.LONG, precision)), 0,
        new long[0], x -> x);
  }

  @Test
  @DisplayName("the loaded value is an operand like a long column's, its check kept")
  void slotTableUnderArithmetic() {
    slotTable("d + 3", precision -> List.of(new IntArith(IntOp.ADD, Overflow.WRAP,
        new DecimalColumn(0, LaneType.LONG, precision), new LiteralSlot(0, LaneType.LONG))), 1,
        new long[] {3L}, x -> x + 3);
  }

  @Test
  @DisplayName("a leaf read twice is loaded and checked once, through its shared local")
  void slotTableReadTwice() {
    slotTable("d + d", precision -> {
      var d = new DecimalColumn(0, LaneType.LONG, precision);
      return List.of(new IntArith(IntOp.ADD, Overflow.WRAP, d, d));
    }, 0, new long[0], x -> x + x);
  }

  @Test
  @DisplayName("an all-null batch reads no slot and does not decline")
  void allNull() {
    for (int lanes : WIDTHS) {
      var k = kernel(List.of(new DecimalColumn(0, LaneType.LONG, 9)), 0, lanes).kernel();
      Slot[] slots = new Slot[37];
      boolean[] nulls = new boolean[37];
      for (int i = 0; i < slots.length; i++) {
        slots[i] = new Slot(power(9), 1L);
        nulls[i] = true;
      }
      assertTrue(!check("all null at " + lanes + " lanes", k, new Batch(slots, nulls), 9,
          new long[0], x -> x));
    }
  }

  @Test
  @DisplayName("a decimal column renders apart from a long column of its ordinal, and by its "
      + "precision")
  void shapeKey() {
    String dec9 = VarkaVectorIR.canonical(new DecimalColumn(0, LaneType.LONG, 9));
    String dec18 = VarkaVectorIR.canonical(new DecimalColumn(0, LaneType.LONG, 18));
    String bigint = VarkaVectorIR.canonical(new ColumnRef(0, LaneType.LONG));
    assertEquals("dec:0:long:p9", dec9);
    assertNotEquals(dec9, dec18);
    assertNotEquals(dec9, bigint);
  }

  @Test
  @DisplayName("a decimal column is refused off the long lane, past precision 18, and beside a "
      + "long read of the same column")
  void refusals() {
    assertThrows(IllegalArgumentException.class,
        () -> new DecimalColumn(0, LaneType.INT, 9));
    assertThrows(IllegalArgumentException.class,
        () -> new DecimalColumn(0, LaneType.LONG, 19));
    assertThrows(IllegalArgumentException.class,
        () -> new DecimalColumn(0, LaneType.LONG, 0));
    var both = List.<VarkaVectorIR>of(new DecimalColumn(0, LaneType.LONG, 9),
        new ColumnRef(0, LaneType.LONG));
    try {
      VarkaEmitterFixtures.emit(both, 1, 0, VarkaMatrixRuns.BASE);
      fail("one column read at two strides was emitted");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("both as a decimal and as a long column"),
          e.getMessage());
    }
  }

  /** The bound a precision admits, read back from the leaf. */
  @Test
  @DisplayName("the bound of precision p is 10^p - 1")
  void bounds() {
    var got = new ArrayList<Long>();
    for (int p : new int[] {1, 9, 18}) {
      got.add(new DecimalColumn(0, LaneType.LONG, p).bound());
    }
    assertEquals(List.of(9L, 999_999_999L, 999_999_999_999_999_999L), got);
  }
}
