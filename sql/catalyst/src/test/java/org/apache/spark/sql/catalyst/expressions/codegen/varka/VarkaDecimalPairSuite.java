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

import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDecimalStoreTables.label;
import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDecimalValueSuite.compile;
import static org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaDecimalValueSuite.seq;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import scala.runtime.BoxedUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaPredicate;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaConjunctSpec;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.CondValue;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.util.ArrowUtils;

/**
 * The comparisons and null tests of decimals wider than 18 digits (`m8/VARKA-359.md` 3 item 2),
 * and the sums and differences under them and as outputs (`m8/VARKA-360.md` 3 item 1),
 * compiled from SQL by the real compiler, each value read as the two words of its unscaled value
 * ({@code DecimalWords.compare}), and run over Arrow decimal vectors at the word edges: a low
 * word with its sign bit set under a high word of 0 (where a signed low compare is wrong), equal
 * high words with low words either side of {@code 2^63}, a high word of -1, a low word of 0 under
 * each high word, the precision's bounds, and every null pattern of two operands.
 *
 * <p>The oracle is Spark's own expression, evaluated over the same values, three-valued: the
 * kernel's condition is written as its one or zero, null where it is unknown. Every kernel runs
 * at two, four and eight long lanes and at the preferred species, so the edges meet the loop and
 * the epilogue. In a JVM of its own, for the several species (VARKA-246).
 */
@VarkaSuite(ownJvm = true)
public final class VarkaDecimalPairSuite {

  private static final int[] WIDTHS = {1, 2, 4, 8};
  private static final List<String> OPS = List.of("<", "<=", ">", ">=", "=", "<>", "<=>");
  private static final BigInteger TWO_63 = BigInteger.ONE.shiftLeft(63);
  private static final BigInteger TWO_64 = BigInteger.ONE.shiftLeft(64);

  /** A row of the two columns: each one's unscaled value, or null. */
  private record Row(BigInteger a, BigInteger b) { }

  private static BigInteger bound(DecimalType t) {
    return BigInteger.TEN.pow(t.precision()).subtract(BigInteger.ONE);
  }

  /** The word edges a value of {@code t} can hold, each at most {@code 10^p - 1} in magnitude. */
  private static List<BigInteger> edges(DecimalType t) {
    BigInteger one = BigInteger.ONE;
    var all = List.of(BigInteger.ZERO, one, one.negate(), TWO_63.subtract(one), TWO_63,
        TWO_63.add(one), TWO_63.negate(), TWO_63.negate().subtract(one), TWO_64.subtract(one),
        TWO_64, TWO_64.add(TWO_63).subtract(one), TWO_64.add(TWO_63), TWO_64.negate(),
        TWO_64.negate().subtract(one), TWO_64.negate().add(TWO_63), bound(t), bound(t).negate());
    var held = new LinkedHashSet<BigInteger>();
    for (BigInteger v : all) {
      if (v.abs().compareTo(bound(t)) <= 0) {
        held.add(v);
      }
    }
    return List.copyOf(held);
  }

  /** Every ordered pair of the two types' edges, then every null pattern beside a few. */
  private static List<Row> rows(DecimalType ta, DecimalType tb) {
    var rows = new ArrayList<Row>();
    for (BigInteger a : edges(ta)) {
      for (BigInteger b : edges(tb)) {
        rows.add(new Row(a, b));
      }
    }
    BigInteger both = bound(ta).min(bound(tb));
    for (BigInteger v : List.of(BigInteger.ZERO, both, both.negate())) {
      rows.add(new Row(null, v));
      rows.add(new Row(v, null));
    }
    rows.add(new Row(null, null));
    return rows;
  }

  /**
   * {@link #rows} without a null, so that a kernel's null-free (dense) body meets the edges too:
   * every batch of {@link #rows} holds nulls, and takes the masked body.
   */
  private static List<Row> denseRows(DecimalType ta, DecimalType tb) {
    return rows(ta, tb).stream().filter(r -> r.a() != null && r.b() != null).toList();
  }

  private static DecimalVector vector(String name, List<Row> rows, boolean left,
      DecimalType type, BufferAllocator allocator) {
    var v = new DecimalVector(name, allocator, type.precision(), type.scale());
    v.allocateNew(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      BigInteger u = left ? rows.get(i).a() : rows.get(i).b();
      if (u == null) {
        v.setNull(i);
      } else {
        DecimalWords w = DecimalWords.of(u);
        v.getDataBuffer().setLong(i * 16L, w.lo());
        v.getDataBuffer().setLong(i * 16L + 8, w.hi());
        v.setIndexDefined(i);
      }
    }
    v.setValueCount(rows.size());
    return v;
  }

  private static Decimal decimal(BigInteger unscaled, DecimalType t) {
    return unscaled == null ? null
        : Decimal.apply(new BigDecimal(unscaled, t.scale()), t.precision(), t.scale());
  }

  /**
   * Runs {@code compiled}'s one condition as its one or zero over the columns {@code a} and
   * {@code b} at {@code lanes}, and returns per row 1, 0, or null where it is unknown.
   */
  private static Long[] answers(CompiledVarkaPredicate compiled, List<Row> rows, DecimalType ta,
      DecimalType tb, int lanes) {
    var fused = compiled.fused();
    assertEquals(1, fused.outputs().size());
    long[] lits = new long[fused.longLiterals().size()];
    for (int i = 0; i < lits.length; i++) {
      lits[i] = fused.longLiterals().get(i);
    }
    VarkaVectorIR root = new CondValue((Cond) fused.outputs().get(0), LaneType.LONG);
    List<Integer> ordinals = fused.inputOrdinals();
    var emitted = VarkaEmitterFixtures.emit(List.of(root), ordinals.size(), lits.length,
        VarkaMatrixRuns.BASE.withLanesOverride(lanes));
    VarkaFusedKernel kernel = VarkaEmitterFixtures.load(emitted).kernel();
    int length = rows.size();
    int inputs = ordinals.size();
    try (BufferAllocator allocator =
        ArrowUtils.rootAllocator().newChildAllocator("varka-decimal-pair", 0, Long.MAX_VALUE);
        DecimalVector va = vector("a", rows, true, ta, allocator);
        DecimalVector vb = vector("b", rows, false, tb, allocator);
        var arena = Arena.ofConfined()) {
      DecimalVector[] byOrdinal = {va, vb};
      long[] srcData = new long[inputs];
      long[] srcValidity = new long[inputs];
      int[] nullCounts = new int[inputs];
      long[] inStride = new long[inputs];
      for (int i = 0; i < inputs; i++) {
        DecimalVector v = byOrdinal[ordinals.get(i)];
        srcData[i] = v.getDataBuffer().memoryAddress();
        nullCounts[i] = v.getNullCount();
        srcValidity[i] = nullCounts[i] == length ? 0L : v.getValidityBuffer().memoryAddress();
        inStride[i] = 16L;
      }
      long wordBytes = ((length + 63) / 64) * 8L;
      MemorySegment out = arena.allocate(length * 8L, 64);
      MemorySegment outValid = arena.allocate(wordBytes, 64);
      long[] outData = {out.address()};
      long[] outValidity = {outValid.address()};
      long[] outStride = {8L};
      int status = VarkaKernelChecks.sanitized(kernel, length, inStride, outStride, srcData,
          srcValidity, outData, outValidity,
          scratch -> kernel.run(srcData, srcValidity, nullCounts, outData, outValidity,
              new int[0], lits, length, scratch));
      assertEquals(0, status, "an in-precision batch runs");
      var got = new Long[length];
      for (int i = 0; i < length; i++) {
        boolean valid = (outValid.get(ValueLayout.JAVA_LONG, (i >>> 6) * 8L) >>> (i & 63) & 1L)
            != 0;
        got[i] = valid ? out.get(ValueLayout.JAVA_LONG, i * 8L) : null;
      }
      return got;
    }
  }

  /**
   * Compiles {@code sql} over {@code a} of {@code ta} and {@code b} of {@code tb}, runs it over
   * {@link #rows} at every width, and holds each row's answer to Spark's evaluation of the same
   * expression, its {@code nullif} and {@code nullifzero} replaced, as they have no evaluation of
   * their own.
   */
  private static void check(String sql, DecimalType ta, DecimalType tb) {
    List<Attribute> columns =
        List.of(VarkaSqlResolve.column("a", ta), VarkaSqlResolve.column("b", tb));
    CompiledVarkaPredicate compiled = compile(sql, columns);
    Expression bound = VarkaSqlResolve.bind(
        VarkaSqlResolve.replaced(VarkaSqlResolve.parse(sql, seq(columns))), seq(columns));
    for (List<Row> rows : List.of(rows(ta, tb), denseRows(ta, tb))) {
      checkRows(sql, compiled, bound, rows, ta, tb);
    }
  }

  private static void checkRows(String sql, CompiledVarkaPredicate compiled, Expression bound,
      List<Row> rows, DecimalType ta, DecimalType tb) {
    for (int lanes : WIDTHS) {
      Long[] got = answers(compiled, rows, ta, tb, lanes);
      for (int i = 0; i < rows.size(); i++) {
        Row r = rows.get(i);
        Object spark = bound.eval(new GenericInternalRow(new Object[] {decimal(r.a(), ta),
            decimal(r.b(), tb)}));
        Long want = spark == null ? null : ((Boolean) spark ? 1L : 0L);
        int row = i;
        assertEquals(want, got[i], () -> sql + " over " + ta.simpleString() + ", "
            + tb.simpleString() + " " + label(lanes) + ", row " + row + ": " + r);
      }
    }
  }

  @Test
  @DisplayName("the comparisons of two wide columns answer as Spark on the word edges, at 19, "
      + "20 and 38 digits")
  void comparisonsOfTwoWideColumns() {
    for (int p : new int[] {19, 20, 38}) {
      var t = new DecimalType(p, 2);
      for (String op : OPS) {
        check("a " + op + " b", t, t);
      }
    }
  }

  @Test
  @DisplayName("a narrower side at one scale is widened into the pair: a 64-bit decimal by its "
      + "sign extension, a wide one by keeping its words")
  void widenedSides() {
    var wide = new DecimalType(38, 2);
    for (DecimalType narrow : List.of(new DecimalType(18, 2), new DecimalType(19, 2))) {
      for (String op : OPS) {
        check("a " + op + " b", narrow, wide);
        check("b " + op + " a", narrow, wide);
      }
    }
  }

  @Test
  @DisplayName("a literal wider than a long takes its two words in two slots of the long table, "
      + "and one of a smaller scale is folded at the column's")
  void wideLiterals() {
    var t = new DecimalType(38, 2);
    for (BigInteger u : List.of(TWO_64.add(TWO_63), TWO_63.negate().subtract(BigInteger.ONE),
        bound(t).negate())) {
      String literal = new BigDecimal(u, t.scale()).toPlainString();
      for (String op : OPS) {
        check("a " + op + " " + literal, t, t);
      }
      DecimalWords w = DecimalWords.of(u);
      List<Attribute> columns = List.of(VarkaSqlResolve.column("a", t));
      List<Long> slots = compile("a < " + literal, columns).fused().longLiterals();
      assertTrue(slots.contains(w.lo()) && slots.contains(w.hi()), () -> literal + ": " + slots);
    }
    // A literal of a smaller scale arrives under coercion's cast up, folded with it.
    for (String op : OPS) {
      check("a " + op + " 1.5", t, t);
    }
  }

  @Test
  @DisplayName("the null tests of a wide column, of a widening cast, a choice, a pick, a negation "
      + "or an abs over one read its validity, and a wide sum or lowering cast declines")
  void nullTests() {
    var t = new DecimalType(20, 2);
    for (String sql : List.of("a IS NULL", "a IS NOT NULL", "CAST(a AS DECIMAL(38, 2)) IS NULL",
        "b IS NOT NULL", "IF(a > 0, a, b) IS NULL", "coalesce(a, b) IS NULL",
        "greatest(a, b) IS NOT NULL", "least(b, a) IS NULL",
        "CASE WHEN a > 0 THEN b WHEN a < 0 THEN a ELSE b END IS NULL", "-b IS NULL",
        "abs(b) IS NOT NULL")) {
      check(sql, t, new DecimalType(38, 2));
    }
    // Each can raise, or null a row whose leaves are valid: a sum by overflowing, and a
    // lowering cast likewise.
    for (String sql : List.of("(a + b) IS NULL", "CAST(b AS DECIMAL(20, 2)) IS NULL")) {
      String reason = decline(sql, t, new DecimalType(38, 2));
      assertTrue(reason.contains("row"), () -> sql + ": " + reason);
    }
  }

  @Test
  @DisplayName("the null family's null tests over wide decimals answer as Spark: the NULL of "
      + "nullif and nullifzero and a missing ELSE are never valid, under a choice, a coalesce or "
      + "a pick")
  void nullFamilyNullTests() {
    // The null constant and the missing ELSE hold no words; their validity is false, and the
    // value's is the choice's condition picking its branches' (VARKA-578). The word edges make
    // nullif's a = b and the CASE's a < b turn on the high word and on the low word alone.
    var t = new DecimalType(20, 2);
    for (DecimalType tb : List.of(t, new DecimalType(38, 2))) {
      for (String sql : List.of("nullif(a, b) IS NULL", "nullif(b, a) IS NOT NULL",
          "nullifzero(a) IS NULL", "nullifzero(b) IS NOT NULL",
          "CASE WHEN a < b THEN a END IS NULL",
          "CASE WHEN a < b THEN b WHEN a > b THEN a END IS NOT NULL",
          "IF(a IS NULL, b, NULL) IS NULL", "nvl2(a, NULL, b) IS NOT NULL",
          "coalesce(nullif(a, b), b) IS NULL", "coalesce(a, NULL) IS NULL",
          "coalesce(CASE WHEN a > 0 THEN a END, nullif(b, a)) IS NOT NULL",
          "greatest(nullif(a, b), NULL) IS NULL", "least(a, NULL, b) IS NOT NULL")) {
        check(sql, t, tb);
      }
    }
  }

  @Test
  @DisplayName("a wide negation and abs answer as Spark on the word edges, a low word of 0 "
      + "borrowing from the high word, and keep all 38 digits of a value at the bound")
  void negationAndAbs() {
    // Spark negates a wide value exactly (Decimal.unary_-, SPARK-45438), so -x and abs(x) of a
    // 38-digit value keep every digit; the edges hold 10^p - 1 and -(10^p - 1), and 2^64 and
    // -2^64, whose low word is 0.
    for (int p : new int[] {19, 20, 38}) {
      var t = new DecimalType(p, 2);
      for (String sql : List.of("-a < b", "-a = b", "-a <=> b", "abs(a) < b", "abs(a) = b",
          "abs(a) >= b", "-(-a) = a", "abs(-a) = abs(a)", "-abs(a) <= a", "b > -a")) {
        check(sql, t, t);
      }
    }
    // A narrower side under the negation is widened first, a 64-bit one by its sign extension.
    check("-a < b", new DecimalType(18, 2), new DecimalType(38, 2));
    check("abs(a) = b", new DecimalType(19, 2), new DecimalType(38, 2));
  }

  @Test
  @DisplayName("a wide IN answers as Spark: each literal's two words in the long table, a "
      + "literal past a long and the bound among them")
  void wideIn() {
    for (int p : new int[] {20, 38}) {
      var t = new DecimalType(p, 2);
      String list = String.join(", ", List.of(TWO_64.add(TWO_63), TWO_63, BigInteger.ZERO,
          TWO_64.negate(), bound(t).negate()).stream()
          .map(u -> new BigDecimal(u, t.scale()).toPlainString()).toList());
      check("a IN (" + list + ")", t, t);
      check("a NOT IN (" + list + ")", t, t);
      check("-b IN (" + list + ")", t, t);
    }
  }

  @Test
  @DisplayName("the wide choices and picks answer as Spark as compared values, word by word "
      + "under one condition: IF, CASE, coalesce, nvl, nullif, greatest and least")
  void choicesAndPicks() {
    var narrow = new DecimalType(20, 2);
    var wide = new DecimalType(38, 2);
    for (DecimalType ta : List.of(narrow, wide)) {
      for (String sql : List.of("IF(a < b, a, b) < b", "IF(a IS NULL, b, a) = b",
          "CASE WHEN a > 0 THEN a WHEN b > 0 THEN b ELSE -a END <= b",
          "CASE WHEN a < 0 THEN b END <=> a", "coalesce(a, b) <=> b", "nvl(a, b) > 0.00",
          "nullif(a, b) <=> a", "greatest(a, b) = a", "greatest(a, b) >= b", "least(a, b) < b",
          "least(a, b, -a) <=> least(-a, b)", "-greatest(a, b) <= abs(a)",
          "coalesce(nullif(a, b), -b) IN (0.00, 1.00)")) {
        check(sql, ta, wide);
      }
    }
  }

  /**
   * Runs {@code p}'s one output, a decimal pair's store, over the columns {@code a} and {@code b}
   * at {@code lanes}, emitted under {@code options}, and returns per row the unscaled value its
   * sixteen-byte slot holds, or null where the output's validity is clear.
   */
  private static BigInteger[] stored(CompiledVarkaProjection p, List<Row> rows, DecimalType ta,
      DecimalType tb, VarkaEmitOptions options, int lanes) {
    assertEquals(1, p.outputs().size());
    VarkaVectorIR root = p.outputs().get(0);
    assertTrue(root instanceof VarkaVectorIR.InterleaveDecimal,
        () -> "not the pair's store: " + VarkaVectorIR.canonical(root));
    long[] lits = new long[p.longLiterals().size()];
    for (int i = 0; i < lits.length; i++) {
      lits[i] = p.longLiterals().get(i);
    }
    int[] intLits = p.literals().stream().mapToInt(Integer::intValue).toArray();
    List<Integer> ordinals = p.inputOrdinals();
    var emitted = VarkaEmitterFixtures.emit(List.of(root), ordinals.size(), lits.length,
        options.withLanesOverride(lanes));
    VarkaFusedKernel kernel = VarkaEmitterFixtures.load(emitted).kernel();
    int length = rows.size();
    int inputs = ordinals.size();
    try (BufferAllocator allocator =
        ArrowUtils.rootAllocator().newChildAllocator("varka-decimal-pair-store", 0,
            Long.MAX_VALUE);
        DecimalVector va = vector("a", rows, true, ta, allocator);
        DecimalVector vb = vector("b", rows, false, tb, allocator);
        var arena = Arena.ofConfined()) {
      DecimalVector[] byOrdinal = {va, vb};
      long[] srcData = new long[inputs];
      long[] srcValidity = new long[inputs];
      int[] nullCounts = new int[inputs];
      long[] inStride = new long[inputs];
      for (int i = 0; i < inputs; i++) {
        DecimalVector v = byOrdinal[ordinals.get(i)];
        srcData[i] = v.getDataBuffer().memoryAddress();
        nullCounts[i] = v.getNullCount();
        srcValidity[i] = nullCounts[i] == length ? 0L : v.getValidityBuffer().memoryAddress();
        inStride[i] = 16L;
      }
      long wordBytes = ((length + 63) / 64) * 8L;
      MemorySegment out = arena.allocate(length * 16L, 64);
      MemorySegment outValid = arena.allocate(wordBytes, 64);
      long[] outData = {out.address()};
      long[] outValidity = {outValid.address()};
      long[] outStride = {16L};
      int status = VarkaKernelChecks.sanitized(kernel, length, inStride, outStride, srcData,
          srcValidity, outData, outValidity,
          scratch -> kernel.run(srcData, srcValidity, nullCounts, outData, outValidity,
              intLits, lits, length, scratch));
      assertEquals(0, status, "an in-precision batch runs");
      var got = new BigInteger[length];
      for (int i = 0; i < length; i++) {
        boolean valid = (outValid.get(ValueLayout.JAVA_LONG, (i >>> 6) * 8L) >>> (i & 63) & 1L)
            != 0;
        got[i] = valid ? new DecimalWords(out.get(ValueLayout.JAVA_LONG, i * 16L),
            out.get(ValueLayout.JAVA_LONG, i * 16L + 8)).value() : null;
      }
      return got;
    }
  }

  /**
   * Compiles {@code sql}, a decimal wider than 18 digits, as a projection's one output over
   * {@code a} of {@code ta} and {@code b} of {@code tb}, runs its store over {@link #rows} at
   * every width, and holds each row's slot to Spark's evaluation of the same expression, read as
   * its unscaled value at the output's scale.
   */
  private static void checkStored(String sql, DecimalType ta, DecimalType tb) {
    checkStored(sql, ta, tb, VarkaMatrixRuns.BASE);
  }

  private static void checkStored(String sql, DecimalType ta, DecimalType tb,
      VarkaEmitOptions options) {
    List<Attribute> columns =
        List.of(VarkaSqlResolve.column("a", ta), VarkaSqlResolve.column("b", tb));
    Expression e = VarkaSqlResolve.parse(sql, seq(columns));
    var type = (DecimalType) e.dataType();
    assertTrue(type.precision() > 18, () -> sql + " is not wide: " + type);
    NamedExpression entry = VarkaSqlResolve.named(e, "c");
    CompiledVarkaProjection p = VarkaExpressionCompiler.compile(
        CollectionConverters.asScala(List.of(entry)).toSeq(), seq(columns), options)
        .orElseThrow(() -> new AssertionError(sql + " does not fuse as an output"));
    Expression bound = VarkaSqlResolve.bind(VarkaSqlResolve.replaced(e), seq(columns));
    for (List<Row> rows : List.of(rows(ta, tb), denseRows(ta, tb))) {
      checkStoredRows(sql, p, bound, type, rows, ta, tb, options);
    }
  }

  private static void checkStoredRows(String sql, CompiledVarkaProjection p, Expression bound,
      DecimalType type, List<Row> rows, DecimalType ta, DecimalType tb,
      VarkaEmitOptions options) {
    for (int lanes : WIDTHS) {
      BigInteger[] got = stored(p, rows, ta, tb, options, lanes);
      for (int i = 0; i < rows.size(); i++) {
        Row r = rows.get(i);
        Object spark = bound.eval(new GenericInternalRow(new Object[] {decimal(r.a(), ta),
            decimal(r.b(), tb)}));
        BigInteger want = spark == null ? null
            : ((Decimal) spark).toJavaBigDecimal().setScale(type.scale()).unscaledValue();
        int row = i;
        assertEquals(want, got[i], () -> sql + " over " + ta.simpleString() + ", "
            + tb.simpleString() + " " + label(lanes) + ", row " + row + ": " + r);
      }
    }
  }

  @Test
  @DisplayName("a wide negation, abs, choice and pick are stored as their two words in Arrow's "
      + "slot, as Spark answers, every null pattern and the word edges included")
  void storedValues() {
    var narrow = new DecimalType(20, 2);
    var wide = new DecimalType(38, 2);
    for (DecimalType ta : List.of(narrow, wide)) {
      for (String sql : List.of("-b", "abs(b)", "IF(a < b, a, b)", "IF(a IS NULL, b, -b)",
          "CASE WHEN a > 0 THEN a WHEN b > 0 THEN b END", "coalesce(a, b)", "nvl(b, a)",
          "nullif(b, a)", "greatest(a, b)", "least(a, b, -b)", "-greatest(a, abs(b))")) {
        checkStored(sql, ta, wide);
      }
      // A wide literal's two words beside a column: 2^64 + 5 at scale 2, a small negative one,
      // and the 38-digit bound.
      String top = new BigDecimal(bound(wide), wide.scale()).toPlainString();
      for (String sql : List.of("coalesce(a, 18446744073709551621.00)",
          "IF(a IS NULL, -0.01, a)", "greatest(a, " + top + ")", "least(a, -" + top + ")")) {
        checkStored(sql, ta, wide);
      }
    }
    // At 19 digits, the narrowest pair, and with a 64-bit side widened by its sign extension.
    var nineteen = new DecimalType(19, 2);
    checkStored("abs(a)", nineteen, nineteen);
    checkStored("IF(a < b, -a, b)", new DecimalType(18, 2), wide);
  }

  @Test
  @DisplayName("with the bitmap validity pass off, a pair store's per-group validity reads both "
      + "words, so a choice's high-word blend is no dead word (validityOrFirst either way)")
  void storedValuesWithoutTheBitmapPass() {
    // The output's word is the low word's (Validity.Operand(0)); with the pass on only that word
    // is read, and here the AND of both is. Both runs equal to Spark's nulls hold, for every
    // producer of a pair (a column's words, a literal's, the negation, abs, a choice, coalesce,
    // nullif and a pick), that the high word is valid wherever the low word is.
    var wide = new DecimalType(38, 2);
    for (boolean first : new boolean[] {false, true}) {
      VarkaEmitOptions options = VarkaMatrixRuns.BASE.toBuilder().validityByBitmap(false)
          .denseValidityOnce(false).validityOrFirst(first).build();
      for (String sql : List.of("IF(a < b, a, b)", "CASE WHEN a > 0 THEN a END",
          "coalesce(a, b)", "greatest(a, b)", "-b", "abs(a)", "nullif(a, b)", "least(a, b)",
          "coalesce(a, 18446744073709551621.00)")) {
        checkStored(sql, wide, wide, options);
      }
    }
  }

  @Test
  @DisplayName("the decimal output's type admits every precision, and the pair's store renders "
      + "apart from every other root")
  void storeAdmission() {
    var wide = new DecimalType(38, 2);
    assertTrue(VarkaTypes.decimalOutputDeclineReason(wide).isEmpty());
    VarkaVectorIR root = new VarkaVectorIR.InterleaveDecimal(
        new VarkaVectorIR.DecimalWord(0, VarkaVectorIR.Word.LOW, 38),
        new VarkaVectorIR.DecimalWord(0, VarkaVectorIR.Word.HIGH, 38));
    assertEquals("(idec decw:0:lo:p38 decw:0:hi:p38)", VarkaVectorIR.canonical(root));
  }

  @Test
  @DisplayName("a wide value cast down to 18 digits names the rows of the same cast to 38")
  void wideCastDownNamesTheWideRows() {
    var t = new DecimalType(38, 10);
    String cast = decline("CAST(a AS DECIMAL(18, 2)) > 0", t, t);
    assertEquals(decline("CAST(a AS DECIMAL(38, 2)) > 0", t, t), cast);
    assertTrue(cast.contains("rows 350, 359 and 363"), cast);
  }

  @Test
  @DisplayName("two wide scales decline naming the rescale's rows: up 360's, down 363's")
  void unequalScalesDecline() {
    record Case(DecimalType ta, DecimalType tb, String row) { }
    for (Case c : List.of(new Case(new DecimalType(20, 2), new DecimalType(20, 4), "360"),
        new Case(new DecimalType(38, 10), new DecimalType(38, 2), "363"))) {
      String reason = decline("a < b", c.ta(), c.tb());
      assertTrue(reason.contains(c.row()), () -> c + ": " + reason);
    }
  }

  /** Runs {@code body} with {@code spark.sql.ansi.enabled} set to {@code ansi}. */
  private static void inMode(boolean ansi, Runnable body) {
    SQLConf conf = SQLConf.get().clone();
    conf.setConfString(SQLConf.ANSI_ENABLED().key(), Boolean.toString(ansi));
    SQLConf.withExistingConf(conf, () -> {
      body.run();
      return BoxedUnit.UNIT;
    });
  }

  /**
   * The sums and differences past 18 digits whose exact type is within 38 (VARKA-360's class U):
   * the forms below over every pair of the word edges and the null patterns, at 2, 4 and 8 long
   * lanes, each answer held to Spark's own expression under ANSI and under LEGACY, and
   * {@code try_add} and {@code try_subtract} for TRY. Each form reads the sum's words through a
   * comparison, against an operand, against zero, and {@code (a + b) - b = a}, which a wrong
   * carry in either word fails; {@link #storedSumsAndDifferences} reads them as an output.
   * {@code DECIMAL(37, 2)} operands give a 38-digit result, so values of 35 to 37 digits are
   * among the edges.
   */
  @Test
  @DisplayName("a wide sum or difference at one scale answers as Spark's in every mode, on the "
      + "word edges, from DECIMAL(19, 2) to DECIMAL(38, 2)")
  void wideSumsAndDifferences() {
    record Types(DecimalType ta, DecimalType tb) { }
    var forms = List.of("a + b < b", "a - b > a", "a + b > 0", "a - b <= 0", "a + b = b",
        "try_add(a, b) < a", "try_subtract(a, b) >= b", "(a + b) IS NULL", "(a - b) IS NOT NULL",
        // The sum under the pair's negation, choice and coalesce (VARKA-359's forms).
        "-(a + b) > 0", "IF(a > b, a + b, a) > 0", "coalesce(a + b, b) > 0");
    for (Types t : List.of(new Types(new DecimalType(18, 2), new DecimalType(18, 2)),
        new Types(new DecimalType(19, 2), new DecimalType(19, 2)),
        new Types(new DecimalType(20, 2), new DecimalType(18, 2)),
        new Types(new DecimalType(30, 10), new DecimalType(30, 10)),
        new Types(new DecimalType(37, 2), new DecimalType(37, 2)),
        new Types(new DecimalType(37, 0), new DecimalType(18, 0)))) {
      for (boolean ansi : new boolean[] {true, false}) {
        inMode(ansi, () -> {
          for (String sql : forms) {
            check(sql, t.ta(), t.tb());
          }
          if (t.ta().precision() <= 36) {
            check("(a + b) - b = a", t.ta(), t.tb());
          }
        });
      }
    }
  }

  @Test
  @DisplayName("a 64-bit operand or a constant of a smaller scale is rescaled within a long, then "
      + "sign-extended into the sum; one whose rescale leaves the long declines naming row 360")
  void narrowOperandsRescaled() {
    record Types(DecimalType ta, DecimalType tb) { }
    for (Types t : List.of(new Types(new DecimalType(20, 2), new DecimalType(9, 0)),
        new Types(new DecimalType(37, 2), new DecimalType(16, 0)))) {
      for (boolean ansi : new boolean[] {true, false}) {
        inMode(ansi, () -> {
          for (String sql : List.of("a + b > a", "b - a < 0", "a + 1 > a", "a - 0.5 < a",
              "try_add(b, a) <= a")) {
            check(sql, t.ta(), t.tb());
          }
        });
      }
    }
    String past = decline("a + b > a", new DecimalType(20, 2), new DecimalType(17, 0));
    assertTrue(past.contains("WideMul, row 360"), past);
    String bigint = decline("CAST(a AS BIGINT) + b > b", new DecimalType(9, 0),
        new DecimalType(20, 2));
    assertTrue(bigint.contains("360"), bigint);
  }

  @Test
  @DisplayName("a wide sum compiles to one kernel in LEGACY, ANSI and TRY")
  void oneKernelInEveryMode() {
    var t = new DecimalType(20, 2);
    List<Attribute> columns =
        List.of(VarkaSqlResolve.column("a", t), VarkaSqlResolve.column("b", t));
    var kernels = new ArrayList<Object>();
    for (boolean ansi : new boolean[] {true, false}) {
      inMode(ansi, () -> kernels.add(compile("a + b < b", columns).fused().outputs()));
    }
    kernels.add(compile("try_add(a, b) < b", columns).fused().outputs());
    assertEquals(1, new LinkedHashSet<>(kernels).size(), () -> kernels.toString());
  }

  @Test
  @DisplayName("a wide sum Spark caps at 38 digits, or over two scales, declines naming its rows")
  void cappedAndRescaledSumsDecline() {
    var t38 = new DecimalType(38, 2);
    for (String sql : List.of("a + b < b", "try_subtract(a, b) > b", "(a + b) IS NULL")) {
      String reason = decline(sql, t38, t38);
      assertTrue(reason.contains("rows 360 and 363"), () -> sql + ": " + reason);
    }
    // The worked case: DECIMAL(39, 10) exact, capped with the scale kept or cut by the setting.
    var t3810 = new DecimalType(38, 10);
    String worked = decline("a + b > a", t3810, t3810);
    assertTrue(worked.contains("rows 360 and 363"), worked);
    String rescaled = decline("a - b < 0", new DecimalType(20, 2), new DecimalType(20, 4));
    assertTrue(rescaled.contains("360"), rescaled);
  }

  /**
   * The sums and differences of {@link #wideSumsAndDifferences} as a projection's output: the
   * sum's two words under the pair's store (VARKA-359's {@code InterleaveDecimal}), each slot held
   * to Spark's own expression under ANSI and under LEGACY, and through {@code try_add} and
   * {@code try_subtract}. No form here can overflow, its exact type being within 38 digits, so
   * neither a null from an overflow nor ANSI's error has a row; the operands' bounds, which give
   * the largest and the smallest result of each type, are among the edges. The forms are the
   * sum alone, in both operand orders, and under each form the pair builds over a value: the
   * negation, {@code abs}, a choice, a {@code coalesce} whose second sum is cast to the first's
   * type, and a pick.
   */
  @Test
  @DisplayName("a wide sum or difference as an output is stored as its two words and answers as "
      + "Spark's in every mode, every null pattern and the word edges included")
  void storedSumsAndDifferences() {
    record Types(DecimalType ta, DecimalType tb) { }
    var forms = List.of("a + b", "a - b", "b - a", "try_add(a, b)", "try_subtract(b, a)",
        "-(a + b)", "abs(a - b)", "IF(a > b, a + b, a - b)", "coalesce(a - b, b + b)",
        "greatest(a + b, a - b)");
    for (Types t : List.of(new Types(new DecimalType(18, 2), new DecimalType(18, 2)),
        new Types(new DecimalType(19, 2), new DecimalType(19, 2)),
        new Types(new DecimalType(20, 2), new DecimalType(18, 2)),
        new Types(new DecimalType(30, 10), new DecimalType(30, 10)),
        new Types(new DecimalType(37, 2), new DecimalType(37, 2)),
        new Types(new DecimalType(37, 0), new DecimalType(18, 0)))) {
      for (boolean ansi : new boolean[] {true, false}) {
        inMode(ansi, () -> {
          for (String sql : forms) {
            checkStored(sql, t.ta(), t.tb());
          }
          // A sum of a sum, where the second result is still within 38 digits.
          if (t.ta().precision() <= 36) {
            checkStored("(a + b) - b", t.ta(), t.tb());
            checkStored("a - (b + a)", t.ta(), t.tb());
          }
        });
      }
    }
  }

  @Test
  @DisplayName("a wide sum is stored over a 64-bit operand or a constant rescaled within a long, "
      + "and over a BIGINT under coercion's cast, which is stored alone too")
  void storedSumsOverRescaledAndBigintOperands() {
    var wide = new DecimalType(20, 2);
    var narrow = new DecimalType(9, 0);
    var whole = new DecimalType(18, 0);
    for (boolean ansi : new boolean[] {true, false}) {
      inMode(ansi, () -> {
        for (String sql : List.of("a + b", "b - a", "a + 1", "a - 0.5", "0.5 - a",
            "try_add(b, a)")) {
          checkStored(sql, wide, narrow);
          checkStored(sql, new DecimalType(37, 2), new DecimalType(16, 0));
        }
        // The null constant is two null words, so the sum is null on every row.
        checkStored("a + CAST(NULL AS DECIMAL(20, 2))", wide, narrow);
        // CAST(a AS BIGINT) reads every long of 18 digits; coercion casts it to DECIMAL(20, 0).
        for (String sql : List.of("CAST(a AS BIGINT) + b", "b - CAST(a AS BIGINT)",
            "CAST(CAST(a AS BIGINT) AS DECIMAL(20, 0))",
            "CAST(CAST(a AS BIGINT) AS DECIMAL(20, 0)) - CAST(CAST(a AS BIGINT) AS "
                + "DECIMAL(20, 0))")) {
          checkStored(sql, whole, new DecimalType(20, 0));
        }
      });
    }
  }

  @Test
  @DisplayName("with the bitmap validity pass off, a stored sum's two words are valid on the "
      + "same rows: the carry and a sign extension are null where their operand is")
  void storedSumsWithoutTheBitmapPass() {
    // The high word holds the carry, an unsigned compare made a value, and for a 64-bit operand
    // its sign extension: both must be null exactly where the low word is, since the output's
    // validity is the low word's (storedValuesWithoutTheBitmapPass).
    for (boolean first : new boolean[] {false, true}) {
      VarkaEmitOptions options = VarkaMatrixRuns.BASE.toBuilder().validityByBitmap(false)
          .denseValidityOnce(false).validityOrFirst(first).build();
      for (String sql : List.of("a + b", "a - b", "IF(a > b, a + b, a - b)",
          "coalesce(a - b, b + b)")) {
        checkStored(sql, new DecimalType(20, 2), new DecimalType(20, 2), options);
        checkStored(sql, new DecimalType(18, 2), new DecimalType(18, 2), options);
      }
    }
  }

  @Test
  @DisplayName("a wide sum as an output declines where it declines under a comparison: capped at "
      + "38 digits, where it could overflow, and over an operand rescaled past a long")
  void cappedAndRescaledSumsDeclineAsOutputs() {
    var t38 = new DecimalType(38, 2);
    for (String sql : List.of("a + b", "a - b", "try_add(a, b)", "-(a + b)")) {
      String reason = outputDecline(sql, t38, t38);
      assertTrue(reason.contains("rows 360 and 363"), () -> sql + ": " + reason);
    }
    String rescaled = outputDecline("a - b", new DecimalType(20, 2), new DecimalType(20, 4));
    assertTrue(rescaled.contains("360"), rescaled);
    String past = outputDecline("a + b", new DecimalType(20, 2), new DecimalType(17, 0));
    assertTrue(past.contains("WideMul, row 360"), past);
    String bigint = outputDecline("CAST(a AS BIGINT) + b", new DecimalType(9, 0),
        new DecimalType(20, 2));
    assertTrue(bigint.contains("360"), bigint);
  }

  /** The decline reason of {@code sql} as a projection's one output, or "fused". */
  private static String outputDecline(String sql, DecimalType ta, DecimalType tb) {
    List<Attribute> columns =
        List.of(VarkaSqlResolve.column("a", ta), VarkaSqlResolve.column("b", tb));
    NamedExpression entry = VarkaSqlResolve.named(VarkaSqlResolve.parse(sql, seq(columns)), "c");
    var declines = VarkaExpressionCompiler.declines(
        CollectionConverters.asScala(List.of(entry)).toSeq(), seq(columns), VarkaMatrixRuns.BASE);
    return declines.isEmpty() ? "fused" : declines.get(declines.firstKey()).reason();
  }

  /** The decline reason of the one conjunct {@code sql} over {@code a}, {@code b}, or "fused". */
  private static String decline(String sql, DecimalType ta, DecimalType tb) {
    List<Attribute> columns =
        List.of(VarkaSqlResolve.column("a", ta), VarkaSqlResolve.column("b", tb));
    Expression e = VarkaSqlResolve.parse(sql, seq(columns));
    List<VarkaConjunctSpec> specs = VarkaExpressionCompiler.explainPredicate(e, seq(columns),
        VarkaMatrixRuns.BASE, VarkaAdmission.NO_HOST_REFUSAL);
    assertEquals(1, specs.size(), sql);
    return specs.get(0).decline().map(d -> d.reason()).orElse("fused");
  }
}
