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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import scala.Option;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaPredicate;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelChecks.Batch;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelChecks.CallHook;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelChecks.SparkAnswers;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.SQLConf$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * The conformance contract a type is held to (VARKA-484, `VARKA-475.md` 3.1): seven checks -
 * load, round trip, null handling, SQL ordering, choice, the type's row of the cast matrix and
 * overflow in LEGACY, ANSI and TRY - each over fixtures generated from the type's values rather
 * than written per suite. A suite per type supplies the type and carries {@code @VarkaSuite};
 * the checks are inherited.
 *
 * <p>Each check compiles Catalyst expressions over two columns of the type, {@code x} and
 * {@code y}, through {@link VarkaExpressionCompiler}, and runs the emitted class over every pair
 * of the type's values under every null pattern of a pair, at lengths that reach a partial lane
 * group and the epilogue, and over a batch with no null for the dense body. Every row is compared
 * with {@link VarkaReference} and with {@link VarkaSparkOracle}, Spark's own {@code eval} of the
 * same expression, in both bodies and at both output validity fills.
 *
 * <p>Whether a form must fuse is the registers' word, not this class's: a form whose census entry
 * (or, for a cast, its form pair in {@code casts_fused.tsv}, which the compiler writes) marks the
 * type fused today must compile and agree; one it does not mark must decline, and is owed to the
 * rows the register names. A check whose forms all hold passes; one with an owed form is
 * cancelled naming the owners, after its fused forms have run. A form that fuses where the
 * register says it does not, or declines where it says it does, fails: the register has drifted
 * from the compiler.
 *
 * <p>A type is read at the lane the type registry names for it, and a check runs at the lane its
 * kernel is driven at: the int lane's batches for a narrow, int-lane or condition type, the long
 * lane's 64-bit batches for a long-lane one, with both literal tables where a kernel converts
 * between the two (a cast to {@code DATE}).
 *
 * <p>Every check also holds its fixture to what a hand-written one missed in the narrow lanes'
 * reviews (`VARKA-329.md` 9.8): each condition root is true on some row and false on another,
 * each value root is valid on some row and, where it can be null, null on another, so that a
 * kernel answering a constant could not pass it.
 */
public abstract class VarkaTypeConformance {

  // -------------------------------------------------------------------------------------------
  // The type under test
  // -------------------------------------------------------------------------------------------

  /**
   * What the checks read of a type: its census name, its Spark type and SQL spelling, a literal's
   * spelling for one of its values, and its values, from which every fixture is generated.
   */
  protected record ConformanceType(String census, DataType spark, String sql,
      LongFunction<String> literal, long[] values) {
    /** A type whose literal is a format of its value, {@code %dS} for a {@code SMALLINT}. */
    protected ConformanceType(String census, DataType spark, String sql, String format,
        long[] values) {
      this(census, spark, sql, v -> String.format(format, v), values);
    }

    boolean isBoolean() {
      return spark.equals(DataTypes.BooleanType);
    }

    /**
     * Whether Spark has the arithmetic the overflow check builds - {@code +}, {@code -},
     * {@code *}, negation and {@code abs} - over two values of the type. A {@code BOOLEAN} has
     * none, and a {@code TIMESTAMP_NTZ} has none of these: its subtraction is an interval and its
     * addition takes one ({@code SubtractTimestamps}, {@code TimestampAddInterval}).
     */
    boolean hasArithmetic() {
      return !spark.equals(DataTypes.BooleanType) && !spark.equals(DataTypes.TimestampNTZType);
    }

    /**
     * The lane a column of the type is read at, the registry's; a {@code BOOLEAN}, read as a
     * condition and on no lane, is counted with the int lane, as its bitmaps' words are.
     */
    LaneType lane() {
      return VarkaTypes.lane(spark).orElse(LaneType.INT);
    }
  }

  /** The type this suite holds to the contract. */
  protected abstract ConformanceType type();

  /** The integral values a fixture draws: the extremes, their neighbours, zero and one each way. */
  protected static long[] integralValues(long min, long max) {
    return new long[] {min, min + 1, -1, 0, 1, max - 1, max};
  }

  // -------------------------------------------------------------------------------------------
  // The checks
  // -------------------------------------------------------------------------------------------

  @Test
  @DisplayName("load: a column of the type is read as its values, null rows included")
  void load() {
    ConformanceType t = type();
    if (t.isBoolean()) {
      run(List.of(condition("x", null), condition("NOT x", "not")));
    } else {
      run(List.of(condition("x < y", "<"), condition("x = " + lit(1), "="),
          condition("x IS NOT NULL", "isnotnull"),
          condition("x IN (" + lit(1) + ", " + lit(-1) + ")", "in")));
    }
  }

  @Test
  @DisplayName("round trip: a value of the type stored by a kernel is the value read")
  void roundTrip() {
    ConformanceType t = type();
    if (t.isBoolean()) {
      run(List.of(value("x", null)));
    } else {
      run(List.of(value("if(x IS NULL, " + lit(1) + ", x)", "if"),
          value("coalesce(x, " + lit(-1) + ")", "coalesce")));
    }
  }

  @Test
  @DisplayName("null handling: every null pattern of a pair, null-intolerant and null-safe")
  void nullHandling() {
    ConformanceType t = type();
    if (t.isBoolean()) {
      run(List.of(condition("x AND y", "and"), condition("x OR y", "or"),
          condition("x <=> y", "<=>"), condition("x IS NULL", "isnull")));
    } else {
      // The null-intolerant value is an addition where the type has one; `x = y` is the
      // null-intolerant form of every type.
      var forms = new ArrayList<Form>();
      if (t.hasArithmetic()) {
        forms.add(value("x + y", "+").legacy());
      }
      forms.addAll(List.of(condition("x = y", "="), condition("x <=> y", "<=>"),
          condition("x IS NULL", "isnull")));
      run(forms);
    }
  }

  @Test
  @DisplayName("SQL ordering: every comparison over every pair of the type's values")
  void ordering() {
    var forms = new ArrayList<Form>();
    for (String op : List.of("<", "<=", ">", ">=", "=", "<=>")) {
      forms.add(condition("x " + op + " y", op));
    }
    if (!type().isBoolean()) {
      forms.add(value("greatest(x, y)", "greatest"));
      forms.add(value("least(x, y)", "least"));
    }
    run(forms);
  }

  @Test
  @DisplayName("choice: CASE WHEN, IF and coalesce over the type, unknown taking ELSE")
  void choice() {
    if (type().isBoolean()) {
      run(List.of(condition("IF(x, y, NOT y)", "if"), condition("CASE WHEN x THEN y END", "when"),
          condition("coalesce(x, y)", "coalesce")));
    } else {
      run(List.of(value("CASE WHEN x > y THEN x ELSE y END", "when"),
          value("if(x < y, x, y)", "if"), value("coalesce(x, y)", "coalesce")));
    }
  }

  @Test
  @DisplayName("the type's row of the cast matrix, in LEGACY, ANSI and TRY")
  void castRow() {
    var forms = new ArrayList<Form>();
    for (CastPair pair : Registers.casts(type().census())) {
      String target = CAST_TARGETS.get(pair.to());
      boolean toBoolean = pair.to().equals("bool");
      for (Mode mode : Mode.values()) {
        String sql = (mode == Mode.TRY ? "try_cast" : "CAST") + "(x AS " + target + ")";
        Form f = new Form(sql, toBoolean ? Kind.CONDITION : Kind.VALUE,
            "cast:" + pair.to(), mode, false);
        forms.add(f);
      }
    }
    assertTrue(!forms.isEmpty(), "casts.tsv has no row from " + type().census());
    run(forms);
  }

  @Test
  @DisplayName("overflow: LEGACY wraps, ANSI declines what Spark raises on, TRY nulls it")
  void overflow() {
    if (!type().hasArithmetic()) {
      // Not applicable, and stated rather than skipped: the census must list no arithmetic for
      // the type, so a row that adds one fails here until the type has an overflow check.
      for (String op : ARITHMETIC) {
        assertTrue(!Registers.census(op).types().contains(type().census()),
            "the census admits " + type().census() + " for " + op
                + ": the overflow check needs forms for it");
      }
      return;
    }
    var forms = new ArrayList<Form>();
    for (Mode mode : List.of(Mode.LEGACY, Mode.ANSI)) {
      for (String op : List.of("+", "-", "*")) {
        forms.add(value("x " + op + " y", op).in(mode).reachingOverflow());
      }
      forms.add(value("-x", "negative").in(mode).reachingOverflow());
      forms.add(value("abs(x)", "abs").in(mode).reachingOverflow());
    }
    for (String fn : List.of("try_add", "try_subtract", "try_multiply")) {
      forms.add(value(fn + "(x, y)", fn).in(Mode.TRY).reachingOverflow());
    }
    run(forms);
  }

  // -------------------------------------------------------------------------------------------
  // Forms and their verdicts
  // -------------------------------------------------------------------------------------------

  /** A form is a condition (a filter's predicate) or a value (a projection's output). */
  private enum Kind { CONDITION, VALUE }

  /** The session's evaluation mode a form is resolved under; TRY is spelled in the form. */
  private enum Mode { LEGACY, ANSI, TRY }

  /**
   * One expression a check runs: its SQL over {@code x} and {@code y}, its kind, the register
   * entry that says whether it fuses ({@code null} for a bare column, which every type reads),
   * the mode, and whether it is meant to reach the type's overflow.
   */
  private record Form(String sql, Kind kind, String register, Mode mode, boolean overflowing) {
    Form legacy() {
      return in(Mode.LEGACY);
    }

    Form in(Mode m) {
      return new Form(sql, kind, register, m, overflowing);
    }

    Form reachingOverflow() {
      return new Form(sql, kind, register, mode, true);
    }

    @Override public String toString() {
      return sql + " (" + mode + ")";
    }
  }

  private static Form condition(String sql, String register) {
    return new Form(sql, Kind.CONDITION, register, Mode.ANSI, false);
  }

  private static Form value(String sql, String register) {
    return new Form(sql, Kind.VALUE, register, Mode.ANSI, false);
  }

  private String lit(long v) {
    return type().literal().apply(v);
  }

  /**
   * The rows that own a form the registers do not mark fused, or empty where they do. A value of
   * type {@code BOOLEAN} is owed to row 27, which stores one, whatever its operator's entry says.
   */
  private Optional<String> owedTo(Form f, DataType result) {
    String census = type().census();
    if (f.kind() == Kind.VALUE && result.equals(DataTypes.BooleanType)) {
      return Optional.of("27 (a BOOLEAN output)");
    }
    if (f.register() == null) {
      return Optional.empty();
    }
    if (f.register().startsWith("cast:")) {
      CastPair pair = Registers.cast(census, f.register().substring(5));
      return pair.fusedIn(f.mode(), type().spark(), result) ? Optional.empty()
          : Optional.of(pair.rows());
    }
    CensusEntry e = Registers.census(f.register());
    return e.fused().contains(census) ? Optional.empty() : Optional.of(e.rows());
  }

  // -------------------------------------------------------------------------------------------
  // Running a check
  // -------------------------------------------------------------------------------------------

  private static final AtomicInteger CLASS_COUNTER = new AtomicInteger();

  /**
   * One compiled form: the emitted class, its roots, whether its kernel is driven at the long
   * lane, its int and long literal tables, and the expressions Spark answers.
   */
  private record Compiled(String className, byte[] bytes, List<VarkaVectorIR> roots,
      List<Integer> inputs, boolean onLong, int[] lits, long[] longLits,
      List<Expression> outputs) {}

  /**
   * Runs every form of a check: each that fuses is held to the reference and to Spark, each the
   * registers owe is required to decline. A check with an owed form is cancelled naming them.
   */
  private void run(List<Form> forms) {
    var owed = new ArrayList<String>();
    for (Form f : forms) {
      Expression expr = resolve(f);
      Optional<String> owner = owedTo(f, expr.dataType());
      Optional<Compiled> compiled = compile(f, expr);
      if (owner.isPresent()) {
        if (compiled.isPresent()) {
          fail(f + " fuses, and the register owes it to rows " + owner.get()
              + ": mark the type fused in the census or casts.tsv");
        }
        owed.add(f + ": rows " + owner.get());
        continue;
      }
      if (compiled.isEmpty()) {
        cancelIfTheWidthCannotWiden(f);
        fail(f + " declines, and the register marks " + type().census() + " fused for "
            + f.register());
      }
      check(f, compiled.get());
    }
    assumeTrue(owed.isEmpty(), "owed by the registers: " + String.join("; ", owed));
  }

  /**
   * A narrow column at a width whose int species cannot widen it is declined by the compiler and
   * answered by the row engine (VARKA-329): the check is cancelled naming the refusal, as the
   * narrow suites cancel theirs, rather than failing on a configuration of the option matrix.
   */
  private void cancelIfTheWidthCannotWiden(Form f) {
    LaneType lane = type().lane();
    if (!lane.isNarrow()) {
      return;
    }
    Optional<String> refusal = VarkaKernelPlan.refusalOf(List.of(new ColumnRef(0, lane)),
        VarkaMatrixRuns.BASE.toBuilder().narrowLanes(false).build());
    assumeTrue(refusal.isEmpty(), f + ": this configuration cannot widen the " + lane
        + " lane (" + refusal.orElse("") + "), so the compiler declines it and the row engine "
        + "answers it");
  }

  /** The two columns, {@code x} and {@code y}: built once, since a form binds their ids. */
  private List<Attribute> columns;

  private List<Attribute> columns() {
    if (columns == null) {
      columns = List.of(VarkaSqlResolve.column("x", type().spark()),
          VarkaSqlResolve.column("y", type().spark()));
    }
    return columns;
  }

  /** {@code f}'s SQL resolved over the two columns, ANSI on or off as its mode says. */
  private Expression resolve(Form f) {
    SQLConf conf = SQLConf.get().clone();
    conf.setConfString(SQLConf.ANSI_ENABLED().key(), f.mode() == Mode.LEGACY ? "false" : "true");
    var cols = CollectionConverters.asScala(columns()).toSeq();
    return SQLConf$.MODULE$.withExistingConf(conf, () -> VarkaSqlResolve.parse(f.sql(), cols));
  }

  private Optional<Compiled> compile(Form f, Expression expr) {
    var cols = CollectionConverters.asScala(columns()).toSeq();
    CompiledVarkaProjection plan;
    List<Expression> outputs;
    if (f.kind() == Kind.CONDITION) {
      Optional<CompiledVarkaPredicate> p =
          VarkaExpressionCompiler.compilePredicate(expr, cols, VarkaMatrixRuns.BASE);
      if (p.isEmpty() || !p.get().residualConjuncts().isEmpty()) {
        return Optional.empty();
      }
      plan = p.get().fused();
      // The fused conjuncts recombined into one root, which is the whole form when all fuse.
      outputs = List.of(expr);
    } else {
      NamedExpression entry = VarkaSqlResolve.named(expr, "c");
      Optional<CompiledVarkaProjection> p = VarkaExpressionCompiler.compile(
          CollectionConverters.asScala(List.of(entry)).toSeq(), cols, VarkaMatrixRuns.BASE);
      if (p.isEmpty()) {
        return Optional.empty();
      }
      plan = p.get();
      outputs = List.of(expr);
    }
    assertEquals(outputs.size(), plan.outputs().size(),
        f + ": one Spark expression a kernel output");
    String className = "org.apache.spark.sql.varka.execution.VarkaConformance"
        + CLASS_COUNTER.incrementAndGet();
    byte[] bytes = VarkaLoopEmitter.emit(className, plan.outputs(), plan.inputOrdinals().size(),
        plan.numLiterals(), null, null, VarkaMatrixRuns.BASE);
    int[] lits = plan.literals().stream().mapToInt(Integer::intValue).toArray();
    long[] longLits = plan.longLiterals().stream().mapToLong(Long::longValue).toArray();
    boolean onLong = plan.lane(VarkaMatrixRuns.BASE) == LaneType.LONG;
    return Optional.of(new Compiled(className, bytes, plan.outputs(), plan.inputOrdinals(),
        onLong, lits, longLits, outputs));
  }

  /** The fixture's rows: every pair of the type's values under every null pattern of a pair. */
  private record Row(long x, long y, boolean xNull, boolean yNull) {
    long value(int column) {
      return column == 0 ? x : y;
    }

    boolean isNull(int column) {
      return column == 0 ? xNull : yNull;
    }
  }

  private List<Row> rows(boolean withNulls) {
    var rows = new ArrayList<Row>();
    for (long a : type().values()) {
      for (long b : type().values()) {
        for (int pattern = 0; pattern < (withNulls ? 4 : 1); pattern++) {
          rows.add(new Row(a, b, (pattern & 1) != 0, (pattern & 2) != 0));
        }
      }
    }
    return rows;
  }

  /** Lengths that reach a single row, a partial lane group, whole groups and the epilogue. */
  private static final int[] TAILS = {1, 7, 8, 9, 17, 63, 64, 65};

  /**
   * One batch's columns over the kernel's inputs, at full width: each lane's batch is built from
   * it, the int lane's narrowing every value to 32 bits, which holds every value of a type read
   * there.
   */
  private record Cols(int length, boolean[][] nulls, long[][] data) {
    Batch ints() {
      var ints = new int[data.length][length];
      for (int c = 0; c < data.length; c++) {
        for (int i = 0; i < length; i++) {
          ints[c][i] = (int) data[c][i];
        }
      }
      return new Batch(length, nulls, ints, false);
    }

    VarkaKernelChecks.LongBatch longs() {
      return new VarkaKernelChecks.LongBatch(length, nulls, data, false);
    }
  }

  /** {@code rows} from {@code offset} on, cycled to {@code length}, over the kernel's inputs. */
  private static Cols batch(List<Row> rows, int offset, int length, List<Integer> inputs) {
    var nulls = new boolean[inputs.size()][length];
    var data = new long[inputs.size()][length];
    for (int c = 0; c < inputs.size(); c++) {
      int column = inputs.get(c);
      for (int i = 0; i < length; i++) {
        Row r = rows.get((offset + i) % rows.size());
        nulls[c][i] = r.isNull(column);
        data[c][i] = r.value(column);
      }
    }
    return new Cols(length, nulls, data);
  }

  /** Every batch a form runs: the fixture whole at several offsets, its tails, and dense. */
  private static List<Cols> batches(List<Row> all, List<Row> dense, List<Integer> inputs) {
    var out = new ArrayList<Cols>();
    for (int offset : new int[] {0, 1, 5}) {
      out.add(batch(all, offset, all.size(), inputs));
    }
    for (int length : TAILS) {
      out.add(batch(all, 3, length, inputs));
    }
    for (int length : new int[] {1, 9, 65, dense.size()}) {
      out.add(batch(dense, 0, length, inputs));
    }
    return out;
  }

  private void check(Form f, Compiled c) {
    var oracle = new VarkaSparkOracle(CollectionConverters.asScala(c.outputs()).toSeq(),
        CollectionConverters.asScala(c.inputs().stream().map(columns()::get).toList()).toSeq());
    List<Row> all = rows(true);
    requireTheFixtureDiscriminates(f, c, oracle, all);
    var loaded = VarkaEmitterFixtures.load(new VarkaEmitterFixtures.Emitted(c.className(),
        c.bytes()));
    try {
      int n = 0;
      for (Cols b : batches(all, rows(false), c.inputs())) {
        String ctx = getClass().getSimpleName() + " " + f + " batch " + n++ + " length "
            + b.length();
        boolean raises = raises(oracle, c, b);
        int status = c.onLong()
            ? VarkaKernelChecks.runLoadedLong(ctx, loaded.kernel(), c.roots(), c.inputs().size(),
                c.longLits(), c.lits(), b.longs(), raises, answers(oracle), CallHook.NONE)
            : VarkaKernelChecks.runLoadedNarrow(ctx, loaded.kernel(), c.roots(),
                c.inputs().size(), c.lits(), b.ints(), raises, answers(oracle), CallHook.NONE);
        assertEquals(raises, status != 0, ctx + ": the kernel " + (status != 0 ? "declined"
            : "answered") + " a batch on which Spark " + (raises ? "raises" : "does not raise"));
      }
    } finally {
      loaded.loader().release();
    }
  }

  /**
   * The rule the narrow lanes' hand-written fixtures broke: over the fixture, each condition root
   * is true on a row and false on another, each value root valid on a row and, where Spark's
   * expression is null-intolerant, null on another; a form meant to reach the type's overflow
   * does (raises in ANSI, nulls a row of valid inputs in TRY; its LEGACY twin runs over the same
   * fixture, which the ANSI form shows reaches the overflow).
   */
  private void requireTheFixtureDiscriminates(Form f, Compiled c, VarkaSparkOracle oracle,
      List<Row> rows) {
    for (int o = 0; o < c.outputs().size(); o++) {
      boolean sawTrue = false;
      boolean sawFalse = false;
      boolean sawValue = false;
      boolean sawNull = false;
      boolean sawNullFromValid = false;
      boolean sawRaise = false;
      for (Row r : rows) {
        var answer = oracle.answer(o, rowOf(r, c.inputs()));
        if (answer instanceof VarkaSparkOracle.Value v) {
          sawValue = true;
          sawTrue |= v.v() == 1L;
          sawFalse |= v.v() == 0L;
        } else if (answer instanceof VarkaSparkOracle.Raised) {
          sawRaise = true;
        } else {
          sawNull = true;
          sawNullFromValid |= !r.xNull() && !r.yNull();
        }
      }
      String at = f + " output " + o + " (" + oracle.sql(o) + ")";
      if (f.kind() == Kind.CONDITION) {
        assertTrue(sawTrue && sawFalse, at + ": the fixture never makes it "
            + (sawTrue ? "false" : "true") + ", so a constant kernel would pass");
      } else {
        assertTrue(sawValue, at + ": the fixture never gives it a value");
        // A null-intolerant expression is null exactly where an input is; one that can mask a
        // null (IF, coalesce) need not be null anywhere, and so is not asked to be.
        assertTrue(sawNull || !c.outputs().get(o).nullIntolerant(),
            at + ": the fixture never makes it null");
      }
      if (f.overflowing() && f.mode() == Mode.ANSI) {
        assertTrue(sawRaise, at + ": the fixture never overflows it");
      }
      if (f.overflowing() && f.mode() == Mode.TRY) {
        assertTrue(sawNullFromValid, at + ": the fixture never overflows it to a null");
      }
    }
  }

  private static scala.collection.immutable.Seq<Option<Object>> rowOf(Row r,
      List<Integer> inputs) {
    var row = new ArrayList<Option<Object>>();
    for (int column : inputs) {
      row.add(r.isNull(column) ? Option.empty() : Option.apply((Object) r.value(column)));
    }
    return CollectionConverters.asScala(row).toList();
  }

  /** Whether Spark raises on some row of {@code b}: the batch an ANSI kernel must decline. */
  private static boolean raises(VarkaSparkOracle oracle, Compiled c, Cols b) {
    for (int i = 0; i < b.length(); i++) {
      var row = new ArrayList<Option<Object>>();
      for (int col = 0; col < c.inputs().size(); col++) {
        row.add(b.nulls()[col][i] ? Option.empty() : Option.apply((Object) b.data()[col][i]));
      }
      var seq = CollectionConverters.asScala(row).toList();
      for (int o = 0; o < c.outputs().size(); o++) {
        if (oracle.answer(o, seq) instanceof VarkaSparkOracle.Raised) {
          return true;
        }
      }
    }
    return false;
  }

  /** Spark's answers for one batch, as {@link VarkaKernelChecks} reads them. */
  private static SparkAnswers answers(VarkaSparkOracle oracle) {
    return new SparkAnswers() {
      @Override
      public VarkaKernelChecks.Answer answer(int output, long[] values, boolean[] isNull) {
        var row = new ArrayList<Option<Object>>();
        for (int c = 0; c < values.length; c++) {
          row.add(isNull[c] ? Option.empty() : Option.apply((Object) values[c]));
        }
        VarkaSparkOracle.Answer a = oracle.answer(output, CollectionConverters.asScala(row)
            .toList());
        if (a instanceof VarkaSparkOracle.Value v) {
          return new VarkaKernelChecks.Answer.Value(v.v());
        } else if (a instanceof VarkaSparkOracle.Raised r) {
          return new VarkaKernelChecks.Answer.Raised(r.e());
        }
        return new VarkaKernelChecks.Answer.Null();
      }

      @Override
      public String sql(int output) {
        return oracle.sql(output);
      }
    };
  }

  // -------------------------------------------------------------------------------------------
  // The registers
  // -------------------------------------------------------------------------------------------

  /** The arithmetic the overflow check covers, by census name. */
  private static final List<String> ARITHMETIC = List.of("+", "-", "*", "negative", "abs",
      "try_add", "try_subtract", "try_multiply");

  /** Each census type as a cast target's SQL, for the cast row: the type registry's. */
  private static final Map<String, String> CAST_TARGETS = castTargets();

  private static Map<String, String> castTargets() {
    var out = new HashMap<String, String>();
    for (VarkaTypes.Info i : VarkaTypes.all()) {
      if (i.census() != null) {
        out.put(i.census(), i.sql());
      }
    }
    return Map.copyOf(out);
  }

  /** A census entry: the types it admits, those it fuses today, and the rows that own the rest. */
  private record CensusEntry(Set<String> types, Set<String> fused, String rows) {}

  /**
   * A {@code casts.tsv} pair: the target and the rows that own its gaps. Whether a form of it
   * fuses is {@code casts_fused.tsv}'s answer for that form pair and mode (VARKA-486), which the
   * compiler writes; a form pair the file lacks fails rather than being guessed at.
   */
  private record CastPair(String from, String to, String rows) {
    boolean fusedIn(Mode mode, DataType source, DataType target) {
      String key = source.sql() + ">" + target.sql();
      String[] answers = Registers.FUSED.get(key);
      assertTrue(answers != null, "casts_fused.tsv has no line for " + from + " -> " + to + " ("
          + key + "): add the form to VarkaCastMatrixSuite's TYPES and regenerate it");
      return answers[mode.ordinal()].equals("fused");
    }
  }

  /** The two registers, read once from the committed files. */
  private static final class Registers {
    private static final Map<String, CensusEntry> CENSUS = new HashMap<>();
    private static final List<String[]> CASTS = new ArrayList<>();
    /** {@code casts_fused.tsv} by {@code <from type>><to type>}: the LEGACY, ANSI, TRY answers. */
    private static final Map<String, String[]> FUSED = new HashMap<>();

    static {
      for (String[] f : tsv("census.tsv")) {
        CENSUS.put(f[0], new CensusEntry(Set.of(f[5].split(",")),
            Set.of(f[7].isEmpty() ? new String[0] : f[7].split(",")), f[10]));
      }
      CASTS.addAll(tsv("casts.tsv"));
      for (String[] f : tsv("casts_fused.tsv")) {
        FUSED.put(f[2] + ">" + f[3], new String[] {f[4], f[5], f[6]});
      }
    }

    /** The data lines of {@code m8/<name>}, split on tabs with empty fields kept. */
    private static List<String[]> tsv(String name) {
      String home = System.getProperty("spark.test.home", System.getenv("SPARK_HOME"));
      assertTrue(home != null, "spark.test.home or SPARK_HOME is not set.");
      try {
        List<String> lines = Files.readAllLines(
            Path.of(home, "sql", "varka", "plans", "m8", name), StandardCharsets.UTF_8);
        return lines.stream().skip(1).filter(l -> !l.isBlank()).map(l -> l.split("\t", -1))
            .toList();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    static CensusEntry census(String name) {
      CensusEntry e = CENSUS.get(name);
      assertTrue(e != null, "census.tsv has no entry " + name);
      return e;
    }

    /** The pairs from {@code from}, its identity cast left out: the analyzer removes that one. */
    static List<CastPair> casts(String from) {
      return CASTS.stream().filter(f -> f[0].equals(from) && !f[1].equals(from))
          .map(f -> new CastPair(f[0], f[1], f[9])).toList();
    }

    static CastPair cast(String from, String to) {
      return casts(from).stream().filter(p -> p.to().equals(to)).findFirst().orElseThrow();
    }
  }
}
