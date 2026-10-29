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

package org.apache.spark.sql.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.InterpretedUnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.execution.columnar.VarkaCacheBatches;
import org.apache.spark.sql.execution.varka.VarkaBatchDeclined;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.TimeType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A Varka kernel's answers held to Spark's, with no session (VARKA-509, {@code m8/VARKA-509.md}
 * section 2). The rows are a fixture's, written into Arrow batches by the cache's own iterators
 * ({@link VarkaCacheBatches}); Varka's answer is the production evaluator's, run through
 * {@code serveBatch} as the exec nodes run it; Spark's is its own projection of the same
 * expression over the same rows, interpreted and generated.
 *
 * <p>A batch the kernel answers must hold Spark's answer on every row, and no row Spark raises
 * on. A batch it declines is answered by the row path, which is Spark's evaluation, so it is
 * Spark's by construction and only counted. Any other failure of the kernel's path - an
 * exception that is not a decline - is a finding: the exec node would hide it behind the
 * fallback, and the end-to-end suites only counted batches. A row on which Spark's two
 * projections disagree is reported as Spark's disagreement and not compared.
 */
public final class VarkaExpressionHarness {

  private VarkaExpressionHarness() {}

  /** What a check found, one line a disagreement, and how its batches went. */
  public static final class Outcome {
    public final List<String> findings = new ArrayList<>();
    public int answered;
    public int declined;

    public boolean ok() {
      return findings.isEmpty();
    }
  }

  /** Spark's answer for one row: the projected row, or the error evaluating it raised. */
  private record Expected(@Nullable InternalRow row, @Nullable Throwable error) {}

  /** Each row projected by {@code projection}, one at a time, so an error is the row's own. */
  private static List<Expected> project(UnsafeProjection projection, List<InternalRow> rows) {
    var out = new ArrayList<Expected>(rows.size());
    for (InternalRow row : rows) {
      try {
        out.add(new Expected(projection.apply(row).copy(), null));
      } catch (RuntimeException e) {
        out.add(new Expected(null, e));
      }
    }
    return out;
  }

  /**
   * Spark's answers for {@code outputs} over {@code rows}, interpreted and generated, the
   * generated one's where they agree and null for a row where they do not, which is reported.
   */
  private static List<Expected> spark(String context, List<Expression> outputs,
      Seq<Attribute> columns, List<InternalRow> rows, DataType[] types, Outcome outcome) {
    var bound = new ArrayList<Expression>(outputs.size());
    for (Expression e : outputs) {
      bound.add(VarkaSqlResolve.bind(e, columns));
    }
    Seq<Expression> exprs = CollectionConverters.asScala(bound).toList();
    List<Expected> interpreted = project(InterpretedUnsafeProjection.createProjection(exprs), rows);
    List<Expected> generated = project(GenerateUnsafeProjection.generate(exprs, false), rows);
    var out = new ArrayList<Expected>(rows.size());
    for (int i = 0; i < rows.size(); i++) {
      Expected a = interpreted.get(i);
      Expected b = generated.get(i);
      boolean agree = a.error() != null
          ? b.error() != null && a.error().getClass() == b.error().getClass()
              && Objects.equals(a.error().getMessage(), b.error().getMessage())
          : b.error() == null && sameRow(a.row(), b.row(), types) == null;
      if (!agree) {
        outcome.findings.add(context + ", row " + i + ": Spark's interpreted and generated "
            + "projections disagree (" + describe(a, types) + " against " + describe(b, types)
            + "); the row is not compared");
        out.add(null);
      } else {
        out.add(b);
      }
    }
    return out;
  }

  /**
   * Varka's projection evaluator over {@code rows}, as {@code VarkaProjectExec} serves a batch,
   * against Spark's projection of the same list.
   */
  public static Outcome projection(String context, Seq<NamedExpression> projectList,
      VarkaHarnessPlans.Fixture fixture, SQLConf conf) {
    var outcome = new Outcome();
    Seq<Attribute> columns = fixture.columns();
    List<InternalRow> all = fixture.rows();
    List<NamedExpression> list = CollectionConverters.asJava(projectList);
    // A Scala trait over a class: Java sees NamedExpression without Expression's methods.
    var outputs = new ArrayList<Expression>();
    for (NamedExpression e : list) {
      outputs.add((Expression) e);
    }
    DataType[] types = outputs.stream().map(Expression::dataType).toArray(DataType[]::new);
    List<Expected> expected = spark(context, outputs, columns, all, types, outcome);
    VarkaCacheBatches.inTask(() -> {
      var evaluator =
          new VarkaKernelEvaluator(projectList, columns, false, "VarkaExpressionHarness");
      int[] offset = {0};
      VarkaCacheBatches.each(all, columns, conf, batch -> {
        int start = offset[0];
        offset[0] += batch.numRows();
        ColumnarBatch out = serve(context, evaluator, batch, () -> evaluator.project(batch),
            outcome);
        if (out != null) {
          try {
            for (int i = 0; i < batch.numRows(); i++) {
              compare(context, start + i, expected.get(start + i), out.getRow(i), types,
                  outcome);
            }
          } finally {
            evaluator.release(out);
          }
        }
      });
      return null;
    });
    return outcome;
  }

  /**
   * Varka's filter evaluator over {@code rows}, as {@code VarkaFilterExec} serves a batch, against
   * Spark's evaluation of {@code condition}: the rows the kernel keeps, by their {@code rid}, are
   * exactly those the condition is true on.
   */
  public static Outcome filter(String context, Expression condition,
      VarkaHarnessPlans.Fixture fixture, SQLConf conf) {
    var outcome = new Outcome();
    Seq<Attribute> columns = fixture.columns();
    List<InternalRow> all = fixture.rows();
    int ridOrdinal = fixture.ordinal("rid");
    DataType[] types = {condition.dataType()};
    List<Expected> expected = spark(context, List.of(condition), columns, all, types, outcome);
    VarkaCacheBatches.inTask(() -> {
      var evaluator =
          new VarkaFilterEvaluator(condition, columns, false, "VarkaExpressionHarness");
      int[] offset = {0};
      VarkaCacheBatches.each(all, columns, conf, batch -> {
        int start = offset[0];
        offset[0] += batch.numRows();
        ColumnarBatch out = serve(context, evaluator, batch,
            () -> evaluator.filterCompact(batch), outcome);
        if (out != null) {
          try {
            var want = new ArrayList<Integer>();
            for (int i = 0; i < batch.numRows(); i++) {
              Expected e = expected.get(start + i);
              if (e == null) {
                continue;
              }
              if (e.error() != null) {
                outcome.findings.add(context + ", row " + (start + i) + ": the kernel answered "
                    + "a batch holding a row Spark raises on: " + e.error());
              } else if (!e.row().isNullAt(0) && e.row().getBoolean(0)) {
                want.add(all.get(start + i).getInt(ridOrdinal));
              }
            }
            var got = new ArrayList<Integer>();
            for (int i = 0; i < out.numRows(); i++) {
              got.add(out.getRow(i).getInt(ridOrdinal));
            }
            if (!got.equals(want)) {
              outcome.findings.add(context + ", rows " + start + " to "
                  + (start + batch.numRows() - 1) + ": kept rid " + got + ", Spark keeps rid "
                  + want);
            }
          } finally {
            evaluator.release(out);
          }
        }
      });
      return null;
    });
    return outcome;
  }

  /**
   * One batch through {@code serveBatch}: the kernel path's batch, or null for a batch the row
   * path took. A failure of the kernel's path other than a decline is recorded before the
   * evaluator falls back on it.
   */
  private static @Nullable ColumnarBatch serve(String context, VarkaEvaluatorBase evaluator,
      ColumnarBatch batch, Supplier<ColumnarBatch> kernel, Outcome outcome) {
    boolean[] fell = {false};
    ColumnarBatch out = evaluator.serveBatch(batch, () -> {
      try {
        return kernel.get();
      } catch (VarkaBatchDeclined declined) {
        throw declined;
      } catch (RuntimeException | Error e) {
        // Recorded for the message; the evaluator's guard (VARKA-275) then raises its own
        // INTERNAL_ERROR out of serveBatch, so the test fails on that, loudly, as it should.
        outcome.findings.add(context + ": the kernel's path failed on a batch of "
            + batch.numRows() + " rows: " + e);
        throw e;
      }
    }, () -> {
      fell[0] = true;
      return null;
    });
    if (fell[0]) {
      outcome.declined++;
      return null;
    }
    outcome.answered++;
    return out;
  }

  private static void compare(String context, int row, @Nullable Expected expected,
      InternalRow got, DataType[] types, Outcome outcome) {
    if (expected == null) {
      return;
    }
    if (expected.error() != null) {
      outcome.findings.add(context + ", row " + row + ": the kernel answered a row Spark raises "
          + "on: " + expected.error());
      return;
    }
    String difference = sameRow(expected.row(), got, types);
    if (difference != null) {
      outcome.findings.add(context + ", row " + row + ": " + difference);
    }
  }

  /**
   * Null when {@code want} and {@code got} hold the same value in every column, else what
   * differs: floating values by their raw bits, so a NaN's payload and a zero's sign count, as
   * they do in {@code VarkaSparkOracle}.
   */
  private static @Nullable String sameRow(InternalRow want, InternalRow got, DataType[] types) {
    for (int o = 0; o < types.length; o++) {
      boolean wantNull = want.isNullAt(o);
      boolean gotNull = got.isNullAt(o);
      if (wantNull || gotNull) {
        if (wantNull != gotNull) {
          return "output " + o + ": Spark " + (wantNull ? "null" : value(want, o, types[o]))
              + ", Varka " + (gotNull ? "null" : value(got, o, types[o]));
        }
        continue;
      }
      DataType t = types[o];
      boolean same;
      if (t instanceof DoubleType) {
        same = Double.doubleToRawLongBits(want.getDouble(o))
            == Double.doubleToRawLongBits(got.getDouble(o));
      } else if (t instanceof FloatType) {
        same = Float.floatToRawIntBits(want.getFloat(o))
            == Float.floatToRawIntBits(got.getFloat(o));
      } else {
        same = Objects.equals(value(want, o, t), value(got, o, t));
      }
      if (!same) {
        return "output " + o + ": Spark " + value(want, o, t) + ", Varka " + value(got, o, t);
      }
    }
    return null;
  }

  /**
   * Column {@code o}'s value by its physical type: a columnar batch's row reads no {@code TIME}
   * through {@code get}, so the temporal types are read as the int or long they are stored as.
   */
  private static Object value(InternalRow row, int o, DataType t) {
    if (t instanceof TimeType || t instanceof DayTimeIntervalType || t instanceof TimestampType
        || t instanceof TimestampNTZType) {
      return row.getLong(o);
    }
    if (t instanceof DateType || t instanceof YearMonthIntervalType) {
      return row.getInt(o);
    }
    return row.get(o, t);
  }

  private static String describe(Expected e, DataType[] types) {
    if (e.error() != null) {
      return e.error().toString();
    }
    var parts = new ArrayList<String>();
    for (int o = 0; o < types.length; o++) {
      parts.add(e.row().isNullAt(o) ? "null" : String.valueOf(value(e.row(), o, types[o])));
    }
    return String.join(", ", parts);
  }
}
