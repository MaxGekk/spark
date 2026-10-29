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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.internal.SQLConf;

/**
 * Hand-written queries held to Spark's answer through {@link VarkaExpressionHarness} (VARKA-509,
 * {@code m8/VARKA-509.md} step 3): the shapes the end-to-end differential suites ran through two
 * sessions, each a query over a table named {@code t} that the harness reads as a scan of a
 * fixture. A case is one test of those suites, under its name, so that the option matrix's skip
 * lines move with it by the suite's name alone.
 *
 * <p>A projection runs under both consumers, the row node's {@code projectFused} only where every
 * output is fused, as the row node is planned only then; a filter runs through the filter
 * evaluator. Each shape then states what the kernel must have done beside answering as Spark
 * does: served every batch, declined some, or nothing more.
 */
final class VarkaHarnessCases {

  private VarkaHarnessCases() {}

  /** What a shape's kernel must have done, beside every answer it gave being Spark's. */
  enum Expect {
    /** Fused whole and served every batch, none declined. */
    SERVED,
    /** Fused whole, and declined at least one batch, by a status the kernel returned. */
    DECLINES,
    /** Nothing beyond the answers: a shape Spark raises on, or one the compiler may refuse. */
    ANSWERS
  }

  /** One query over {@code t} and what its kernel must have done. */
  record Shape(String sql, Expect expect) {}

  /**
   * The table a case's queries read: {@code schema}, SQL whose columns are the fixture's, for a
   * session to plan the same queries over, and the fixture the harness reads.
   */
  record Source(String schema, VarkaHarnessPlans.Fixture fixture) {}

  /** One test of an end-to-end suite: its name, its table and its queries. */
  record Case(String name, Source source, List<Shape> shapes) {}

  /** A source whose rows are its SQL's own. */
  static Source source(String sql) {
    return new Source(sql, VarkaHarnessPlans.fixture(sql));
  }

  /**
   * A source with {@code schema}'s columns and {@code rows}, each row's values in Catalyst's
   * internal form: for a table too large to write as SQL.
   */
  static Source source(String schema, List<Object[]> rows) {
    var columns = VarkaHarnessPlans.fixture(schema).columns();
    var internal = new ArrayList<InternalRow>(rows.size());
    for (Object[] row : rows) {
      internal.add(new GenericInternalRow(row));
    }
    return new Source(schema, new VarkaHarnessPlans.Fixture(columns, internal));
  }

  /** {@code c}'s shapes under the thread's ANSI mode: what disagreed, one line a finding. */
  static List<String> check(Case c) {
    SQLConf conf = VarkaCoverageHarnessSuite.conf(SQLConf.get().ansiEnabled());
    return VarkaCoverageHarnessSuite.withConf(conf, () -> {
      var findings = new ArrayList<String>();
      for (Shape shape : c.shapes()) {
        findings.addAll(check(shape, c.source().fixture(), conf));
      }
      return findings;
    });
  }

  private static List<String> check(Shape shape, VarkaHarnessPlans.Fixture fixture,
      SQLConf conf) {
    String context = shape.sql();
    var outcomes = new ArrayList<VarkaExpressionHarness.Outcome>();
    int residual;
    switch (VarkaHarnessPlans.query(shape.sql(), fixture.columns())) {
      case VarkaHarnessPlans.Query.Projection p -> {
        var columns = VarkaExpressionHarness.projection(context + ", COLUMNS", p.list(),
            fixture, conf, VarkaExpressionHarness.Consumer.COLUMNS);
        outcomes.add(columns);
        residual = columns.residual;
        if (residual == 0) {
          outcomes.add(VarkaExpressionHarness.projection(context + ", ROWS", p.list(), fixture,
              conf, VarkaExpressionHarness.Consumer.ROWS));
        }
      }
      case VarkaHarnessPlans.Query.Selection s -> {
        outcomes.add(VarkaExpressionHarness.filter(context, s.condition(), fixture, conf));
        // The conjuncts the compiler leaves to the row FilterExec above the Varka node, by the
        // compile the filter evaluator makes; all of them where none compiles.
        residual = VarkaExpressionCompiler.compilePredicate(s.condition(), fixture.columns(),
                VarkaColumnarToRowExec$.MODULE$.currentEmitOptions())
            .map(p -> (int) p.specs().stream().filter(c -> !c.fused()).count())
            .orElse(1);
      }
      case VarkaHarnessPlans.Query.Constant k -> {
        return List.of(context + ": the optimizer folded the query to a constant answer");
      }
    }
    var findings = new ArrayList<String>();
    for (VarkaExpressionHarness.Outcome o : outcomes) {
      findings.addAll(o.findings);
      String batches = o.answered + " answered, " + o.declined + " declined";
      switch (shape.expect()) {
        case SERVED -> {
          if (!o.served() || residual > 0) {
            findings.add(context + ": expected a kernel to serve every batch: " + batches
                + ", " + residual + " left to the row path");
          }
        }
        case DECLINES -> {
          boolean kernelDeclined = o.batches.stream()
              .anyMatch(b -> b.status() != VarkaExpressionHarness.Batch.ANSWERED
                  && b.status() != VarkaExpressionHarness.Batch.FELL_BACK);
          if (residual > 0 || !kernelDeclined) {
            findings.add(context + ": expected a fused kernel to decline a batch: " + batches
                + ", " + residual + " left to the row path");
          }
        }
        case ANSWERS -> { }
      }
    }
    return findings;
  }
}
