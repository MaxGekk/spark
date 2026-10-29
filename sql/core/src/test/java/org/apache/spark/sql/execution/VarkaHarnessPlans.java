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

import org.apache.hadoop.conf.Configuration;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.ExperimentalMethods;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.FakeV2SessionCatalog$;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry$;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.analysis.TableFunctionRegistry$;
import org.apache.spark.sql.catalyst.catalog.InMemoryCatalog;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.connector.catalog.DefaultCatalogManager;

/**
 * The plans a session would build for a Varka differential check, built with no session
 * (VARKA-509, {@code m8/VARKA-509.md} section 2): the SQL parsed, resolved by Spark's analyzer
 * and rewritten by {@code SparkOptimizer}, the optimizer a session runs, over an in-memory
 * catalog with the builtin functions.
 *
 * <p>A query reads {@link VarkaHarnessScan}, an opaque leaf with the fixture's columns, as a
 * session's query reads its cached view's scan: over a {@code LocalRelation} the optimizer would
 * fold the projection or the filter into a relation of results, and nothing would be left for
 * Varka to compile. A fixture, on the other hand, is meant to fold: its {@code VALUES} rows and
 * derived columns become the {@code LocalRelation} whose rows a check runs on.
 */
public final class VarkaHarnessPlans {

  private VarkaHarnessPlans() {}

  private static final SessionCatalog CATALOG = new SessionCatalog(
      new InMemoryCatalog(new SparkConf(), new Configuration()),
      FunctionRegistry$.MODULE$.builtin(), TableFunctionRegistry$.MODULE$.builtin());

  private static final SparkOptimizer OPTIMIZER = new SparkOptimizer(
      new DefaultCatalogManager(FakeV2SessionCatalog$.MODULE$, CATALOG), CATALOG,
      new ExperimentalMethods());

  /** A fixture's columns and rows, from evaluating its SQL. */
  public record Fixture(Seq<Attribute> columns, List<InternalRow> rows) {
    /** The ordinal of the column named {@code name}. */
    public int ordinal(String name) {
      List<Attribute> all = CollectionConverters.asJava(columns);
      for (int i = 0; i < all.size(); i++) {
        if (all.get(i).name().equals(name)) {
          return i;
        }
      }
      throw new IllegalArgumentException("no column " + name + " in " + all);
    }
  }

  /** {@code plan} resolved and optimized as a session's query would be. */
  public static LogicalPlan optimize(LogicalPlan plan) {
    LogicalPlan analyzed = SimpleAnalyzer$.MODULE$.execute(plan);
    SimpleAnalyzer$.MODULE$.checkAnalysis(analyzed);
    return OPTIMIZER.execute(analyzed);
  }

  /** {@code sql}'s rows, evaluated by Catalyst to the relation the query folds to. */
  public static Fixture fixture(String sql) {
    LogicalPlan plan = optimize(CatalystSqlParser$.MODULE$.parsePlan(sql));
    if (!(plan instanceof LocalRelation local)) {
      throw new IllegalArgumentException("the fixture did not fold to its rows:\n"
          + plan.treeString());
    }
    var rows = new ArrayList<InternalRow>();
    for (InternalRow row : CollectionConverters.asJava(local.data())) {
      rows.add(row.copy());
    }
    return new Fixture(local.output(), rows);
  }

  /**
   * {@code SELECT <expression> AS v} over a scan of {@code columns}, as the optimizer leaves it:
   * the projection list Varka's projection evaluator compiles.
   */
  public static Seq<NamedExpression> projection(String expression, Seq<Attribute> columns) {
    Expression parsed = CatalystSqlParser$.MODULE$.parseExpression(expression);
    NamedExpression alias = VarkaSqlResolve.named(parsed, "v");
    var list = CollectionConverters.asScala(List.of(alias)).toList();
    LogicalPlan plan = optimize(new Project(list, new VarkaHarnessScan(columns)));
    if (plan instanceof Project p && p.child() instanceof VarkaHarnessScan) {
      return p.projectList();
    }
    throw new IllegalArgumentException(expression + " did not stay a projection over the "
        + "scan:\n" + plan.treeString());
  }

  /**
   * {@code WHERE <predicate>} over a scan of {@code columns}, as the optimizer leaves it: the
   * condition Varka's filter evaluator compiles, with the conjuncts the optimizer infers beside
   * it.
   */
  public static Expression condition(String predicate, Seq<Attribute> columns) {
    Expression parsed = CatalystSqlParser$.MODULE$.parseExpression(predicate);
    LogicalPlan plan = optimize(new Filter(parsed, new VarkaHarnessScan(columns)));
    if (plan instanceof Filter f && f.child() instanceof VarkaHarnessScan) {
      return f.condition();
    }
    throw new IllegalArgumentException(predicate + " did not stay a filter over the scan:\n"
        + plan.treeString());
  }
}
