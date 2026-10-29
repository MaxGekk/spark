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

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuite;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.test.TestSparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The harness's expression is the one a session hands Varka (VARKA-509, {@code m8/VARKA-509.md}
 * section 4): for every row of the coverage table on every answer fixture, the projection or
 * condition {@link VarkaHarnessPlans} optimizes with no session equals, bound to the fixture's
 * columns and canonicalized, the one in a session's optimized plan over the cached fixture.
 * The session plans and executes nothing - the fixtures are cached lazily and never read - so
 * this costs the analyzer and the optimizer only, and it fails the day the harness's optimizer
 * and a session's part. The hand-written queries of {@link VarkaHarnessCases} are held to a
 * session's plan the same way, over a view with their fixture's columns.
 */
@VarkaSuite
public final class VarkaHarnessParitySuite {

  private static TestSparkSession spark;

  @BeforeAll
  static void session() {
    spark = new TestSparkSession(new SparkConf(), 1, 1);
    for (Tuple2<String, String> f :
        CollectionConverters.asJava(VarkaCoverageDifferentialSuite$.MODULE$.fixtures())) {
      spark.sql(f._2()).createOrReplaceTempView(f._1());
      spark.catalog().cacheTable(f._1());
    }
  }

  @AfterAll
  static void stop() {
    if (spark != null) {
      spark.stop();
    }
  }

  /** {@code e} bound to {@code columns} by ordinal and canonicalized: ids and names erased. */
  private static Expression canonical(Expression e, Seq<Attribute> columns) {
    return VarkaSqlResolve.bind(e, columns).canonicalized();
  }

  @Test
  @DisplayName("every coverage row's harness expression is the one a session's optimizer plans")
  void harnessExpressionIsTheSessions() {
    var differences = new ArrayList<String>();
    int compared = 0;
    for (Tuple2<String, String> f :
        CollectionConverters.asJava(VarkaCoverageDifferentialSuite$.MODULE$.fixtures())) {
      VarkaHarnessPlans.Fixture fixture = VarkaHarnessPlans.fixture(f._2());
      Seq<Attribute> columns = fixture.columns();
      for (VarkaCoverageHarnessSuite.Row row : VarkaCoverageHarnessSuite.rows()) {
        boolean predicate = row.form().equals("predicate");
        LogicalPlan plan = spark.sql(predicate
                ? "SELECT d, d2, i FROM " + f._1() + " WHERE " + row.executable()
                : "SELECT " + row.executable() + " AS v FROM " + f._1())
            .queryExecution().optimizedPlan();
        String where = f._1() + ", " + row.sql();
        Expression mine;
        Expression theirs;
        Seq<Attribute> theirColumns;
        if (predicate) {
          LogicalPlan child = plan instanceof Project p ? p.child() : plan;
          if (!(child instanceof Filter filter)) {
            differences.add(where + ": the session's plan has no filter:\n" + plan.treeString());
            continue;
          }
          mine = VarkaHarnessPlans.condition(row.executable(), columns);
          theirs = filter.condition();
          theirColumns = filter.child().output();
        } else {
          if (!(plan instanceof Project p)) {
            differences.add(where + ": the session's plan is no projection:\n"
                + plan.treeString());
            continue;
          }
          List<NamedExpression> list = CollectionConverters.asJava(
              VarkaHarnessPlans.projection(row.executable(), columns));
          // The alias's id is the plan's own, which canonicalization keeps: its child is compared.
          mine = ((Alias) list.get(0)).child();
          theirs = ((Alias) CollectionConverters.asJava(p.projectList()).get(0)).child();
          theirColumns = p.child().output();
        }
        compared++;
        Expression a = canonical(mine, columns);
        Expression b = canonical(theirs, theirColumns);
        // An InSet's literals are a Scala set, whose boxed numbers compare numerically, so a set
        // holding NaN never equals another: the canonical forms are compared as printed too.
        if (!a.equals(b) && !a.toString().equals(b.toString())) {
          differences.add(where + ": the harness has " + mine.sql() + ", the session "
              + theirs.sql());
        }
      }
    }
    assertTrue(compared > 0, "no row compared");
    assertTrue(differences.isEmpty(), differences.size() + " of " + compared
        + " expressions differ:\n" + String.join("\n", differences));
  }

  /** {@code e} itself, or an alias's child: the alias's id is the plan's own. */
  private static Expression unaliased(NamedExpression e) {
    return e instanceof Alias a ? a.child() : (Expression) e;
  }

  @Test
  @DisplayName("every hand-written harness query's expressions are the ones a session's "
      + "optimizer plans")
  void handWrittenQueriesAreTheSessions() {
    var differences = new ArrayList<String>();
    int compared = 0;
    var cases = new ArrayList<VarkaHarnessCases.Case>(VarkaTimeHarnessSuite.all());
    cases.addAll(VarkaDifferentialHarnessSuite.all());
    String ansiKey = SQLConf.ANSI_ENABLED().key();
    String ansiBefore = spark.conf().get(ansiKey);
    for (VarkaHarnessCases.Case c : cases) {
      for (VarkaHarnessCases.Part part : c.parts()) {
        // The cache is lazy and never read: it keeps the optimizer from folding the view's rows.
        if (spark.catalog().tableExists("t")) {
          spark.catalog().uncacheTable("t");
        }
        spark.sql(part.source().schema()).createOrReplaceTempView("t");
        spark.catalog().cacheTable("t");
        spark.conf().set(ansiKey, part.ansi().map(String::valueOf).orElse(ansiBefore));
        Seq<Attribute> columns = part.source().fixture().columns();
        for (VarkaHarnessCases.Shape shape : part.shapes()) {
          String where = c.name() + ", " + shape.sql();
          LogicalPlan plan = spark.sql(shape.sql()).queryExecution().optimizedPlan();
          LogicalPlan below = plan instanceof Project p && p.child() instanceof Filter
              ? p.child() : plan;
          var mine = new ArrayList<Expression>();
          var theirs = new ArrayList<Expression>();
          Seq<Attribute> theirColumns;
          SQLConf conf = VarkaCoverageHarnessSuite.conf(
              part.ansi().orElse(Boolean.parseBoolean(ansiBefore)));
          switch (VarkaCoverageHarnessSuite.withConf(conf,
              () -> VarkaHarnessPlans.query(shape.sql(), columns))) {
            case VarkaHarnessPlans.Query.Selection s when below instanceof Filter f -> {
              mine.add(s.condition());
              theirs.add(f.condition());
              theirColumns = f.child().output();
            }
            case VarkaHarnessPlans.Query.Projection p when plan instanceof Project q
                && !(q.child() instanceof Filter) -> {
              CollectionConverters.asJava(p.list()).forEach(e -> mine.add(unaliased(e)));
              CollectionConverters.asJava(q.projectList()).forEach(e -> theirs.add(unaliased(e)));
              theirColumns = q.child().output();
            }
            default -> {
              differences.add(where + ": the session plans another shape:\n"
                  + plan.treeString());
              continue;
            }
          }
          compared++;
          if (mine.size() != theirs.size()) {
            differences.add(where + ": the harness has " + mine.size() + " outputs, the session "
                + theirs.size());
            continue;
          }
          for (int k = 0; k < mine.size(); k++) {
            Expression a = canonical(mine.get(k), columns);
            Expression b = canonical(theirs.get(k), theirColumns);
            if (!a.equals(b) && !a.toString().equals(b.toString())) {
              differences.add(where + ": the harness has " + mine.get(k).sql() + ", the session "
                  + theirs.get(k).sql());
            }
          }
        }
      }
    }
    spark.conf().set(ansiKey, ansiBefore);
    spark.catalog().uncacheTable("t");
    spark.catalog().dropTempView("t");
    assertTrue(compared > 0, "no query compared");
    assertTrue(differences.isEmpty(), differences.size() + " of " + compared
        + " queries differ:\n" + String.join("\n", differences));
  }
}
