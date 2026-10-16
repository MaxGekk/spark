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

package org.apache.spark.sql.execution.benchmark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import scala.collection.immutable.Seq;

import org.apache.spark.metrics.source.CodegenMetrics;
import org.apache.spark.sql.VarkaBenchmarkBase;
import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.DateAdd;
import org.apache.spark.sql.catalyst.expressions.DateDiff;
import org.apache.spark.sql.catalyst.expressions.DateSub;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.NumericEvalContext;
import org.apache.spark.sql.catalyst.expressions.NumericEvalContext$;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus;
import org.apache.spark.sql.catalyst.expressions.Year;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeCacheImpl;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeKey;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeLookup;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSqlResolve;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.util.Utils;

/**
 * Class-generation time benchmark (VARKA-7): Gen-time for the Janino string path
 * ({@code GenerateUnsafeProjection.generate}, i.e. string generation + {@code
 * CodeGenerator.compile}) vs the Varka Class-File path - {@code VarkaLoopEmitter.emit} of a
 * representative fused kernel (two outputs sharing a subchain, the shape the throughput
 * benchmark's DAG-CSE case runs) plus define, load and instantiate through a fresh {@link
 * VarkaGeneratedClassLoader}, which is the exact per-task work {@code VarkaKernelEvaluator}'s
 * runner does. A fresh literal each Janino iteration defeats the global compiler cache so every
 * iteration is a cold compile, matching the first-query cost.
 *
 * <p>VARKA-14 added the fused case beside milestone 1's per-op dispatcher case; VARKA-17 retired
 * the dispatchers with the rest of that layer, so what remains measures only machinery that runs.
 *
 * <p>The first section's two arms make different classes: Janino compiles a one-output {@code
 * date_add} projection with Spark's row writer, Varka emits a two-output kernel. VARKA-259 added
 * the second section, which hands one expression list to both: per list, Spark's {@code
 * GenerateUnsafeProjection.generate} against Varka's compiler, shape cache, emitter, define and
 * instantiate, each cold and each with its own cache allowed to hit. Before timing, each list is
 * printed and checked: Varka fuses every entry, its shape does not depend on the constants, the
 * cold arms compile on every call and the hit arms do not.
 *
 * <p>To run this benchmark:
 * <pre>
 *   dev/varka_bench_regen.sh core VarkaCodegenBenchmark
 * </pre>
 * or, for the table alone, {@code build/sbt "sql/Test/runMain
 * org.apache.spark.sql.execution.benchmark.VarkaCodegenBenchmark"}; the results file is
 * "benchmarks/VarkaCodegenBenchmark-jdk&lt;NN&gt;-results.txt".
 */
public class VarkaCodegenBenchmark extends VarkaBenchmarkBase {

  private static final Attribute START_ATTR = VarkaSqlResolve.column("d", DataTypes.DateType);
  private static final Seq<Attribute> FIELDS = seq(List.of(START_ATTR));
  // The representative fused-kernel IR: `date_add(d, 1) AS a, datediff(date_add(d, 1), d2) AS b`
  // - two outputs, a shared subchain, two inputs, one literal slot. The same shape as the
  // throughput benchmark's DAG-CSE case, so the two committed files describe one kernel.
  private static final VarkaVectorIR SHARED_CHAIN = new VarkaVectorIR.AddDays(
      new VarkaVectorIR.ColumnRef(0), new VarkaVectorIR.LiteralSlot(0));
  private static final List<VarkaVectorIR> FUSED_OUTPUTS = List.of(
      SHARED_CHAIN, new VarkaVectorIR.DateDiff(SHARED_CHAIN, new VarkaVectorIR.ColumnRef(1)));

  /**
   * Keeps the compiled result alive so the compile work cannot be elided: a plain field written
   * each iteration, as the Scala form's captured {@code var} was, and published once to {@link
   * #blackhole} after the run.
   */
  private Object sink;

  private volatile Object blackhole;

  @Override
  public void runBenchmarkSuite(String[] mainArgs) {
    section("Varka per-op class generation (Gen-time)", () -> {
      var benchmark = benchmark("class generation time", 1)
          .minIterations(2).warmup(Duration.ofSeconds(1)).minTime(Duration.ofSeconds(1)).build();

      benchmark.addCase("janino: GenerateUnsafeProjection.generate", 5, i -> {
        // A fresh literal per iteration defeats the CodeGenerator cache so each iteration is a
        // cold string-generate + Janino compile.
        Expression add = new DateAdd(START_ATTR, Literal.create(i + 1, DataTypes.IntegerType));
        sink = GenerateUnsafeProjection.generate(seq(List.of(add)), FIELDS);
      });

      benchmark.addCase("varka: fused emit + define + load + instantiate", 5, i -> {
        // The per-task sequence VarkaKernelEvaluator's runner performs for the fused loop.
        String className = "org.apache.spark.sql.varka.execution.VarkaFusedProjectionBench" + i;
        byte[] bytes = VarkaLoopEmitter.emit(className, FUSED_OUTPUTS, 2, 1);
        var loader = new VarkaGeneratedClassLoader(Thread.currentThread().getContextClassLoader());
        sink = defineKernel(loader, className, bytes);
        loader.release();
      });

      runCases(benchmark);
      blackhole = sink;
    });

    section(SAME_LIST_SECTION, () -> {
      // A private cache of capacity 0 is the cache disabled: every lookup emits and defines, and
      // the entry is evicted (its loader released) as it is loaded. The warm one keeps every
      // list's class.
      var cold = new VarkaShapeCacheImpl(0);
      var warm = new VarkaShapeCacheImpl(LISTS.size());
      for (ListSpec spec : LISTS) {
        checkSameList(spec, cold, warm);
        var benchmark = benchmark(spec.title(), 1).timed().build();
        benchmark.addCase("spark: generate + Janino compile, cold",
            i -> sink = sparkProjection(projectList(spec, nextColdRound())));
        benchmark.addCase("varka: compile + emit + define + load + instantiate, cold",
            i -> sink = varkaKernel(cold, projectList(spec, nextColdRound())).entry().newKernel());
        benchmark.addCase("spark: generate, class cache hit",
            i -> sink = sparkProjection(projectList(spec, HIT_ROUND)));
        benchmark.addCase("varka: compile, shape cache hit",
            i -> sink = varkaKernel(warm, projectList(spec, HIT_ROUND)).entry().newKernel());
        runCases(benchmark);
      }
      blackhole = sink;
    });
  }

  // ---------------------------------------------------------------------------------------
  // One expression list through both compilers (VARKA-259)
  // ---------------------------------------------------------------------------------------

  private static final String SAME_LIST_SECTION =
      "One expression list through Janino and through Varka (VARKA-259)";

  private static final Attribute END_ATTR = VarkaSqlResolve.column("d2", DataTypes.DateType);
  private static final Attribute INT_ATTR = VarkaSqlResolve.column("i", DataTypes.IntegerType);
  /** The child output every list is bound against, by Janino and by Varka alike. */
  private static final Seq<Attribute> LIST_INPUT = seq(List.of(START_ATTR, END_ATTR, INT_ATTR));

  /** The widest list; entry {@code k}'s constant is offset by a multiple of it per round. */
  private static final int MAX_SIZE = 64;

  /**
   * The constants start here so that no list's source text is one the first section compiled
   * (its literals are 0 to 5): a size-1 list would otherwise meet its class in Janino's cache.
   */
  private static final int CONSTANT_BASE = 1000;

  /**
   * The cold arms cycle through this many rounds of constants. Janino's cache keeps 100 classes
   * by default ({@code spark.sql.codegen.cache.maxEntries}), so a round's source text has been
   * evicted long before the cycle comes back to it; the check before each table confirms it.
   */
  private static final int COLD_ROUNDS = 1000;

  /** The round both hit arms compile every time, outside the cold cycle. */
  private static final int HIT_ROUND = COLD_ROUNDS;

  /** Two more rounds outside the cycle, which only the check compiles. */
  private static final int CHECK_ROUND = COLD_ROUNDS + 1;

  private static final String EXECUTION = "VarkaCodegenBenchmark";

  private enum Family { DATE, INT }

  /** One expression list of the second section: a family and a number of entries. */
  private record ListSpec(Family family, int size) {
    ListSpec {
      if (size < 1 || size > MAX_SIZE) {
        throw new IllegalArgumentException("a list has 1 to " + MAX_SIZE + " entries: " + size);
      }
    }

    String title() {
      String noun = family == Family.DATE ? "date" : "int";
      return size + " " + noun + (size == 1 ? " expression" : " expressions");
    }
  }

  private static final List<ListSpec> LISTS = List.of(
      new ListSpec(Family.DATE, 1), new ListSpec(Family.DATE, 4), new ListSpec(Family.DATE, 16),
      new ListSpec(Family.DATE, MAX_SIZE), new ListSpec(Family.INT, 16));

  private int coldRound;

  private int nextColdRound() {
    int round = coldRound;
    coldRound = (coldRound + 1) % COLD_ROUNDS;
    return round;
  }

  /**
   * The list both arms compile: {@code spec.size()} entries named {@code o0, o1, ...}, entry
   * {@code k} taking the {@code k % 4}-th (dates) or {@code k % 3}-th (ints) form below with a
   * constant of its own, so no two entries share a subexpression and neither compiler's
   * elimination of common subexpressions has anything to find. A date list of one entry is
   * {@code date_add(d, c)}, the first section's Janino projection. The round moves every
   * constant and nothing else: a new source text for Janino, the same shape for Varka. The ints
   * hold no {@code i * c}: Varka declines a checked int multiply whose operands do not rule out
   * overflow, and a list it does not fuse whole would compare nothing.
   */
  private static List<NamedExpression> projectList(ListSpec spec, int round) {
    NumericEvalContext arithmetic = NumericEvalContext$.MODULE$.fromSQLConf(SQLConf.get());
    boolean ansi = SQLConf.get().ansiEnabled();
    var list = new ArrayList<NamedExpression>(spec.size());
    for (int k = 0; k < spec.size(); k++) {
      Expression c = Literal.create(CONSTANT_BASE + k + MAX_SIZE * round, DataTypes.IntegerType);
      Expression entry = switch (spec.family()) {
        case DATE -> switch (k % 4) {
          case 0 -> new DateAdd(START_ATTR, c);
          case 1 -> new DateSub(START_ATTR, c);
          case 2 -> new DateDiff(new DateAdd(START_ATTR, c), END_ATTR);
          default -> new Year(new DateAdd(START_ATTR, c));
        };
        case INT -> switch (k % 3) {
          case 0 -> new Add(INT_ATTR, c, arithmetic);
          case 1 -> new Subtract(INT_ATTR, c, arithmetic);
          default -> new UnaryMinus(new Add(INT_ATTR, c, arithmetic), ansi);
        };
      };
      list.add(VarkaSqlResolve.named(entry, "o" + k));
    }
    return list;
  }

  /** Spark's arm: the source, then Janino unless the text is cached, then the instance. */
  private static Object sparkProjection(List<NamedExpression> list) {
    // Java sees NamedExpression, a Scala trait, as an interface apart from Expression, so the
    // entries are cast one by one; each is an Alias, which is both.
    var entries = new ArrayList<Expression>(list.size());
    for (NamedExpression e : list) {
      entries.add((Expression) e);
    }
    return GenerateUnsafeProjection.generate(seq(entries), LIST_INPUT);
  }

  /**
   * Varka's arm up to the class, as a task's evaluator obtains it: the compiler, the shape key
   * and the cache lookup, which on a miss hashes the shape, emits the class and defines it in a
   * fresh loader. The caller makes the instance, as each task makes its own.
   *
   * <p>The compiler asks the JVM-wide cache whether the emitter serves the shape ({@code
   * VarkaSizeAdmission}), which builds the class's bytes the first time and keeps them for the
   * first lookup that defines it. Here that first time is the check before the table; after it the
   * question is a map lookup, and the private cache builds the bytes again on every cold call. A
   * cold first query in production also builds them once, at the question, and defines them at
   * the lookup, so the cold row holds the same one build and one definition.
   */
  private static VarkaShapeLookup varkaKernel(VarkaShapeCacheImpl cache,
      List<NamedExpression> list) {
    CompiledVarkaProjection compiled = varkaCompile(list);
    return cache.getOrEmit(Utils.getContextOrSparkClassLoader(), shapeKey(compiled), EXECUTION);
  }

  /** Varka's compiler over the whole list, refusing one it does not fuse entry for entry. */
  private static CompiledVarkaProjection varkaCompile(List<NamedExpression> list) {
    var compiled = VarkaExpressionCompiler.compile(seq(list), LIST_INPUT);
    if (compiled.isEmpty()) {
      throw requirementFailed("Varka does not fuse every entry of a list of " + list.size()
          + "; the declines by position: "
          + VarkaExpressionCompiler.declines(seq(list), LIST_INPUT));
    }
    return compiled.get();
  }

  private static VarkaShapeKey shapeKey(CompiledVarkaProjection compiled) {
    return new VarkaShapeKey(compiled.outputs(), compiled.inputOrdinals().size(),
        compiled.numLiterals());
  }

  /** How many classes Janino has compiled in this JVM; a cache hit compiles none. */
  private static long janinoCompilations() {
    return CodegenMetrics.METRIC_COMPILATION_TIME().getCount();
  }

  /**
   * Prints the list and checks, before its table is timed, what the arms claim: that Varka fuses
   * every entry into one output each, that its shape is the same in every round, that the cold
   * arms compile on every call and that the hit arms compile nothing once warm. The warm cache
   * is warmed here, by the second Varka call of the hit round.
   */
  private static void checkSameList(ListSpec spec, VarkaShapeCacheImpl cold,
      VarkaShapeCacheImpl warm) {
    List<NamedExpression> list = projectList(spec, HIT_ROUND);
    System.out.println("VARKA-259 list \"" + spec.title() + "\", the hit round:");
    for (NamedExpression e : list) {
      System.out.println("  " + ((Expression) e).sql());
    }

    CompiledVarkaProjection compiled = varkaCompile(list);
    require(compiled.outputs().size() == spec.size(), spec.title() + ": Varka made "
        + compiled.outputs().size() + " outputs of " + spec.size() + " entries");
    VarkaShapeKey key = shapeKey(compiled);
    for (int round : new int[] {0, COLD_ROUNDS - 1, CHECK_ROUND, CHECK_ROUND + 1}) {
      require(shapeKey(varkaCompile(projectList(spec, round))).equals(key),
          spec.title() + ": Varka's shape in round " + round + " differs from the hit round's");
    }

    long compiles = janinoCompilations();
    sparkProjection(projectList(spec, CHECK_ROUND));
    sparkProjection(projectList(spec, CHECK_ROUND + 1));
    require(janinoCompilations() - compiles == 2,
        spec.title() + ": Janino did not compile two new rounds twice");
    sparkProjection(list);
    compiles = janinoCompilations();
    sparkProjection(list);
    require(janinoCompilations() == compiles,
        spec.title() + ": Janino compiled the hit round again");

    long misses = cold.missCount();
    VarkaShapeLookup first = varkaKernel(cold, projectList(spec, CHECK_ROUND));
    varkaKernel(cold, projectList(spec, CHECK_ROUND));
    require(cold.missCount() - misses == 2 && cold.size() == 0,
        spec.title() + ": the cold cache did not emit on every call");
    varkaKernel(warm, list);
    long hits = warm.hitCount();
    require(varkaKernel(warm, list).hit() && varkaKernel(warm, projectList(spec, 0)).hit()
        && warm.hitCount() - hits == 2,
        spec.title() + ": the warm cache did not hit, on the same constants or on new ones");

    System.out.println("  Varka: " + compiled.outputs().size() + " outputs, "
        + compiled.inputOrdinals().size() + " inputs, " + compiled.numLiterals()
        + " literal slots, class " + first.entry().shapeHash() + " of "
        + first.entry().classBytes().length + " bytes; Janino and both caches as claimed");
  }
}
