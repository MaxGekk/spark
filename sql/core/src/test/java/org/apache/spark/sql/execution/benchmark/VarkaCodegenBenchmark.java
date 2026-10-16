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
import java.util.Optional;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.metrics.source.CodegenMetrics;
import org.apache.spark.sql.VarkaBenchmark;
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
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaEmitOptions;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelWarmup;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaLoopEmitter;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaShapeCache;
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
      // Janino's cache must be smaller than the cycle, or a cold round could come back cached.
      int janinoCapacity = SQLConf.get().codegenCacheMaxEntries();
      require(janinoCapacity < COLD_ROUNDS, "Janino's cache holds " + janinoCapacity
          + " classes, so a cycle of " + COLD_ROUNDS + " rounds of constants can hit it");
      // A private cache of capacity 0 is the cache disabled: every lookup emits and defines, and
      // the entry is evicted (its loader released) as it is loaded. The warm one keeps every
      // list's class.
      var cold = new VarkaShapeCacheImpl(0);
      var warm = new VarkaShapeCacheImpl(LISTS.size());
      for (ListSpec spec : LISTS) {
        // Every list a timed call compiles is built here, so that no row times building it.
        var rounds = new Prepared[COLD_ROUNDS];
        for (int r = 0; r < COLD_ROUNDS; r++) {
          rounds[r] = prepare(spec, r);
        }
        Prepared hit = prepare(spec, HIT_ROUND);
        checkSameList(spec, rounds, hit, cold, warm);

        VarkaBenchmark benchmark = benchmark(spec.title(), 1).timed().build();
        benchmark.addCase("spark: generate + Janino compile, cold", i -> {
          sparkColdCalls++;
          sink = sparkProjection(rounds[nextColdRound()]);
        });
        benchmark.addCase("varka: compile + emit + define + load + instantiate, cold", i -> {
          varkaColdCalls++;
          sink = varkaKernel(cold, rounds[nextColdRound()]).entry().newKernel();
        });
        benchmark.addCase("spark: generate, class cache hit", i -> sink = sparkProjection(hit));
        benchmark.addCase("varka: compile, shape cache hit",
            i -> sink = varkaKernel(warm, hit).entry().newKernel());

        Counts before = Counts.read(this, cold, warm);
        runCases(benchmark);
        checkTimedCalls(spec, before, Counts.read(this, cold, warm));
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

  /** The cold calls each arm made, which the table's check compares with what was compiled. */
  private long sparkColdCalls;
  private long varkaColdCalls;

  private int nextColdRound() {
    int round = coldRound;
    coldRound = (coldRound + 1) % COLD_ROUNDS;
    return round;
  }

  /**
   * One list in the two forms the arms take: Spark's {@code Seq<Expression>} and Varka's {@code
   * Seq<NamedExpression>}, over the same entries. Java sees {@code NamedExpression}, a Scala
   * trait, as an interface apart from {@code Expression}, so one Scala sequence cannot be both.
   */
  private record Prepared(int size, Seq<Expression> spark, Seq<NamedExpression> varka) {
  }

  private static Prepared prepare(ListSpec spec, int round) {
    List<NamedExpression> list = projectList(spec, round);
    var entries = new ArrayList<Expression>(list.size());
    for (NamedExpression e : list) {
      // Every entry is an Alias, which is both.
      entries.add((Expression) e);
    }
    return new Prepared(list.size(), seq(entries), seq(list));
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
  private static Object sparkProjection(Prepared list) {
    return GenerateUnsafeProjection.generate(list.spark(), LIST_INPUT);
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
   * the lookup, so the cold row holds the same one build and one definition. The check after the
   * table confirms that the JVM-wide cache built nothing while it ran.
   */
  private static VarkaShapeLookup varkaKernel(VarkaShapeCacheImpl cache, Prepared list) {
    CompiledVarkaProjection compiled = varkaCompile(list);
    return cache.getOrEmit(Utils.getContextOrSparkClassLoader(), shapeKey(compiled), EXECUTION);
  }

  /** Varka's compiler over the whole list, refusing one it does not fuse entry for entry. */
  private static CompiledVarkaProjection varkaCompile(Prepared list) {
    Optional<CompiledVarkaProjection> compiled =
        VarkaExpressionCompiler.compile(list.varka(), LIST_INPUT);
    if (compiled.isEmpty()) {
      throw requirementFailed("Varka does not fuse every entry of a list of " + list.size()
          + "; the declines by position: "
          + VarkaExpressionCompiler.declines(list.varka(), LIST_INPUT));
    }
    return compiled.get();
  }

  /**
   * The key the evaluator builds ({@code VarkaEvaluatorBase.shapeKey}) under the default emit
   * options, warmed as the session's warm-up setting says, as the compiler's admission keys it.
   */
  private static VarkaShapeKey shapeKey(CompiledVarkaProjection compiled) {
    return new VarkaShapeKey(compiled.outputs(), compiled.inputOrdinals().size(),
        compiled.numLiterals(), VarkaEmitOptions.DEFAULTS,
        VarkaKernelWarmup.warms(SQLConf.get().varkaWarmupEnabled()));
  }

  /** How many classes Janino has compiled in this JVM; a cache hit compiles none. */
  private static long janinoCompilations() {
    return CodegenMetrics.METRIC_COMPILATION_TIME().getCount();
  }

  /** The counters a table's check reads before and after the table is timed. */
  private record Counts(long sparkColdCalls, long varkaColdCalls, long janinoCompilations,
      long coldMisses, long warmMisses, long sharedBuilds) {
    static Counts read(VarkaCodegenBenchmark bench, VarkaShapeCacheImpl cold,
        VarkaShapeCacheImpl warm) {
      // Qualified: inside the record, the bare name is the component's accessor.
      return new Counts(bench.sparkColdCalls, bench.varkaColdCalls,
          VarkaCodegenBenchmark.janinoCompilations(), cold.missCount(), warm.missCount(),
          VarkaShapeCache.buildCount());
    }
  }

  /**
   * Checks, after a table is timed, that its rows were what they say over every call they timed
   * and warmed: Janino compiled once per cold call, plus at most once for the hit list, whose
   * class the cold rows' hundreds of new classes push out of its cache before the hit row's
   * warm-up compiles it back; Varka's cold cache emitted once per cold call; its warm cache
   * emitted nothing; and the JVM-wide cache, where the compiler's admission asks, built nothing.
   */
  private static void checkTimedCalls(ListSpec spec, Counts before, Counts after) {
    if (dryRun()) {
      return;
    }
    long sparkCold = after.sparkColdCalls() - before.sparkColdCalls();
    long compiled = after.janinoCompilations() - before.janinoCompilations();
    require(compiled == sparkCold || compiled == sparkCold + 1, spec.title() + ": Janino compiled "
        + compiled + " classes over " + sparkCold + " cold calls, so a cold row hit its cache");
    long varkaCold = after.varkaColdCalls() - before.varkaColdCalls();
    require(after.coldMisses() - before.coldMisses() == varkaCold, spec.title()
        + ": Varka's cold cache did not emit on each of its " + varkaCold + " calls");
    require(after.warmMisses() == before.warmMisses(),
        spec.title() + ": Varka's warm cache missed while its row was timed");
    require(after.sharedBuilds() == before.sharedBuilds(), spec.title()
        + ": the compiler's admission built a class while the table was timed");
  }

  /**
   * Prints the list and checks, before its table is timed, what the arms claim: that Varka fuses
   * every entry into one output each, that its shape is the same in every round, that new
   * constants compile in Janino and a repeated list does not, that Varka's cold cache emits on
   * every call and its warm one hits on the same constants and on new ones, and that a repeated
   * compile asks the admission without building again. The warm cache is warmed here, by its
   * first lookup of the hit list. {@link #checkTimedCalls} checks the timed calls themselves.
   */
  private static void checkSameList(ListSpec spec, Prepared[] rounds, Prepared hit,
      VarkaShapeCacheImpl cold, VarkaShapeCacheImpl warm) {
    System.out.println("VARKA-259 list \"" + spec.title() + "\", the hit round:");
    for (Expression e : CollectionConverters.asJava(hit.spark())) {
      System.out.println("  " + e.sql());
    }

    CompiledVarkaProjection compiled = varkaCompile(hit);
    require(compiled.outputs().size() == spec.size(), spec.title() + ": Varka made "
        + compiled.outputs().size() + " outputs of " + spec.size() + " entries");
    VarkaShapeKey key = shapeKey(compiled);
    Prepared check = prepare(spec, CHECK_ROUND);
    Prepared checkNext = prepare(spec, CHECK_ROUND + 1);
    for (Prepared other : List.of(rounds[0], rounds[COLD_ROUNDS - 1], check, checkNext)) {
      require(shapeKey(varkaCompile(other)).equals(key),
          spec.title() + ": Varka's shape with other constants differs from the hit list's");
    }
    long builds = VarkaShapeCache.buildCount();
    varkaCompile(hit);
    require(VarkaShapeCache.buildCount() == builds,
        spec.title() + ": the compiler's admission built a class it had already built");

    long compiles = janinoCompilations();
    sparkProjection(check);
    sparkProjection(checkNext);
    require(janinoCompilations() - compiles == 2,
        spec.title() + ": Janino did not compile two new rounds twice");
    sparkProjection(hit);
    compiles = janinoCompilations();
    sparkProjection(hit);
    require(janinoCompilations() == compiles,
        spec.title() + ": Janino compiled the hit list again");

    long misses = cold.missCount();
    VarkaShapeLookup first = varkaKernel(cold, check);
    varkaKernel(cold, check);
    require(cold.missCount() - misses == 2 && cold.size() == 0,
        spec.title() + ": the cold cache did not emit on every call");
    varkaKernel(warm, hit);
    long hits = warm.hitCount();
    require(varkaKernel(warm, hit).hit() && varkaKernel(warm, rounds[0]).hit()
        && warm.hitCount() - hits == 2,
        spec.title() + ": the warm cache did not hit, on the same constants or on new ones");

    System.out.println("  Varka: " + compiled.outputs().size() + " outputs, "
        + compiled.inputOrdinals().size() + " inputs, " + compiled.numLiterals()
        + " literal slots, class " + first.entry().shapeHash() + " of "
        + first.entry().classBytes().length + " bytes; Janino and both caches as claimed");
  }
}
