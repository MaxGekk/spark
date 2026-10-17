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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import javax.management.JMException;
import javax.management.ObjectName;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.codegen.CompiledVarkaProjection;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.sql.types.DataTypes;

/**
 * The child process behind {@code dev/varka_inlining_cliff.sh} and {@code VarkaInliningCliffSuite}
 * (VARKA-209): does a kernel's loop method keep its vector intrinsics, or run as the Vector API's
 * scalar fallback, and does the same class land on either side from one JVM to the next?
 *
 * <p>It emits VARKA-198's cheap-tail shape - {@code year(d) + k} for {@code k} up to the output
 * count, over one date column - at the given fused ceiling, which decides how many loop methods the
 * outputs are grouped into (400 puts sixty-four in one group of 225 {@code IntVector} call sites,
 * 100 in two, 50 in six), drives the kernel over null-free batches for a number of seconds, and
 * prints its rate every second. The verdict is the rate of the last seconds: the two sides
 * VARKA-198 measured are about 4 and 250 nanoseconds a row apart, and
 * {@code dev/varka_inlining_cliff.py} cuts between them. The evidence on why is the JVM's, from the
 * flags the launcher passes: {@code PrintInlining} and {@code PrintIntrinsics} scoped to the
 * emitted class print into this process's output, between the markers, and
 * {@code -XX:+LogCompilation} writes the compile log {@code dev/varka_c2_report.py} reads.
 *
 * <p>Two shapes: {@code cheap}, the tails above, and {@code makedate}, the deopt guard's
 * {@code make_date(year(d), month(d), k)} outputs, whose every call site carries more of C1's
 * virtual registers. And an optional compiler directive on the emitted class, {@code inline},
 * which forces {@code VarkaVectorSupport}'s helpers inline whatever C2's budgets say and raises the
 * class's node limit: the arm that asks whether steering the JIT lifts the cliff on a stock JDK.
 *
 * <p>The last two arguments are the emitter's call-site budget and its heavy-group exemption, by
 * default the production values; a budget of {@code 0} turns it off, the control it is measured
 * against, and an exemption of {@code 0} splits the make_date shape too. Either may be
 * {@code default}, which is how the launcher asks for the production value of one while it sets
 * the other.
 *
 * <p>Lines the reader looks for:
 * <ul>
 *   <li>{@code VARKA_CLIFF_BEGIN=<class> pid=<pid> outputs=<n> ceiling=<c> c1=<on|off>
 *       xbatch=<on|off> shape=<cheap|makedate> directive=<none|inline> budget=<sites>
 *       heavy=<outputs>};</li>
 *   <li>{@code VARKA_CLIFF_METHODS=<method>:<bytes>:<vector call sites>,...} for the loop
 *       methods, the sites in the budget's unit;</li>
 *   <li>{@code VARKA_CLIFF_RATE=<second> <nanoseconds a row>} once a second;</li>
 *   <li>{@code VARKA_CLIFF_ALLOC=<bytes a call>} over the last second, since a scalar fallback
 *       boxes;</li>
 *   <li>{@code VARKA_CLIFF_DONE=<class> status=<status>}.</li>
 * </ul>
 *
 * <p>{@code c1off} keeps C1 off the emitted class through a compiler directive, as the kernel
 * warm-up keeps it in production; the default is C1 on, the condition VARKA-198 measured under.
 * Its timed loop is the Scala form's statement for statement (VARKA-321).
 */
public final class VarkaInliningCliffProbe {

  private VarkaInliningCliffProbe() {
  }

  public static final String BEGIN_PREFIX = "VARKA_CLIFF_BEGIN=";
  public static final String METHODS_PREFIX = "VARKA_CLIFF_METHODS=";
  public static final String RATE_PREFIX = "VARKA_CLIFF_RATE=";
  public static final String ALLOC_PREFIX = "VARKA_CLIFF_ALLOC=";
  public static final String DONE_PREFIX = "VARKA_CLIFF_DONE=";

  /** The emitted classes' prefix, which the launcher's {@code CompileCommand} patterns name. */
  public static final String CLASS_PREFIX = "org.apache.spark.sql.varka.execution.VarkaCliffProbe_";

  public static String className(int outputs, int ceiling, String c1, String shape,
      String directive, int budget, int heavy) {
    return CLASS_PREFIX + outputs + "_" + ceiling + "_" + c1 + "_" + shape + "_" + directive
        + "_" + budget + "_" + heavy;
  }

  /** The C2 options the {@code inline} directive sets on the emitted class. */
  private static final String INLINE_DIRECTIVE = "c2: { inline: [\"+org/apache/spark/sql/varka/"
      + "vector/VarkaVectorSupport.*\"], MaxNodeLimit: 240000 }";

  /** Installs a compiler directive on the emitted classes: C1 excluded, the C2 options, or both. */
  private static void addDirective(boolean excludeC1, boolean inline)
      throws IOException, JMException {
    var parts = new ArrayList<String>();
    if (excludeC1) {
      parts.add("c1: { Exclude: true }");
    }
    if (inline) {
      parts.add(INLINE_DIRECTIVE);
    }
    var file = Files.createTempFile("varka-cliff-probe-directive", ".json");
    try {
      Files.writeString(file, "[{ match: \"org/apache/spark/sql/varka/execution/"
          + "VarkaCliffProbe_*.*\", " + String.join(", ", parts) + " }]");
      Object reply = ManagementFactory.getPlatformMBeanServer().invoke(
          new ObjectName("com.sun.management:type=DiagnosticCommand"), "compilerDirectivesAdd",
          new Object[] {new String[] {file.toString()}}, new String[] {String[].class.getName()});
      if (!String.valueOf(reply).contains("added")) {
        throw new IllegalArgumentException("the directive was refused: " + reply);
      }
    } finally {
      Files.deleteIfExists(file);
    }
  }

  private static final Seq<Attribute> COLUMNS = CollectionConverters.asScala(
      List.<Attribute>of(VarkaSqlResolve.column("d", DataTypes.DateType))).toSeq();

  /**
   * The projection: VARKA-198's cheap tails, {@code year(d) + k}, the prefix most of each output's
   * work; or the deopt guard's {@code make_date(year(d), month(d), k)} outputs.
   */
  private static CompiledVarkaProjection shape(String kind, int n) {
    var exprs = new ArrayList<NamedExpression>();
    for (int k = 1; k <= n; k++) {
      String sql = switch (kind) {
        case "cheap" -> "year(d) + " + k;
        case "makedate" -> {
          int day = (k - 1) % 28 + 1;
          int yearOffset = (k - 1) / 28;
          String year = yearOffset == 0 ? "year(d)" : "year(d) + " + yearOffset;
          yield "make_date(" + year + ", month(d), " + day + ")";
        }
        default -> throw new IllegalArgumentException("shape " + kind
            + " is not cheap or makedate");
      };
      exprs.add(VarkaSqlResolve.named(VarkaSqlResolve.parse(sql, COLUMNS), "c" + k));
    }
    return VarkaExpressionCompiler.compile(CollectionConverters.asScala(exprs).toSeq(), COLUMNS)
        .orElseThrow(() -> new IllegalStateException("the " + n + "-output " + kind
            + " projection did not fuse"));
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("usage: VarkaInliningCliffProbe <outputs> <fusedCeiling> <seconds> "
          + "[rows] [c1on|c1off] [cheap|makedate] [none|inline] [callSiteBudget|default] "
          + "[heavyGroupOutputs|default]");
      System.exit(2);
    }
    int outputs = Integer.parseInt(args[0]);
    int ceiling = Integer.parseInt(args[1]);
    int seconds = Integer.parseInt(args[2]);
    int rows = args.length > 3 ? Integer.parseInt(args[3]) : 1024;
    String c1 = args.length > 4 ? args[4].replaceFirst("^c1", "") : "on";
    if (!c1.equals("on") && !c1.equals("off")) {
      throw new IllegalArgumentException("requirement failed: C1 is on or off, not " + c1);
    }
    String kind = args.length > 5 ? args[5] : "cheap";
    String directive = args.length > 6 ? args[6] : "none";
    if (!directive.equals("none") && !directive.equals("inline")) {
      throw new IllegalArgumentException(
          "requirement failed: the directive is none or inline, not " + directive);
    }
    int budget = countOrDefault(args, 7, VarkaEmitOptions.DEFAULTS.callSiteBudget());
    int heavy = countOrDefault(args, 8, VarkaEmitOptions.DEFAULTS.heavyGroupOutputs());
    String xbatch = ManagementFactory.getRuntimeMXBean().getInputArguments().contains("-Xbatch")
        ? "on" : "off";
    String name = className(outputs, ceiling, c1, kind, directive, budget, heavy);
    System.out.println(BEGIN_PREFIX + name + " pid=" + ProcessHandle.current().pid()
        + " outputs=" + outputs + " ceiling=" + ceiling + " c1=" + c1 + " xbatch=" + xbatch
        + " shape=" + kind + " directive=" + directive + " budget=" + budget + " heavy=" + heavy);
    if (c1.equals("off") || directive.equals("inline")) {
      addDirective(c1.equals("off"), directive.equals("inline"));
    }
    var fused = shape(kind, outputs);
    byte[] bytes = VarkaLoopEmitter.emit(name, fused.outputs(), fused.inputOrdinals().size(),
        fused.numLiterals(), null, null,
        // The budget is read against the measured split, not the predicted one (VARKA-236).
        VarkaEmitOptions.DEFAULTS.withFusedCeiling(ceiling).withCallSiteBudget(budget)
            .withHeavyGroupOutputs(heavy).withPredictGrouping(false).withPlanSize(false));
    var measured = VarkaEmittedClass.measure(bytes);
    var methods = new ArrayList<String>();
    for (String m : VarkaEmitterTestSupport.methodNames(bytes)) {
      if (VarkaMethodNames.isLoop(m)) {
        methods.add(m + ":" + measured.codeLength().get(m) + ":"
            + measured.vectorCallSites().get(m));
      }
    }
    System.out.println(METHODS_PREFIX + String.join(",", methods));
    var loader = new VarkaGeneratedClassLoader(VarkaInliningCliffProbe.class.getClassLoader());
    loader.defineGeneratedClass(name, bytes);
    var kernel = (VarkaFusedKernel) loader.loadClass(name).getConstructor().newInstance();
    var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    try (var arena = Arena.ofConfined()) {
      var data = arena.allocate(rows * 4L, 64);
      for (int i = 0; i < rows; i++) {
        data.set(ValueLayout.JAVA_INT, i * 4L, i * 7 % 20000 - 10000);
      }
      int[] literals = fused.literals().stream().mapToInt(Integer::intValue).toArray();
      long[] dst = new long[outputs];
      long[] dstValidity = new long[outputs];
      for (int o = 0; o < outputs; o++) {
        dst[o] = arena.allocate(rows * 4L, 64).address();
      }
      for (int o = 0; o < outputs; o++) {
        dstValidity[o] = arena.allocate(rows / 8L + 8, 64).address();
      }
      long[] src = {data.address()};
      long[] noValidity = {0L};
      int[] noNulls = {0};
      int status = 0;
      long start = System.nanoTime();
      long end = start + seconds * 1000000000L;
      int second = 1;
      long windowStart = start;
      long windowRows = 0L;
      long windowCalls = 0L;
      long windowBytes = threads.getCurrentThreadAllocatedBytes();
      double bytesPerCall = 0.0;
      while (System.nanoTime() < end) {
        status |= kernel.run(src, noValidity, noNulls, dst, dstValidity, literals, rows);
        windowRows += rows;
        windowCalls += 1;
        long now = System.nanoTime();
        if (now - windowStart >= 1000000000L) {
          long allocated = threads.getCurrentThreadAllocatedBytes();
          bytesPerCall = (double) (allocated - windowBytes) / windowCalls;
          System.out.println(RATE_PREFIX + second + " "
              + String.format("%.2f", (double) (now - windowStart) / windowRows));
          second += 1;
          windowStart = now;
          windowRows = 0;
          windowCalls = 0;
          windowBytes = allocated;
        }
      }
      System.out.println(ALLOC_PREFIX + String.format("%.0f", bytesPerCall));
      System.out.println(DONE_PREFIX + name + " status=" + status);
    }
  }

  private static int countOrDefault(String[] args, int i, int fallback) {
    return args.length > i && !args[i].equals("default") ? Integer.parseInt(args[i]) : fallback;
  }
}
