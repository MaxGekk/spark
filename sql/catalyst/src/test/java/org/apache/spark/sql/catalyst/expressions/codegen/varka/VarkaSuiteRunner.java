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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.platform.commons.support.AnnotationSupport;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.scalatest.Reporter;
import org.scalatest.events.Event;
import org.scalatest.events.RunAborted;
import org.scalatest.events.RunCompleted;
import org.scalatest.events.Summary;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuiteListener.Counts;

/**
 * Runs ScalaTest suites and {@link VarkaSuite} JUnit classes in one JVM, as the matrix's and the
 * gate's JVMs do (VARKA-312): {@code dev/varka_matrix.sh} starts this main where it started
 * ScalaTest's {@code org.scalatest.tools.Runner}, with the same arguments - {@code -u <dir>},
 * {@code -s <class>} per suite, and ScalaTest's output flags such as {@code -oW}.
 *
 * <p>It sorts the named classes by reflection, without initialising them: an
 * {@code org.scalatest.Suite} goes to ScalaTest's Runner with every flag as given, a
 * {@code @VarkaSuite} class to the JUnit Platform Launcher with a {@link VarkaSuiteListener},
 * which prints its tests as ScalaTest would and writes its {@code TEST-<class>.xml} into the same
 * {@code -u} directory. ScalaTest's suites run first, and its Runner prints its own summary; then
 * the JUnit classes; then one combined {@code Suites:} line and one {@code Tests:} line over both,
 * last, because {@code dev/varka_matrix_report.py} reads the last of each in a JVM's log. A shard
 * with no JUnit class prints ScalaTest's counts again. ScalaTest's counts come from a reporter
 * this class attaches ({@code -C}) that keeps the run's summary, so nothing parses the output.
 *
 * <p>Exit status 0 when nothing failed or aborted on either side, 1 otherwise, as ScalaTest's
 * Runner exits.
 */
public final class VarkaSuiteRunner {

  private VarkaSuiteRunner() {}

  public static void main(String[] args) throws Exception {
    // Exit explicitly, as ScalaTest's main does: a suite may leave a non-daemon thread behind.
    System.exit(run(args) ? 0 : 1);
  }

  /** The named suites run as described above; true when nothing failed or aborted. */
  public static boolean run(String[] args) throws Exception {
    var scalaArgs = new ArrayList<String>();
    var scalaSuites = new ArrayList<String>();
    var junitClasses = new ArrayList<Class<?>>();
    Optional<File> reportDir = Optional.empty();
    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-s") && i + 1 < args.length) {
        String name = args[++i];
        Class<?> c = Class.forName(name, false, loader);
        if (org.scalatest.Suite.class.isAssignableFrom(c)) {
          scalaSuites.add(name);
        } else if (AnnotationSupport.isAnnotated(c, VarkaSuite.class)) {
          junitClasses.add(c);
        } else {
          throw new IllegalArgumentException(
              name + " is neither a ScalaTest suite nor a @VarkaSuite JUnit class");
        }
      } else {
        if (args[i].equals("-u") && i + 1 < args.length) {
          reportDir = Optional.of(new File(args[i + 1]));
        }
        scalaArgs.add(args[i]);
      }
    }

    Counts scala = Counts.ZERO;
    boolean scalaPassed = true;
    if (!scalaSuites.isEmpty()) {
      scalaArgs.add("-C");
      scalaArgs.add(SummaryReporter.class.getName());
      for (String suite : scalaSuites) {
        scalaArgs.add("-s");
        scalaArgs.add(suite);
      }
      SummaryReporter.LAST.set(null);
      scalaPassed = org.scalatest.tools.Runner.run(scalaArgs.toArray(new String[0]));
      Counts reported = SummaryReporter.LAST.get();
      // A run ScalaTest aborted leaves no summary; it counts as one aborted suite, as the
      // matrix's report counts a JVM that printed no Suites: line.
      scala = reported != null ? reported : new Counts(0, 0, 0, 0, 0, 0, 1);
    }

    Counts junit = Counts.ZERO;
    if (!junitClasses.isEmpty()) {
      var listener = new VarkaSuiteListener(reportDir, System.out);
      var config = LauncherConfig.builder()
          .enableTestExecutionListenerAutoRegistration(false)
          .addTestExecutionListeners(listener)
          .build();
      List<DiscoverySelector> selectors = junitClasses.stream()
          .<DiscoverySelector>map(DiscoverySelectors::selectClass).toList();
      LauncherDiscoveryRequest request =
          LauncherDiscoveryRequestBuilder.request().selectors(selectors).build();
      LauncherFactory.create(config).execute(request);
      junit = listener.counts();
    }

    Counts total = scala.plus(junit);
    System.out.println("Run completed: " + scalaSuites.size() + " ScalaTest suite(s) and "
        + junitClasses.size() + " JUnit class(es) in this JVM.");
    System.out.println("Total number of tests run: "
        + (total.succeeded() + total.failed() + total.canceled()));
    System.out.println(total.suitesLine());
    System.out.println(total.testsLine());
    boolean passed = scalaPassed && total.failed() == 0 && total.suitesAborted() == 0;
    System.out.println(passed ? "All tests passed." : "*** " + total.failed()
        + " TESTS FAILED, " + total.suitesAborted() + " SUITES ABORTED ***");
    return passed;
  }

  /**
   * The ScalaTest reporter {@link #run} attaches: keeps the counts of the run's summary, which
   * ScalaTest's own summary lines print. ScalaTest builds it by its class name, so it hands the
   * counts back through a static.
   */
  public static final class SummaryReporter implements Reporter {

    static final AtomicReference<Counts> LAST = new AtomicReference<>();

    @Override
    public void apply(Event event) {
      if (event instanceof RunCompleted completed && completed.summary().isDefined()) {
        Summary s = completed.summary().get();
        LAST.set(new Counts(s.testsSucceededCount(), s.testsFailedCount(),
            s.testsCanceledCount(), s.testsIgnoredCount(), s.testsPendingCount(),
            s.suitesCompletedCount(), s.suitesAbortedCount()));
      } else if (event instanceof RunAborted aborted && aborted.summary().isDefined()) {
        Summary s = aborted.summary().get();
        LAST.set(new Counts(s.testsSucceededCount(), s.testsFailedCount(),
            s.testsCanceledCount(), s.testsIgnoredCount(), s.testsPendingCount(),
            s.suitesCompletedCount(), s.suitesAbortedCount() + 1));
      }
    }
  }
}
