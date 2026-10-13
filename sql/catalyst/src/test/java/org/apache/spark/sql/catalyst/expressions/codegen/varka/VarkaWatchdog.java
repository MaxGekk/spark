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

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import org.apache.spark.util.Utils;

/**
 * A cap on each test of a Varka suite that ends a hung test by name. {@code SparkFunSuite}
 * already wraps every test in {@code failAfter(20 minutes)}, but ScalaTest's {@code failAfter}
 * without a signaler cannot stop a running body: it reports the overrun after the body returns,
 * and a body that never returns is never reported. A compiler loop that does not terminate
 * therefore held the fork's one Build slot for the job's whole limit, printing only ScalaTest's
 * "still running", and named no test ({@code m6/PLAN.md} row 226). Interrupting the test thread
 * would not stop such a loop either, since a CPU-bound loop checks no interrupt.
 *
 * <p>So each test runs beside a watchdog thread. At the cap it interrupts the test thread, which
 * ends a body that blocks or checks interrupts, and gives it a grace period; a body still
 * running after that is not going to stop, so the watchdog writes every thread's stack under
 * the test's name and halts the JVM. The test runner then reports the suite aborted, and the
 * log's last lines say which test and what it was doing, minutes after the hang rather than at
 * the job's limit.
 *
 * <p>The cap is {@code varka.test.watchdog.minutes}, ten by default: the slowest Varka test on
 * record, the width audit's census, takes about two minutes on the laptop, and the suites' CI
 * jobs finish in eleven to seventeen minutes whole, so a single test past ten is a hang. The
 * mechanism knows no test framework; {@code VarkaTestWatchdog} is its ScalaTest face (VARKA-312).
 */
public final class VarkaWatchdog {

  private VarkaWatchdog() {}

  /** The exit status of a JVM the watchdog halted. */
  public static final int HALT_STATUS = 3;

  /** The property naming the cap in minutes. */
  public static final String MINUTES_PROPERTY = "varka.test.watchdog.minutes";

  private static final String WATCHDOG = "VARKA WATCHDOG:";

  /** The cap this JVM's tests run under, in minutes. */
  public static long defaultMinutes() {
    return Long.parseLong(System.getProperty(MINUTES_PROPERTY, "10"));
  }

  /**
   * Runs {@code body} under a watchdog of {@code minutes}, and thirty seconds of grace after the
   * interrupt, reporting to standard error and halting the JVM.
   */
  public static <T> T guard(String suite, String test, long minutes,
      VarkaMatrixRuns.Body<T> body) throws Throwable {
    return guard(suite, test, minutes * 60 * 1000L, 30 * 1000L, System.err::println,
        status -> Runtime.getRuntime().halt(status), body);
  }

  /**
   * The mechanism, with its clock and its effects as parameters so that it can be tested
   * without waiting minutes or losing the JVM: {@code report} receives the two messages the
   * watchdog writes, and {@code halt} is what it does to a body that survives the interrupt.
   */
  public static <T> T guard(String suite, String test, long millis, long graceMillis,
      Consumer<String> report, IntConsumer halt, VarkaMatrixRuns.Body<T> body) throws Throwable {
    Thread worker = Thread.currentThread();
    Thread watchdog = new Thread(() -> {
      try {
        Thread.sleep(millis);
        report.accept(WATCHDOG + " '" + test + "' of " + suite + " has run for " + millis
            + " ms; interrupting it, and halting the JVM in " + graceMillis
            + " ms if it is still running");
        worker.interrupt();
        Thread.sleep(graceMillis);
        report.accept(WATCHDOG + " '" + test + "' of " + suite + " is still running; every "
            + "thread's stack, then the JVM halts with status " + HALT_STATUS + ":\n"
            + Arrays.stream(Utils.getThreadDump()).map(Object::toString)
                .collect(Collectors.joining("\n")));
        halt.accept(HALT_STATUS);
      } catch (InterruptedException e) {
        // The test ended and its finally below interrupted this thread: nothing to do.
      }
    }, "varka-watchdog: " + test);
    watchdog.setDaemon(true);
    watchdog.start();
    try {
      return body.run();
    } finally {
      watchdog.interrupt();
    }
  }
}
