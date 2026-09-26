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

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * Keeps HotSpot's first-tier compiler, C1, away from the emitted kernel classes, so that their
 * methods go from the interpreter to C2 and to nothing in between. One compiler directive,
 * added once per JVM when the first kernel warm-up starts ({@link VarkaKernelWarmup}).
 *
 * <p><b>Why.</b> A kernel's loop and epilogue methods are too large for C1 to compile with full
 * profiling, tier 3: its LIR generator runs out of virtual registers, the compile is skipped with
 * "retry at different tier", and the method is profiled in the interpreter until C2 takes it. But
 * they are not always too large for tier 2, the limited-profile code the tiered policy asks C1
 * for instead of tier 3 while the C2 queue is long. A method compiled at tier 2 that way is
 * stranded: from tier 2 the policy climbs only to tier 3, which C1 cannot compile, and tier-2 code
 * does not update the profile a direct climb to C2 would read. It then runs C1 code, boxing every
 * vector operation, for as long as its class lives. Whether a kernel falls into this depends on
 * how busy C2 is at the moment the kernel crosses its first threshold, which a new query's own
 * compiles make likely (`PLAN_TASK_212.md` 10).
 *
 * <p>With C1 excluded, the first C1 request for a kernel method is refused and marks the method
 * not C1-compilable - where a tier-3 failure leaves it anyway - so the interpreter profiles it and
 * C2 compiles it, whatever the queues were doing. C1 code for these methods boxes every vector
 * operation just as the interpreter does, so the tiers lose nothing - but the profiling starts
 * later: the failed tier-3 request is what creates a method's profile, and without it the
 * interpreter creates one only at twice the tier-3 threshold. A kernel fed by its own batches, with
 * no warm-up, then reaches C2 about a hundred batches later - one more slow query of two million
 * rows at 54 entries (`PLAN_TASK_212.md` 10.6). That is why the directive comes with the warm-up,
 * where C2 is busiest and a stranded method would never be warm, rather than with the first
 * kernel: a JVM that never warms a kernel compiles its kernels as it did before.
 *
 * <p><b>How.</b> The DiagnosticCommand MBean's {@code compilerDirectivesAdd}, the in-process form
 * of {@code jcmd Compiler.directives_add}, reads the directive from a file. It matches the shape
 * cache's class-name prefix, so no other method in the JVM is affected. It is skipped, with one
 * log line, where C2 is not the top tier - excluding C1 there would leave the kernels interpreted
 * for good - and where the JVM has no such command.
 */
public final class VarkaKernelCompileDirective {

  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(VarkaKernelCompileDirective.class);

  /** The kernel classes' methods, in the directive syntax: slashes, and a trailing wildcard. */
  public static final String METHOD_PATTERN =
      VarkaShapeCacheImpl.CLASS_NAME_PREFIX.replace('.', '/') + "*.*";

  private static volatile boolean attempted;

  private static volatile boolean installed;

  private VarkaKernelCompileDirective() {
  }

  /** Adds the directive the first time it is called in this JVM; later calls return at once. */
  public static void ensureInstalled() {
    if (!attempted) {
      synchronized (VarkaKernelCompileDirective.class) {
        if (!attempted) {
          installed = install();
          attempted = true;
        }
      }
    }
  }

  /** Whether this JVM has the directive: installed, and not skipped or failed. */
  public static boolean installed() {
    return installed;
  }

  private static boolean install() {
    try {
      if (!c2IsTopTier()) {
        LOG.info("Varka leaves C1 enabled for its kernel classes: C2 is not this JVM's top tier.");
        return false;
      }
      Path file = Files.createTempFile("varka-kernel-directive", ".json");
      try {
        Files.writeString(file,
            "[{ match: \"" + METHOD_PATTERN + "\", c1: { Exclude: true } }]");
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName command = new ObjectName("com.sun.management:type=DiagnosticCommand");
        Object reply = server.invoke(command, "compilerDirectivesAdd",
            new Object[] {new String[] {file.toString()}},
            new String[] {String[].class.getName()});
        boolean added = String.valueOf(reply).contains("added");
        if (added) {
          LOG.info("Varka excluded C1 for its kernel classes (" + METHOD_PATTERN + "): their "
              + "methods go from the interpreter to C2.");
        } else {
          LOG.warn("Varka could not exclude C1 for its kernel classes; the JVM answered: "
              + reply);
        }
        return added;
      } finally {
        Files.deleteIfExists(file);
      }
    } catch (Exception | LinkageError e) {
      // No DiagnosticCommand MBean (a JVM that is not HotSpot), no writable temporary directory,
      // or a JVM that refuses the directive: the kernels then compile as HotSpot decides.
      LOG.warn("Varka could not exclude C1 for its kernel classes.", e);
      return false;
    }
  }

  /** Whether C2 is the top tier: tiered compilation on, up to level 4, and not a JVMCI compiler. */
  private static boolean c2IsTopTier() {
    HotSpotDiagnosticMXBean bean =
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    if (bean == null) {
      return false;
    }
    return "true".equals(option(bean, "TieredCompilation"))
        && "4".equals(option(bean, "TieredStopAtLevel"))
        && !"true".equals(option(bean, "UseJVMCICompiler"));
  }

  private static String option(HotSpotDiagnosticMXBean bean, String name) {
    try {
      return bean.getVMOption(name).getValue();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
