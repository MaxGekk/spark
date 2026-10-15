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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.sun.management.HotSpotDiagnosticMXBean;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaCompiledCheck.Tier;

/**
 * The JVM's own account of the tier it serves the kernels at and of the Vector API's intrinsics,
 * read back from its flags through the HotSpotDiagnostic MXBean (VARKA-272). A forked arm claims
 * a tier ({@code -Dvarka.fuzz.tier}) and a vector setting ({@code -Dvarka.fuzz.vectorSupport});
 * the compiled test holds the JVM to the claim before it runs, so that an arm whose command line
 * lost a flag fails as "not the tier it claims" and never as a kernel's answer, and prints the
 * facts as one {@code platform:} line, which {@code dev/varka_platform_report.py} reads.
 *
 * <p>{@code EnableVectorSupport} is an experimental flag: locked, the MXBean says it does not
 * exist, so an arm that claims a vector setting passes {@code -XX:+UnlockExperimentalVMOptions}.
 * {@code -XX:+PrintFlagsFinal} is no substitute: it shows the flag false by default on every JDK,
 * since {@code modules.cpp} turns it on only when the vector module is defined, after the flags
 * have printed.
 *
 * @param flags each flag of {@link #FLAGS} the JVM has, with its value, in that order
 * @param javaHome {@code java.home}, which names the build - the fastdebug one by its path
 * @param vmVersion {@code java.vm.version}
 */
public record VarkaPlatformFacts(Map<String, String> flags, String javaHome, String vmVersion) {

  /** The flags the line names, in its order; one the JVM does not have is left out. */
  public static final List<String> FLAGS = List.of("UseCompiler", "UseInterpreter",
      "TieredCompilation", "TieredStopAtLevel", "CompilationMode", "BackgroundCompilation",
      "EnableVectorSupport", "StressLCM", "StressGCM", "StressIGVN", "StressCCP",
      "StressIncrementalInlining", "StressMacroExpansion", "StressUnstableIfTraps", "StressSeed",
      "RepeatCompilation");

  public VarkaPlatformFacts {
    flags = Collections.unmodifiableMap(new LinkedHashMap<>(flags));
  }

  /** This JVM's facts. */
  public static VarkaPlatformFacts read() {
    var bean = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
    var flags = new LinkedHashMap<String, String>();
    for (String name : FLAGS) {
      try {
        flags.put(name, bean.getVMOption(name).getValue());
      } catch (IllegalArgumentException e) {
        // Locked (an experimental flag without the unlock) or not in this build: left out, and
        // a claim about it is then a contradiction that names the unlock.
      }
    }
    return new VarkaPlatformFacts(flags, System.getProperty("java.home"),
        System.getProperty("java.vm.version"));
  }

  /** The line a report reads: {@code platform: java.home=... vm=... Flag=value ...}. */
  public String line() {
    var out = new StringBuilder("platform: java.home=").append(javaHome)
        .append(" vm=").append(vmVersion);
    for (String name : FLAGS) {
      String value = flags.get(name);
      if (value != null) {
        out.append(' ').append(name).append('=').append(value);
      }
    }
    return out.toString();
  }

  /**
   * What contradicts the claim: each flag {@code tier} needs, with the value found, and the vector
   * flag against {@code vectorSupport} where that is claimed. Empty when the JVM is what the run
   * says.
   */
  public List<String> contradictions(Tier tier, Optional<Boolean> vectorSupport) {
    var out = new ArrayList<String>();
    switch (tier) {
      case INTERPRETER -> expect(out, "UseCompiler", "false");
      case C1 -> {
        expect(out, "UseCompiler", "true");
        expect(out, "TieredStopAtLevel", "1");
      }
      case C2 -> {
        expect(out, "UseCompiler", "true");
        expect(out, "UseInterpreter", "true");
        expect(out, "TieredStopAtLevel", "4");
      }
      case XCOMP -> {
        expect(out, "UseCompiler", "true");
        expect(out, "UseInterpreter", "false");
      }
    }
    if (vectorSupport.isPresent()) {
      if (!flags.containsKey("EnableVectorSupport")) {
        out.add("EnableVectorSupport is not readable in this JVM; pass "
            + "-XX:+UnlockExperimentalVMOptions, which makes the experimental flag visible");
      } else {
        expect(out, "EnableVectorSupport", vectorSupport.get().toString());
      }
    }
    return out;
  }

  private void expect(List<String> out, String flag, String want) {
    String got = flags.get(flag);
    if (!want.equals(got)) {
      out.add(flag + "=" + got + " where the claimed tier needs " + want);
    }
  }
}
