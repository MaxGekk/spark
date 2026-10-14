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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordingFile;

/**
 * The JIT's own account of one kernel, for a test whose verdict is a JIT outcome: a JFR recording
 * of every compile, compile failure, deoptimization and inlining decision, and of the allocation
 * samples, read back for one kernel class when the test fails (VARKA-295).
 *
 * <p>A warm-up that ends at its deadline leaves a kernel that still allocates, and the allocation
 * alone cannot say why: a loop method still interpreted, a compile that failed, a deoptimization
 * cycle, or a compiled loop whose vectors C2 could not keep in registers. This tells them apart.
 * Every kernel method's compiles with their tier and outcome; the deoptimizations of its compiled
 * code with their reason; the inlining C2 refused in its compiles, and every Vector API class it
 * inlined there, where a second class of one lane type is the species pollution of
 * {@code vector-api-and-width.md}; and where the warm-up thread's allocation samples of the last
 * seconds came from, with each frame's kind (interpreted, compiled, inlined).
 *
 * <p>The events are the JDK's own, enabled by name, with no flag on the JVM. The recording costs
 * the JVM one event per compile and inlining decision while it runs, so a test starts it just
 * before the queries it checks and closes it on every path.
 */
public final class VarkaJitEvidence implements AutoCloseable {

  /** The summary's bound, so that a failure message stays readable in a CI log. */
  static final int MAX_CHARS = 8000;

  private static final int TOP = 8;

  private static final long MAX_RECORDING_BYTES = 256L << 20;

  private final Recording recording = new Recording();
  private final Instant started = Instant.now();

  private VarkaJitEvidence() {
  }

  /** Starts a recording of the JIT's events and the JVM's allocation samples. */
  public static VarkaJitEvidence start() {
    var evidence = new VarkaJitEvidence();
    var r = evidence.recording;
    r.enable("jdk.Compilation").withThreshold(Duration.ZERO);
    r.enable("jdk.CompilationFailure");
    r.enable("jdk.Deoptimization").withoutStackTrace();
    r.enable("jdk.CompilerInlining");
    r.enable("jdk.CodeCacheFull");
    r.enable("jdk.ObjectAllocationSample").withStackTrace().with("throttle", "1000/s");
    // On disk and bounded: the recording runs through a whole warm-up, up to its deadline, in a
    // JVM whose compilers may be busy with everything else.
    r.setToDisk(true);
    r.setMaxSize(MAX_RECORDING_BYTES);
    r.start();
    return evidence;
  }

  /**
   * Stops the recording and summarizes what it holds about {@code className}'s methods, and the
   * allocation samples of the threads whose name starts with {@code threadName} over the
   * recording's last {@code tail} - a prefix, since Spark names a task's thread after the task
   * ({@code Executor.TASK_THREAD_NAME_PREFIX}). Never throws: a recording that cannot be read
   * says so in the summary.
   */
  public String summary(String className, String threadName, Duration tail) {
    try {
      recording.stop();
      Path dump = Files.createTempFile("varka-jit-evidence", ".jfr");
      try {
        recording.dump(dump);
        return render(dump, className, threadName, tail);
      } finally {
        Files.deleteIfExists(dump);
      }
    } catch (IOException | RuntimeException e) {
      return "JIT evidence unavailable: " + e;
    }
  }

  @Override
  public void close() {
    recording.close();
  }

  /**
   * Reads the dump twice, an event at a time - a whole recording in memory could exhaust the very
   * JVM whose failure it is meant to explain: the first pass finds the kernel's compiles and the
   * recording's end, the second reads everything that refers to them.
   */
  private String render(Path dump, String className, String threadName, Duration tail)
      throws IOException {
    var out = new StringBuilder("JIT evidence for ").append(className).append(": ");
    // Compile ids of the kernel's compiles, and the method each was for.
    var kernelCompiles = new HashMap<Integer, String>();
    var compiles = new ArrayList<String>();
    Instant end = started;
    try (var file = new RecordingFile(dump)) {
      while (file.hasMoreEvents()) {
        RecordedEvent e = file.readEvent();
        if (e.getEndTime().isAfter(end)) {
          end = e.getEndTime();
        }
        RecordedMethod m = e.getEventType().getName().equals("jdk.Compilation")
            ? e.getValue("method") : null;
        if (m != null && m.getType().getName().equals(className)) {
          int id = e.getInt("compileId");
          kernelCompiles.put(id, m.getName());
          compiles.add(String.format("%.1fs #%d %s L%d%s %s %dB", seconds(e.getStartTime()), id,
              m.getName(), e.getShort("compileLevel"), e.getBoolean("isOsr") ? " osr" : "",
              e.getBoolean("succeded") ? "ok" : "FAILED", e.getLong("codeSize")));
        }
      }
    }
    out.append(compiles.size()).append(" compiles [").append(String.join("; ", compiles))
        .append("]");

    var failures = new ArrayList<String>();
    var deopts = new LinkedHashMap<String, Integer>();
    var refused = new LinkedHashMap<String, Integer>();
    var vectorClasses = new TreeSet<String>();
    var allocations = new HashMap<String, Long>();
    var allocatedClasses = new HashMap<String, Long>();
    long sampled = 0;
    Instant tailFrom = end.minus(tail);
    boolean codeCacheFull = false;
    try (var file = new RecordingFile(dump)) {
      while (file.hasMoreEvents()) {
        RecordedEvent e = file.readEvent();
        switch (e.getEventType().getName()) {
          case "jdk.CompilationFailure" -> {
            String method = kernelCompiles.get(e.getInt("compileId"));
            if (method != null) {
              failures.add(method + " #" + e.getInt("compileId") + ": "
                  + e.getString("failureMessage"));
            }
          }
          case "jdk.Deoptimization" -> {
            RecordedMethod m = e.getValue("method");
            if (kernelCompiles.containsKey(e.getInt("compileId"))
                || (m != null && m.getType().getName().equals(className))) {
              String key = kernelCompiles.getOrDefault(e.getInt("compileId"), "?") + " at "
                  + (m == null ? "?" : simple(m)) + "@" + e.getInt("bci") + " "
                  + e.getString("reason") + "/" + e.getString("action");
              deopts.merge(key, 1, Integer::sum);
            }
          }
          case "jdk.CompilerInlining" -> {
            if (kernelCompiles.containsKey(e.getInt("compileId"))) {
              String type = e.getString("callee.type");
              String simpleType = type.substring(type.lastIndexOf('/') + 1);
              if (type.startsWith("jdk/incubator/vector/") && simpleType.matches(".*\\d.*")) {
                vectorClasses.add(simpleType);
              }
              if (!e.getBoolean("succeeded")) {
                refused.merge(simpleType + "." + e.getString("callee.name") + ": "
                    + e.getString("message"), 1, Integer::sum);
              }
            }
          }
          case "jdk.CodeCacheFull" -> codeCacheFull = true;
          case "jdk.ObjectAllocationSample" -> {
            var thread = e.getThread("eventThread");
            if (thread != null && thread.getJavaName().startsWith(threadName)
                && !e.getStartTime().isBefore(tailFrom) && e.getStackTrace() != null) {
              long weight = e.getLong("weight");
              sampled += weight;
              allocations.merge(where(e.getStackTrace().getFrames(), className), weight,
                  Long::sum);
              allocatedClasses.merge(e.getClass("objectClass").getName(), weight, Long::sum);
            }
          }
          default -> { }
        }
      }
    }
    out.append("; compile failures ").append(failures);
    out.append("; deoptimizations ").append(top(deopts));
    out.append("; vector classes inlined ").append(vectorClasses);
    out.append("; inlining refused ").append(top(refused));
    if (codeCacheFull) {
      out.append("; the code cache was full");
    }
    out.append("; allocation samples of threads named ").append(threadName)
        .append("... over the last ")
        .append(tail.toSeconds()).append(" s: ").append(sampled).append(" bytes, by site ")
        .append(top(allocations)).append(", by class ").append(top(allocatedClasses));
    return out.length() <= MAX_CHARS ? out.toString() : out.substring(0, MAX_CHARS) + "...";
  }

  /**
   * Where a sample was allocated: its top frame, and the first frame in the kernel's class, each
   * with its kind - so a box made in an interpreted Vector API call reads apart from one a compiled
   * loop made where C2 could not remove it.
   */
  private static String where(List<RecordedFrame> frames, String className) {
    if (frames.isEmpty()) {
      return "?";
    }
    String top = frame(frames.get(0));
    for (RecordedFrame f : frames) {
      if (f.getMethod() != null && f.getMethod().getType().getName().equals(className)) {
        return f == frames.get(0) ? top : top + " < " + frame(f);
      }
    }
    return top;
  }

  private static String frame(RecordedFrame f) {
    return simple(f.getMethod()) + ":" + f.getLineNumber() + " (" + f.getType() + ")";
  }

  private static String simple(RecordedMethod m) {
    String type = m.getType().getName();
    return type.substring(type.lastIndexOf('.') + 1) + "." + m.getName();
  }

  private double seconds(Instant t) {
    return Duration.between(started, t).toMillis() / 1000.0;
  }

  /** The {@link #TOP} largest entries, largest first, each as {@code key=value}. */
  private static <V extends Comparable<V>> String top(Map<String, V> counts) {
    var entries = new ArrayList<>(counts.entrySet());
    entries.sort(Map.Entry.<String, V>comparingByValue().reversed());
    var shown = new ArrayList<String>();
    for (int i = 0; i < Math.min(TOP, entries.size()); i++) {
      shown.add(entries.get(i).getKey() + "=" + entries.get(i).getValue());
    }
    int rest = entries.size() - shown.size();
    return shown + (rest == 0 ? "" : " and " + rest + " more");
  }
}
