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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.ObjectName;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordingStream;

/**
 * Which tier of HotSpot's JIT served each compared kernel call (VARKA-274): the interpreter, C1,
 * C2, an on-stack replacement, or code thrown away by a deoptimization in the middle of the call.
 * The fuzzers compare a kernel's answers with a reference; this says what code gave the answers,
 * so that a check meant to hold C2's vector code to the reference can show that C2 code ran.
 *
 * <p><b>Two sources, read at both edges of a call.</b> Before and after each call the witness
 * reads the code cache's list of installed methods, the DiagnosticCommand MBean's
 * {@code compilerCodelist} (the in-process {@code jcmd Compiler.codelist}): one line per compiled
 * method version, {@code <compile id> <level> <state> <class.method(signature)> [addresses]}, state
 * 0 for code in use and 1 for code made not entrant. What happens inside the call comes from one
 * JFR {@link RecordingStream} per JVM, on {@code jdk.Compilation} (every compile, threshold 0),
 * {@code jdk.Deoptimization} and {@link VarkaCheckedCallEvent}, which the check commits around the
 * call so the other two can be placed inside or outside it. The code list alone would miss a trap
 * taken and recompiled within one call; the events alone would miss code leaving the cache
 * without a trap, which an invalidation by a broken dependency does without any JFR event.
 *
 * <p><b>On-stack replacement.</b> The code list does not mark an OSR version: a loop method can
 * show a level-4 line in use while calls to it still enter the interpreter, the level-4 code
 * being reachable only from inside a running loop. So the witness keeps the compile ids that
 * {@code jdk.Compilation} reports as OSR, and a method counts as compiled at a level only through
 * a version that is not one of them.
 *
 * <p><b>Which methods.</b> A call's verdict is about the loop and epilogue methods of the body the
 * batch selects ({@link VarkaMethodNames#isGroupMethod}), where all the per-row work is. The
 * drivers and the dispatch are left out: they hold no per-row work and are often inlined into
 * their caller or never compiled at all (`VARKA-274.md` 2(b)).
 *
 * <p><b>Late delivery.</b> JFR hands events to a stream about once a second, so a verdict is read
 * only after {@link #settle}, which commits a marker event and returns once the stream has
 * delivered it and flushed once more. It never sleeps a guessed time.
 */
public final class VarkaTierWitness {

  // ---------------------------------------------------------------------------------------------
  // The verdict on one call.
  // ---------------------------------------------------------------------------------------------

  /** What served one call; {@link #label} names its kind for a count. */
  public sealed interface Verdict {
    String label();
  }

  /**
   * Every loop and epilogue method of the call's body was C2 code in use when the call began and
   * when it ended, and nothing was thrown away or replaced on the stack in between.
   */
  public record C2() implements Verdict {
    @Override public String label() {
      return "C2";
    }
  }

  /** A deoptimization of one of the kernel's methods fell inside the call. */
  public record Trapped(String method, String reason, String action) implements Verdict {
    @Override public String label() {
      return "trapped";
    }
  }

  /** An on-stack-replacement compile of one of the call's methods finished inside the call. */
  public record Osr(String method, int level) implements Verdict {
    @Override public String label() {
      return "osr" + level;
    }
  }

  /**
   * Nothing happened inside the call, but some of its methods were not C2 code at one of its
   * edges: each method's level before and after, 0 for the interpreter.
   */
  public record Mixed(Map<String, Integer> before, Map<String, Integer> after)
      implements Verdict {
    public Mixed {
      before = Collections.unmodifiableSortedMap(new TreeMap<>(before));
      after = Collections.unmodifiableSortedMap(new TreeMap<>(after));
    }

    @Override public String label() {
      return "mixed";
    }

    /** Whether every method was interpreted at both edges: a call no compiler had touched. */
    public boolean interpreted() {
      return before.values().stream().allMatch(l -> l == 0)
          && after.values().stream().allMatch(l -> l == 0);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The code list.
  // ---------------------------------------------------------------------------------------------

  /** One line of the code list for a method of a watched class. */
  public record CodeEntry(int compileId, int level, int state, String method) {
    /** Whether the version is in use: state 0. Not entrant is 1. */
    public boolean inUse() {
      return state == 0;
    }
  }

  /** The DiagnosticCommand MBean's code list: every compiled method version in this JVM. */
  public static String readCodeList() {
    try {
      Object reply = ManagementFactory.getPlatformMBeanServer().invoke(
          new ObjectName("com.sun.management:type=DiagnosticCommand"), "compilerCodelist",
          new Object[] {new String[0]}, new String[] {String[].class.getName()});
      return String.valueOf(reply);
    } catch (Exception e) {
      throw new IllegalStateException("could not read the code list", e);
    }
  }

  /**
   * The lines of {@code codeList} for methods of {@code className}. A line reads
   * {@code 1732 4 0 pkg.Class.method(I)J [0x..., 0x... - 0x...]}; a line of another shape is
   * skipped.
   */
  public static List<CodeEntry> parse(String codeList, String className) {
    var out = new ArrayList<CodeEntry>();
    String prefix = className + ".";
    for (String line : codeList.split("\n")) {
      if (!line.contains(prefix)) {
        continue;
      }
      String[] parts = line.trim().split(" ", 4);
      if (parts.length < 4 || !parts[3].startsWith(prefix)) {
        continue;
      }
      int paren = parts[3].indexOf('(');
      if (paren < 0) {
        continue;
      }
      try {
        out.add(new CodeEntry(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]),
            Integer.parseInt(parts[2]), parts[3].substring(prefix.length(), paren)));
      } catch (NumberFormatException e) {
        // Not a code line: a header or a message the command printed instead.
      }
    }
    return out;
  }

  /**
   * Each of {@code methods}' level as the code list shows it: its highest level among its versions
   * in use that are not on-stack replacements ({@code osrIds}) - 4 for C2 code, 1 to 3 for C1's,
   * 0 where none is installed - raised to 4 where C2 inlined it into a caller's version in use
   * ({@code inlined}, the methods each compile inlined, by compile id). A small kernel's loop is
   * often compiled only inside its driver or {@code run}, and then never on its own, since its
   * own invocation counter stops once its caller is C2 code.
   */
  public static Map<String, Integer> levels(List<CodeEntry> entries, Set<String> methods,
      Set<Integer> osrIds, Map<Integer, Set<String>> inlined) {
    var levels = new TreeMap<String, Integer>();
    for (String m : methods) {
      levels.put(m, 0);
    }
    for (CodeEntry e : entries) {
      if (!e.inUse() || osrIds.contains(e.compileId())) {
        continue;
      }
      if (levels.containsKey(e.method())) {
        levels.merge(e.method(), e.level(), Math::max);
      }
      if (e.level() == 4) {
        for (String callee : inlined.getOrDefault(e.compileId(), Set.of())) {
          if (levels.containsKey(callee)) {
            levels.put(callee, 4);
          }
        }
      }
    }
    return levels;
  }

  /**
   * Each of {@code methods}' highest level among its on-stack-replacement versions in use, 0
   * where it has none.
   */
  public static Map<String, Integer> osrLevels(List<CodeEntry> entries, Set<String> methods,
      Set<Integer> osrIds) {
    var levels = new TreeMap<String, Integer>();
    for (String m : methods) {
      levels.put(m, 0);
    }
    for (CodeEntry e : entries) {
      if (e.inUse() && osrIds.contains(e.compileId()) && levels.containsKey(e.method())) {
        levels.merge(e.method(), e.level(), Math::max);
      }
    }
    return levels;
  }

  /** Whether every one of {@code methods} is C2 code in use, read as {@link #levels} does. */
  public static boolean allC2(List<CodeEntry> entries, Set<String> methods, Set<Integer> osrIds,
      Map<Integer, Set<String>> inlined) {
    for (int level : levels(entries, methods, osrIds, inlined).values()) {
      if (level != 4) {
        return false;
      }
    }
    return true;
  }

  // ---------------------------------------------------------------------------------------------
  // One call.
  // ---------------------------------------------------------------------------------------------

  /** What the stream reported for a watched class: a finished compile or a trap. */
  private record Compiled(int compileId, String method, int level, boolean osr, Instant end) { }

  private record Trap(String method, String reason, String action, Instant at) { }

  private record Window(Instant start, Instant end) { }

  /**
   * One compared call: the kernel, the methods its verdict reads, and the code list at its two
   * edges. It is the {@link VarkaKernelChecks.CallHook} the check runs the call with.
   */
  public final class Call implements VarkaKernelChecks.CallHook {
    private final long id;
    private final String className;
    private final Set<String> methods;
    private List<CodeEntry> before = List.of();
    private List<CodeEntry> after = List.of();
    private VarkaCheckedCallEvent event;

    private Call(long id, String className, Set<String> methods) {
      this.id = id;
      this.className = className;
      this.methods = Set.copyOf(methods);
    }

    @Override public void before() {
      // The previous call's closing read stands for this one's opening read, when nothing but
      // the harness ran on the class in between (see `lastRead`).
      List<CodeEntry> last = lastRead.remove(className);
      before = last != null ? last : parse(readCodeList(), className);
      event = new VarkaCheckedCallEvent();
      event.call = id;
      event.kernel = className;
      event.begin();
    }

    @Override public void after(int status) {
      event.end();
      event.commit();
      after = parse(readCodeList(), className);
      lastRead.put(className, after);
    }

    /** The loop and epilogue methods the verdict reads; empty for a body that has none. */
    public Set<String> methods() {
      return methods;
    }

    /** The code list's lines for the kernel at the call's start, for a failure message. */
    public List<CodeEntry> codeBefore() {
      return before;
    }

    /** The verdict, which needs the stream to have settled since the call ({@link #settle}). */
    public Verdict verdict() {
      return verdictOf(this);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The recording.
  // ---------------------------------------------------------------------------------------------

  private static VarkaTierWitness instance;

  /** The JVM's witness, its recording started on first use. */
  public static synchronized VarkaTierWitness get() {
    if (instance == null) {
      instance = new VarkaTierWitness();
    }
    return instance;
  }

  private final RecordingStream stream = new RecordingStream();
  private final AtomicLong calls = new AtomicLong();
  private final Set<String> watched = ConcurrentHashMap.newKeySet();
  private final Set<Integer> osrIds = ConcurrentHashMap.newKeySet();
  // Guarded by `this`: what the stream reported, and how far it has got.
  private final Map<String, List<Compiled>> compiles = new HashMap<>();
  private final Map<String, List<Trap>> traps = new HashMap<>();
  private final Map<Long, Window> windows = new HashMap<>();
  private final Map<Integer, Set<String>> inlined = new ConcurrentHashMap<>();
  /**
   * Each class's code list as its last call closed, which the next call on it opens with: a read
   * costs about 3 ms, a whole history's worth of them doubled its time, and between two compared
   * calls only the harness runs. What a compile installs in that gap shows a call a level lower
   * than it ran at, never higher, and an OSR version installed there is still found, from the
   * stream's compile events and the call's closing read. {@link #touched} drops it when the
   * kernel itself ran in between.
   */
  private final Map<String, List<CodeEntry>> lastRead = new ConcurrentHashMap<>();
  private long markerSeen;
  private long markers;
  private long flushes;

  private VarkaTierWitness() {
    stream.enable("jdk.Compilation").withThreshold(Duration.ZERO);
    stream.enable("jdk.Deoptimization").withoutStackTrace();
    stream.enable("jdk.CompilerInlining").withoutStackTrace();
    stream.enable(VarkaCheckedCallEvent.class).withThreshold(Duration.ZERO);
    stream.onEvent("jdk.Compilation", this::onCompilation);
    stream.onEvent("jdk.Deoptimization", this::onDeoptimization);
    stream.onEvent("jdk.CompilerInlining", this::onInlining);
    stream.onEvent("varka.CheckedCall", this::onCall);
    stream.onFlush(this::onFlush);
    stream.startAsync();
  }

  /** The class a recorded method belongs to, or null. */
  private static String classOf(RecordedMethod m) {
    return m == null || m.getType() == null ? null : m.getType().getName();
  }

  private void onCompilation(RecordedEvent e) {
    RecordedMethod m = e.getValue("method");
    String cls = classOf(m);
    // The success field is spelled `succeded` in the JDK's own metadata.
    if (cls == null || !watched.contains(cls) || !e.getBoolean("succeded")) {
      return;
    }
    boolean osr = e.getBoolean("isOsr");
    int id = e.getInt("compileId");
    if (osr) {
      osrIds.add(id);
    }
    synchronized (this) {
      compiles.computeIfAbsent(cls, k -> new ArrayList<>())
          .add(new Compiled(id, m.getName(), e.getInt("compileLevel"), osr, e.getEndTime()));
    }
  }

  /** A method of a watched class inlined into a compile of another of its methods. */
  private void onInlining(RecordedEvent e) {
    RecordedMethod caller = e.getValue("caller");
    String cls = classOf(caller);
    if (cls == null || !watched.contains(cls) || !e.getBoolean("succeeded")) {
      return;
    }
    jdk.jfr.consumer.RecordedObject callee = e.getValue("callee");
    if (callee == null || !cls.equals(callee.getString("type").replace('/', '.'))) {
      return;
    }
    inlined.computeIfAbsent(e.getInt("compileId"), k -> ConcurrentHashMap.newKeySet())
        .add(callee.getString("name"));
  }

  private void onDeoptimization(RecordedEvent e) {
    RecordedMethod m = e.getValue("method");
    String cls = classOf(m);
    if (cls == null || !watched.contains(cls)) {
      return;
    }
    synchronized (this) {
      traps.computeIfAbsent(cls, k -> new ArrayList<>()).add(new Trap(m.getName(),
          e.getString("reason"), e.getString("action"), e.getStartTime()));
    }
  }

  private void onCall(RecordedEvent e) {
    long call = e.getLong("call");
    synchronized (this) {
      if (call < 0) {
        markerSeen = Math.max(markerSeen, -call);
      } else {
        windows.put(call, new Window(e.getStartTime(), e.getEndTime()));
      }
      notifyAll();
    }
  }

  private synchronized void onFlush() {
    flushes++;
    notifyAll();
  }

  /** Starts reading the stream's events for {@code className}; before its first call. */
  public void watch(String className) {
    watched.add(className);
  }

  /** The kernel ran outside a compared call, as a warm-up does: its next call reads afresh. */
  public void touched(String className) {
    lastRead.remove(className);
  }

  /** Every trap the stream reported for {@code className}, in or out of a call, by reason. */
  public synchronized Map<String, Integer> trapReasons(String className) {
    var out = new TreeMap<String, Integer>();
    for (Trap t : traps.getOrDefault(className, List.of())) {
      out.merge(t.reason(), 1, Integer::sum);
    }
    return out;
  }

  /**
   * The compiles the stream reported for {@code className}, for a person reading why a history
   * did not happen: each as {@code method@level}, {@code osr} marked, and {@code (in k)} for one
   * that finished inside the window of the {@code k}-th of {@code calls}.
   */
  public synchronized List<String> compiles(String className, List<Call> calls) {
    var out = new ArrayList<String>();
    for (Compiled k : compiles.getOrDefault(className, List.of())) {
      String where = "";
      for (int i = 0; i < calls.size(); i++) {
        Window w = windows.get(calls.get(i).id);
        if (w != null && inside(k.end(), w)) {
          where = " (in " + i + ")";
        }
      }
      out.add(k.method() + "@" + k.level() + (k.osr() ? " osr" : "") + where);
    }
    return out;
  }

  /** A new call on {@code className}, whose verdict reads {@code methods}. */
  public Call call(String className, Set<String> methods) {
    return new Call(calls.incrementAndGet(), className, methods);
  }

  /**
   * Whether every one of {@code methods} of {@code className} is C2 code in use now, read from
   * the code list, an on-stack replacement the stream has reported not counting.
   */
  public boolean allC2Now(String className, Set<String> methods) {
    return allC2(parse(readCodeList(), className), methods, osrIds, inlined);
  }

  /** {@link #levels} of {@code methods} of {@code className} now, for a message. */
  public Map<String, Integer> levelsNow(String className, Set<String> methods) {
    return levels(parse(readCodeList(), className), methods, osrIds, inlined);
  }

  /**
   * Waits until the stream has delivered every event committed before this call: a marker event
   * is committed, and once it has arrived the stream flushes once more, a flush writing every
   * thread's buffer. Throws after {@code timeoutMillis}, which a loaded machine can reach; that
   * is a failure of the witness, never of a kernel's answer.
   */
  public void settle(long timeoutMillis) {
    long token;
    synchronized (this) {
      token = ++markers;
    }
    var marker = new VarkaCheckedCallEvent();
    marker.call = -token;
    marker.kernel = "settle";
    marker.commit();
    long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
    synchronized (this) {
      try {
        while (markerSeen < token) {
          waitUntil(deadline, token);
        }
        long seen = flushes;
        while (flushes == seen) {
          waitUntil(deadline, token);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("interrupted while the tier witness settled", e);
      }
    }
  }

  /** The default bound on {@link #settle}: a minute, far past JFR's one-second flushes. */
  public void settle() {
    settle(60_000L);
  }

  private void waitUntil(long deadline, long token) throws InterruptedException {
    long left = deadline - System.nanoTime();
    if (left <= 0) {
      throw new IllegalStateException("the tier witness's JFR stream did not deliver marker "
          + token + " in time (seen " + markerSeen + ", " + flushes + " flushes)");
    }
    wait(Math.max(1L, left / 1_000_000L));
  }

  /**
   * The verdict on {@code c}, in this order: a trap inside the call; an on-stack replacement that
   * served it, where one of its methods had an OSR version in use at the call's start, or one
   * finished inside the call or before it and in use at its end, at a level above the method's
   * own - the interpreter, or C1's code,
   * then jumps into it at the loop's next back edge; C2 code in use for every method at both
   * edges; and otherwise each method's level at the two edges.
   */
  private synchronized Verdict verdictOf(Call c) {
    Window w = windows.get(c.id);
    if (w == null) {
      throw new IllegalStateException("no JFR window for call " + c.id + " on " + c.className
          + ": read its verdict after settle()");
    }
    for (Trap t : traps.getOrDefault(c.className, List.of())) {
      if (inside(t.at(), w)) {
        return new Trapped(t.method(), t.reason(), t.action());
      }
    }
    var own = levels(c.before, c.methods, osrIds, inlined);
    var osr = osrLevels(c.before, c.methods, osrIds);
    var inUseAfter = new HashSet<Integer>();
    for (CodeEntry e : c.after) {
      if (e.inUse()) {
        inUseAfter.add(e.compileId());
      }
    }
    for (Compiled k : compiles.getOrDefault(c.className, List.of())) {
      // Finished inside the call, or before it and still in use when it ended.
      boolean served = inside(k.end(), w)
          || !k.end().isAfter(w.start()) && inUseAfter.contains(k.compileId());
      if (k.osr() && c.methods.contains(k.method()) && served) {
        osr.merge(k.method(), k.level(), Math::max);
      }
    }
    Osr served = null;
    for (var e : osr.entrySet()) {
      if (e.getValue() > own.get(e.getKey()) && (served == null || e.getValue() > served.level())) {
        served = new Osr(e.getKey(), e.getValue());
      }
    }
    if (served != null) {
      return served;
    }
    var after = levels(c.after, c.methods, osrIds, inlined);
    // A body with no loop or epilogue method of its own - a kernel whose outputs read no column
    // has nothing on its masked side - has no C2 code to show, and is never called C2.
    if (!c.methods.isEmpty() && own.values().stream().allMatch(l -> l == 4)
        && after.values().stream().allMatch(l -> l == 4)) {
      return new C2();
    }
    return new Mixed(own, after);
  }

  private static boolean inside(Instant t, Window w) {
    return !t.isBefore(w.start()) && !t.isAfter(w.end());
  }

  /** The compile ids the stream has reported as on-stack replacements, for a test to read. */
  Set<Integer> osrIds() {
    return new HashSet<>(osrIds);
  }
}
