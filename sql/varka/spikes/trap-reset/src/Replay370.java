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
import java.lang.reflect.Executable;
import java.util.List;

/**
 * Wraps ScalaTest's Runner for the warm-arm replay. Before the suite starts it loads Agent370
 * in-process (every arm, so the arms differ only in the action) and, by -Dr370.mode:
 *   none        reads the counts every period, changes nothing (the control)
 *   flags       WhiteBox sets PerMethodTrapLimit and PerMethodRecompilationCutoff to 100000
 *               once, before the first warm-up
 *   clear       every period, WhiteBox clearMethodState on every jdk.incubator.vector method
 *   redefine    every period, redefines every loaded jdk.incubator.vector class with its own
 *               bytes through the in-process agent
 * Each period it prints the WhiteBox counts over the vector methods before and after.
 */
public final class Replay370 {
  public static void main(String[] args) throws Exception {
    String mode = System.getProperty("r370.mode", "none");
    long period = Long.getLong("r370.periodMs", 30_000L);
    String jar = System.getProperty("r370.jar");
    System.out.println("R370 mode=" + mode + " period=" + period + " agent="
        + Agent370.loadSelf(jar) + " inst=" + (Agent370.inst != null));
    if (mode.equals("flags")) {
      Counts370.WB.setIntxVMFlag("PerMethodTrapLimit", 100_000L);
      Counts370.WB.setIntxVMFlag("PerMethodRecompilationCutoff", 100_000L);
      System.out.println("R370 flags " + Counts370.WB.getIntxVMFlag("PerMethodTrapLimit") + " "
          + Counts370.WB.getIntxVMFlag("PerMethodRecompilationCutoff"));
    }
    long first = Long.getLong("r370.firstMs", period);
    boolean once = Boolean.getBoolean("r370.once");
    long start = System.nanoTime();
    Thread t = new Thread(() -> {
      int tick = 0;
      while (true) {
        try {
          Thread.sleep(tick++ == 0 ? first : period);
          boolean act0 = !once || tick == 1;
          List<Class<?>> cs = Agent370.vectorClasses();
          List<Executable> ms = Counts370.methods(cs);
          long[] before = Counts370.sums(ms);
          long t0 = System.nanoTime();
          String act = "";
          if (act0 && mode.equals("clear")) {
            for (Executable m : ms) Counts370.WB.clearMethodState(m);
            act = " cleared " + ms.size();
          } else if (act0 && mode.equals("redefine")) {
            act = " redefined " + Agent370.redefine(cs);
          }
          long took = (System.nanoTime() - t0) / 1_000_000;
          String after = act.isEmpty() ? "" : " | after " + Counts370.fmt(Counts370.sums(ms));
          if (tick % 4 == 2) {
            StringBuilder sb = new StringBuilder("R370 trapped at t=" + tick + ":");
            for (Executable m : ms) {
              int cc = Counts370.WB.getMethodTrapCount(m, "class_check");
              int dc = Counts370.WB.getMethodDecompileCount(m);
              if (cc > 0 || dc > 2) {
                sb.append(' ').append(m.getDeclaringClass().getSimpleName()).append('.')
                    .append(m.getName()).append("=cc").append(cc).append("/d").append(dc);
              }
            }
            System.out.println(sb);
          }
          System.out.println("R370 t=" + (System.nanoTime() - start) / 1_000_000_000 + "s classes="
              + cs.size() + " methods=" + ms.size() + " before " + Counts370.fmt(before) + act
              + " in " + took + " ms" + after);
        } catch (InterruptedException e) {
          return;
        } catch (Throwable e) {
          System.out.println("R370 error " + e);
        }
      }
    }, "r370");
    t.setDaemon(true);
    t.start();
    Class.forName("org.scalatest.tools.Runner").getMethod("main", String[].class)
        .invoke(null, (Object) args);
  }
}
