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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import jdk.test.whitebox.WhiteBox;

/** Reads the MethodData counts of a set of methods through WhiteBox (read-only use). */
public final class Counts370 {
  static final WhiteBox WB = WhiteBox.getWhiteBox();

  public static List<Executable> methods(List<Class<?>> classes) {
    List<Executable> out = new ArrayList<>();
    for (Class<?> c : classes) {
      try {
        for (Method m : c.getDeclaredMethods()) out.add(m);
      } catch (Throwable t) {
        // a class whose methods cannot be listed is skipped
      }
    }
    return out;
  }

  /** {all traps, class_check traps, bimorphic traps, decompiles, methods with any trap}. */
  public static long[] sums(List<Executable> ms) {
    long all = 0, cc = 0, bi = 0, dec = 0, with = 0;
    for (Executable m : ms) {
      int a = WB.getMethodTrapCount(m);
      all += a;
      cc += WB.getMethodTrapCount(m, "class_check");
      bi += WB.getMethodTrapCount(m, "bimorphic_or_optimized_type_check");
      dec += WB.getMethodDecompileCount(m);
      if (a > 0) with++;
    }
    return new long[] {all, cc, bi, dec, with};
  }

  public static String fmt(long[] s) {
    return "traps=" + s[0] + " class_check=" + s[1] + " bimorphic=" + s[2] + " decompiles=" + s[3]
        + " methodsWithTraps=" + s[4];
  }
}
