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
import java.lang.instrument.ClassDefinition;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import javax.management.ObjectName;

/**
 * A Java agent loaded into the running JVM by the JVM's own DiagnosticCommand MBean
 * (JVMTI.agent_load), with no startup option, and a helper that redefines the loaded
 * jdk.incubator.vector classes with their own bytes from the runtime image.
 */
public final class Agent370 {
  public static volatile Instrumentation inst;

  public static void agentmain(String args, Instrumentation i) {
    inst = i;
  }

  /** Loads this jar as an agent through the platform MBean server; returns the dcmd output. */
  public static String loadSelf(String jar) throws Exception {
    Object out = ManagementFactory.getPlatformMBeanServer().invoke(
        new ObjectName("com.sun.management:type=DiagnosticCommand"), "jvmtiAgentLoad",
        new Object[] {new String[] {jar}}, new String[] {String[].class.getName()});
    return String.valueOf(out).trim();
  }

  public static List<Class<?>> vectorClasses() {
    List<Class<?>> out = new ArrayList<>();
    for (Class<?> c : inst.getAllLoadedClasses()) {
      if (c.getName().startsWith("jdk.incubator.vector.") && !c.isHidden()
          && !c.isArray() && inst.isModifiableClass(c)) {
        out.add(c);
      }
    }
    return out;
  }

  private static FileSystem jrt;

  /** Redefines every given class with its own bytes; returns the number redefined. */
  public static int redefine(List<Class<?>> classes) throws Exception {
    if (jrt == null) jrt = FileSystems.getFileSystem(URI.create("jrt:/"));
    List<ClassDefinition> defs = new ArrayList<>();
    for (Class<?> c : classes) {
      Path p = jrt.getPath("/modules", c.getModule().getName(),
          c.getName().replace('.', '/') + ".class");
      defs.add(new ClassDefinition(c, Files.readAllBytes(p)));
    }
    inst.redefineClasses(defs.toArray(new ClassDefinition[0]));
    return defs.size();
  }
}
