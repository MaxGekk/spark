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


package org.apache.spark.sql.execution.varka;

import java.io.File;
import java.nio.file.Files;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * Writes an emitted kernel class to the configured dump directory under its {@code SourceFile}
 * name, so {@code javap -c -p} reaches a generated loop with no debugger. Diagnostics only: every
 * failure is logged and swallowed, because a query must not fail over a debug write.
 *
 * <p>Every task of a shape holds identical bytes, so a per-JVM memo makes the shape's first task
 * with the directory configured write the file once, instead of every task re-writing it on the
 * task-setup path. The memo is per-process on purpose: the file name derives from the shape, not
 * the bytes, so a file left by an <i>older</i> emitter must be overwritten, not trusted - each
 * JVM's first write refreshes it. (Two first tasks can still race past the memo; they write the
 * same bytes, so the race is benign.)
 */
public final class VarkaClassDump {

  private static final SparkLogger LOG = SparkLoggerFactory.getLogger(VarkaClassDump.class);

  // The (directory, SourceFile) pairs this JVM has dumped.
  private static final Set<String> DUMPED = ConcurrentHashMap.newKeySet();

  private VarkaClassDump() {}

  /** Dumps {@code bytes} under {@code directory}, which is null when no dump is configured. */
  public static void dump(String directory, String sourceFile, byte[] bytes) {
    if (directory == null) {
      return;
    }
    String memoKey = directory + "|" + sourceFile;
    if (DUMPED.add(memoKey)) {
      try {
        String name = sourceFile.endsWith(".java")
            ? sourceFile.substring(0, sourceFile.length() - ".java".length()) : sourceFile;
        File target = new File(directory, name + ".class");
        Files.createDirectories(target.toPath().getParent());
        Files.write(target.toPath(), bytes);
        LOG.info("Wrote the Varka kernel class to " + target.getAbsolutePath());
      } catch (Throwable e) {
        VarkaBatchLedger.rethrowIfFatal(e);
        DUMPED.remove(memoKey);
        LOG.warn("Could not dump the Varka kernel class to " + directory
            + "; execution is unaffected.", e);
      }
    }
  }
}
