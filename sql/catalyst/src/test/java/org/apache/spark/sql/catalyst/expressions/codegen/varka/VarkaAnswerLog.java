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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * The answers of every compared batch as 64-bit digests in a file, so that JVMs forked under
 * different flags can be compared on what their kernels computed (VARKA-272).
 * {@code -Dvarka.fuzz.answers=<file>} turns it on for a JVM, and every batch that
 * {@link VarkaKernelChecks} compares appends one line, {@code <digest> <repeat> <key>}, tab
 * separated: the digest of the outputs; how many times the key has been seen in this JVM, since a
 * case the shrinker reruns or a known failure replayed keys as its first run did; and the check's
 * context, which is the case's label with its seed and iteration, the history and the batch where
 * the compiled check ran it, and the body. Two JVMs with one seed write the same keys in the same
 * order, so {@code dev/varka_platform_report.py} pairs lines by key and repeat and compares the
 * digests; a history one JVM skips drops its keys and shifts none.
 *
 * <p>The digest folds, per fill, per output and per row, the validity bit and the value where
 * valid, through a splitmix step; a decline folds its status instead, so a kernel that declines a
 * batch on one JVM and answers it on another shows as well. A comparison aid, not a cryptographic
 * hash. Each line is flushed as it is written, so a JVM that crashes leaves the answers it had
 * compared.
 */
public final class VarkaAnswerLog implements AutoCloseable {

  public static final String PROPERTY = "varka.fuzz.answers";

  /** The digest every batch starts from. */
  public static final long START = 0x9E3779B97F4A7C15L;

  private static final Object LOCK = new Object();
  private static VarkaAnswerLog jvm;
  private static boolean opened;

  private final BufferedWriter out;
  private final Map<String, Integer> repeats = new HashMap<>();

  /** A log writing to {@code file}, which is created or truncated. */
  VarkaAnswerLog(Path file) throws IOException {
    out = Files.newBufferedWriter(file, StandardCharsets.UTF_8);
  }

  /** Whether this JVM writes answers: the property names a file. */
  public static boolean enabled() {
    return System.getProperty(PROPERTY) != null;
  }

  /**
   * Records {@code digest} under {@code key} in the JVM's file, opened on the first record;
   * nothing without the property.
   */
  public static void record(String key, long digest) {
    synchronized (LOCK) {
      if (!opened) {
        opened = true;
        String file = System.getProperty(PROPERTY);
        if (file != null) {
          try {
            jvm = new VarkaAnswerLog(Path.of(file));
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }
      if (jvm != null) {
        jvm.write(key, digest);
      }
    }
  }

  /** One line: the digest in hex, the key's repeat count in this log, the key on one line. */
  public synchronized void write(String key, long digest) {
    int repeat = repeats.merge(key, 1, Integer::sum);
    try {
      out.write(Long.toHexString(digest) + "\t" + repeat + "\t"
          + key.replace('\n', ' ').replace('\t', ' '));
      out.newLine();
      out.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  /** {@code h} folded with {@code v}: a splitmix64 step over their combination. */
  public static long fold(long h, long v) {
    long z = h + 0x9E3779B97F4A7C15L + v * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  /** {@code h} folded with one row of one output: its validity bit, and its value where valid. */
  public static long row(long h, boolean valid, long value) {
    return valid ? fold(fold(h, 1L), value) : fold(h, 0L);
  }

  /** The digest of a batch the kernel declined with {@code status}. */
  public static long declined(int status) {
    return fold(fold(START, -1L), status);
  }
}
