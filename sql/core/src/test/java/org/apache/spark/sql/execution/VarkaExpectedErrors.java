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

package org.apache.spark.sql.execution;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The errors a case drawn by the Spark-level differential may raise (VARKA-319, after SQLancer's
 * expected-errors lists), {@code sql/varka/fuzz/expected_errors.tsv}: tab separated condition (or
 * the class name of a non-Spark exception), side, modes, families and reason; {@code #} starts a
 * comment line. A raise on the list, for that side and mode, is expected; any other is a finding.
 * The families are documentation: a listed condition is expected from any expression. A listed
 * condition that only one engine raises is still a disagreement, which the comparison of
 * the two outcomes reports.
 */
public final class VarkaExpectedErrors {

  private VarkaExpectedErrors() {
  }

  /** The committed list, relative to {@code spark.test.home}. */
  public static final String PATH = "sql/varka/fuzz/expected_errors.tsv";

  /** The engine an error came from, or the engines an entry allows it from. */
  public enum Side {
    ROW, VARKA, BOTH;

    boolean covers(Side engine) {
      return this == BOTH || this == engine;
    }
  }

  /** The ANSI setting a case ran under. */
  public enum Mode {
    LEGACY, ANSI
  }

  public record Entry(
      String condition, Side side, Set<Mode> modes, List<String> families, String reason) {

    public Entry {
      if (condition.isBlank() || modes.isEmpty() || families.isEmpty() || reason.isBlank()) {
        throw new IllegalArgumentException(PATH + ": an entry needs a condition, modes, families"
            + " and a reason: " + condition);
      }
    }
  }

  public static List<Entry> parse(String text) {
    return text.lines()
        .map(String::stripTrailing)
        .filter(l -> !l.isBlank() && !l.startsWith("#"))
        .map(VarkaExpectedErrors::entry)
        .toList();
  }

  private static Entry entry(String line) {
    String[] f = line.split("\t", -1);
    if (f.length != 5) {
      throw new IllegalArgumentException(PATH
          + ": expected condition<TAB>side<TAB>modes<TAB>families<TAB>reason, got '" + line + "'");
    }
    return new Entry(f[0], Side.valueOf(f[1].toUpperCase(Locale.ROOT)),
        Arrays.stream(f[2].split(",")).map(Mode::valueOf).collect(Collectors.toSet()),
        List.of(f[3].split(";")), f[4]);
  }

  // The committed list once read; null until a read succeeds, so that a malformed file throws to
  // each caller (VarkaKnownFailures does the same).
  private static volatile List<Entry> committed;

  /** The committed list, read once. */
  public static List<Entry> entries() {
    List<Entry> list = committed;
    if (list == null) {
      synchronized (VarkaExpectedErrors.class) {
        list = committed;
        if (list == null) {
          list = read();
          committed = list;
        }
      }
    }
    return list;
  }

  private static List<Entry> read() {
    Path file = Path.of(System.getProperty("spark.test.home", "."), PATH);
    try {
      return parse(Files.readString(file, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Whether {@code raised} may come from {@code engine} (ROW or VARKA) under {@code mode}. */
  public static boolean expected(List<Entry> list, String raised, Side engine, Mode mode) {
    return list.stream().anyMatch(e -> e.condition().equals(raised)
        && e.side().covers(engine) && e.modes().contains(mode));
  }
}
