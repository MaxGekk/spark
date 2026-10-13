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

package org.apache.spark.sql;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * The golden-file suite's exclusion list ({@code sql/varka/spark_suites/excluded.tsv}): the tests
 * of Spark's {@code SQLQueryTestSuite} that may fail when {@link VarkaSQLQueryTestSuite} runs
 * them through its Arrow source, each with the arm it fails in and why.
 *
 * <p>A line is four tab-separated fields: the test's name as ScalaTest prints it, the arm
 * ({@code varka}, {@code cacheOnly} or {@code both}), the kind and the reason. Blank lines and
 * lines starting with {@code #} are comments. The kind decides what a pass means:
 *
 * <ul>
 *   <li>{@code plans}: the file prints physical plans, which show the Arrow source's cache. Its
 *       {@code EXPLAIN} statements are not compared and every other statement is, so a wrong
 *       answer elsewhere in the file still fails the test. Once no {@code EXPLAIN} output differs
 *       any more, the entry is stale and the test fails until the line is removed.
 *   <li>{@code harness}: the Arrow source itself changes the answer in a way the suite cannot
 *       narrow to statements. The test must fail and is cancelled; once it passes, the entry is
 *       stale and the test fails, so the list cannot outlive the reason it was written for.
 *   <li>{@code environment}: the test needs something this machine may lack (Python for a
 *       Python UDTF). It is cancelled when it fails and may pass where that is installed, so a
 *       pass is not stale.
 * </ul>
 *
 * <p>{@code SQLQueryTestSuite.ignoreList} is not used for this, because an ignored test never
 * runs and so can never show that its entry is stale.
 */
public record VarkaGoldenExclusions(List<Entry> entries) {

  /** The two arms of the suite: Varka on, and the same Arrow source with Varka off. */
  public enum Arm {
    VARKA("varka"),
    CACHE_ONLY("cacheOnly");

    private final String spelling;

    Arm(String spelling) {
      this.spelling = spelling;
    }

    /** The arm's name in the list and in the suite's {@code varka.goldenSuite.arm} property. */
    public String spelling() {
      return spelling;
    }

    /** The arm a spelling names, or an error listing the two spellings. */
    public static Arm named(String spelling) {
      for (Arm arm : values()) {
        if (arm.spelling.equals(spelling)) {
          return arm;
        }
      }
      throw new IllegalArgumentException(
          "unknown arm '" + spelling + "': expected varka or cacheOnly");
    }
  }

  /** What an entry excuses: printed plans, a whole file's failure, or a missing environment. */
  public enum Kind {
    PLANS,
    HARNESS,
    ENVIRONMENT
  }

  /** One line: the test, the arms it applies to, its kind and its reason. */
  public record Entry(String test, Set<Arm> arms, Kind kind, String reason) {
    public Entry {
      arms = Set.copyOf(arms);
    }
  }

  /** What the suite does with a test's outcome, given the entry it has, if any. */
  public sealed interface Verdict {
    /** The test passed and nothing lists it, or an environment entry lists it. */
    record Pass() implements Verdict {}

    /** A listed failure: the test is cancelled with the entry's reason. */
    record Cancel(String reason) implements Verdict {}

    /** The test fails: its own failure, unlisted, or a harness entry that has gone stale. */
    record Fail(String message, Optional<Throwable> cause) implements Verdict {}
  }

  public VarkaGoldenExclusions {
    entries = List.copyOf(entries);
  }

  /** Reads the list from a file. */
  public static VarkaGoldenExclusions read(Path path) {
    try {
      return parse(Files.readAllLines(path, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Parses the list's lines. A malformed line - not four fields, an unknown arm or kind, an empty
   * reason, or a test listed twice for one arm - fails with its line number, so a typo cannot
   * silently drop an entry.
   */
  public static VarkaGoldenExclusions parse(List<String> lines) {
    List<Entry> entries = new ArrayList<>();
    Set<String> seen = new HashSet<>();
    for (int i = 0; i < lines.size(); i++) {
      String line = lines.get(i);
      if (line.isBlank() || line.startsWith("#")) {
        continue;
      }
      String where = "excluded.tsv line " + (i + 1) + ": ";
      String[] fields = line.split("\t", -1);
      if (fields.length != 4) {
        throw new IllegalArgumentException(
            where + "expected 4 tab-separated fields, found " + fields.length);
      }
      Set<Arm> arms = switch (fields[1]) {
        case "both" -> Set.of(Arm.VARKA, Arm.CACHE_ONLY);
        case "varka" -> Set.of(Arm.VARKA);
        case "cacheOnly" -> Set.of(Arm.CACHE_ONLY);
        default -> throw new IllegalArgumentException(
            where + "unknown arm '" + fields[1] + "': expected varka, cacheOnly or both");
      };
      Kind kind = switch (fields[2]) {
        case "plans" -> Kind.PLANS;
        case "harness" -> Kind.HARNESS;
        case "environment" -> Kind.ENVIRONMENT;
        default -> throw new IllegalArgumentException(
            where + "unknown kind '" + fields[2] + "': expected plans, harness or environment");
      };
      if (fields[0].isBlank() || fields[3].isBlank()) {
        throw new IllegalArgumentException(where + "the test name and the reason are required");
      }
      for (Arm arm : arms) {
        if (!seen.add(fields[0] + "\t" + arm)) {
          throw new IllegalArgumentException(
              where + "'" + fields[0] + "' is listed twice for the " + arm.spelling() + " arm");
        }
      }
      entries.add(new Entry(fields[0], arms, kind, fields[3]));
    }
    return new VarkaGoldenExclusions(entries);
  }

  /** The entry for a test in an arm, if the list has one. */
  public Optional<Entry> lookup(String test, Arm arm) {
    return entries.stream().filter(e -> e.test().equals(test) && e.arms().contains(arm))
        .findFirst();
  }

  /**
   * The verdict on a test's outcome: {@code failure} is empty when the test passed, and
   * {@code differingPlans} counts the {@code EXPLAIN} statements whose output differed from the
   * golden file and so were not compared. An unlisted test keeps its outcome. A {@code plans}
   * entry keeps the outcome of everything compared, and is stale once no plan differs. A listed
   * {@code harness} or {@code environment} failure is cancelled, and a listed {@code harness}
   * test that passed is stale.
   */
  public static Verdict decide(Optional<Entry> entry, Optional<Throwable> failure,
      int differingPlans) {
    Verdict asRun = failure.<Verdict>map(f -> new Verdict.Fail(String.valueOf(f.getMessage()),
        Optional.of(f))).orElseGet(Verdict.Pass::new);
    if (entry.isEmpty()) {
      return asRun;
    }
    Entry e = entry.get();
    if (e.kind() == Kind.PLANS) {
      if (failure.isPresent() || differingPlans > 0) {
        return asRun;
      }
      return new Verdict.Fail("stale entry: no EXPLAIN output of '" + e.test() + "' differs " +
          "from its golden file any more, so its line in sql/varka/spark_suites/excluded.tsv " +
          "has to be removed", Optional.empty());
    }
    if (failure.isPresent()) {
      return new Verdict.Cancel(
          "listed in excluded.tsv (" + e.kind().name().toLowerCase(Locale.ROOT) + "): " +
              e.reason());
    }
    return switch (e.kind()) {
      case PLANS, ENVIRONMENT -> new Verdict.Pass();
      case HARNESS -> new Verdict.Fail("stale entry: '" + e.test() + "' passes, so its line in " +
          "sql/varka/spark_suites/excluded.tsv has to be removed", Optional.empty());
    };
  }
}
