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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The defaults' store ({@link VarkaFuzzDefaultsStore}) gives the reach filter the verdict a second
 * emission at the defaults gives, tree by tree, and is ignored wherever it cannot be trusted: a
 * file of another commit or options table, a file that cannot be read whole, a tree two files
 * disagree on, and every read in a JVM of the defaults.
 */
@VarkaSuite
public final class VarkaFuzzDefaultsStoreSuite {

  private static final String COMMIT = "0123456789abcdef0123456789abcdef01234567";
  private static final String OTHER_COMMIT = "fedcba9876543210fedcba9876543210fedcba98";
  private static final String PREFIX = "org.apache.spark.sql.varka.execution.VarkaStoreProbe";

  @TempDir
  Path dir;

  /** A series tree as `VarkaSmallTreeSuite` emits it: its class, its key, and its emitter. */
  private record Tree(String className, String key, Function<VarkaEmitOptions, byte[]> emit) {}

  /** The int series to size 2 and the long series to size 2, each tree under its own name. */
  private static List<Tree> trees() {
    var out = new ArrayList<Tree>();
    for (boolean isLong : new boolean[] {false, true}) {
      VarkaIrSeries.Series series =
          isLong ? new VarkaIrSeries.LongSeries() : new VarkaIrSeries.IntSeries();
      int numInputs = isLong ? VarkaIrSeries.LONG_NUM_INPUTS : VarkaIrSeries.NUM_INPUTS;
      int index = 0;
      for (var it = series.upTo(2); it.hasNext(); index++) {
        VarkaVectorIR root = it.next().root();
        String name = PREFIX + (isLong ? "Long" : "") + index;
        String key = "probe\t" + name + "\t" + VarkaVectorIR.canonical(root);
        out.add(new Tree(name, key, o -> {
          try {
            return VarkaLoopEmitter.emit(name, List.of(root), numInputs, VarkaIrSeries.LITERALS,
                null, null, o);
          } catch (VarkaEmitDeclined declined) {
            return null;
          }
        }));
      }
    }
    return out;
  }

  /** The store the defaults run keeps over {@code trees}, written to {@code dir}. */
  private void keep(List<Tree> trees, String commit, String table) {
    VarkaFuzzDefaultsStore store = VarkaFuzzDefaultsStore.writing(commit, table);
    for (Tree t : trees) {
      store.record(t.key(), VarkaEmitOptions.DEFAULTS, t.emit().apply(VarkaEmitOptions.DEFAULTS));
    }
    store.write(dir);
  }

  @Test
  @DisplayName("with the store the filter skips exactly the trees it skips by emitting again")
  void storeGivesTheEmissionsVerdict() {
    List<Tree> trees = trees();
    String table = VarkaFuzzDefaultsStore.tableHash();
    keep(trees, COMMIT, table);
    VarkaFuzzDefaultsStore store = VarkaFuzzDefaultsStore.reading(dir, COMMIT, table);
    assertEquals(List.of(), store.warnings());
    // cse=false leaves most of these trees' bytes alone; misdescribeAdd moves only the trees
    // with an addition of days, so the two verdicts are compared where they differ by tree.
    long moved = 0;
    for (String config : List.of("cse=false", "misdescribeAdd=true", "validityByBitmap=false")) {
      VarkaEmitOptions options = VarkaMatrixRuns.parse(config);
      var withStore = new VarkaReach.Tally();
      var without = new VarkaReach.Tally();
      var emittedAgain = new AtomicInteger();
      for (Tree t : trees) {
        byte[] bytes = t.emit().apply(options);
        if (bytes == null) {
          continue;
        }
        boolean emitting = VarkaReach.ranAtDefaults(config, true, options,
            VarkaEmitOptions.DEFAULTS, bytes, t.emit(), without);
        boolean storing = VarkaReach.ranAtDefaults(config, true, options,
            VarkaEmitOptions.DEFAULTS, bytes, o -> {
              emittedAgain.incrementAndGet();
              return t.emit().apply(o);
            }, store, t.key(), withStore);
        assertEquals(emitting, storing, config + ": " + t.key());
      }
      assertEquals(0, emittedAgain.get(), config + ": the store lacked a tree it was given");
      assertEquals(without.line("probe"), withStore.line("probe"));
      assertTrue(without.skipped() > 0, config + " skipped no tree, so compared no verdict");
      moved += without.moved();
    }
    assertTrue(moved > 0, "no configuration moved a tree, so no verdict of moved was compared");
    assertEquals(0, store.unrecorded.get());
  }

  @Test
  @DisplayName("a tree the store lacks is emitted at the defaults as without it")
  void lackedTreeIsEmittedAgain() {
    List<Tree> trees = trees();
    String table = VarkaFuzzDefaultsStore.tableHash();
    keep(trees.subList(0, 1), COMMIT, table);
    VarkaFuzzDefaultsStore store = VarkaFuzzDefaultsStore.reading(dir, COMMIT, table);
    Tree lacked = trees.get(1);
    VarkaEmitOptions options = VarkaMatrixRuns.parse("cse=false");
    var emitted = new AtomicInteger();
    VarkaReach.ranAtDefaults("cse=false", true, options, VarkaEmitOptions.DEFAULTS,
        lacked.emit().apply(options), o -> {
          emitted.incrementAndGet();
          return lacked.emit().apply(o);
        }, store, lacked.key(), new VarkaReach.Tally());
    assertEquals(1, emitted.get());
    assertEquals(1, store.unrecorded.get());
  }

  @Test
  @DisplayName("a file of another commit or options table is ignored with a warning")
  void staleFilesAreIgnored() {
    List<Tree> trees = trees();
    String table = VarkaFuzzDefaultsStore.tableHash();
    keep(trees, OTHER_COMMIT, table);
    VarkaFuzzDefaultsStore store = VarkaFuzzDefaultsStore.reading(dir, COMMIT, table);
    assertEquals(0, store.size());
    assertEquals(1, store.warnings().size());
    assertTrue(store.warnings().get(0).contains("kept at commit " + OTHER_COMMIT),
        store.warnings().get(0));

    VarkaFuzzDefaultsStore otherTable =
        VarkaFuzzDefaultsStore.reading(dir, OTHER_COMMIT, "0".repeat(64));
    assertEquals(0, otherTable.size());
    assertTrue(otherTable.warnings().get(0).contains("kept under options table"),
        otherTable.warnings().get(0));

    // The stale store answers nothing, so the filter emits at the defaults.
    Tree t = trees.get(0);
    VarkaEmitOptions options = VarkaMatrixRuns.parse("cse=false");
    var emitted = new AtomicInteger();
    VarkaReach.ranAtDefaults("cse=false", true, options, VarkaEmitOptions.DEFAULTS,
        t.emit().apply(options), o -> {
          emitted.incrementAndGet();
          return t.emit().apply(o);
        }, store, t.key(), new VarkaReach.Tally());
    assertEquals(1, emitted.get());
  }

  @Test
  @DisplayName("a missing directory reads as empty, and an unreadable file is ignored whole")
  void missingAndUnreadable() throws IOException {
    String table = VarkaFuzzDefaultsStore.tableHash();
    VarkaFuzzDefaultsStore missing =
        VarkaFuzzDefaultsStore.reading(dir.resolve("absent"), COMMIT, table);
    assertEquals(0, missing.size());
    assertEquals(List.of(), missing.warnings());

    keep(trees(), COMMIT, table);
    Path good;
    try (var files = Files.list(dir)) {
      good = files.findFirst().orElseThrow();
    }
    byte[] whole = Files.readAllBytes(good);
    Files.write(dir.resolve("fuzz-defaults-truncated.bin"),
        Arrays.copyOf(whole, whole.length / 2));
    Files.write(dir.resolve("fuzz-defaults-garbage.bin"), new byte[] {1, 2, 3});
    VarkaFuzzDefaultsStore store = VarkaFuzzDefaultsStore.reading(dir, COMMIT, table);
    assertEquals(2, store.warnings().size(), String.valueOf(store.warnings()));
    store.warnings().forEach(w -> assertTrue(w.contains("unreadable"), w));
    assertEquals(trees().size(), store.size(), "the readable file was not read whole");
  }

  @Test
  @DisplayName("a tree two files disagree on is left out")
  void disagreementIsLeftOut() {
    List<Tree> trees = trees();
    String table = VarkaFuzzDefaultsStore.tableHash();
    keep(trees, COMMIT, table);
    VarkaFuzzDefaultsStore other = VarkaFuzzDefaultsStore.writing(COMMIT, table);
    other.record(trees.get(0).key(), VarkaEmitOptions.DEFAULTS, new byte[] {42});
    other.write(dir);
    VarkaFuzzDefaultsStore store = VarkaFuzzDefaultsStore.reading(dir, COMMIT, table);
    assertEquals(trees.size() - 1, store.size());
    assertNull(store.sameAsDefaults(trees.get(0).key(), new byte[] {42}));
  }

  @Test
  @DisplayName("a store of the defaults never answers, and keeps the defaults' emissions alone")
  void writingStoreNeverReads() {
    Tree t = trees().get(0);
    VarkaFuzzDefaultsStore store =
        VarkaFuzzDefaultsStore.writing(COMMIT, VarkaFuzzDefaultsStore.tableHash());
    byte[] bytes = t.emit().apply(VarkaEmitOptions.DEFAULTS);
    store.record(t.key(), VarkaEmitOptions.DEFAULTS, bytes);
    assertNull(store.sameAsDefaults(t.key(), bytes));
    store.record("other", VarkaMatrixRuns.parse("cse=false"), bytes);
    assertEquals(1, store.size(), "an emission under other options was kept as the defaults'");
  }

  @Test
  @DisplayName("a tree the defaults declined moves under a configuration that emits it")
  void declinedAtTheDefaults() {
    String table = VarkaFuzzDefaultsStore.tableHash();
    VarkaFuzzDefaultsStore kept = VarkaFuzzDefaultsStore.writing(COMMIT, table);
    kept.record("declined", VarkaEmitOptions.DEFAULTS, null);
    kept.write(dir);
    VarkaFuzzDefaultsStore store = VarkaFuzzDefaultsStore.reading(dir, COMMIT, table);
    assertEquals(Boolean.FALSE, store.sameAsDefaults("declined", new byte[] {1}));
  }
}
