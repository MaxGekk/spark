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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * What a fuzz suite's tree emitted to at the defaults, written by the run of the defaults and read
 * by the option matrix's configurations, so that {@link VarkaReach} need not emit every tree a
 * second time at the defaults in each configuration's JVM to learn whether the configuration
 * moved its bytes.
 *
 * <p>With {@code -Dvarka.fuzz.defaultsStore=<dir>}, a JVM of the defaults ({@code
 * varka.matrix.config} unset) runs as without it and, for each tree a suite records, keeps the
 * SHA-256 of the class the tree emitted to, or that the emitter declined it; as it exits it writes
 * them to a file of its own in {@code dir}. A JVM under a configuration reads every file there as
 * it starts and, for a tree it finds, compares the SHA-256 of its own bytes with the stored one
 * instead of emitting the tree again. A tree the files lack is emitted at the defaults as without
 * the store. A JVM of the defaults never reads. Both print one {@code VARKA_FUZZ_STORE} line as
 * they exit.
 *
 * <p>The hash is of the whole class file, constant pool included, so two emissions hash alike
 * exactly where their bytes are equal (up to a SHA-256 collision): the same verdict as comparing
 * the bytes. A key is what determines the tree and its emission at the defaults: the caller's
 * description of the tree, which must name the class it is emitted as, and this JVM's
 * {@link #host} - the {@code UseAVX} level and preferred vector lengths the emitter reads, and the
 * JDK. Everything else the emission depends on is the code, so each file starts with the commit
 * the classpath was built at and a hash of the options table, and a file from another commit or
 * table is ignored with a warning, never trusted. A JVM that cannot read its commit neither writes
 * nor reads. The commit is the build's, not the working tree's: two runs of one uncommitted tree
 * share it, which is why a store is only ever used by runs of one build.
 */
public final class VarkaFuzzDefaultsStore {

  /** The system property naming the store's directory. */
  public static final String PROPERTY = "varka.fuzz.defaultsStore";

  /** Whether this store keeps what the defaults emit, or reads what the defaults' run kept. */
  public enum Mode { WRITE, READ }

  private static final int MAGIC = 0x56464453;
  private static final int VERSION = 1;
  private static final String PREFIX = "fuzz-defaults-";
  private static final byte[] DECLINED = new byte[0];

  private final Mode mode;
  private final String commit;
  private final String table;
  private final Map<String, byte[]> entries;
  private final List<String> warnings;

  final AtomicLong taken = new AtomicLong();
  final AtomicLong unrecorded = new AtomicLong();

  private VarkaFuzzDefaultsStore(Mode mode, String commit, String table,
      Map<String, byte[]> entries, List<String> warnings) {
    this.mode = mode;
    this.commit = commit;
    this.table = table;
    this.entries = entries;
    this.warnings = warnings;
  }

  /** A store that keeps what the defaults emit, in memory, as the defaults' run does. */
  static VarkaFuzzDefaultsStore writing(String commit, String table) {
    return new VarkaFuzzDefaultsStore(Mode.WRITE, commit, table, new ConcurrentHashMap<>(),
        new ArrayList<>());
  }

  /** The store a configuration's JVM at {@code commit} and {@code table} reads from {@code dir}. */
  static VarkaFuzzDefaultsStore reading(Path dir, String commit, String table) {
    var warnings = new ArrayList<String>();
    return new VarkaFuzzDefaultsStore(Mode.READ, commit, table,
        read(dir, commit, table, warnings), warnings);
  }

  private static final VarkaFuzzDefaultsStore CURRENT = fromProperty();

  /** This JVM's store, or null where none is named or this JVM cannot tell its commit. */
  public static VarkaFuzzDefaultsStore current() {
    return CURRENT;
  }

  private static VarkaFuzzDefaultsStore fromProperty() {
    String dir = System.getProperty(PROPERTY);
    if (dir == null || dir.isBlank()) {
      return null;
    }
    String commit = commit();
    if (commit == null) {
      System.err.println("VARKA_FUZZ_STORE\tno commit in spark-version-info.properties: the "
          + "store at " + dir + " is neither written nor read");
      return null;
    }
    Path path = Path.of(dir);
    VarkaFuzzDefaultsStore store = VarkaMatrixRuns.CONFIG.isEmpty()
        ? writing(commit, tableHash())
        : reading(path, commit, tableHash());
    store.warnings.forEach(w -> System.err.println("VARKA_FUZZ_STORE\twarning\t" + w));
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (store.mode == Mode.WRITE && !store.entries.isEmpty()) {
        try {
          store.write(path);
        } catch (UncheckedIOException e) {
          System.err.println("VARKA_FUZZ_STORE\twarning\tnot written to " + dir + ": " + e);
        }
      }
      System.out.println(store.line());
    }));
    return store;
  }

  /** The line a JVM prints as it exits: what it kept, or what it took and emitted itself. */
  String line() {
    String config = VarkaMatrixRuns.CONFIG.isEmpty() ? "defaults" : VarkaMatrixRuns.CONFIG;
    return "VARKA_FUZZ_STORE\t" + mode + "\t" + config + "\tentries " + entries.size()
        + "\ttaken " + taken.get() + "\temitted at the defaults " + unrecorded.get()
        + "\tfiles ignored " + warnings.size();
  }

  Mode mode() {
    return mode;
  }

  /** What reading ignored and why: one line a file. */
  List<String> warnings() {
    return List.copyOf(warnings);
  }

  /** The number of trees this store holds. */
  int size() {
    return entries.size();
  }

  // ---------------------------------------------------------------------------------------------
  // Keeping and comparing
  // ---------------------------------------------------------------------------------------------

  /**
   * Keeps what the tree {@code tree} emitted to at the defaults - {@code bytes}, or null where the
   * emitter declined it - in a store that writes, for a tree emitted under the defaults' options.
   * Anything else is not the defaults' emission and is not kept.
   */
  public void record(String tree, VarkaEmitOptions options, byte[] bytes) {
    if (mode == Mode.WRITE && options.equals(VarkaEmitOptions.DEFAULTS)) {
      entries.put(key(tree), bytes == null ? DECLINED : sha256(bytes));
    }
  }

  /**
   * Whether {@code bytes}, the tree's class under this JVM's configuration, are what the defaults
   * emitted for it: true for the same hash, false for another hash or a tree the defaults
   * declined, and null for a tree the store lacks - or a store that writes - which the caller
   * then emits at the defaults itself.
   */
  public Boolean sameAsDefaults(String tree, byte[] bytes) {
    if (mode != Mode.READ) {
      return null;
    }
    byte[] stored = entries.get(key(tree));
    if (stored == null) {
      unrecorded.incrementAndGet();
      return null;
    }
    taken.incrementAndGet();
    return stored.length != 0 && Arrays.equals(stored, sha256(bytes));
  }

  /** The key a tree is stored under: its description and the host's facts, hashed. */
  static String key(String tree) {
    return HexFormat.of().formatHex(sha256((tree + "\t" + HOST).getBytes(StandardCharsets.UTF_8)));
  }

  /**
   * What the emitter reads from the JVM it runs on, besides its options: the {@code UseAVX}
   * level, the preferred vector lengths per element size, and the JDK, whose class-file library
   * writes the bytes.
   */
  static final String HOST = host();

  private static String host() {
    return "useAVX=" + VarkaEmitOptions.HOST_USE_AVX
        + " lanes=" + jdk.incubator.vector.ByteVector.SPECIES_PREFERRED.length()
        + "/" + jdk.incubator.vector.ShortVector.SPECIES_PREFERRED.length()
        + "/" + jdk.incubator.vector.IntVector.SPECIES_PREFERRED.length()
        + "/" + jdk.incubator.vector.LongVector.SPECIES_PREFERRED.length()
        + " jdk=" + Runtime.version();
  }

  // ---------------------------------------------------------------------------------------------
  // What a file must match
  // ---------------------------------------------------------------------------------------------

  /**
   * The commit the classpath was built at, from the {@code spark-version-info.properties} the
   * build writes beside Spark's classes; null where there is none or it names no commit.
   */
  static String commit() {
    try (InputStream in = VarkaFuzzDefaultsStore.class.getClassLoader()
        .getResourceAsStream("spark-version-info.properties")) {
      if (in == null) {
        return null;
      }
      var props = new Properties();
      props.load(in);
      String revision = props.getProperty("revision", "").trim();
      return revision.matches("[0-9a-f]{40}") ? revision : null;
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * A hash of the options table: each option's kind, name, reason and default as the table
   * renders it, and the defaults' canonical text, so that a store kept under another table is
   * never read as this one's.
   */
  static String tableHash() {
    var sb = new StringBuilder();
    for (VarkaEmitOption o : VarkaEmitOption.TABLE) {
      sb.append(o.getClass().getSimpleName()).append(':').append(o.name()).append(':')
          .append(o.reason()).append(':').append(o.text(VarkaEmitOptions.DEFAULTS));
      if (o instanceof VarkaEmitOption.Choice<?> c) {
        sb.append(':').append(c.constants());
      }
      sb.append('\n');
    }
    sb.append(VarkaEmitOptions.DEFAULTS.canonical());
    return HexFormat.of().formatHex(sha256(sb.toString().getBytes(StandardCharsets.UTF_8)));
  }

  // ---------------------------------------------------------------------------------------------
  // Files
  // ---------------------------------------------------------------------------------------------

  /**
   * Every {@code fuzz-defaults-*.bin} in {@code dir} kept at {@code commit} under {@code table}.
   * A file of another commit or table, or one that cannot be read whole, is left out and named in
   * {@code warnings}; a tree two files disagree on is left out, so it is emitted again.
   */
  static Map<String, byte[]> read(Path dir, String commit, String table, List<String> warnings) {
    var out = new ConcurrentHashMap<String, byte[]>();
    if (!Files.isDirectory(dir)) {
      return out;
    }
    List<Path> files;
    try (var list = Files.list(dir)) {
      files = list.filter(f -> f.getFileName().toString().startsWith(PREFIX)
          && f.getFileName().toString().endsWith(".bin")).sorted().toList();
    } catch (IOException e) {
      warnings.add(dir + " not listed: " + e);
      return out;
    }
    var disagreed = new HashSet<String>();
    for (Path file : files) {
      var mine = new TreeMap<String, byte[]>();
      try (var in = new DataInputStream(new GZIPInputStream(Files.newInputStream(file),
          1 << 16))) {
        if (in.readInt() != MAGIC || in.readInt() != VERSION) {
          warnings.add(file + " ignored: not a store of this format");
          continue;
        }
        String theirCommit = in.readUTF();
        String theirTable = in.readUTF();
        if (!theirCommit.equals(commit)) {
          warnings.add(file + " ignored: kept at commit " + theirCommit + ", this JVM is at "
              + commit);
          continue;
        }
        if (!theirTable.equals(table)) {
          warnings.add(file + " ignored: kept under options table " + theirTable
              + ", this JVM's is " + table);
          continue;
        }
        int n = in.readInt();
        for (int i = 0; i < n; i++) {
          String key = HexFormat.of().formatHex(in.readNBytes(32));
          int length = in.readUnsignedByte();
          if (length != 0 && length != 32) {
            throw new IOException("an entry of " + length + " bytes");
          }
          byte[] value = in.readNBytes(length);
          if (value.length != length) {
            throw new IOException("truncated");
          }
          mine.put(key, value);
        }
      } catch (IOException | RuntimeException e) {
        warnings.add(file + " ignored: unreadable: " + e);
        continue;
      }
      for (var e : mine.entrySet()) {
        byte[] before = out.putIfAbsent(e.getKey(), e.getValue());
        if (before != null && !Arrays.equals(before, e.getValue())) {
          disagreed.add(e.getKey());
        }
      }
    }
    disagreed.forEach(out::remove);
    return out;
  }

  /** The entries as a file of this JVM's own in {@code dir}, written whole or not at all. */
  void write(Path dir) {
    try {
      Files.createDirectories(dir);
      String name = PREFIX + ProcessHandle.current().pid() + "-" + System.nanoTime();
      Path tmp = dir.resolve(name + ".tmp");
      try (var out = new DataOutputStream(new GZIPOutputStream(Files.newOutputStream(tmp),
          1 << 16))) {
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.writeUTF(commit);
        out.writeUTF(table);
        var copy = new TreeMap<>(entries);
        out.writeInt(copy.size());
        for (var e : copy.entrySet()) {
          out.write(HexFormat.of().parseHex(e.getKey()));
          out.writeByte(e.getValue().length);
          out.write(e.getValue());
        }
      }
      Files.move(tmp, dir.resolve(name + ".bin"), StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
