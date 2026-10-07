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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * The Arrow question of VARKA-291: what an Arrow batch costs as the store or the wire image of an
 * IR graph held as layout D's six columns. Three measures, for each graph:
 *
 * <ul>
 *   <li>{@code wire}: IPC stream size against the raw column bytes, and the time to write the
 *       stream and to read it back, the columns checked equal after the round trip;
 *   <li>{@code create}: the time to allocate and close the batch (allocator, six vectors, the
 *       validity buffers) against an arena's allocation of the same columns, first call and warm;
 *   <li>{@code access}: a per-row hash over the columns read through Arrow's {@code get}, through
 *       the raw buffer wrapped as a segment, and through an arena's segments.
 * </ul>
 *
 * The pool of ints (lists and wide scalars) is not in the batch: a second, small stream would
 * carry it, and it is empty for most graphs.
 */
public final class ArrowSpike {

  private static volatile long sink;

  private ArrowSpike() {}

  private static final Schema SCHEMA = new Schema(List.of(
      field("kind", 32), field("c0", 32), field("c1", 32), field("c2", 32), field("p0", 64),
      field("p1", 64)));

  private static Field field(String name, int bits) {
    return new Field(name, FieldType.notNullable(new ArrowType.Int(bits, true)), null);
  }

  public static void main(String[] args) throws IOException {
    Path graphs = Path.of(args[0]);
    for (int g = 1; g < args.length; g++) {
      LoadedGraph graph = find(graphs, args[g]);
      try (SegmentColumns rows = (SegmentColumns) FfmRows.create("D", graph.size(),
          graph.listInts(), graph.listNodes())) {
        rows.build(graph);
        measure(graph, rows);
      }
    }
  }

  private static void measure(LoadedGraph graph, SegmentColumns rows) throws IOException {
    int n = rows.count;
    long raw = 0;
    for (int f = 0; f < 6; f++) {
      raw += (long) n * (f < 4 ? 4 : 8);
    }
    try (BufferAllocator allocator = new RootAllocator()) {
      try (VectorSchemaRoot root = batch(allocator, rows, n)) {
        byte[] stream = write(root);
        check(rows, n, stream, allocator);
        long writeNs = timed(() -> write(root).length);
        long readNs = timed(() -> read(stream, allocator));
        System.out.printf("ARROW wire %s nodes %d raw_bytes %d ipc_bytes %d write_ns %d "
            + "read_ns %d%n", graph.name(), n, raw, stream.length, writeNs, readNs);
      }
      long first = System.nanoTime();
      try (VectorSchemaRoot root = batch(allocator, rows, n)) {
        first = System.nanoTime() - first;
        sink += root.getRowCount();
      }
      long arrowWarm = timed(() -> {
        try (VectorSchemaRoot root = batch(allocator, rows, n)) {
          return root.getRowCount();
        }
      });
      long arenaWarm = timed(() -> {
        try (var arena = java.lang.foreign.Arena.ofConfined()) {
          long bytes = 0;
          for (int f = 0; f < 6; f++) {
            long size = (long) n * (f < 4 ? 4 : 8);
            bytes += arena.allocate(size, 64).byteSize();
          }
          return bytes;
        }
      });
      System.out.printf("ARROW create %s nodes %d first_batch_ns %d arrow_warm_ns %d "
          + "arena_warm_ns %d%n", graph.name(), n, first, arrowWarm, arenaWarm);
      try (VectorSchemaRoot root = batch(allocator, rows, n)) {
        long viaGet = timed(() -> hashByGet(root, n));
        long viaBuffer = timed(() -> hashBySegments(root, n));
        long viaArena = timed(() -> hashByArena(rows, n));
        System.out.printf("ARROW access %s nodes %d get_ns %d buffer_segment_ns %d "
            + "arena_segment_ns %d%n", graph.name(), n, viaGet, viaBuffer, viaArena);
      }
    }
  }

  /** The six columns of {@code rows} as an Arrow batch, copied in bulk. */
  private static VectorSchemaRoot batch(BufferAllocator allocator, SegmentColumns rows, int n) {
    VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
    for (int f = 0; f < 6; f++) {
      FieldVector vector = root.getVector(f);
      ((BaseFixedWidthVector) vector).allocateNew(n);
      long bytes = (long) n * (f < 4 ? 4 : 8);
      ArrowBuf data = vector.getDataBuffer();
      MemorySegment.copy(rows.column(f), 0, segment(data), 0, bytes);
      vector.getValidityBuffer().setOne(0, (n + 7) / 8);
      vector.setValueCount(n);
    }
    root.setRowCount(n);
    return root;
  }

  private static MemorySegment segment(ArrowBuf buf) {
    return MemorySegment.ofAddress(buf.memoryAddress()).reinterpret(buf.capacity());
  }

  private static byte[] write(VectorSchemaRoot root) throws IOException {
    var out = new ByteArrayOutputStream();
    try (var writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return out.toByteArray();
  }

  private static long read(byte[] stream, BufferAllocator allocator) throws IOException {
    try (var reader = new ArrowStreamReader(new ByteArrayInputStream(stream), allocator)) {
      reader.loadNextBatch();
      return reader.getVectorSchemaRoot().getRowCount();
    }
  }

  private static void check(SegmentColumns rows, int n, byte[] stream, BufferAllocator allocator)
      throws IOException {
    try (var reader = new ArrowStreamReader(new ByteArrayInputStream(stream), allocator)) {
      reader.loadNextBatch();
      VectorSchemaRoot back = reader.getVectorSchemaRoot();
      for (int f = 0; f < 6; f++) {
        MemorySegment column = rows.column(f);
        for (int i = 0; i < n; i++) {
          long expected = f < 4 ? column.getAtIndex(ValueLayout.JAVA_INT, i)
              : column.getAtIndex(ValueLayout.JAVA_LONG, i);
          long actual = f < 4 ? ((IntVector) back.getVector(f)).get(i)
              : ((BigIntVector) back.getVector(f)).get(i);
          if (expected != actual) {
            throw new IllegalStateException("column " + f + " row " + i + " differs");
          }
        }
      }
    }
  }

  private static int mix(int h, int v) {
    h = (h ^ v) * 0x9E3779B1;
    return h ^ (h >>> 15);
  }

  private static long hashByGet(VectorSchemaRoot root, int n) {
    var kind = (IntVector) root.getVector(0);
    var c0 = (IntVector) root.getVector(1);
    var c1 = (IntVector) root.getVector(2);
    var c2 = (IntVector) root.getVector(3);
    long h = 0;
    for (int i = 0; i < n; i++) {
      h += mix(mix(mix(kind.get(i), c0.get(i)), c1.get(i)),
          c2.get(i));
    }
    return h;
  }

  private static long hashBySegments(VectorSchemaRoot root, int n) {
    MemorySegment kind = segment(root.getVector(0).getDataBuffer());
    MemorySegment c0 = segment(root.getVector(1).getDataBuffer());
    MemorySegment c1 = segment(root.getVector(2).getDataBuffer());
    MemorySegment c2 = segment(root.getVector(3).getDataBuffer());
    return hash(kind, c0, c1, c2, n);
  }

  private static long hashByArena(SegmentColumns rows, int n) {
    return hash(rows.column(0), rows.column(1), rows.column(2), rows.column(3), n);
  }

  private static long hash(MemorySegment kind, MemorySegment c0, MemorySegment c1,
      MemorySegment c2, int n) {
    long h = 0;
    for (int i = 0; i < n; i++) {
      h += mix(mix(mix(
          kind.getAtIndex(ValueLayout.JAVA_INT, i), c0.getAtIndex(ValueLayout.JAVA_INT, i)),
          c1.getAtIndex(ValueLayout.JAVA_INT, i)), c2.getAtIndex(ValueLayout.JAVA_INT, i));
    }
    return h;
  }

  /** ns a call: warm-up for a second, then the minimum over five windows of a second. */
  private static long timed(IoSupplier op) throws IOException {
    long best = Long.MAX_VALUE;
    for (int window = 0; window < 6; window++) {
      long start = System.nanoTime();
      long end = start + 1_000_000_000L;
      long calls = 0;
      long now;
      do {
        sink += op.get();
        calls++;
        now = System.nanoTime();
      } while (now < end);
      if (window >= 1) {
        best = Math.min(best, (now - start) / calls);
      }
    }
    return best;
  }

  @FunctionalInterface
  private interface IoSupplier {
    long get() throws IOException;
  }

  private static LoadedGraph find(Path dir, String name) throws IOException {
    var all = new ArrayList<LoadedGraph>();
    try (var files = Files.list(dir)) {
      for (Path file : files.filter(f -> f.toString().endsWith(".graphs")).sorted().toList()) {
        all.addAll(LoadedGraph.loadAll(KindNames.TABLE,
            Files.readString(file, StandardCharsets.UTF_8)));
      }
    }
    return all.stream().filter(g -> g.name().equals(name)).findFirst().orElseThrow();
  }
}
