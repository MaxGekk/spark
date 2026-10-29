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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.annotation.Nullable;

import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.InSet;
import org.apache.spark.sql.catalyst.expressions.InterpretedUnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMatrixRuns;
import org.apache.spark.sql.catalyst.trees.Origin;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.internal.SQLConf;

/**
 * What the expression harness's run at the defaults leaves for its runs under the option
 * matrix's configurations, which would otherwise do the same work again: the plans
 * {@link VarkaHarnessPlans} builds and Spark's answers {@link VarkaExpressionHarness} computes. A
 * configuration changes Varka's kernels and nothing else the harness reads, so under it a plan is
 * the defaults' plan and Spark's answer is the defaults' answer.
 *
 * <p>With {@code -Dvarka.harness.store=<dir>}, a JVM of the defaults ({@code
 * varka.matrix.config} unset) does everything as without it and, as it exits, writes what it
 * computed to a file of its own in {@code dir}; a JVM under a configuration reads every file
 * there and, where an entry has what it is about to compute, takes the entry. A JVM of the
 * defaults never reads, so a check at the defaults is the same with or without the store. Both
 * print one {@code VARKA_HARNESS_STORE} line as they exit with what they wrote or took.
 *
 * <p>An entry is keyed by what its value depends on, and by nothing that differs between two JVMs:
 * a plan by its kind, its SQL text, the scan's columns by name, type, nullability, qualifier and
 * metadata, and the SQL settings the thread sets ({@link #settings}); a Spark answer by its
 * expressions as {@link #describe} prints them, the fixture's rows by content, and the settings. A
 * plan is written with its columns as ordinals and read back over the reading JVM's own columns,
 * since the expression ids of a fixture's columns are made afresh in every JVM, and it is written
 * only when it comes back from its bytes printed whole as it was ({@link #storable}), so a plan
 * that does not survive the round trip is planned afresh by every JVM.
 */
public final class VarkaHarnessStore {

  /** The system property naming the store's directory. */
  public static final String PROPERTY = "varka.harness.store";

  /** Whether this store writes what the run computes, or reads what the defaults' run wrote. */
  public enum Mode { WRITE, READ }

  private final Mode mode;
  private final String config;
  private final Map<String, byte[]> entries;

  final AtomicLong planHits = new AtomicLong();
  final AtomicLong planMisses = new AtomicLong();
  final AtomicLong planUnstorable = new AtomicLong();
  final AtomicLong answerHits = new AtomicLong();
  final AtomicLong answerMisses = new AtomicLong();

  private VarkaHarnessStore(Mode mode, String config, Map<String, byte[]> entries) {
    this.mode = mode;
    this.config = config;
    this.entries = entries;
  }

  /** A store that writes, as the defaults' run has, keeping its entries in memory. */
  static VarkaHarnessStore writing() {
    return new VarkaHarnessStore(Mode.WRITE, "", new ConcurrentHashMap<>());
  }

  /**
   * A store that reads this one's entries as a JVM under {@code config} would: the seam the
   * store's own tests use, with the configuration named rather than read from the property.
   */
  VarkaHarnessStore readingAs(String config) {
    return new VarkaHarnessStore(Mode.READ, config, new ConcurrentHashMap<>(entries));
  }

  private static volatile @Nullable VarkaHarnessStore current = fromProperty();

  /** This JVM's store, or null where none is named. */
  static @Nullable VarkaHarnessStore current() {
    return current;
  }

  /** {@code body} with {@code store} as the JVM's store, the one before it restored after. */
  static <T> T with(@Nullable VarkaHarnessStore store, Supplier<T> body) {
    VarkaHarnessStore before = current;
    current = store;
    try {
      return body.get();
    } finally {
      current = before;
    }
  }

  private static @Nullable VarkaHarnessStore fromProperty() {
    String dir = System.getProperty(PROPERTY);
    if (dir == null || dir.isBlank()) {
      return null;
    }
    Path path = Path.of(dir);
    String config = VarkaMatrixRuns.CONFIG;
    VarkaHarnessStore store = config.isEmpty()
        ? new VarkaHarnessStore(Mode.WRITE, "", new ConcurrentHashMap<>())
        : new VarkaHarnessStore(Mode.READ, config, read(path));
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (store.mode == Mode.WRITE && !store.entries.isEmpty()) {
        write(path, store.entries);
      }
      System.out.println(store.line());
    }));
    return store;
  }

  /** The line a JVM prints as it exits: what it wrote or took, and what it did itself. */
  String line() {
    return "VARKA_HARNESS_STORE\t" + mode + "\t" + (config.isEmpty() ? "defaults" : config)
        + "\tentries " + entries.size()
        + "\tplans taken " + planHits.get() + " planned " + planMisses.get()
        + " unstorable " + planUnstorable.get()
        + "\tanswers taken " + answerHits.get() + " computed " + answerMisses.get();
  }

  Mode mode() {
    return mode;
  }

  /** The entries, as a file of this store would hold them. */
  Map<String, byte[]> entries() {
    return entries;
  }

  /** Drops the entries whose key {@code which} accepts: a test's file lacking them. */
  void remove(java.util.function.Predicate<String> which) {
    entries.keySet().removeIf(which);
  }

  /** Puts {@code value} under {@code key}: a test's planted entry. */
  void plant(String key, byte[] value) {
    entries.put(key, value);
  }

  // ---------------------------------------------------------------------------------------------
  // Plans
  // ---------------------------------------------------------------------------------------------

  /**
   * What {@code plan} plans over {@code columns}, under {@code kind} and {@code text} and the
   * thread's settings. Read where the defaults' run wrote it, and planned where it did not; in
   * the defaults' run planned always and written when it survives the round trip.
   */
  <T> T plan(String kind, String text, Seq<Attribute> columns,
      Function<Seq<Attribute>, T> plan) {
    String key = "plan\t" + kind + "\t" + text + "\t" + columns(columns) + "\t"
        + settings(SQLConf.get(), false);
    if (mode == Mode.READ) {
      byte[] stored = entries.get(key);
      if (stored != null) {
        try {
          @SuppressWarnings("unchecked")
          T value = (T) fromOrdinals(deserialize(stored), columns);
          planHits.incrementAndGet();
          return value;
        } catch (RuntimeException unreadable) {
          // Planned afresh below, as an entry the file lacks is.
        }
      }
      planMisses.incrementAndGet();
      return plan.apply(columns);
    }
    T value = plan.apply(columns);
    planMisses.incrementAndGet();
    Written written = storable(value, columns);
    if (written.bytes() == null) {
      planUnstorable.incrementAndGet();
    } else {
      entries.put(key, written.bytes());
    }
    PlanWatcher w = watcher;
    if (w != null) {
      w.planned(kind + "\t" + text, columns, value, written, plan);
    }
    return value;
  }

  /**
   * What a test hears of each plan a writing store plans: the value, how it was written (its
   * bytes, or why it did not survive the round trip) and the planner, which plans the same text
   * over other columns.
   */
  interface PlanWatcher {
    void planned(String what, Seq<Attribute> columns, Object value, Written written,
        Function<Seq<Attribute>, ?> plan);
  }

  private volatile @Nullable PlanWatcher watcher;

  /** This store with {@code w} hearing its plans. */
  VarkaHarnessStore watched(PlanWatcher w) {
    watcher = w;
    return this;
  }

  /**
   * {@code value}'s bytes, its columns as ordinals, or why it cannot be written: it does not come
   * back from them printed the same over the same columns ({@link #describe}, ids included) - an
   * attribute that is no column of the scan, a value Java serialization does not carry exactly,
   * such as a NaN's payload, which it canonicalizes.
   */
  static Written storable(Object value, Seq<Attribute> columns) {
    try {
      byte[] bytes = serialize(toOrdinals(value, columns));
      Object back = fromOrdinals(deserialize(bytes), columns);
      // Printed whole, ids, origins and tags included, and printed as Catalyst prints it; equal
      // as well, unless it holds an InSet, whose NaN never equals another.
      String read = describe(back, true);
      String planned = describe(value, true);
      if (!read.equals(planned)) {
        return new Written(null, "read back as another plan: " + difference(read, planned));
      }
      if (!String.valueOf(back).equals(String.valueOf(value))) {
        return new Written(null, "read back printing otherwise: " + back);
      }
      if (!Objects.equals(back, value) && !read.contains(InSet.class.getName())) {
        return new Written(null, "read back unequal");
      }
      return new Written(bytes, null);
    } catch (Unstorable | UncheckedIOException e) {
      return new Written(null, e.getClass().getSimpleName() + ": " + e.getMessage());
    }
  }

  /** Where two printed plans first differ, with a little of each around it. */
  private static String difference(String a, String b) {
    int at = 0;
    while (at < Math.min(a.length(), b.length()) && a.charAt(at) == b.charAt(at)) {
      at++;
    }
    int from = Math.max(0, at - 160);
    return "read ..." + a.substring(from, Math.min(a.length(), at + 160)) + "... planned ..."
        + b.substring(from, Math.min(b.length(), at + 160)) + "...";
  }

  /** A value as written: its bytes, or the reason it could not be. */
  record Written(@Nullable byte[] bytes, @Nullable String reason) {}

  /** A value the store cannot write: an attribute outside the scan's columns. */
  private static final class Unstorable extends RuntimeException {
    Unstorable(String message) {
      super(message);
    }
  }

  /** A planned value's columns as ordinals: one of the shapes {@link VarkaHarnessPlans} plans. */
  static Object toOrdinals(@Nullable Object value, Seq<Attribute> columns) {
    List<Attribute> cols = CollectionConverters.asJava(columns);
    return switch (value) {
      case null -> new Shape("null", null, false);
      case Expression e -> {
        Expression unbound = unbind(e, cols);
        var origins = new ArrayList<Origin>();
        var tags = new ArrayList<Object>();
        for (Expression node : nodes(unbound)) {
          origins.add(node.origin());
          tags.add(field(TAGS, node));
        }
        yield new Shape("expression", new Object[] {unbound, origins, tags}, false);
      }
      case Seq<?> s -> new Shape("seq", listToOrdinals(CollectionConverters.asJava(s), columns),
          false);
      case List<?> l -> new Shape("list", listToOrdinals(l, columns), false);
      case VarkaHarnessPlans.Query.Selection s ->
          new Shape("selection", toOrdinals(s.condition(), columns), false);
      case VarkaHarnessPlans.Query.Projection p ->
          new Shape("projection", toOrdinals(p.list(), columns), false);
      case VarkaHarnessPlans.Query.Constant c -> new Shape("constant", null, c.all());
      default -> throw new Unstorable("no shape for " + value.getClass().getName());
    };
  }

  private static ArrayList<Object> listToOrdinals(List<?> values, Seq<Attribute> columns) {
    var out = new ArrayList<Object>(values.size());
    for (Object v : values) {
      out.add(toOrdinals(v, columns));
    }
    return out;
  }

  /** {@link #toOrdinals}'s inverse over {@code columns}, the reading JVM's own. */
  @SuppressWarnings("unchecked")
  static @Nullable Object fromOrdinals(Object stored, Seq<Attribute> columns) {
    Shape s = (Shape) stored;
    List<Attribute> cols = CollectionConverters.asJava(columns);
    return switch (s.kind()) {
      case "null" -> null;
      case "expression" -> {
        Object[] parts = (Object[]) s.value();
        Expression unbound = (Expression) parts[0];
        restore(unbound, (List<?>) parts[1], (List<?>) parts[2]);
        yield bind(unbound, cols);
      }
      case "seq" -> CollectionConverters.asScala(listFromOrdinals(s, columns)).toList();
      case "list" -> listFromOrdinals(s, columns);
      case "selection" -> new VarkaHarnessPlans.Query.Selection(
          (Expression) fromOrdinals(s.value(), columns));
      case "projection" -> new VarkaHarnessPlans.Query.Projection(
          (Seq<org.apache.spark.sql.catalyst.expressions.NamedExpression>)
              fromOrdinals(s.value(), columns));
      case "constant" -> new VarkaHarnessPlans.Query.Constant(s.flag());
      default -> throw new IllegalStateException("no shape " + s.kind());
    };
  }

  private static List<Object> listFromOrdinals(Shape s, Seq<Attribute> columns) {
    var out = new ArrayList<Object>();
    for (Object v : (List<?>) s.value()) {
      out.add(fromOrdinals(v, columns));
    }
    return out;
  }

  /**
   * Every node of {@code e}, each once, in an order that depends on the tree's shape alone: a
   * node, then the expressions among its constructor's arguments, then any child that is not one
   * of them - a {@code RuntimeReplaceable}'s replacement, which is its child and no argument.
   */
  static List<Expression> nodes(Expression e) {
    var out = new ArrayList<Expression>();
    nodes(e, out, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
    return out;
  }

  private static void nodes(Expression e, List<Expression> out, java.util.Set<Expression> seen) {
    if (!seen.add(e)) {
      return;
    }
    out.add(e);
    for (Expression arg : expressionArguments(e)) {
      nodes(arg, out, seen);
    }
    for (Expression child : CollectionConverters.asJava(e.children())) {
      nodes(child, out, seen);
    }
  }

  /** The expressions among {@code e}'s constructor's arguments, in or out of a collection. */
  private static List<Expression> expressionArguments(Expression e) {
    var out = new ArrayList<Expression>();
    var args = e.productIterator();
    while (args.hasNext()) {
      collect(args.next(), out);
    }
    return out;
  }

  private static void collect(@Nullable Object o, List<Expression> out) {
    switch (o) {
      case Expression x -> out.add(x);
      case scala.Option<?> opt -> {
        if (opt.isDefined()) {
          collect(opt.get(), out);
        }
      }
      case scala.collection.Iterable<?> it -> {
        var i = it.iterator();
        while (i.hasNext()) {
          collect(i.next(), out);
        }
      }
      case null, default -> {}
    }
  }

  // TreeNode's origin and tags. TreeNode is not Serializable, so a deserialized node's fields
  // of it are made by its constructor, as a new node's are: its origin is wherever the reading
  // thread is, and it has no tags. The origin is what an ANSI error's message quotes the SQL by,
  // and a tag can name the function a node prints as (FunctionRegistry.FUNC_ALIAS: day(d) is a
  // DayOfMonth printed as day).
  private static final java.lang.reflect.Field ORIGIN = treeNodeField("origin");
  private static final java.lang.reflect.Field TAGS = treeNodeField("_tags");

  private static java.lang.reflect.Field treeNodeField(String name) {
    try {
      var f = org.apache.spark.sql.catalyst.trees.TreeNode.class.getDeclaredField(name);
      f.setAccessible(true);
      return f;
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  private static @Nullable Object field(java.lang.reflect.Field f, Object node) {
    try {
      return f.get(node);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Each of {@link #nodes}' origin and tags set back, in the order they were taken. */
  private static void restore(Expression e, List<?> origins, List<?> tags) {
    List<Expression> all = nodes(e);
    if (all.size() != origins.size() || all.size() != tags.size()) {
      throw new IllegalStateException(all.size() + " nodes read, " + origins.size() + " written");
    }
    try {
      for (int i = 0; i < all.size(); i++) {
        ORIGIN.set(all.get(i), origins.get(i));
        TAGS.set(all.get(i), tags.get(i));
      }
    } catch (IllegalAccessException x) {
      throw new IllegalStateException(x);
    }
  }

  /** One planned value as written: its kind, its contents, and a constant's answer. */
  private record Shape(String kind, @Nullable Object value, boolean flag)
      implements java.io.Serializable {}

  /** {@code e} with each column it reads as a {@link BoundReference} to the column's ordinal. */
  private static Expression unbind(Expression e, List<Attribute> columns) {
    return map(e, x -> {
      if (!(x instanceof AttributeReference a)) {
        return null;
      }
      for (int i = 0; i < columns.size(); i++) {
        if (columns.get(i).exprId().equals(a.exprId())) {
          if (!columns.get(i).equals(a)) {
            throw new Unstorable(a + " differs from the column " + columns.get(i));
          }
          return new BoundReference(i, a.dataType(), a.nullable());
        }
      }
      throw new Unstorable(a + " is no column of " + columns);
    });
  }

  /** {@link #unbind}'s inverse: each ordinal the column of {@code columns} it names. */
  private static Expression bind(Expression e, List<Attribute> columns) {
    return map(e, x -> {
      if (!(x instanceof BoundReference b)) {
        return null;
      }
      Attribute column = columns.get(b.ordinal());
      if (!column.dataType().equals(b.dataType()) || column.nullable() != b.nullable()) {
        throw new IllegalStateException("stored column " + b + " is not " + column);
      }
      return column;
    });
  }

  /** {@code e} with every node {@code f} maps replaced, bottom up; null leaves a node as it is. */
  private static Expression map(Expression e,
      java.util.function.Function<Expression, Expression> f) {
    return e.transformUp(new scala.runtime.AbstractPartialFunction<>() {
      @Override
      public boolean isDefinedAt(Expression x) {
        return f.apply(x) != null;
      }

      @Override
      public Expression apply(Expression x) {
        Expression y = f.apply(x);
        return y != null ? y : x;
      }
    });
  }

  // ---------------------------------------------------------------------------------------------
  // Spark's answers
  // ---------------------------------------------------------------------------------------------

  /** The key of Spark's answers to {@code outputs}, bound, over {@code rows}. */
  static String answerKey(List<Expression> bound, Seq<Attribute> columns,
      List<InternalRow> rows) {
    return "answer\t" + describe(bound) + "\t" + fixture(columns, rows) + "\t"
        + settings(SQLConf.get(), false);
  }

  /**
   * The stored answer for {@code key} in a configuration's run, or null where there is none or
   * this store writes, counting both.
   */
  @Nullable byte[] answer(String key) {
    if (mode != Mode.READ) {
      return null;
    }
    byte[] stored = entries.get(key);
    (stored != null ? answerHits : answerMisses).incrementAndGet();
    return stored;
  }

  /** Writes Spark's answer under {@code key}, in the defaults' run. */
  void putAnswer(String key, byte[] answer) {
    if (mode == Mode.WRITE) {
      answerMisses.incrementAndGet();
      entries.put(key, answer);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Keys
  // ---------------------------------------------------------------------------------------------

  /**
   * The SQL settings {@code conf} sets, sorted, and its session time zone, which a plan's casts
   * take when they are resolved. Without {@code withBatchSize}, the rows per Arrow batch the
   * matrix's batch-size axis sets are left out: neither the analyzer, the optimizer nor Spark's
   * projections read them, only the harness's batches.
   */
  static String settings(SQLConf conf, boolean withBatchSize) {
    var all = new TreeMap<String, String>(CollectionConverters.asJava(conf.getAllConfs()));
    if (!withBatchSize) {
      all.remove(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH().key());
    }
    return all + " tz=" + conf.sessionLocalTimeZone();
  }

  /** The columns by everything but their expression ids, which every JVM makes afresh. */
  static String columns(Seq<Attribute> columns) {
    var sb = new StringBuilder();
    for (Attribute a : CollectionConverters.asJava(columns)) {
      sb.append(a.name()).append(':').append(a.dataType().json()).append(':')
          .append(a.nullable()).append(':').append(a.qualifier()).append(':')
          .append(a.metadata().json()).append(';');
    }
    return sb.toString();
  }

  // The last fixtures' digests, by the identity of their row lists: a check reads one fixture
  // many times in a row.
  private static final Map<List<InternalRow>, String> DIGESTS = new IdentityHashMap<>();

  /** {@code columns} and a digest of {@code rows}' values, each as an {@code UnsafeRow}. */
  static String fixture(Seq<Attribute> columns, List<InternalRow> rows) {
    synchronized (DIGESTS) {
      String digest = DIGESTS.get(rows);
      if (digest == null) {
        digest = digest(columns, rows);
        if (DIGESTS.size() > 64) {
          DIGESTS.clear();
        }
        DIGESTS.put(rows, digest);
      }
      return columns(columns) + rows.size() + "@" + digest;
    }
  }

  private static String digest(Seq<Attribute> columns, List<InternalRow> rows) {
    List<Attribute> cols = CollectionConverters.asJava(columns);
    var refs = new ArrayList<Expression>();
    for (int i = 0; i < cols.size(); i++) {
      refs.add(new BoundReference(i, cols.get(i).dataType(), cols.get(i).nullable()));
    }
    UnsafeProjection unsafe =
        InterpretedUnsafeProjection.createProjection(CollectionConverters.asScala(refs).toList());
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      for (InternalRow row : rows) {
        byte[] bytes = unsafe.apply(row).getBytes();
        sha.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
        sha.update(bytes);
      }
      return HexFormat.of().formatHex(sha.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * {@code o} printed whole: an expression as its class, its constructor's arguments and its
   * origin, which an error's message quotes; a floating value by its raw bits, which
   * {@code toString} and Java serialization would both lose; a container by its elements. Any
   * other value prints as its class and {@code toString}, so a value whose {@code toString} is
   * its identity keys nothing another JVM finds, and is only computed again.
   */
  static String describe(@Nullable Object o) {
    return describe(o, false);
  }

  /** {@link #describe}, each attribute with its expression id where {@code ids}. */
  static String describe(@Nullable Object o, boolean ids) {
    var sb = new StringBuilder();
    describe(o, sb, ids);
    return sb.toString();
  }

  private static void describe(@Nullable Object o, StringBuilder sb, boolean ids) {
    switch (o) {
      case null -> sb.append("null");
      case Expression e -> {
        sb.append(e.getClass().getName()).append('(');
        var args = e.productIterator();
        while (args.hasNext()) {
          describe(args.next(), sb, ids);
          sb.append(',');
        }
        sb.append(")@").append(e.origin());
        if (field(TAGS, e) instanceof scala.collection.Map<?, ?> tags && !tags.isEmpty()) {
          var sorted = new TreeMap<String, Object>();
          var pairs = tags.iterator();
          while (pairs.hasNext()) {
            scala.Tuple2<?, ?> pair = pairs.next();
            sorted.put(String.valueOf(pair._1()), pair._2());
          }
          sb.append("+tags:");
          describe(sorted.entrySet().stream().map(x -> x.getKey() + "=" + describe(x.getValue(),
              ids)).toList(), sb, ids);
        }
        java.util.Set<Expression> arguments =
            java.util.Collections.newSetFromMap(new IdentityHashMap<>());
        arguments.addAll(expressionArguments(e));
        for (Expression child : CollectionConverters.asJava(e.children())) {
          if (!arguments.contains(child)) {
            sb.append("+child:");
            describe(child, sb, ids);
          }
        }
        if (ids && e instanceof AttributeReference a) {
          sb.append('#').append(a.exprId());
        }
      }
      case Double d -> sb.append("d:").append(Long.toHexString(Double.doubleToRawLongBits(d)));
      case Float f -> sb.append("f:").append(Integer.toHexString(Float.floatToRawIntBits(f)));
      case byte[] b -> sb.append("b:").append(HexFormat.of().formatHex(b));
      case Object[] a -> elements(sb, "array", Arrays.asList(a).iterator(), ids);
      case List<?> l -> elements(sb, "list", l.iterator(), ids);
      case scala.Option<?> opt -> {
        sb.append("option(");
        if (opt.isDefined()) {
          describe(opt.get(), sb, ids);
        }
        sb.append(')');
      }
      // A sequence's or a set's elements, not its class: a transform rebuilds a List as a Vector.
      case scala.collection.Seq<?> s ->
          elements(sb, "seq", CollectionConverters.asJava(s.iterator()), ids);
      case scala.collection.Set<?> s ->
          elements(sb, "set", CollectionConverters.asJava(s.iterator()), ids);
      case scala.collection.Iterable<?> s ->
          elements(sb, s.getClass().getName(), CollectionConverters.asJava(s.iterator()), ids);
      case UnsafeRow r -> sb.append("unsafeRow:").append(HexFormat.of().formatHex(r.getBytes()));
      case GenericInternalRow r -> elements(sb, "row", Arrays.asList(r.values()).iterator(), ids);
      case UnsafeArrayData a -> sb.append("unsafeArray:")
          .append(HexFormat.of().formatHex(a.toByteArray()));
      case GenericArrayData a ->
          elements(sb, "arrayData", Arrays.asList(a.array()).iterator(), ids);
      case ArrayBasedMapData m -> {
        sb.append("map(");
        describe(m.keyArray(), sb, ids);
        sb.append("->");
        describe(m.valueArray(), sb, ids);
        sb.append(')');
      }
      case Record r -> {
        sb.append(r.getClass().getName()).append('{');
        for (var component : r.getClass().getRecordComponents()) {
          try {
            describe(component.getAccessor().invoke(r), sb, ids);
          } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
          }
          sb.append(',');
        }
        sb.append('}');
      }
      case scala.Product p -> elements(sb, p.getClass().getName(),
          CollectionConverters.asJava(p.productIterator()), ids);
      default -> sb.append(o.getClass().getName()).append(':').append(o);
    }
  }

  private static void elements(StringBuilder sb, String what, java.util.Iterator<?> it,
      boolean ids) {
    sb.append(what).append('[');
    while (it.hasNext()) {
      describe(it.next(), sb, ids);
      sb.append(',');
    }
    sb.append(']');
  }

  // ---------------------------------------------------------------------------------------------
  // Bytes and files
  // ---------------------------------------------------------------------------------------------

  static byte[] serialize(Object value) {
    var bytes = new ByteArrayOutputStream();
    try (var out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  static Object deserialize(byte[] bytes) {
    try (var in = new ContextObjectInputStream(new ByteArrayInputStream(bytes))) {
      return in.readObject();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Classes resolved by the thread's context loader first, as Spark's own deserializer does. */
  private static final class ContextObjectInputStream extends ObjectInputStream {
    ContextObjectInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc)
        throws IOException, ClassNotFoundException {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      if (loader != null) {
        try {
          return Class.forName(desc.getName(), false, loader);
        } catch (ClassNotFoundException e) {
          // The default resolution below.
        }
      }
      return super.resolveClass(desc);
    }
  }

  private static final int MAGIC = 0x56484153;
  private static final int VERSION = 1;

  /** Every {@code harness-*.bin} in {@code dir}, the first file's entry where two share a key. */
  static Map<String, byte[]> read(Path dir) {
    var out = new ConcurrentHashMap<String, byte[]>();
    if (!Files.isDirectory(dir)) {
      return out;
    }
    try (var files = Files.list(dir)) {
      for (Path file : files.filter(f -> f.getFileName().toString().startsWith("harness-")
          && f.getFileName().toString().endsWith(".bin")).sorted().toList()) {
        try (var in = new DataInputStream(new GZIPInputStream(Files.newInputStream(file),
            1 << 16))) {
          if (in.readInt() != MAGIC || in.readInt() != VERSION) {
            continue;
          }
          int n = in.readInt();
          for (int i = 0; i < n; i++) {
            String key = new String(in.readNBytes(in.readInt()), StandardCharsets.UTF_8);
            byte[] value = in.readNBytes(in.readInt());
            out.putIfAbsent(key, value);
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out;
  }

  /** {@code entries} as a file of this JVM's own in {@code dir}, written whole or not at all. */
  static void write(Path dir, Map<String, byte[]> entries) {
    try {
      Files.createDirectories(dir);
      String name = "harness-" + ProcessHandle.current().pid() + "-" + System.nanoTime();
      Path tmp = dir.resolve(name + ".tmp");
      try (var out = new DataOutputStream(new GZIPOutputStream(Files.newOutputStream(tmp),
          1 << 16))) {
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        var copy = new TreeMap<>(entries);
        out.writeInt(copy.size());
        for (Map.Entry<String, byte[]> e : copy.entrySet()) {
          byte[] key = e.getKey().getBytes(StandardCharsets.UTF_8);
          out.writeInt(key.length);
          out.write(key);
          out.writeInt(e.getValue().length);
          out.write(e.getValue());
        }
      }
      Files.move(tmp, dir.resolve(name + ".bin"), StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
