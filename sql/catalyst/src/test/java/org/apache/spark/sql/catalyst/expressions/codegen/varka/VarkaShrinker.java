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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.AddMonths;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.ColumnRef;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LiteralSlot;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.TruncDateDynamic;

/**
 * Reduces a failing fuzz case to a small one that fails the same way (VARKA-277): delta
 * debugging, Zeller and Hildebrandt's ddmin, whose result is 1-minimal, over the case's parts in
 * the order the plan gives - the roots, each tree, the options, the batch - repeated until a
 * pass changes nothing.
 *
 * <p>A candidate is kept only if it fails with the original's {@link VarkaFailureSignature}, so a
 * reduction cannot slide from one bug to another. A run that passes, is skipped or fails
 * differently is "not smaller".
 */
public final class VarkaShrinker {

  private VarkaShrinker() {
  }

  /** What a shrink produced and what it cost. */
  public record Shrunk(
      VarkaFuzzCase small, int runs, long millis, boolean stoppedEarly, boolean stable) { }

  /** The number of IR nodes of a tree. */
  public static int size(VarkaVectorIR node) {
    int n = 1;
    for (VarkaVectorIR child : VarkaVectorIR.childrenOf(node)) {
      n += size(child);
    }
    return n;
  }

  /**
   * ddmin: the smallest sublist of {@code items}, in order, that still makes {@code fails} true,
   * 1-minimal (removing any one item stops it failing). {@code fails} is asked about the whole
   * list first by the caller's contract and is not asked again here; an empty result is possible
   * when {@code fails} holds for the empty list.
   */
  public static <A> List<A> ddmin(List<A> items, Predicate<List<A>> fails) {
    List<A> current = List.copyOf(items);
    int n = 2;
    while (current.size() >= 2) {
      int chunk = Math.max(1, (current.size() + n - 1) / n);
      var parts = new ArrayList<List<A>>();
      for (int from = 0; from < current.size(); from += chunk) {
        parts.add(List.copyOf(current.subList(from, Math.min(current.size(), from + chunk))));
      }
      List<A> part = null;
      for (List<A> p : parts) {
        if (fails.test(p)) {
          part = p;
          break;
        }
      }
      if (part != null) {
        current = part;
        n = 2;
        continue;
      }
      // At n = 2 a complement is the other part, which was just tried.
      List<A> complement = null;
      if (n != 2) {
        for (int i = 0; i < parts.size(); i++) {
          var rest = new ArrayList<A>();
          for (int j = 0; j < parts.size(); j++) {
            if (j != i) {
              rest.addAll(parts.get(j));
            }
          }
          if (!rest.isEmpty() && fails.test(rest)) {
            complement = List.copyOf(rest);
            break;
          }
        }
      }
      if (complement != null) {
        current = complement;
        n = Math.max(n - 1, 2);
      } else {
        if (n >= current.size()) {
          return current;
        }
        n = Math.min(current.size(), 2 * n);
      }
    }
    return current.size() == 1 && fails.test(List.of()) ? List.of() : current;
  }

  /** {@link #shrink(VarkaFuzzCase, VarkaFailureSignature, Function, int, long)} at 600 runs and
   *  a minute. */
  public static Shrunk shrink(VarkaFuzzCase original, VarkaFailureSignature signature,
      Function<VarkaFuzzCase, Optional<VarkaFailureSignature>> outcome) {
    return shrink(original, signature, outcome, 600, 60000L);
  }

  /**
   * Shrinks {@code original}, which fails with {@code signature} under {@code outcome}. At most
   * {@code maxRuns} runs and {@code maxMillis} of wall time are spent; a shrink that hits either
   * returns what it has.
   */
  public static Shrunk shrink(VarkaFuzzCase original, VarkaFailureSignature signature,
      Function<VarkaFuzzCase, Optional<VarkaFailureSignature>> outcome, int maxRuns,
      long maxMillis) {
    var budget = new Budget<VarkaFuzzCase>(signature, outcome, maxRuns, maxMillis);
    VarkaFuzzCase best = original;
    boolean changed = true;
    while (changed && !budget.stopped) {
      VarkaFuzzCase before = best;
      best = shrinkRoots(best, budget::stillFails);
      best = shrinkTrees(best, budget::stillFails);
      best = shrinkOptions(best, budget::stillFails);
      best = shrinkBatch(best, budget::stillFails);
      changed = !sameCase(before, best);
    }
    // The stability runs first, so that the time reported counts them.
    boolean stable = budget.stable(best);
    return new Shrunk(best, budget.runs, budget.elapsed(), budget.stopped, stable);
  }

  /**
   * The runs a shrink may spend, and whether a candidate still fails the original's way. The
   * composition shrinker (VARKA-294) spends its runs through it too.
   */
  public static final class Budget<C> {
    private final VarkaFailureSignature signature;
    private final Function<C, Optional<VarkaFailureSignature>> outcome;
    private final int maxRuns;
    private final long maxMillis;
    private final long start = System.nanoTime();
    private int runs = 0;
    private boolean stopped = false;

    public Budget(VarkaFailureSignature signature,
        Function<C, Optional<VarkaFailureSignature>> outcome, int maxRuns, long maxMillis) {
      this.signature = signature;
      this.outcome = outcome;
      this.maxRuns = maxRuns;
      this.maxMillis = maxMillis;
    }

    public long elapsed() {
      return (System.nanoTime() - start) / 1000000L;
    }

    public int runs() {
      return runs;
    }

    public boolean stopped() {
      return stopped;
    }

    /** Whether {@code c} fails with the signature; false, and stopped, once the budget is spent. */
    public boolean stillFails(C c) {
      if (runs >= maxRuns || elapsed() >= maxMillis) {
        stopped = true;
        return false;
      }
      runs += 1;
      return outcome.apply(c).filter(signature::equals).isPresent();
    }

    /**
     * A failure that depends on the JIT may not repeat: the result is run three times more, and
     * is reported as unstable unless every run fails the same way. These runs are not counted.
     */
    public boolean stable(C best) {
      for (int i = 0; i < 3; i++) {
        if (outcome.apply(best).filter(signature::equals).isEmpty()) {
          return false;
        }
      }
      return true;
    }
  }

  private static boolean sameCase(VarkaFuzzCase a, VarkaFuzzCase b) {
    return a.roots().equals(b.roots()) && a.options().equals(b.options())
        && a.length() == b.length() && a.forceMasked() == b.forceMasked()
        && Arrays.deepEquals(a.nulls(), b.nulls()) && Arrays.deepEquals(a.data(), b.data());
  }

  private static VarkaFuzzCase shrinkRoots(VarkaFuzzCase c, Predicate<VarkaFuzzCase> fails) {
    if (c.roots().size() < 2) {
      return c;
    }
    return c.withRoots(ddmin(c.roots(), rs -> !rs.isEmpty() && fails.test(c.withRoots(rs))));
  }

  // ---- trees ----------------------------------------------------------------------------

  private static VarkaVectorIR nodeAt(VarkaVectorIR root, List<Integer> path) {
    VarkaVectorIR n = root;
    for (int i : path) {
      n = VarkaVectorIR.childrenOf(n)[i];
    }
    return n;
  }

  private static List<Integer> append(List<Integer> path, int i) {
    var out = new ArrayList<>(path);
    out.add(i);
    return List.copyOf(out);
  }

  /** The positions of a tree, root first and then level by level. */
  private static List<List<Integer>> levelOrder(VarkaVectorIR root) {
    var out = new ArrayList<List<Integer>>();
    List<List<Integer>> level = List.of(List.of());
    while (!level.isEmpty()) {
      out.addAll(level);
      var next = new ArrayList<List<Integer>>();
      for (List<Integer> p : level) {
        int children = VarkaVectorIR.childrenOf(nodeAt(root, p)).length;
        for (int i = 0; i < children; i++) {
          next.add(append(p, i));
        }
      }
      level = next;
    }
    return out;
  }

  /**
   * {@code root} with the node at {@code path} replaced, rebuilt up the spine by
   * {@code withChildren}; empty when a constructor refuses the result, which means the
   * replacement does not type there.
   */
  private static Optional<VarkaVectorIR> replaceAt(VarkaVectorIR root, List<Integer> path,
      VarkaVectorIR repl) {
    if (path.isEmpty()) {
      return Optional.of(repl);
    }
    VarkaVectorIR[] children = VarkaVectorIR.childrenOf(root);
    int head = path.get(0);
    return replaceAt(children[head], path.subList(1, path.size()), repl).flatMap(child -> {
      VarkaVectorIR[] updated = children.clone();
      updated[head] = child;
      try {
        return Optional.of(VarkaVectorIR.withChildren(root, updated));
      } catch (IllegalArgumentException e) {
        return Optional.empty();
      }
    });
  }

  /**
   * The operand positions whose value domain the grammar constrains, which a move must leave
   * alone: the month count of {@code add_months} and the level of a dynamic truncation read the
   * two special columns, whose data is drawn to fit them.
   */
  private static boolean isProtected(VarkaVectorIR root, List<Integer> path) {
    for (int depth = 0; depth < path.size(); depth++) {
      VarkaVectorIR parent = nodeAt(root, path.subList(0, depth));
      int index = path.get(depth);
      if ((parent instanceof AddMonths || parent instanceof TruncDateDynamic) && index == 1) {
        return true;
      }
    }
    return false;
  }

  /** The smaller trees to try in {@code node}'s place: a leaf first, then each same-kind child. */
  private static List<VarkaVectorIR> replacements(VarkaVectorIR node, VarkaFuzzCase c) {
    var out = new ArrayList<VarkaVectorIR>();
    LaneType lane = node.laneType();
    // A leaf has no smaller leaf to try.
    boolean isLeaf = node instanceof ColumnRef || node instanceof LiteralSlot;
    if (node instanceof Cond) {
      List<Integer> columns = usableColumns(c);
      if (!columns.isEmpty()) {
        VarkaVectorIR leaf = new IsNotNull(new ColumnRef(columns.get(0), lane));
        if (!leaf.equals(node)) {
          out.add(leaf);
        }
      }
    } else if (!isLeaf) {
      for (int o : usableColumns(c)) {
        out.add(new ColumnRef(o, lane));
      }
      for (int j = 0; j < c.lits().length; j++) {
        out.add(new LiteralSlot(j, lane));
      }
    }
    for (VarkaVectorIR ch : VarkaVectorIR.childrenOf(node)) {
      if ((ch instanceof Cond) == (node instanceof Cond) && ch.laneType() == lane) {
        out.add(ch);
      }
    }
    return out;
  }

  /** The columns a leaf may read: all but the two whose data the grammar constrains. */
  private static List<Integer> usableColumns(VarkaFuzzCase c) {
    var out = new ArrayList<Integer>();
    for (int o = 0; o < c.numInputs(); o++) {
      if (o != c.smallOrdinal() && o != c.levelOrdinal()) {
        out.add(o);
      }
    }
    return out;
  }

  private static List<VarkaVectorIR> updated(List<VarkaVectorIR> roots, int i,
      VarkaVectorIR t) {
    var out = new ArrayList<>(roots);
    out.set(i, t);
    return out;
  }

  private static VarkaFuzzCase shrinkTrees(VarkaFuzzCase c, Predicate<VarkaFuzzCase> fails) {
    VarkaFuzzCase current = c;
    for (int i = 0; i < current.roots().size(); i++) {
      boolean improved = true;
      while (improved) {
        improved = false;
        VarkaVectorIR root = current.roots().get(i);
        int rootSize = size(root);
        VarkaVectorIR found = null;
        search:
        for (List<Integer> p : levelOrder(root)) {
          if (isProtected(root, p)) {
            continue;
          }
          for (VarkaVectorIR r : replacements(nodeAt(root, p), current)) {
            Optional<VarkaVectorIR> t = replaceAt(root, p, r);
            if (t.isPresent() && size(t.get()) < rootSize
                && fails.test(current.withRoots(updated(current.roots(), i, t.get())))) {
              found = t.get();
              break search;
            }
          }
        }
        if (found != null) {
          current = current.withRoots(updated(current.roots(), i, found));
          improved = true;
        }
      }
    }
    return current;
  }

  /**
   * How {@link #shrinkTree} reads and rebuilds a tree of some other grammar than the IR: a node's
   * children, the node rebuilt over new children, and the smaller trees to try in a node's place,
   * each of which must type wherever the node does.
   */
  public interface TreeMoves<T> {
    List<T> children(T node);

    T withChildren(T node, List<T> children);

    List<T> replacements(T node);
  }

  private static <T> int sizeOf(T node, TreeMoves<T> moves) {
    int n = 1;
    for (T child : moves.children(node)) {
      n += sizeOf(child, moves);
    }
    return n;
  }

  private static <T> T replaceIn(T root, List<Integer> path, T repl, TreeMoves<T> moves) {
    if (path.isEmpty()) {
      return repl;
    }
    var children = new ArrayList<>(moves.children(root));
    int head = path.get(0);
    children.set(head, replaceIn(children.get(head), path.subList(1, path.size()), repl, moves));
    return moves.withChildren(root, children);
  }

  /**
   * {@code root} reduced while {@code fails} holds, the way {@link #shrinkTrees} reduces an IR
   * tree (VARKA-298): positions root first and then level by level, each node replaced by the first
   * of its {@code replacements} that is smaller and still fails, until no replacement does. With
   * {@code freeRoot} the root may also become any of its children whatever its type, which suits a
   * position that takes any type, such as a query's output.
   */
  public static <T> T shrinkTree(T root, TreeMoves<T> moves, boolean freeRoot,
      Predicate<T> fails) {
    T current = root;
    // Each accepted move restarts the search from the root, where the candidates already tried
    // are the same trees again; one that did not fail is not asked again, since each ask can be a
    // whole query on two engines.
    var passed = new HashSet<T>();
    boolean improved = true;
    while (improved) {
      improved = false;
      int currentSize = sizeOf(current, moves);
      var level = List.<List<Integer>>of(List.of());
      search:
      while (!level.isEmpty()) {
        var next = new ArrayList<List<Integer>>();
        for (List<Integer> p : level) {
          T node = current;
          for (int k : p) {
            node = moves.children(node).get(k);
          }
          var candidates = new ArrayList<>(moves.replacements(node));
          if (freeRoot && p.isEmpty()) {
            for (T child : moves.children(node)) {
              if (!candidates.contains(child)) {
                candidates.add(child);
              }
            }
          }
          for (T r : candidates) {
            T t = replaceIn(current, p, r, moves);
            if (sizeOf(t, moves) < currentSize && !passed.contains(t)) {
              if (!fails.test(t)) {
                passed.add(t);
                continue;
              }
              current = t;
              improved = true;
              break search;
            }
          }
          for (int k = 0; k < moves.children(node).size(); k++) {
            next.add(append(p, k));
          }
        }
        level = next;
      }
    }
    return current;
  }

  // ---- options --------------------------------------------------------------------------

  private static VarkaFuzzCase shrinkOptions(VarkaFuzzCase c, Predicate<VarkaFuzzCase> fails) {
    return c.withOptions(shrinkOptionsOf(c.options(), o -> fails.test(c.withOptions(o))));
  }

  /**
   * {@code options} with as few of its differences from the defaults as still make
   * {@code fails} true, by {@code ddmin} over the differing options. Shared with the composition
   * shrinker (VARKA-294).
   */
  public static VarkaEmitOptions shrinkOptionsOf(VarkaEmitOptions options,
      Predicate<VarkaEmitOptions> fails) {
    List<VarkaEmitOption> changed = VarkaEmitOption.TABLE.stream()
        .filter(o -> !o.text(options).equals(o.text(VarkaEmitOptions.DEFAULTS)))
        .toList();
    if (changed.isEmpty()) {
      return options;
    }
    return keeping(options, changed,
        ddmin(changed, subset -> fails.test(keeping(options, changed, subset))));
  }

  /** {@code options} with every option of {@code changed} not in {@code kept} at its default. */
  private static VarkaEmitOptions keeping(VarkaEmitOptions options,
      List<VarkaEmitOption> changed, List<VarkaEmitOption> kept) {
    var builder = options.toBuilder();
    for (VarkaEmitOption o : changed) {
      if (!kept.contains(o)) {
        o.applyDefault(builder);
      }
    }
    return builder.build();
  }

  // ---- the batch ------------------------------------------------------------------------

  /** {@code c} over only the rows {@code rows}, in order. */
  private static VarkaFuzzCase onlyRows(VarkaFuzzCase c, List<Integer> rows) {
    boolean[][] nulls = new boolean[c.nulls().length][];
    for (int col = 0; col < nulls.length; col++) {
      nulls[col] = new boolean[rows.size()];
      for (int k = 0; k < rows.size(); k++) {
        nulls[col][k] = c.nulls()[col][rows.get(k)];
      }
    }
    long[][] data = new long[c.data().length][];
    for (int col = 0; col < data.length; col++) {
      data[col] = new long[rows.size()];
      for (int k = 0; k < rows.size(); k++) {
        data[col][k] = c.data()[col][rows.get(k)];
      }
    }
    // The harness never forces the masked path at length 1 (a null count equal to the length
    // is the contract's all-null column).
    return new VarkaFuzzCase(c.lane(), c.roots(), c.numInputs(), c.lits(), rows.size(), nulls,
        data, c.forceMasked() && rows.size() > 1, c.options(), c.smallOrdinal(),
        c.levelOrdinal(), c.label());
  }

  private static boolean anyNull(boolean[] column) {
    for (boolean b : column) {
      if (b) {
        return true;
      }
    }
    return false;
  }

  /** 0 for a column of zeros, 1 for a column of ones, 2 for anything else. */
  private static int rank(long[] column) {
    if (Arrays.stream(column).allMatch(v -> v == 0L)) {
      return 0;
    }
    return Arrays.stream(column).allMatch(v -> v == 1L) ? 1 : 2;
  }

  private static VarkaFuzzCase shrinkBatch(VarkaFuzzCase c, Predicate<VarkaFuzzCase> fails) {
    VarkaFuzzCase current = c;
    if (current.length() > 1) {
      var all = new ArrayList<Integer>();
      for (int i = 0; i < current.length(); i++) {
        all.add(i);
      }
      VarkaFuzzCase whole = current;
      List<Integer> rows = ddmin(all, rs -> !rs.isEmpty() && fails.test(onlyRows(whole, rs)));
      current = onlyRows(current, rows);
    }
    if (current.forceMasked()) {
      VarkaFuzzCase plain = current.withForceMasked(false);
      if (fails.test(plain)) {
        current = plain;
      }
    }
    // Nulls: all valid if the failure survives it, else column by column.
    if (Arrays.stream(current.nulls()).anyMatch(VarkaShrinker::anyNull)) {
      boolean[][] none = new boolean[current.nulls().length][];
      for (int col = 0; col < none.length; col++) {
        none[col] = new boolean[current.nulls()[col].length];
      }
      VarkaFuzzCase clear = current.withNulls(none);
      if (fails.test(clear)) {
        current = clear;
      } else {
        for (int col = 0; col < current.numInputs(); col++) {
          if (anyNull(current.nulls()[col])) {
            boolean[][] nulls = current.nulls().clone();
            nulls[col] = new boolean[current.length()];
            VarkaFuzzCase one = current.withNulls(nulls);
            if (fails.test(one)) {
              current = one;
            }
          }
        }
      }
    }
    // Data: all zero, else all one, for each column the grammar does not constrain; a column
    // already simpler than a value is not moved to it.
    for (int col = 0; col < current.numInputs(); col++) {
      if (col == current.smallOrdinal() || col == current.levelOrdinal()) {
        continue;
      }
      for (long value = 0L; value <= 1L; value++) {
        if (value >= rank(current.data()[col])) {
          continue;
        }
        long[][] data = current.data().clone();
        data[col] = new long[current.length()];
        Arrays.fill(data[col], value);
        VarkaFuzzCase filled = current.withData(data);
        if (fails.test(filled)) {
          current = filled;
          break;
        }
      }
    }
    return current;
  }
}
