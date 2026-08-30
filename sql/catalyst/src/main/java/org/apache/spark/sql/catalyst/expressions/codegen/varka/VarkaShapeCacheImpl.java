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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader;
import org.apache.spark.util.NonFateSharingCache;
import org.apache.spark.util.SparkStringUtils$;

/**
 * The bounded cross-task cache of loaded fused-kernel classes (task 18, {@code
 * PLAN_MILESTONE_3.md} 2.1). Task 14's diagnosis set its design: emission costs ~80 us and was
 * never the problem, but a re-defined class is a new class to HotSpot and re-pays the whole tier
 * ladder - a fixed 13-50 ms per task. Only reusing the loaded class amortises that, so this is an
 * LRU over classes each held by its own {@link VarkaGeneratedClassLoader}, released (and so
 * unloadable) on eviction rather than on task end. Metaspace is bounded by cache size instead of
 * task lifetime - a weaker guarantee than milestone 1's per-task unloading, proven the same way
 * ({@code VarkaShapeCacheSuite}, weak references against eviction).
 *
 * <p><b>Correctness before performance.</b> A wrong hit returns wrong results and the ghost
 * fallback cannot catch it - it catches failures, not silently different answers. The key is
 * therefore {@link VarkaShapeKey}, derived structurally from the same records the emitter walks,
 * never assembled by hand at a call site; the differential suites run warm as well as cold. The
 * one emit input outside the shape - the emitter's static test hooks - is guarded instead:
 * {@link #getOrEmit} refuses every lookup while a hook is set (a hit would serve plain bytes to a
 * hooked caller), and {@link #emit} snapshots the emitter's hook-write generation around the
 * emission - a hook set and cleared mid-emission restores the values but not the generation, so
 * its bytes are never cached.
 *
 * <p><b>The loader is part of the key.</b> Each entry's generated loader parents the class loader
 * its caller passed in - in production the context class loader of the task that emitted it, which
 * {@code VarkaShapeCache} resolves (as the per-task loaders had it): the emitted bytes call the
 * engine's {@code VarkaVectorSupport}, which in the documented deployment arrives via
 * {@code --jars} and is visible only through the context loader - the loader of
 * {@link VarkaFusedKernel} (catalyst, app classpath) cannot see it. That makes the parent an input
 * to linkage, so it rides the key by identity: with one executor-wide context loader nothing
 * changes (one class per shape), and under session artifact isolation each session's loader gets
 * its own entry rather than linking through another session's - possibly closed - chain. A closed
 * session's entries are not released eagerly (nothing here observes session close); they age out
 * of the LRU, so the retained loaders are bounded by the cache capacity like everything else.
 *
 * <p><b>Naming and telemetry.</b> The class is named by its shape ({@link #classNameFor}:
 * {@code VarkaFusedProjection_<hash>}, 16 hex chars of SHA-256 over the key's canonical rendering -
 * {@code VarkaVectorIR.canonical}, hand-pinned so the hash never rides an unspecified
 * {@code Record.toString} format), {@code SourceFile} carries the same name, and the
 * {@link VarkaDebugInfo} attribute's plan fragment carries {@code shape <hash>} - the bytes
 * describe the shape, which is exactly what is shared. The map is keyed on the full structural
 * key, so a hash collision cannot cause a wrong hit; it could only give two distinct shapes one
 * <i>name</i> (their loaders keep the runtime classes distinct). The per-execution identity that
 * used to be baked into the bytes - operator, stage, the projection list - lives in a bounded side
 * table keyed by the hash, recorded (truncated) on every lookup at every capacity,
 * {@code maxEntries} = 0 included, so the diagnostics join {@link #executionsFor} keeps working
 * when sharing is off.
 *
 * <p><b>Concurrency and failure.</b> Lookups go through {@link NonFateSharingCache} (SPARK-43300):
 * tasks racing on one shape serialize on a per-key lock, the winner emits once, and a cancelled or
 * failed emit fails only its own task - the co-waiters retry for themselves instead of inheriting
 * the failure into their ghost fallbacks. Guava wraps what the callable throws
 * ({@link ExecutionException}, {@link UncheckedExecutionException}, {@link ExecutionError});
 * {@link #getOrEmit} unwraps to the cause, because the wrapper would defeat the evaluator's
 * fatal-error discipline - an {@code OutOfMemoryError} inside emit must reach the task as itself,
 * and an interrupt must cancel the task. Eviction while a task still runs the class is safe:
 * {@code release()} only drops the loader's registry, and the task's strong references keep the
 * class alive until it completes (the owner-side contract the engine's {@code VarkaClassLoader}
 * documents).
 *
 * <p>With {@code maxEntries} = 0 the same single path degenerates to the per-task class lifecycle:
 * Guava evicts each entry immediately after loading it, the removal listener releases the loader,
 * and the task's strong references carry the class to task end - the pre-task-18 unload contract,
 * with no second code path to keep in step. (Racing lookups of one shape may still share the one
 * in-flight load; that is fine - nothing is retained either way.)
 *
 * <p><b>What is deliberately not here</b> (task 23, which split this class out of the Scala
 * {@code VarkaShapeCache}): nothing that reads Spark's configuration or environment. Capacity and
 * the parent class loader arrive as plain values, so this class has one behaviour per constructed
 * instance rather than one per whichever thread happened to touch a singleton first - which is
 * what made the old lazily-sized singleton nondeterministic on an executor. {@code VarkaShapeCache}
 * is the facade that resolves both and owns the JVM-wide instance.
 */
public final class VarkaShapeCacheImpl {

  private static final SparkLogger LOG =
      SparkLoggerFactory.getLogger(VarkaShapeCacheImpl.class);

  /**
   * The longest execution identity the side table stores; longer ones are abbreviated to exactly
   * this length, marker included. Operator, stage and the leading projection entries survive,
   * which is what the diagnostics join needs - and callers building an identity string need not
   * render more than this.
   */
  public static final int MAX_EXECUTION_IDENTITY_LENGTH = 256;

  /** How many per-execution identities the side table keeps per shape, oldest evicted first. */
  private static final int MAX_EXECUTIONS_PER_SHAPE = 8;

  /**
   * The full cache key: the shape, plus the identity of the class loader the entry's generated
   * loader parents. The parent loader is an input to how the class links (it resolves the engine's
   * support classes), so two contexts with different loaders must not share an entry - under
   * executor-side session artifact isolation each session has its own context loader, and a class
   * linked through session A's chain must not serve session B. In the documented {@code --jars}
   * deployment the executor has one context loader, so this key degenerates to the shape alone.
   * {@code ClassLoader} has identity equality, which is exactly the sharing rule wanted.
   */
  private record LoaderShapeKey(ClassLoader parent, VarkaShapeKey shape) {
  }

  private final int maxEntries;

  private final LongAdder hits = new LongAdder();
  private final LongAdder misses = new LongAdder();

  private final NonFateSharingCache<LoaderShapeKey, VarkaShapeEntry> cache;

  // The side table: shape hash -> the most recent execution identities that used the shape.
  // Bounded three times over - entries here, identities per entry, characters per identity -
  // because it is diagnostics, not bookkeeping: it answers "which operators ran
  // VarkaFusedProjection_<hash>?" for a name seen in a profile, a class dump or a JFR event,
  // without that identity riding the shared bytes.
  // Floored at 64 shapes so the diagnostics join works at every capacity: with maxEntries = 0
  // the classes still carry only their shape name, and a table sized off the (zero) class
  // capacity would evict live shapes' identities mid-query. 64 shapes of 8 truncated
  // identities is at most a few hundred KB.
  private final Cache<String, LinkedHashSet<String>> executions;

  public VarkaShapeCacheImpl(int maxEntries) {
    if (maxEntries < 0) {
      throw new IllegalArgumentException("maxEntries must not be negative: " + maxEntries);
    }
    this.maxEntries = maxEntries;
    Cache<LoaderShapeKey, VarkaShapeEntry> classes = CacheBuilder.newBuilder()
        .maximumSize(maxEntries)
        // Guava swallows a throwing listener; release() cannot throw, and must not be more
        // than "stop retaining": running tasks still hold the class until they complete.
        .<LoaderShapeKey, VarkaShapeEntry>removalListener(n -> n.getValue().loader().release())
        .build();
    this.cache = new NonFateSharingCache<>(classes);
    this.executions = CacheBuilder.newBuilder()
        .maximumSize(Math.max((long) maxEntries * 4, 64))
        .build();
  }

  public int maxEntries() {
    return maxEntries;
  }

  /**
   * Returns the loaded class for the shape under {@code parent}, emitting and defining it if no
   * live entry holds it, and records {@code execution} (the caller's per-execution identity,
   * truncated) in the side table either way. With {@code maxEntries} = 0 every lookup emits: the
   * entry is evicted (and its loader released) as it is loaded, restoring the per-task class
   * lifecycle through this same path.
   */
  public VarkaShapeLookup getOrEmit(ClassLoader parent, VarkaShapeKey key, String execution) {
    if (VarkaLoopEmitter.anyTestHookSet()) {
      // Checked on every lookup, not only under emit: a hit would hand a hooked caller the
      // plain bytes, as silently wrong as caching hooked bytes under the plain key.
      throw new IllegalStateException("a VarkaLoopEmitter test hook is set: the shape cache "
          + "serves and caches only plain bytes. Suites that set hooks call "
          + "VarkaLoopEmitter.emit directly and bypass the cache.");
    }
    LoaderShapeKey loaderKey = new LoaderShapeKey(parent, key);
    // A one-element array, not a local: the loading callable has to report back whether it ran,
    // and a lambda can only capture something effectively final.
    boolean[] emitted = {false};
    VarkaShapeEntry entry;
    try {
      entry = cache.get(loaderKey, () -> {
        emitted[0] = true;
        return emit(loaderKey);
      });
    } catch (ExecutionError | UncheckedExecutionException e) {
      // Unwrap Guava's wrappers: the cause must reach the evaluator's isCatchable test as
      // itself - a fatal error fails the task, an interrupt cancels it. Rethrown unwrapped
      // rather than re-wrapped, which is why it goes through sneakyThrow.
      throw sneakyThrow(e.getCause());
    } catch (Exception e) {
      // Guava's third wrapper, ExecutionException, is checked - and NonFateSharingCache is
      // Scala, so its `get` declares no checked exceptions at all and the compiler will not
      // let one be named in a catch clause here. Catching Exception and testing is the way to
      // reach it; precise rethrow keeps this method free of a `throws` clause.
      throw sneakyThrow(e instanceof ExecutionException ? e.getCause() : e);
    }
    // Bounded once, for both consumers: the side-table entry and the JFR event must carry
    // the same string or the advertised join between them fails on anything abbreviated -
    // and the event payload must not be unbounded (the evaluator's identity builder checks
    // the bound only before appending, so a single long entry can overshoot it by a lot).
    String boundedExecution =
        SparkStringUtils$.MODULE$.abbreviate(execution, MAX_EXECUTION_IDENTITY_LENGTH);
    recordExecution(entry.shapeHash(), boundedExecution);
    if (emitted[0]) {
      misses.increment();
    } else {
      hits.increment();
    }
    // Task 22: the counters' JFR twin - one event per task-level resolution, carrying the
    // per-execution identity that must not ride the shared bytes.
    VarkaCacheLookupEvent lookupEvent = new VarkaCacheLookupEvent();
    if (lookupEvent.isEnabled()) {
      lookupEvent.shapeHash = entry.shapeHash();
      lookupEvent.hit = !emitted[0];
      lookupEvent.execution = boundedExecution;
      lookupEvent.commit();
    }
    return new VarkaShapeLookup(entry, !emitted[0]);
  }

  /** The recorded execution identities for a shape hash, most recent last; empty if unknown. */
  public List<String> executionsFor(String shapeHash) {
    List<String> snapshot = new ArrayList<>();
    // Inside compute, so the copy is taken under the same per-bin lock recordExecution mutates
    // the set under. The set itself is not thread-safe and is never published outside these two.
    executions.asMap().computeIfPresent(shapeHash, (h, set) -> {
      snapshot.addAll(set);
      return set;
    });
    return List.copyOf(snapshot);
  }

  public long hitCount() {
    return hits.sum();
  }

  public long missCount() {
    return misses.sum();
  }

  public long size() {
    return cache.size();
  }

  /** Test hook: drops every entry (releasing the loaders) and the side table. */
  public void invalidateAll() {
    cache.invalidateAll();
    executions.invalidateAll();
  }

  /** The one rendering of the shape-named class name; every caller derives it here. */
  public static String classNameFor(String shapeHash) {
    return "org.apache.spark.sql.varka.execution.VarkaFusedProjection_" + shapeHash;
  }

  /** The one rendering of the shape-named {@code SourceFile}; every caller derives it here. */
  public static String sourceFileFor(String shapeHash) {
    return "VarkaFusedProjection_" + shapeHash + ".java";
  }

  /**
   * The shape's stable name fragment: 16 hex characters of SHA-256 over the key's canonical
   * rendering ({@code VarkaVectorIR.canonical}, hand-pinned - not {@code Record.toString}, whose
   * format no JDK promises). A pure function of the key: equal keys hash equal on every JVM, so
   * one shape carries one class name across executors, mixed-JDK clusters, restarts and class
   * dumps. Computed on the miss path only; a hit reads the entry's stored hash.
   */
  public static String shapeHash(VarkaShapeKey key) {
    StringBuilder canonical = new StringBuilder();
    for (VarkaVectorIR output : key.outputs()) {
      canonical.append(VarkaVectorIR.canonical(output)).append('\n');
    }
    canonical.append(key.numInputs()).append('|').append(key.numLiterals());
    return JavaUtils.sha256Hex(canonical.toString()).substring(0, 16);
  }

  private VarkaShapeEntry emit(LoaderShapeKey loaderKey) {
    VarkaShapeKey key = loaderKey.shape();
    String hash = shapeHash(key);
    String className = classNameFor(hash);
    String sourceFile = sourceFileFor(hash);
    // The getOrEmit gate races a concurrently flipped hook, and the emit walk reads the
    // (volatile) hooks at its own sites - a hook set and cleared inside the window would
    // restore the values, so sampling them again is not enough. The write generation cannot
    // be restored: if it moved at all during the emission, these bytes are not provably
    // plain and must not be cached.
    long hookGeneration = VarkaLoopEmitter.currentTestHookGeneration();
    // Task 22: the emission event times the Class-File walk plus the define - the whole miss
    // cost minus the lookup - identified by shape only (the class is shared).
    VarkaEmissionEvent emissionEvent = new VarkaEmissionEvent();
    emissionEvent.begin();
    byte[] bytes = VarkaLoopEmitter.emit(className, key.outputs(), key.numInputs(),
        key.numLiterals(), sourceFile, "shape " + hash);
    if (VarkaLoopEmitter.currentTestHookGeneration() != hookGeneration) {
      throw new IllegalStateException(
          "a VarkaLoopEmitter test hook was written while emitting; refusing to cache the bytes");
    }
    VarkaGeneratedClassLoader loader = new VarkaGeneratedClassLoader(loaderKey.parent());
    Class<?> klass = loader.defineGeneratedClass(className, bytes);
    emissionEvent.end();
    if (emissionEvent.shouldCommit()) {
      emissionEvent.shapeHash = hash;
      emissionEvent.className = className;
      emissionEvent.numOutputs = key.outputs().size();
      emissionEvent.numInputs = key.numInputs();
      emissionEvent.numLiterals = key.numLiterals();
      emissionEvent.byteCount = bytes.length;
      emissionEvent.commit();
    }
    LOG.debug("Emitted and defined {} for shape {}", className, hash);
    // Resolved once per shape rather than once per task: newKernel runs on every FusedRunner.
    Constructor<?> constructor;
    try {
      constructor = klass.getConstructor();
    } catch (NoSuchMethodException e) {
      throw sneakyThrow(e);
    }
    return new VarkaShapeEntry(loader, klass, bytes, hash, constructor);
  }

  /**
   * Records one already-bounded identity (the caller abbreviates once, for this table and the
   * lookup event alike) against the shape hash.
   *
   * <p>One atomic remapping, which is what makes it correct: {@code compute} holds the bin lock
   * across the whole update, so the set cannot be evicted between reading it and writing to it -
   * the race the hand-rolled {@code getIfPresent}/{@code putIfAbsent} retry loop this replaced
   * existed to converge on (task 23, sweeping the task-18 debt register).
   */
  private void recordExecution(String hash, String identity) {
    executions.asMap().compute(hash, (h, existing) -> {
      LinkedHashSet<String> set = existing == null ? new LinkedHashSet<>() : existing;
      // Re-adding moves nothing in a LinkedHashSet; remove first so recency order holds.
      set.remove(identity);
      set.add(identity);
      while (set.size() > MAX_EXECUTIONS_PER_SHAPE) {
        var it = set.iterator();
        it.next();
        it.remove();
      }
      return set;
    });
  }

  /**
   * Throws {@code t} as itself, checked or not. The cache has two places that must rethrow a
   * cause rather than wrap it - Guava's wrappers around a failed emit, and a reflective failure
   * in {@link VarkaShapeEntry#newKernel()} - because the evaluator's {@code isCatchable} test
   * decides whether the task falls back or dies from the exception's own type. Wrapping would
   * turn an {@code OutOfMemoryError} or an interrupt into an ordinary kernel failure.
   */
  @SuppressWarnings("unchecked")
  static <E extends Throwable> RuntimeException sneakyThrow(Throwable t) throws E {
    throw (E) t;
  }
}
