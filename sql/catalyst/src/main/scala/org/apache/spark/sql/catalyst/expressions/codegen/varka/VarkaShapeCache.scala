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

package org.apache.spark.sql.catalyst.expressions.codegen.varka

import java.util.concurrent.{Callable, ExecutionException}
import java.util.concurrent.atomic.LongAdder

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.{NonFateSharingCache, SparkStringUtils, Utils}

/**
 * The structural identity of one emitted fused-kernel class: exactly the [[VarkaLoopEmitter]]
 * inputs the bytes are a function of, nothing else. Two projections with equal keys compile to
 * byte-identical loops (modulo the name and debug strings the cache derives itself), so they
 * may - and with the cache, do - share one loaded class.
 *
 * Equality is structural for free: the IR nodes are Java records, and the compiler assigns
 * literal slots and column refs dense first-occurrence indices carrying no values
 * (`PLAN_TASK_10.md` built that property for this key; `VarkaExpressionCompilerSuite` pins it).
 * Literal values travel as runtime `scalarArgs` and never enter the key - two queries with the
 * same shape and different constants must hit. `numLiterals` is a component in its own right
 * because it changes the emitted bytecode independently of the IR (per-slot locals are
 * allocated whether referenced or not, and it gates the broadcast-hoist regime).
 *
 * Deliberately absent, recorded in `PLAN_TASK_18.md`: the child plan ordinals (`ColumnRef`
 * carries the dense kernel input index; the evaluator binds actual columns per task) and the
 * output Spark types (they size the evaluator's output vectors and never reach the emitter).
 * Neither affects the bytes, and leaving them out raises the hit rate. The emitter's static
 * test hooks are byte-affecting emit inputs the key also does not carry; the cache refuses
 * every lookup - hit or miss - while one is set (see [[VarkaShapeCacheImpl]]), so a hooked
 * caller can neither cache poisoned bytes nor be served plain ones.
 */
private[sql] case class VarkaShapeKey(
    outputs: Seq[VarkaVectorIR],
    numInputs: Int,
    numLiterals: Int)

/**
 * The full cache key: the shape, plus the identity of the class loader the entry's generated
 * loader parents. The parent loader is an input to how the class links (it resolves the
 * engine's support classes), so two contexts with different loaders must not share an entry -
 * under executor-side session artifact isolation each session has its own context loader, and
 * a class linked through session A's chain must not serve session B. In the documented
 * `--jars` deployment the executor has one context loader, so this key degenerates to the
 * shape alone. `ClassLoader` has identity equality, which is exactly the sharing rule wanted.
 */
private case class VarkaLoaderShapeKey(parent: ClassLoader, shape: VarkaShapeKey)

/**
 * One cached shape: the loader that defined the class, the class itself, and the bytes it was
 * defined from (kept for diagnostics - `VarkaDebugInfo.read` and the class dump work off them;
 * their footprint is bounded by the cache size). The cache owns the loader and releases it on
 * eviction; a running task's strong references to the class and kernel keep them alive past
 * that, which is the whole release contract.
 */
private[sql] class VarkaShapeEntry(
    val loader: VarkaGeneratedClassLoader,
    val klass: Class[_],
    val classBytes: Array[Byte],
    val shapeHash: String,
    val className: String,
    val sourceFile: String) {

  /** A fresh kernel instance; each task instantiates its own, only the class is shared. */
  def newKernel(): VarkaFusedKernel =
    klass.getConstructor().newInstance().asInstanceOf[VarkaFusedKernel]
}

/** One lookup's outcome: the entry, and whether it was served without emitting. */
private[sql] case class VarkaShapeLookup(entry: VarkaShapeEntry, hit: Boolean)

/**
 * The bounded cross-task cache of loaded fused-kernel classes (task 18,
 * `PLAN_MILESTONE_3.md` 2.1). Task 14's diagnosis set its design: emission costs ~80 us and
 * was never the problem, but a re-defined class is a new class to HotSpot and re-pays the
 * whole tier ladder - a fixed 13-50 ms per task. Only reusing the loaded class amortises
 * that, so this is an LRU over classes each held by its own [[VarkaGeneratedClassLoader]],
 * released (and so unloadable) on eviction rather than on task end. Metaspace is bounded by
 * cache size instead of task lifetime - a weaker guarantee than milestone 1's per-task
 * unloading, proven the same way (`VarkaShapeCacheSuite`, weak references against eviction).
 *
 * '''Correctness before performance.''' A wrong hit returns wrong results and the ghost
 * fallback cannot catch it - it catches failures, not silently different answers. The key is
 * therefore [[VarkaShapeKey]], derived structurally from the same records the emitter walks,
 * never assembled by hand at a call site; the differential suites run warm as well as cold.
 * The one emit input outside the shape - the emitter's static test hooks - is guarded
 * instead: [[getOrEmit]] refuses every lookup while a hook is set (a hit would serve plain
 * bytes to a hooked caller), and [[emit]] snapshots the emitter's hook-write generation
 * around the emission - a hook set and cleared mid-emission restores the values but not the
 * generation, so its bytes are never cached.
 *
 * '''The loader is part of the key.''' Each entry's generated loader parents the context
 * class loader of the task that emitted it (`Utils.getContextOrSparkClassLoader`, as the
 * per-task loaders had it): the emitted bytes call the engine's `VarkaVectorSupport`, which
 * in the documented deployment arrives via `--jars` and is visible only through the context
 * loader - the loader of [[VarkaFusedKernel]] (catalyst, app classpath) cannot see it. That
 * makes the parent an input to linkage, so it rides the key by identity
 * ([[VarkaLoaderShapeKey]]): with one executor-wide context loader nothing changes (one class
 * per shape), and under session artifact isolation each session's loader gets its own entry
 * rather than linking through another session's - possibly closed - chain. A closed session's
 * entries are not released eagerly (nothing here observes session close); they age out of the
 * LRU, so the retained loaders are bounded by the cache capacity like everything else.
 *
 * '''Naming and telemetry.''' The class is named by its shape
 * ([[VarkaShapeCache.classNameFor]]: `VarkaFusedProjection_<hash>`, 16 hex chars of SHA-256
 * over the key's canonical rendering - `VarkaVectorIR.canonical`, hand-pinned so the hash
 * never rides an unspecified `Record.toString` format), `SourceFile` carries the same name,
 * and the `VarkaDebugInfo` attribute's plan fragment carries `shape <hash>` - the bytes
 * describe the shape, which is exactly what is shared. The map is keyed on the full
 * structural key, so a hash collision cannot cause a wrong hit; it could only give two
 * distinct shapes one *name* (their loaders keep the runtime classes distinct). The
 * per-execution identity that used to be baked into the bytes - operator, stage, the
 * projection list - lives in a bounded side table keyed by the hash, recorded (truncated) on
 * every lookup at every capacity, `maxEntries` = 0 included, so the diagnostics join
 * [[executionsFor]] keeps working when sharing is off.
 *
 * '''Concurrency and failure.''' Lookups go through [[NonFateSharingCache]] (SPARK-43300):
 * tasks racing on one shape serialize on a per-key lock, the winner emits once, and a
 * cancelled or failed emit fails only its own task - the co-waiters retry for themselves
 * instead of inheriting the failure into their ghost fallbacks. Guava wraps what the callable
 * throws (`ExecutionException`, `UncheckedExecutionException`, `ExecutionError`);
 * [[getOrEmit]] unwraps to the cause, because the wrapper would defeat the evaluator's
 * fatal-error discipline - an `OutOfMemoryError` inside emit must reach the task as itself,
 * and an interrupt must cancel the task. Eviction while a task still runs the class is safe:
 * `release()` only drops the loader's registry, and the task's strong references keep the
 * class alive until it completes (the owner-side contract the engine's `VarkaClassLoader`
 * documents).
 *
 * With `maxEntries` = 0 the same single path degenerates to the per-task class lifecycle:
 * Guava evicts each entry immediately after loading it, the removal listener releases the
 * loader, and the task's strong references carry the class to task end - the pre-task-18
 * unload contract, with no second code path to keep in step. (Racing lookups of one shape
 * may still share the one in-flight load; that is fine - nothing is retained either way.)
 */
private[sql] class VarkaShapeCacheImpl(val maxEntries: Int) extends Logging {

  require(maxEntries >= 0, s"maxEntries must not be negative: $maxEntries")

  // How many per-execution identities the side table keeps per shape, oldest evicted first.
  private val maxExecutionsPerShape = 8

  private val hits = new LongAdder
  private val misses = new LongAdder

  private val cache: NonFateSharingCache[VarkaLoaderShapeKey, VarkaShapeEntry] =
    NonFateSharingCache(CacheBuilder.newBuilder()
      .maximumSize(maxEntries.toLong)
      .removalListener(new RemovalListener[VarkaLoaderShapeKey, VarkaShapeEntry] {
        // Guava swallows a throwing listener; release() cannot throw, and must not be more
        // than "stop retaining": running tasks still hold the class until they complete.
        override def onRemoval(
            n: RemovalNotification[VarkaLoaderShapeKey, VarkaShapeEntry]): Unit = {
          n.getValue.loader.release()
        }
      })
      .build[VarkaLoaderShapeKey, VarkaShapeEntry]())

  // The side table: shape hash -> the most recent execution identities that used the shape.
  // Bounded three times over - entries here, identities per entry, characters per identity -
  // because it is diagnostics, not bookkeeping: it answers "which operators ran
  // VarkaFusedProjection_<hash>?" for a name seen in a profile, a class dump or a JFR event,
  // without that identity riding the shared bytes.
  // Floored at 64 shapes so the diagnostics join works at every capacity: with maxEntries = 0
  // the classes still carry only their shape name, and a table sized off the (zero) class
  // capacity would evict live shapes' identities mid-query. 64 shapes of 8 truncated
  // identities is at most a few hundred KB.
  private val executions: Cache[String, java.util.LinkedHashSet[String]] =
    CacheBuilder.newBuilder()
      .maximumSize(math.max(maxEntries.toLong * 4, 64))
      .build[String, java.util.LinkedHashSet[String]]()

  /**
   * Returns the loaded class for the shape under the current context class loader, emitting
   * and defining it if no live entry holds it, and records `execution` (the caller's
   * per-execution identity, truncated) in the side table either way. With `maxEntries` = 0
   * every lookup emits: the entry is evicted (and its loader released) as it is loaded,
   * restoring the per-task class lifecycle through this same path.
   */
  def getOrEmit(key: VarkaShapeKey, execution: String): VarkaShapeLookup = {
    if (VarkaLoopEmitter.anyTestHookSet()) {
      // Checked on every lookup, not only under emit: a hit would hand a hooked caller the
      // plain bytes, as silently wrong as caching hooked bytes under the plain key.
      throw new IllegalStateException("a VarkaLoopEmitter test hook is set: the shape cache " +
        "serves and caches only plain bytes. Suites that set hooks call " +
        "VarkaLoopEmitter.emit directly and bypass the cache.")
    }
    val loaderKey = VarkaLoaderShapeKey(Utils.getContextOrSparkClassLoader, key)
    var emitted = false
    val entry = try {
      cache.get(loaderKey, new Callable[VarkaShapeEntry] {
        override def call(): VarkaShapeEntry = {
          emitted = true
          emit(loaderKey)
        }
      })
    } catch {
      // Unwrap Guava's wrappers: the cause must reach the evaluator's isCatchable test as
      // itself - a fatal error fails the task, an interrupt cancels it.
      case e: ExecutionError => throw e.getCause
      case e: UncheckedExecutionException => throw e.getCause
      case e: ExecutionException => throw e.getCause
    }
    recordExecution(entry.shapeHash, execution)
    if (emitted) misses.increment() else hits.increment()
    VarkaShapeLookup(entry, hit = !emitted)
  }

  /** The recorded execution identities for a shape hash, most recent last; empty if unknown. */
  def executionsFor(shapeHash: String): Seq[String] = {
    Option(executions.getIfPresent(shapeHash)) match {
      case Some(set) => set.synchronized(set.asScala.toSeq)
      case None => Seq.empty
    }
  }

  def hitCount: Long = hits.sum()
  def missCount: Long = misses.sum()
  def size: Long = cache.size()

  /** Test hook: drops every entry (releasing the loaders) and the side table. */
  def invalidateAll(): Unit = {
    cache.invalidateAll()
    executions.invalidateAll()
  }

  private def emit(loaderKey: VarkaLoaderShapeKey): VarkaShapeEntry = {
    val key = loaderKey.shape
    val hash = VarkaShapeCache.shapeHash(key)
    val className = VarkaShapeCache.classNameFor(hash)
    val sourceFile = VarkaShapeCache.sourceFileFor(hash)
    // The getOrEmit gate races a concurrently flipped hook, and the emit walk reads the
    // (volatile) hooks at its own sites - a hook set and cleared inside the window would
    // restore the values, so sampling them again is not enough. The write generation cannot
    // be restored: if it moved at all during the emission, these bytes are not provably
    // plain and must not be cached.
    val hookGeneration = VarkaLoopEmitter.currentTestHookGeneration()
    val bytes = VarkaLoopEmitter.emit(className, key.outputs.asJava, key.numInputs,
      key.numLiterals, sourceFile, s"shape $hash")
    if (VarkaLoopEmitter.currentTestHookGeneration() != hookGeneration) {
      throw new IllegalStateException(
        "a VarkaLoopEmitter test hook was written while emitting; refusing to cache the bytes")
    }
    val loader = new VarkaGeneratedClassLoader(loaderKey.parent)
    val klass = loader.defineGeneratedClass(className, bytes)
    logDebug(s"Emitted and defined $className for shape $hash")
    new VarkaShapeEntry(loader, klass, bytes, hash, className, sourceFile)
  }

  private def recordExecution(hash: String, execution: String): Unit = {
    // The shared helper keeps the marker inside the bound, so the stored identity never
    // exceeds maxExecutionIdentityLength.
    val identity =
      SparkStringUtils.abbreviate(execution, VarkaShapeCache.maxExecutionIdentityLength)
    var recorded = false
    while (!recorded) {
      val set = executions.get(hash, new Callable[java.util.LinkedHashSet[String]] {
        override def call(): java.util.LinkedHashSet[String] =
          new java.util.LinkedHashSet[String]()
      })
      set.synchronized {
        // Re-adding moves nothing in a LinkedHashSet; remove first so recency order holds.
        set.remove(identity)
        set.add(identity)
        while (set.size() > maxExecutionsPerShape) {
          val it = set.iterator()
          it.next()
          it.remove()
        }
      }
      // Eviction between get() and the write orphans the set and would silently drop the
      // identity. Reinstate only into an empty slot - a plain put could overwrite a set a
      // concurrent thread created (and recorded into) after the eviction; if the slot is
      // taken, loop and record into the live set instead. Converges because a re-eviction
      // of a just-touched key needs another full capacity churn per iteration.
      recorded = (executions.getIfPresent(hash) eq set) ||
        executions.asMap().putIfAbsent(hash, set) == null
    }
  }
}

/**
 * The executor-wide instance (milestone 3 open question 1, settled as per-JVM: the key
 * carries no session state the linkage does not, and Janino's codegen cache is the
 * precedent). Sized once, from whichever conf source actually carries the key: the SQL view
 * first (builder-set static confs land in the session, not in `SparkEnv`'s `SparkConf`),
 * then the JVM-wide `SparkConf` (a task touching this first through a non-`SQLExecution`
 * action sees an empty SQL view, but `--conf`-set statics are in the env), then the default.
 */
private[sql] object VarkaShapeCache {

  /**
   * The longest execution identity the side table stores; longer ones are abbreviated to
   * exactly this length, marker included. Operator, stage and the leading projection entries
   * survive, which is what the diagnostics join needs - and callers building an identity
   * string need not render more than this.
   */
  private[sql] val maxExecutionIdentityLength = 256

  private lazy val instance = new VarkaShapeCacheImpl(configuredMaxEntries())

  private def configuredMaxEntries(): Int = {
    val entry = StaticSQLConf.VARKA_CACHE_MAX_ENTRIES
    val sqlConf = SQLConf.get
    if (sqlConf.contains(entry.key)) {
      // The SQL view has the key: a session on the driver (builder-set static confs land
      // here, not in SparkEnv's SparkConf), or a task with propagated SQL confs.
      sqlConf.getConf(entry)
    } else {
      // A task without conf propagation (a non-SQLExecution action) sees an empty SQL view;
      // --conf-set static confs are still in the JVM-wide SparkConf. Failing both, the
      // entry's default applies.
      val env = SparkEnv.get
      if (env != null && env.conf.contains(entry.key)) {
        env.conf.get(entry)
      } else {
        sqlConf.getConf(entry)
      }
    }
  }

  /** The one rendering of the shape-named class name; every caller derives it here. */
  def classNameFor(shapeHash: String): String =
    s"org.apache.spark.sql.varka.execution.VarkaFusedProjection_$shapeHash"

  /** The one rendering of the shape-named `SourceFile`; every caller derives it here. */
  def sourceFileFor(shapeHash: String): String = s"VarkaFusedProjection_$shapeHash.java"

  /**
   * The shape's stable name fragment: 16 hex characters of SHA-256 over the key's canonical
   * rendering (`VarkaVectorIR.canonical`, hand-pinned - not `Record.toString`, whose format
   * no JDK promises). A pure function of the key: equal keys hash equal on every JVM, so one
   * shape carries one class name across executors, mixed-JDK clusters, restarts and class
   * dumps. Computed on the miss path only; a hit reads the entry's stored hash.
   */
  def shapeHash(key: VarkaShapeKey): String = {
    val canonical = new StringBuilder
    key.outputs.foreach(o => canonical.append(VarkaVectorIR.canonical(o)).append('\n'))
    canonical.append(key.numInputs).append('|').append(key.numLiterals)
    JavaUtils.sha256Hex(canonical.toString).substring(0, 16)
  }

  def getOrEmit(key: VarkaShapeKey, execution: String): VarkaShapeLookup =
    instance.getOrEmit(key, execution)

  def executionsFor(shapeHash: String): Seq[String] = instance.executionsFor(shapeHash)

  def hitCount: Long = instance.hitCount
  def missCount: Long = instance.missCount
  def size: Long = instance.size

  /** Test hook, mirroring `CodeGenerator.invalidateCodegenCache`. */
  private[sql] def invalidateAll(): Unit = instance.invalidateAll()
}
