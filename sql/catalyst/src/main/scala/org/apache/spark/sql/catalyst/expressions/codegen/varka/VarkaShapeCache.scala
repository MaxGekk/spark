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

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.HexFormat
import java.util.concurrent.{Callable, ExecutionException}
import java.util.concurrent.atomic.LongAdder

import scala.jdk.CollectionConverters._

import com.google.common.cache.{Cache, CacheBuilder, RemovalListener, RemovalNotification}
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.Utils

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
 * test hooks are byte-affecting emit inputs the key also does not carry; the cache refuses to
 * emit while one is set (see [[VarkaShapeCacheImpl]]), so they can never poison an entry.
 */
private[sql] case class VarkaShapeKey(
    outputs: Seq[VarkaVectorIR],
    numInputs: Int,
    numLiterals: Int)

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
 * The one emit input outside the key - the emitter's static test hooks - is guarded instead:
 * [[getOrEmit]] refuses to emit while a hook is set, so hook-affected bytes cannot be cached
 * under the plain key and served after the hook is reset.
 *
 * '''Naming and telemetry.''' The class is named by its shape
 * (`VarkaFusedProjection_<hash>`, 16 hex chars of SHA-256 over the key's canonical
 * rendering), `SourceFile` carries the same name, and the `VarkaDebugInfo` attribute's plan
 * fragment carries `shape <hash>` - the bytes describe the shape, which is exactly what is
 * shared (task 16's `LineNumberTable` is indexed by IR node, a shape property). The map is
 * keyed on the full structural key, so a hash collision cannot cause a wrong hit; it could
 * only give two distinct shapes one *name* (their loaders keep the runtime classes distinct).
 * The per-execution identity that used to be baked into the bytes - operator, stage, the
 * projection list - lives in a bounded side table keyed by the hash, recorded on every
 * lookup while the cache is enabled; [[executionsFor]] is the diagnostics join. Identities
 * are truncated to a fixed length before recording: the table is diagnostics with a JVM-wide
 * lifetime, and an unbounded projection string would make its entry bound meaningless.
 *
 * '''Concurrency and failure.''' Lookups go through Guava's `get(key, callable)`, so tasks
 * racing on one shape emit once. `NonFateSharingCache` is not used - it exposes no removal
 * listener - and not needed: a failure out of the lookup lands in the evaluator's existing
 * catch and degrades to the ghost fallback, so a poisoned load can never fail a query. Guava
 * wraps what the callable throws (`ExecutionException`, `UncheckedExecutionException`,
 * `ExecutionError`); [[getOrEmit]] unwraps to the cause, because the wrapper would defeat the
 * evaluator's fatal-error discipline - an `OutOfMemoryError` inside emit must reach the task
 * as itself, not as a catchable-looking wrapper, and an interrupt must cancel the task.
 * Eviction while a task still runs the class is safe: `release()` only drops the loader's
 * registry, and the task's strong references keep the class alive until it completes (the
 * owner-side contract the engine's `VarkaClassLoader` documents).
 *
 * With `maxEntries` = 0 the same single path degenerates to the per-task lifecycle: Guava
 * evicts each entry immediately after loading it, the removal listener releases the loader,
 * and the task's strong references carry the class to task end - observably the pre-task-18
 * contract, with no second code path to keep in step. (Racing lookups of one shape may still
 * share the one in-flight load; that is fine - nothing is retained either way.)
 *
 * The parent of every loader is the context class loader at emit time
 * (`Utils.getContextOrSparkClassLoader`, as the per-task loaders used before the cache): the
 * emitted bytes call the engine's `VarkaVectorSupport`, and in the documented deployment the
 * engine jar arrives via `--jars`, visible only through the executor's context loader - the
 * loader of [[VarkaFusedKernel]] (catalyst, on the app classpath) cannot see it. The loader
 * still stays out of the key: on an executor the context loader is the JVM-wide mutable URL
 * loader, and the generated bytes reference only the JDK, `jdk.incubator.vector` and Varka
 * classes - never session-isolated user code.
 */
private[sql] class VarkaShapeCacheImpl(val maxEntries: Int) extends Logging {

  require(maxEntries >= 0, s"maxEntries must not be negative: $maxEntries")

  // How many per-execution identities the side table keeps per shape, oldest evicted first.
  private val maxExecutionsPerShape = 8

  // The longest execution identity the side table records; longer ones are cut mid-string.
  // Operator, stage and the leading projection entries survive, which is what the join needs.
  private val maxExecutionIdentityLength = 256

  private val hits = new LongAdder
  private val misses = new LongAdder

  private val cache: Cache[VarkaShapeKey, VarkaShapeEntry] = CacheBuilder.newBuilder()
    .maximumSize(maxEntries.toLong)
    .removalListener(new RemovalListener[VarkaShapeKey, VarkaShapeEntry] {
      // Guava swallows a throwing listener; release() cannot throw, and must not be more than
      // "stop retaining": running tasks still hold the class until they complete.
      override def onRemoval(n: RemovalNotification[VarkaShapeKey, VarkaShapeEntry]): Unit = {
        n.getValue.loader.release()
      }
    })
    .build[VarkaShapeKey, VarkaShapeEntry]()

  // The side table: shape hash -> the most recent execution identities that used the shape.
  // Bounded three times over - entries here, identities per entry, characters per identity -
  // because it is diagnostics, not bookkeeping: it answers "which operators ran
  // VarkaFusedProjection_<hash>?" for a name seen in a profile, a class dump or a JFR event,
  // without that identity riding the shared bytes.
  private val executions: Cache[String, java.util.LinkedHashSet[String]] =
    CacheBuilder.newBuilder()
      .maximumSize(math.max(maxEntries, 1).toLong * 4)
      .build[String, java.util.LinkedHashSet[String]]()

  /**
   * Returns the loaded class for the key's shape, emitting and defining it if no live entry
   * holds it, and records `execution` (the caller's per-execution identity, truncated) in the
   * side table. With `maxEntries` = 0 every lookup emits: the entry is evicted (and its
   * loader released) as it is loaded, restoring the per-task lifecycle through this same path.
   */
  def getOrEmit(key: VarkaShapeKey, execution: String): VarkaShapeLookup = {
    var emitted = false
    val entry = try {
      cache.get(key, new Callable[VarkaShapeEntry] {
        override def call(): VarkaShapeEntry = {
          emitted = true
          emit(key, VarkaShapeCache.shapeHash(key))
        }
      })
    } catch {
      // Unwrap Guava's fate-sharing wrappers: the cause must reach the evaluator's
      // isCatchable test as itself - a fatal error fails the task, an interrupt cancels it.
      case e: ExecutionError => throw e.getCause
      case e: UncheckedExecutionException => throw e.getCause
      case e: ExecutionException => throw e.getCause
    }
    if (maxEntries > 0) recordExecution(entry.shapeHash, execution)
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

  private def emit(key: VarkaShapeKey, hash: String): VarkaShapeEntry = {
    if (VarkaLoopEmitter.anyTestHookSet()) {
      throw new IllegalStateException("a VarkaLoopEmitter test hook is set: the shape cache " +
        "must not cache hook-affected bytes under the plain shape key. Suites that set hooks " +
        "call VarkaLoopEmitter.emit directly and bypass the cache.")
    }
    val className = s"org.apache.spark.sql.varka.execution.VarkaFusedProjection_$hash"
    val sourceFile = s"VarkaFusedProjection_$hash.java"
    val bytes = VarkaLoopEmitter.emit(className, key.outputs.asJava, key.numInputs,
      key.numLiterals, sourceFile, s"shape $hash")
    val loader = new VarkaGeneratedClassLoader(Utils.getContextOrSparkClassLoader)
    val klass = loader.defineGeneratedClass(className, bytes)
    logDebug(s"Emitted and defined $className for shape $hash")
    new VarkaShapeEntry(loader, klass, bytes, hash, className, sourceFile)
  }

  private def recordExecution(hash: String, execution: String): Unit = {
    val identity = if (execution.length > maxExecutionIdentityLength) {
      execution.substring(0, maxExecutionIdentityLength) + "..."
    } else {
      execution
    }
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
  }
}

/**
 * The executor-wide instance (milestone 3 open question 1, settled as per-JVM: the key
 * carries no session state, so cross-session sharing is safe by construction, and Janino's
 * codegen cache is the precedent). Sized once, from `SparkConf` when a `SparkEnv` exists: the
 * conf is static, and the first touch of this object can happen inside a task whose
 * `SQLConf.get` view carries no SQL confs (a non-`SQLExecution` action), which would silently
 * freeze the default in. `SQLConf.get` remains the fallback for env-less unit-test JVMs.
 */
private[sql] object VarkaShapeCache {

  private lazy val instance = new VarkaShapeCacheImpl(configuredMaxEntries())

  private def configuredMaxEntries(): Int = {
    val env = SparkEnv.get
    if (env != null) {
      env.conf.get(StaticSQLConf.VARKA_CACHE_MAX_ENTRIES)
    } else {
      SQLConf.get.getConf(StaticSQLConf.VARKA_CACHE_MAX_ENTRIES)
    }
  }

  /**
   * The shape's stable name fragment: 16 hex characters of SHA-256 over the key's canonical
   * rendering. A pure function of the key - equal keys hash equal on every JVM, so one shape
   * carries one class name across executors, restarts and class dumps. Computed on the miss
   * path only; a hit reads the entry's stored hash.
   */
  def shapeHash(key: VarkaShapeKey): String = {
    val canonical = new StringBuilder
    key.outputs.foreach(o => canonical.append(o.toString).append('\n'))
    canonical.append(key.numInputs).append('|').append(key.numLiterals)
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(canonical.toString.getBytes(StandardCharsets.UTF_8))
    HexFormat.of().formatHex(digest, 0, 8)
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
