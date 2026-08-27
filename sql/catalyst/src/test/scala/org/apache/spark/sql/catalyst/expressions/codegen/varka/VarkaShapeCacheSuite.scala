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

import java.lang.management.ManagementFactory
import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite

/**
 * Task 18: the cross-task class cache. The one failure mode the ghost fallback cannot catch is
 * a wrong hit - a cached class served for a shape it was not emitted from - so the sharing
 * tests here assert both directions: equal shapes share one loaded class (constants and all
 * other non-byte-affecting context ignored), and every byte-affecting difference - structure,
 * op kind, input count, literal count - gets its own class. The milestone-3 Metaspace gate
 * moves here too: bounded by cache capacity rather than task lifetime, proven the same way as
 * `VarkaGeneratedClassLoaderSuite`'s per-task proof - weak references, now against eviction.
 */
class VarkaShapeCacheSuite extends SparkFunSuite {

  /** Total budget (ms) for GC-retry loops; generous to stay robust on loaded JVMs. */
  private val gcTimeoutMs = 10000L

  private def columnRef = new VarkaVectorIR.ColumnRef(0)
  private def literal = new VarkaVectorIR.LiteralSlot(0)

  /** An alternating add/sub chain whose op pattern is the bit pattern of `bits`. */
  private def chain(bits: Int, depth: Int): VarkaVectorIR = {
    var node: VarkaVectorIR = columnRef
    (0 until depth).foreach { j =>
      node = if (((bits >> j) & 1) == 1) {
        new VarkaVectorIR.AddDays(node, literal)
      } else {
        new VarkaVectorIR.SubDays(node, literal)
      }
    }
    node
  }

  private def keyOf(root: VarkaVectorIR, numInputs: Int = 1, numLiterals: Int = 1) =
    VarkaShapeKey(Seq(root), numInputs, numLiterals)

  test("equal shapes share one loaded class; the second lookup is a hit") {
    val cache = new VarkaShapeCacheImpl(8)
    // Two independently built but structurally equal keys: what two queries with the same
    // shape and different constants produce, since the constants never enter the IR.
    val first = cache.getOrEmit(keyOf(chain(bits = 5, depth = 3)), "execA")
    val second = cache.getOrEmit(keyOf(chain(bits = 5, depth = 3)), "execB")
    assert(!first.hit && second.hit)
    assert(first.entry eq second.entry, "equal keys must share one entry")
    assert(cache.hitCount === 1 && cache.missCount === 1)
    assert(cache.size === 1)
  }

  test("every byte-affecting difference gets its own class") {
    val cache = new VarkaShapeCacheImpl(16)
    val keys = Seq(
      keyOf(new VarkaVectorIR.AddDays(columnRef, literal)),
      keyOf(new VarkaVectorIR.SubDays(columnRef, literal)),
      // Same structure, one more (unreferenced) literal slot: numLiterals changes the emitted
      // bytecode (per-slot locals, the broadcast-hoist gate), so it must miss.
      keyOf(new VarkaVectorIR.AddDays(columnRef, literal), numLiterals = 2),
      keyOf(new VarkaVectorIR.AddDays(columnRef, literal), numInputs = 2),
      // Swapped op order at depth 2: same op multiset, different structure.
      keyOf(chain(bits = 1, depth = 2)),
      keyOf(chain(bits = 2, depth = 2)))
    val entries = keys.map(cache.getOrEmit(_, "exec").entry)
    assert(cache.missCount === keys.size && cache.hitCount === 0)
    assert(entries.map(_.klass).distinct.size === keys.size)
    assert(entries.map(_.shapeHash).distinct.size === keys.size)
  }

  test("the shape hash is a stable pure function and drives the naming") {
    val a = keyOf(chain(bits = 9, depth = 4))
    val b = keyOf(chain(bits = 9, depth = 4))
    assert(VarkaShapeCache.shapeHash(a) === VarkaShapeCache.shapeHash(b))
    assert(VarkaShapeCache.shapeHash(a) !== VarkaShapeCache.shapeHash(keyOf(chain(6, 4))))
    val entry = new VarkaShapeCacheImpl(2).getOrEmit(a, "exec").entry
    val hash = VarkaShapeCache.shapeHash(a)
    assert(hash.matches("[0-9a-f]{16}"), hash)
    assert(entry.shapeHash === hash)
    assert(entry.className === s"org.apache.spark.sql.varka.execution.VarkaFusedProjection_$hash")
    assert(entry.sourceFile === s"VarkaFusedProjection_$hash.java")
  }

  test("concurrent lookups of one shape emit once and share the class") {
    val cache = new VarkaShapeCacheImpl(8)
    val threads = 8
    val key = keyOf(chain(bits = 3, depth = 5))
    val start = new CountDownLatch(1)
    val pool = Executors.newFixedThreadPool(threads)
    try {
      val futures = (0 until threads).map { _ =>
        pool.submit(new java.util.concurrent.Callable[Class[_]] {
          override def call(): Class[_] = {
            start.await()
            cache.getOrEmit(keyOf(chain(bits = 3, depth = 5)), "exec").entry.klass
          }
        })
      }
      start.countDown()
      val classes = futures.map(_.get(30, TimeUnit.SECONDS))
      assert(classes.forall(_ eq classes.head), "racing lookups must share one class")
      assert(cache.missCount === 1, s"exactly one thread must emit, got ${cache.missCount}")
      assert(cache.hitCount === threads - 1)
    } finally {
      pool.shutdownNow()
    }
    assert(cache.getOrEmit(key, "exec").hit)
  }

  test("eviction releases the evicted loader, and the class unloads once unreferenced") {
    val cache = new VarkaShapeCacheImpl(2)
    val queue = new ReferenceQueue[ClassLoader]()
    val evictedRef = emitForEviction(cache, queue)
    // Two more shapes evict the first (LRU capacity 2).
    cache.getOrEmit(keyOf(chain(bits = 1, depth = 1)), "exec")
    cache.getOrEmit(keyOf(chain(bits = 0, depth = 2)), "exec")
    assert(cache.size <= 2)
    // The removal listener released the loader; with the entry unreferenced the loader (and
    // so its class) must now be collectable - the milestone-3 form of the Metaspace proof.
    assert(awaitCollected(evictedRef, queue),
      "the evicted loader must be collected so its class unloads from Metaspace")
  }

  /**
   * Emits the to-be-evicted shape in its own method frame and hands back only a weak
   * reference to its loader: a block-scoped val in the test body can stay live in the test
   * method's local slots and pin the entry, so nothing would ever unload.
   */
  private def emitForEviction(
      cache: VarkaShapeCacheImpl,
      queue: ReferenceQueue[ClassLoader]): WeakReference[ClassLoader] = {
    val entry = cache.getOrEmit(keyOf(chain(bits = 0, depth = 1)), "exec").entry
    assert(!entry.loader.isReleased)
    new WeakReference[ClassLoader](entry.loader, queue)
  }

  test("a 10k-distinct-shape stress stays at capacity, and every evicted loader collects") {
    val capacity = 64
    val shapes = 10000
    val cache = new VarkaShapeCacheImpl(capacity)
    val queue = new ReferenceQueue[ClassLoader]()
    val before = metaspaceUsed()
    val refs = (0 until shapes).map { i =>
      // Depth 14 gives 16384 distinct op patterns, so every index is its own shape.
      val entry = cache.getOrEmit(keyOf(chain(bits = i, depth = 14)), "stress").entry
      new WeakReference[ClassLoader](entry.loader, queue)
    }
    assert(cache.size <= capacity)
    assert(cache.missCount === shapes)
    val collected = awaitCollectedCount(refs, queue, refs.size - capacity)
    val after = metaspaceUsed()
    logInfo(s"shape stress: shapes=$shapes capacity=$capacity collected=$collected " +
      s"metaspace before=$before after=$after")
    assert(collected >= refs.size - capacity, "every evicted loader must be collected")
    // Lenient, like the integration check: at most `capacity` live kernel classes (a few KB
    // each) must keep the Metaspace footprint far below this bound.
    assert(after - before < 64L * 1024 * 1024,
      s"Metaspace grew by ${after - before} bytes across $shapes shapes")
  }

  test("capacity 0 disables sharing: every lookup emits, evicted and released on load") {
    val cache = new VarkaShapeCacheImpl(0)
    val key = keyOf(chain(bits = 5, depth = 3))
    val first = cache.getOrEmit(key, "exec")
    val second = cache.getOrEmit(key, "exec")
    assert(!first.hit && !second.hit)
    assert(!(first.entry eq second.entry))
    assert(cache.size === 0 && cache.missCount === 2)
    // The single cache path degenerates to the pre-task-18 lifecycle: `maximumSize(0)` evicts
    // each entry as it loads and the removal listener releases its loader - a caller's strong
    // references (here, the lookup results) keep the class usable to task end regardless.
    assert(first.entry.loader.isReleased && second.entry.loader.isReleased)
    assert(first.entry.newKernel() != null)
    // Diagnostics still record while sharing is off: with the bytes carrying only the shape,
    // the side table is the one place the execution identity lives.
    assert(cache.executionsFor(first.entry.shapeHash) === Seq("exec"))
  }

  test("an emitter test hook refuses every cache lookup, hit and miss alike") {
    val cache = new VarkaShapeCacheImpl(4)
    val key = keyOf(chain(bits = 3, depth = 2))
    assert(!cache.getOrEmit(key, "exec").hit) // cached plain, before any hook
    VarkaLoopEmitter.disableCseForTesting = true
    try {
      // Both directions are wrong and both are refused: a hit would serve plain bytes to a
      // hooked caller, a miss would cache hooked bytes under the plain key.
      intercept[IllegalStateException](cache.getOrEmit(key, "exec"))
      intercept[IllegalStateException](cache.getOrEmit(keyOf(chain(bits = 9, depth = 2)), "e"))
    } finally {
      VarkaLoopEmitter.disableCseForTesting = false
    }
    assert(cache.size === 1, "a refused lookup must cache nothing")
    // With the hook reset, the pre-hook entry serves again and new shapes emit normally.
    assert(cache.getOrEmit(key, "exec").hit)
    assert(!cache.getOrEmit(keyOf(chain(bits = 9, depth = 2)), "exec").hit)
  }

  test("the context class loader is part of the key: another loader gets its own entry") {
    val cache = new VarkaShapeCacheImpl(8)
    val key = keyOf(chain(bits = 5, depth = 3))
    val original = Thread.currentThread().getContextClassLoader
    val first = cache.getOrEmit(key, "sessionA")
    val isolated = new java.net.URLClassLoader(Array.empty, original)
    val second = try {
      Thread.currentThread().setContextClassLoader(isolated)
      cache.getOrEmit(key, "sessionB")
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
    // Same shape, different linkage context: no sharing across loaders (a class linked
    // through one session's chain must not serve another), same shape identity outward.
    assert(!second.hit)
    assert(!(first.entry eq second.entry))
    assert(second.entry.loader.getParent eq isolated)
    assert(first.entry.shapeHash === second.entry.shapeHash)
    assert(cache.size === 2)
    // The original loader's entry still hits for the original context.
    assert(cache.getOrEmit(key, "sessionC").hit)
  }

  test("the canonical rendering pins the hash: the committed value never drifts") {
    // SHA-256 over VarkaVectorIR.canonical, not Record.toString - this exact value must
    // hold on every JVM and JDK release, or cluster-wide diagnostics joins break. If this
    // fails, the canonical rendering changed, which renames every dumped class: make sure
    // that is intended, then update the value here and say so in the task plan.
    val key = keyOf(chain(bits = 9, depth = 4))
    assert(VarkaShapeCache.shapeHash(key) === "586434f9b9739c40")
  }

  test("side-table identities are recorded truncated, so one entry cannot grow unbounded") {
    val cache = new VarkaShapeCacheImpl(4)
    val key = keyOf(chain(bits = 6, depth = 3))
    val longIdentity = "Varka_Project_Stage1: " + ("x" * 1000)
    cache.getOrEmit(key, longIdentity)
    val recorded = cache.executionsFor(cache.getOrEmit(key, "short").entry.shapeHash)
    assert(recorded.exists(_.endsWith("...")), recorded.mkString("; "))
    assert(recorded.forall(_.length < 300), "identities must be bounded")
    assert(recorded.contains("short"))
  }

  test("the side table joins a shape hash back to its recorded executions, bounded") {
    val cache = new VarkaShapeCacheImpl(8)
    val key = keyOf(chain(bits = 7, depth = 3))
    val hash = VarkaShapeCache.shapeHash(key)
    cache.getOrEmit(key, "Varka_Project_Stage3: date_add(d#1, 3) AS a#2")
    cache.getOrEmit(key, "Varka_ProjectToRow_Stage4: date_add(d#5, 9) AS b#6")
    val recorded = cache.executionsFor(hash)
    assert(recorded === Seq(
      "Varka_Project_Stage3: date_add(d#1, 3) AS a#2",
      "Varka_ProjectToRow_Stage4: date_add(d#5, 9) AS b#6"))
    // Bounded per shape: the oldest identities fall off, most recent kept in order.
    (0 until 12).foreach(i => cache.getOrEmit(key, s"exec$i"))
    val bounded = cache.executionsFor(hash)
    assert(bounded.size === 8)
    assert(bounded.last === "exec11" && bounded.head === "exec4")
    assert(cache.executionsFor("no such hash") === Seq.empty)
  }

  // --- helpers -------------------------------------------------------------

  private def metaspaceUsed(): Long = {
    ManagementFactory.getMemoryPoolMXBeans.asScala.collect {
      case p if p.getName == "Metaspace" || p.getName == "Compressed Class Space" =>
        p.getUsage.getUsed
    }.sum
  }

  /** Retries `System.gc()` until `ref` is enqueued or the timeout elapses. */
  private def awaitCollected(ref: WeakReference[_], queue: ReferenceQueue[_]): Boolean = {
    val deadline = System.nanoTime() + gcTimeoutMs * 1000000L
    while (System.nanoTime() < deadline) {
      if (queue.poll() eq ref) {
        return true
      }
      System.gc()
      System.runFinalization()
      Thread.sleep(25)
    }
    queue.poll() eq ref
  }

  /**
   * Retries `System.gc()` until at least `expected` references are enqueued or the timeout
   * elapses, returning how many were collected.
   */
  private def awaitCollectedCount(
      refs: Seq[WeakReference[ClassLoader]],
      queue: ReferenceQueue[ClassLoader],
      expected: Int): Int = {
    val deadline = System.nanoTime() + gcTimeoutMs * 1000000L
    var collected = 0
    while (collected < expected && System.nanoTime() < deadline) {
      while (queue.poll() != null) {
        collected += 1
      }
      System.gc()
      System.runFinalization()
      Thread.sleep(25)
    }
    while (queue.poll() != null) {
      collected += 1
    }
    collected
  }
}
