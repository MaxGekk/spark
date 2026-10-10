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

import java.lang.foreign.{Arena, MemorySegment, ValueLayout}

import org.scalatest.Assertions._

import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * The fuzzers' row-by-row check of one emitted kernel against [[VarkaReferenceEvaluator]], at the
 * int lane and at the long lane, shared by the IR fuzzer's drawn, long and wide shapes and the
 * composition fuzzer's compiled kernels (VARKA-238, VARKA-289). Scala because the oracle it drives
 * is a Scala object.
 */
object VarkaKernelCheck {

  /**
   * One batch's columns: `length` rows, each column's null pattern and drawn values, and whether
   * the masked body is forced by reporting a null over a full bitmap.
   */
  case class Batch(length: Int, patterns: Seq[Int => Boolean], data: Array[Array[Int]],
      forceMasked: Boolean)

  /** [[Batch]] at the long lane: the same, with 64-bit column values. */
  case class LongBatch(length: Int, patterns: Seq[Int => Boolean], data: Array[Array[Long]],
      forceMasked: Boolean)

  private def alloc(arena: Arena, bytes: Long): MemorySegment =
    arena.allocate(math.max(bytes, 1L), 8)

  /**
   * A validity bitmap's size as Arrow Java's `allocateNew` gives it, in whole 64-bit words: the
   * kernels write and read a word at a time, so a buffer of the nominal `(length + 7) / 8` bytes
   * would be one a kernel is not entitled to, and the memory sanitizer (VARKA-263) says so.
   */
  private def wordBytes(length: Int): Long = ((length + 63) / 64) * 8L

  /**
   * Runs `run` with the scratch address the kernel takes, inside the memory sanitizer's window over
   * the buffers this harness allocated, so that every mapping the kernel makes is checked against
   * them when the sanitizer is on. `stride` is a value's bytes in the lane. Nothing is registered,
   * and nothing changes, when it is off.
   */
  private def sanitized(kernel: VarkaFusedKernel, length: Int, stride: Long,
      srcData: Array[Long], srcValidity: Array[Long], outData: Array[Long],
      outValidity: Array[Long])(run: Long => Int): Int = {
    val scratch = VarkaEmitterTestSupport.scratch(kernel, length)
    VarkaMemorySanitizer.begin()
    try {
      if (VarkaMemorySanitizer.ENABLED) {
        for (c <- srcData.indices) {
          VarkaMemorySanitizer.register("input data", c, srcData(c), length * stride)
          if (srcValidity(c) != 0L) {
            VarkaMemorySanitizer.register("input validity", c, srcValidity(c), wordBytes(length))
          }
        }
        for (o <- outData.indices) {
          if (outData(o) != 0L) {
            VarkaMemorySanitizer.register("output data", o, outData(o), length * stride)
          }
          VarkaMemorySanitizer.register("output validity", o, outValidity(o), wordBytes(length))
        }
        if (scratch != 0L) {
          VarkaMemorySanitizer.register("kernel scratch", 0, scratch,
            kernel.scratchBytesPerRow().toLong * length)
        }
      }
      run(scratch)
    } finally {
      VarkaMemorySanitizer.end()
    }
  }

  /** Answers the reference evaluator has been held to Spark on, so a suite can see it was. */
  val sparkComparisons = new java.util.concurrent.atomic.AtomicLong()

  /**
   * The reference evaluator's answer for output `o` on row `i` against Spark's (VARKA-276): equal
   * values or both null, a predicate's answers being 1, 0 or null. A difference is the reference's
   * to fix, or a named delta (row 244), before the kernel is held to it. Spark raising is a row the
   * kernel must decline the batch over, and this is called only for a batch the kernel answered.
   */
  private def agreeWithSpark(context: String, spark: VarkaSparkOracle, o: Int, i: Int,
      row: Seq[Option[Long]], reference: Option[Long]): Unit = {
    sparkComparisons.incrementAndGet()
    val answer = spark.answer(o, row)
    answer match {
      case VarkaSparkOracle.Raised(e) => fail(s"$context: Spark raises on output $o " +
        s"(${spark.sql(o)}) at row $i, inputs ${row.mkString("(", ", ", ")")}, and the kernel " +
        s"answered the batch instead of declining it: ${e.getMessage}")
      case _ =>
    }
    val agree = (answer, reference) match {
      case (VarkaSparkOracle.Value(v), Some(r)) => v == r
      case (VarkaSparkOracle.Null, None) => true
      case _ => false
    }
    assert(agree, s"$context: the reference evaluator and Spark disagree on output $o " +
      s"(${spark.sql(o)}) at row $i, inputs ${row.mkString("(", ", ", ")")}: reference " +
      s"$reference, Spark $answer")
  }

  /**
   * Runs one emitted int-lane kernel over the drawn columns and checks every row and validity bit
   * against [[VarkaReferenceEvaluator]]. Null lanes are poisoned; `forceMasked` reports a null over
   * a full bitmap so the masked body runs. A batch that is compared is then run again through the
   * body it did not take ([[otherBody]]), so both of the kernel's bodies meet the oracle. Returns
   * whether the rows were compared: false only where `declineAllowed` and the kernel declined the
   * batch, which a kernel over columns drawn without its domains may do.
   */
  def runAndCompare(context: String, className: String, bytes: Array[Byte],
      roots: Seq[VarkaVectorIR], numInputs: Int, lits: Array[Int], batch: Batch,
      declineAllowed: Boolean = false, spark: Option[VarkaSparkOracle] = None): Boolean = {
    val compared = runBatch(context, className, bytes, roots, numInputs, lits, batch,
      declineAllowed, spark)
    if (compared) otherBody(numInputs, batch.length, batch.patterns, batch.forceMasked).foreach {
      case (patterns, forced) =>
        runBatch(s"$context (the other body)", className, bytes, roots, numInputs, lits,
          batch.copy(patterns = patterns, forceMasked = forced), declineAllowed, spark)
    }
    compared
  }

  /**
   * The same values through the body the drawn batch did not take (VARKA-284): a kernel has a
   * dense body for a batch with no nulls and a masked one for every other, and a shape run on one
   * kind of batch has only that body compared. A batch that took the masked body goes again with
   * no nulls; one that took the dense body goes again with the masked one forced - a null count
   * over a full bitmap, or for one row every column null, a forced batch of one row being all-null
   * by contract and a null in a column the kernel does not read selecting nothing. None for a
   * kernel that reads no column, which has no masked body to choose.
   */
  private def otherBody(numInputs: Int, length: Int, patterns: Seq[Int => Boolean],
      forceMasked: Boolean): Option[(Seq[Int => Boolean], Boolean)] = {
    if (numInputs == 0 || length == 0) return None
    val masked = forceMasked || patterns.exists(p => (0 until length).exists(p))
    if (masked) Some((Seq.fill(numInputs)((_: Int) => false), false))
    else if (length > 1) Some((patterns, true))
    else Some((Seq.fill(numInputs)((_: Int) => true), false))
  }

  /** One batch through [[runAndCompare]]'s check, whichever body its null counts select. */
  private def runBatch(context: String, className: String, bytes: Array[Byte],
      roots: Seq[VarkaVectorIR], numInputs: Int, lits: Array[Int], batch: Batch,
      declineAllowed: Boolean, spark: Option[VarkaSparkOracle]): Boolean = {
    val Batch(length, patterns, data, forceMasked) = batch
    VarkaSpeciesGuard.check(className, bytes)
    val loader = new VarkaGeneratedClassLoader(getClass.getClassLoader)
    loader.defineGeneratedClass(className, bytes)
    val kernel = loader.loadClass(className).getConstructor().newInstance()
      .asInstanceOf[VarkaFusedKernel]
    val arena = Arena.ofConfined()
    try {
      val srcData = new Array[Long](numInputs)
      val srcValidity = new Array[Long](numInputs)
      val nullCounts = new Array[Int](numInputs)
      for (c <- 0 until numInputs) {
        val d = alloc(arena, length * 4L)
        val v = alloc(arena, wordBytes(length))
        v.fill(0.toByte)
        var nulls = 0
        for (i <- 0 until length) {
          if (patterns(c)(i)) {
            // Poisoned, not left at the drawn value (VARKA-70's harness rule, the same one
            // VarkaEmitterTestBase.poison states). `data` is drawn inside `columnBound` and
            // `MONTH_ARITH_MAX_MONTHS`, so a null lane holding its drawn value is in range by
            // construction and can never reach a guard's condemning comparison - which is the
            // one thing the fuzzer is here to reach. Alternating on the null ordinal puts each
            // extreme on both sides of every bound whatever the null pattern is.
            d.set(ValueLayout.JAVA_INT, i * 4L,
              if ((nulls & 1) == 0) Int.MinValue else Int.MaxValue)
            nulls += 1
          } else {
            d.set(ValueLayout.JAVA_INT, i * 4L, data(c)(i))
            val off = i / 8L
            v.set(ValueLayout.JAVA_BYTE, off,
              (v.get(ValueLayout.JAVA_BYTE, off) | (1 << (i % 8))).toByte)
          }
        }
        srcData(c) = d.address()
        nullCounts(c) = if (forceMasked && nulls == 0) 1 else nulls
        srcValidity(c) =
          if (nullCounts(c) == 0 || nulls == length) 0L else v.address()
      }
      val outs = roots.map { r =>
        val d = alloc(arena, length * 4L)
        for (i <- 0 until length) d.set(ValueLayout.JAVA_INT, i * 4L, 0xDEADBEEF)
        val v = alloc(arena, wordBytes(length))
        v.fill(0xFF.toByte)
        (if (r.isInstanceOf[Cond]) 0L else d.address(), d, v)
      }
      val outData = outs.map(_._1).toArray
      val outValidity = outs.map(_._3.address()).toArray
      val status = sanitized(kernel, length, 4L, srcData, srcValidity, outData, outValidity) {
        scratch => kernel.run(srcData, srcValidity, nullCounts, outData, outValidity, lits, length,
          scratch)
      }
      if (status != 0 && declineAllowed) {
        return false
      }
      assert(status === 0, s"$context: the kernel declined the batch (status $status)")
      for (i <- 0 until length) {
        val row = (0 until numInputs).map(c => if (patterns(c)(i)) None else Some(data(c)(i)))
        for ((root, o) <- roots.zipWithIndex) {
          val bit = (outs(o)._3.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0
          spark.foreach { s =>
            val reference = root match {
              case c: Cond =>
                VarkaReferenceEvaluator.evalCond(c, row, lits).map(b => if (b) 1L else 0L)
              case _ => VarkaReferenceEvaluator.evalValue(root, row, lits).map(_.toLong)
            }
            agreeWithSpark(context, s, o, i, row.map(_.map(_.toLong)), reference)
          }
          root match {
            case c: Cond =>
              val want = VarkaReferenceEvaluator.evalCond(c, row, lits).contains(true)
              assert(bit === want, s"$context: selection row $i differs (want $want)")
            case _ =>
              val want = VarkaReferenceEvaluator.evalValue(root, row, lits)
              assert(bit === want.isDefined,
                s"$context: validity of output $o row $i differs (want $want)")
              want.foreach { v =>
                assert(outs(o)._2.get(ValueLayout.JAVA_INT, i * 4L) === v,
                  s"$context: output $o row $i differs (want $v)")
              }
          }
        }
      }
      true
    } finally {
      arena.close()
      loader.release()
    }
  }

  /**
   * [[runAndCompare]] at the long lane: 64-bit buffers, the eight-argument `run` with `lits` as
   * the long literal table, and `evalLong` as the oracle. Null lanes are poisoned with the lane's
   * own extremes. A narrowing root stores four bytes a row, so its output is read at that width
   * and sign-extended: a value that did not fit 32 bits then differs from the evaluator's 64-bit
   * answer instead of being truncated on both sides.
   */
  def runAndCompareLong(context: String, className: String, bytes: Array[Byte],
      roots: Seq[VarkaVectorIR], numInputs: Int, lits: Array[Long], batch: LongBatch,
      declineAllowed: Boolean = false, spark: Option[VarkaSparkOracle] = None): Boolean = {
    val compared = runLongBatch(context, className, bytes, roots, numInputs, lits, batch,
      declineAllowed, spark)
    if (compared) otherBody(numInputs, batch.length, batch.patterns, batch.forceMasked).foreach {
      case (patterns, forced) =>
        runLongBatch(s"$context (the other body)", className, bytes, roots, numInputs, lits,
          batch.copy(patterns = patterns, forceMasked = forced), declineAllowed, spark)
    }
    compared
  }

  /** One batch through [[runAndCompareLong]]'s check. */
  private def runLongBatch(context: String, className: String, bytes: Array[Byte],
      roots: Seq[VarkaVectorIR], numInputs: Int, lits: Array[Long], batch: LongBatch,
      declineAllowed: Boolean, spark: Option[VarkaSparkOracle]): Boolean = {
    val LongBatch(length, patterns, data, forceMasked) = batch
    VarkaSpeciesGuard.check(className, bytes)
    val loader = new VarkaGeneratedClassLoader(getClass.getClassLoader)
    loader.defineGeneratedClass(className, bytes)
    val kernel = loader.loadClass(className).getConstructor().newInstance()
      .asInstanceOf[VarkaFusedKernel]
    val arena = Arena.ofConfined()
    try {
      val srcData = new Array[Long](numInputs)
      val srcValidity = new Array[Long](numInputs)
      val nullCounts = new Array[Int](numInputs)
      for (c <- 0 until numInputs) {
        val d = alloc(arena, length * 8L)
        val v = alloc(arena, wordBytes(length))
        v.fill(0.toByte)
        var nulls = 0
        for (i <- 0 until length) {
          if (patterns(c)(i)) {
            d.set(ValueLayout.JAVA_LONG, i * 8L,
              if ((nulls & 1) == 0) Long.MinValue else Long.MaxValue)
            nulls += 1
          } else {
            d.set(ValueLayout.JAVA_LONG, i * 8L, data(c)(i))
            val off = i / 8L
            v.set(ValueLayout.JAVA_BYTE, off,
              (v.get(ValueLayout.JAVA_BYTE, off) | (1 << (i % 8))).toByte)
          }
        }
        srcData(c) = d.address()
        nullCounts(c) = if (forceMasked && nulls == 0) 1 else nulls
        srcValidity(c) =
          if (nullCounts(c) == 0 || nulls == length) 0L else v.address()
      }
      val outs = roots.map { r =>
        val d = alloc(arena, length * 8L)
        for (i <- 0 until length) d.set(ValueLayout.JAVA_LONG, i * 8L, 0xDEADBEEFCAFEBABEL)
        val v = alloc(arena, wordBytes(length))
        v.fill(0xFF.toByte)
        (if (r.isInstanceOf[Cond]) 0L else d.address(), d, v)
      }
      val outData = outs.map(_._1).toArray
      val outValidity = outs.map(_._3.address()).toArray
      val status = sanitized(kernel, length, 8L, srcData, srcValidity, outData, outValidity) {
        scratch => kernel.run(srcData, srcValidity, nullCounts, outData, outValidity,
          Array.empty[Int], lits, length, scratch)
      }
      if (status != 0 && declineAllowed) {
        return false
      }
      assert(status === 0, s"$context: the kernel declined the batch (status $status)")
      for (i <- 0 until length) {
        val row = (0 until numInputs).map(c => if (patterns(c)(i)) None else Some(data(c)(i)))
        for ((root, o) <- roots.zipWithIndex) {
          val bit = (outs(o)._3.get(ValueLayout.JAVA_BYTE, i / 8L) & (1 << (i % 8))) != 0
          spark.foreach { s =>
            val reference = root match {
              case c: Cond =>
                VarkaReferenceEvaluator.evalCondLong(c, row, lits).map(b => if (b) 1L else 0L)
              case _ => VarkaReferenceEvaluator.evalLong(root, row, lits)
            }
            agreeWithSpark(context, s, o, i, row, reference)
          }
          root match {
            case c: Cond =>
              val want = VarkaReferenceEvaluator.evalCondLong(c, row, lits).contains(true)
              assert(bit === want, s"$context: selection row $i differs (want $want)")
            case _ =>
              val want = VarkaReferenceEvaluator.evalLong(root, row, lits)
              assert(bit === want.isDefined,
                s"$context: validity of output $o row $i differs (want $want)")
              val got: Long = root match {
                case _: NarrowLane => outs(o)._2.get(ValueLayout.JAVA_INT, i * 4L).toLong
                case _ => outs(o)._2.get(ValueLayout.JAVA_LONG, i * 8L)
              }
              want.foreach { v =>
                assert(got === v, s"$context: output $o row $i differs (want $v)")
              }
          }
        }
      }
      true
    } finally {
      arena.close()
      loader.release()
    }
  }
}
