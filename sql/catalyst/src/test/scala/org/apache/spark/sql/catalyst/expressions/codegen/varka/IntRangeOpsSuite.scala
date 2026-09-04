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

import java.lang.foreign.{Arena, ValueLayout}

import org.apache.spark.SparkFunSuite

/**
 * [[IntRangeOps.allWithin]] against a scalar oracle over every length shape the lane count can
 * produce - one lane, a whole group, a group and one, and long - with the violating value in a
 * loop lane, in the tail, and under a null lane, where it must be ignored. Runs at both vector
 * widths through the standing gate's narrow pass.
 */
class IntRangeOpsSuite extends SparkFunSuite {

  private val sizes = Seq(1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 1000)
  private val lo = -1000
  private val hi = 1000

  /** The scalar definition the kernel is held to: every live value in the interval. */
  private def oracle(values: Array[Int], isNull: Int => Boolean, lo: Int, hi: Int): Boolean =
    values.indices.forall(i => isNull(i) || (values(i) >= lo && values(i) <= hi))

  private def kernel(values: Array[Int], isNull: Int => Boolean, lo: Int, hi: Int): Boolean = {
    val n = values.length
    val arena = Arena.ofConfined()
    try {
      val data = arena.allocate(math.max(n * 4L, 1L), 8)
      val validity = arena.allocate(math.max((n + 7) / 8L, 1L), 8)
      validity.fill(0.toByte)
      var nulls = 0
      for (i <- 0 until n) {
        data.set(ValueLayout.JAVA_INT, i * 4L, values(i))
        if (isNull(i)) {
          nulls += 1
        } else {
          val off = i / 8L
          val old = validity.get(ValueLayout.JAVA_BYTE, off)
          validity.set(ValueLayout.JAVA_BYTE, off, (old | (1 << (i % 8))).toByte)
        }
      }
      // Per the contract a null-free or all-null column may pass 0L for the bitmap.
      val validityAddress = if (nulls == 0 || nulls == n) 0L else validity.address()
      IntRangeOps.allWithin(data.address(), validityAddress, nulls, n, lo, hi)
    } finally {
      arena.close()
    }
  }

  private def inRange(n: Int): Array[Int] = Array.tabulate(n)(i => (i * 37) % 2001 - 1000)

  test("every live value inside is true on every shape, with and without nulls") {
    val patterns: Seq[Int => Boolean] = Seq(_ => false, _ % 2 == 0, _ == 0, _ => true)
    for (n <- sizes; isNull <- patterns) {
      val v = inRange(n)
      assert(kernel(v, isNull, lo, hi), s"n=$n")
      assert(kernel(v, isNull, lo, hi) === oracle(v, isNull, lo, hi), s"n=$n")
    }
  }

  test("one live value outside, anywhere, is false") {
    for (n <- sizes; at <- Seq(0, n / 2, n - 1);
        bad <- Seq(lo - 1, hi + 1, Int.MinValue, Int.MaxValue)) {
      val v = inRange(n)
      v(at) = bad
      assert(!kernel(v, _ => false, lo, hi), s"n=$n at=$at bad=$bad")
      assert(kernel(v, _ => false, lo, hi) === oracle(v, _ => false, lo, hi))
    }
  }

  test("an outside value under a null lane is ignored, and the same value live beside it is not") {
    // A null row's data is undefined, so a violating value there must not condemn the batch.
    for (n <- sizes; at <- Seq(0, n / 2, n - 1)) {
      val v = inRange(n)
      v(at) = Int.MinValue
      assert(kernel(v, _ == at, lo, hi), s"n=$n at=$at")
      if (n > 1) {
        val other = if (at == 0) 1 else at - 1
        v(other) = hi + 1
        assert(!kernel(v, _ == at, lo, hi), s"n=$n at=$at")
      }
    }
  }

  test("the bounds are inclusive and an empty column is inside") {
    val v = Array(lo, hi, lo, hi, 0, 0, 0, 0, lo, hi, lo, hi, 0, 0, 0, 0, 0)
    assert(kernel(v, _ => false, lo, hi))
    assert(!kernel(v, _ => false, lo + 1, hi))
    assert(!kernel(v, _ => false, lo, hi - 1))
    assert(kernel(Array.empty[Int], _ => false, lo, hi))
  }
}
