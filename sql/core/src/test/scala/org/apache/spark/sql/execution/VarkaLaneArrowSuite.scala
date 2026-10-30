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

package org.apache.spark.sql.execution

import scala.jdk.CollectionConverters._

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BaseFixedWidthVector

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaTypeId, VarkaTypes}

/**
 * The evaluator's Arrow table held to the type registry (VARKA-494): a type has a vector exactly
 * when a lane reads it, and every form of the type allocates that vector at the registry's
 * storage width. Without it, a type the registry puts on a lane while the table has no vector for
 * it compiles, fuses, and is declined on every batch, which reads as a slow query.
 */
class VarkaLaneArrowSuite extends SparkFunSuite {

  test("a type has an Arrow vector exactly when a lane reads it") {
    for (id <- VarkaTypeId.values()) {
      val info = VarkaTypes.info(id)
      assert((VarkaLaneArrow.of(id) != null) === info.lane().isPresent, id.name)
    }
  }

  test("every form of a type a lane reads allocates its vector at the registry's width") {
    val allocator = new RootAllocator(Long.MaxValue)
    try {
      for (id <- VarkaTypeId.values(); v = VarkaLaneArrow.of(id) if v != null) {
        val info = VarkaTypes.info(id)
        for (form <- info.forms().asScala) {
          val vector = VarkaLaneArrow.allocate("x", form, allocator)
          try {
            assert(v.vectorClass().isInstance(vector), s"${form.sql}: ${vector.getClass}")
            assert(VarkaLaneArrow.reads(vector), form.sql)
            val width = vector.asInstanceOf[BaseFixedWidthVector].getTypeWidth
            assert(width * 8 === info.storageBits(), form.sql)
          } finally {
            vector.close()
          }
        }
      }
    } finally {
      allocator.close()
    }
  }
}
