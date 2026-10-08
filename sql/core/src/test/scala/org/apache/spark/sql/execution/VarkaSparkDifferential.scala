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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTestWatchdog

/**
 * The check the random differential and its replay share (VARKA-262): a fixture registered and
 * cached in both sessions, a query run in both, the outcomes compared, and the ternary partition
 * of a filter. `fused` counts the plans that took a Varka node, so that a run can say how much
 * of what it compared the kernels could have answered.
 */
trait VarkaSparkDifferential extends QueryTest with VarkaSharedSessions with VarkaTestWatchdog {
  import VarkaSparkFuzz._

  /** Plans of the Varka session, over the checks run, that took a Varka node. */
  protected var fused = 0
  protected var compared = 0
  protected var bothErrored = 0

  /**
   * The kind of disagreement `select` over `where` shows on `fixture` under `ansi`, or None.
   * `partitions` also checks the ternary partition of the filter on both engines.
   */
  protected def disagreement(
      fixture: String,
      select: String,
      where: Option[String],
      ansi: Boolean,
      partitions: Boolean): Option[String] = {
    for (session <- Seq(spark, varkaSpark)) {
      session.sql(fixture).createOrReplaceTempView("fz")
      session.catalog.cacheTable("fz")
    }
    try withAnsi(ansi) {
      val query = querySql(select, where)
      val off = outcome(spark, query)
      val on = outcome(varkaSpark, query)
      compared += 1
      if (scala.util.Try(varkaSpark.sql(query).queryExecution.executedPlan).toOption
          .exists(p => find(p)(isVarkaNode).isDefined)) {
        fused += 1
      }
      if (off.isLeft && on.isLeft) bothErrored += 1
      difference(off, on).orElse {
        if (partitions && where.isDefined) {
          Seq(spark, varkaSpark).iterator.flatMap(s =>
            partitionBroken(select, where.get, outcome(s, _))).toSeq.headOption
        } else {
          None
        }
      }
    } finally {
      for (session <- Seq(spark, varkaSpark)) session.catalog.uncacheTable("fz")
    }
  }
}
