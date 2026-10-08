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

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.execution.VarkaSparkFuzz._

/**
 * Replays every reproducer in `sql/varka/fuzz/spark/` (VARKA-262): a file marked
 * `status: regression` must now agree between Varka off and on, and one marked
 * `status: known <row>` must still disagree, so that the list cannot outlive its bugs.
 */
class VarkaSparkReproducerSuite extends VarkaSparkDifferential {

  private lazy val files: Seq[java.nio.file.Path] = {
    val dir = getWorkspaceFilePath("sql", "varka", "fuzz", "spark")
    if (!Files.isDirectory(dir)) Seq.empty
    else Files.list(dir).iterator().asScala.filter(_.toString.endsWith(".sql")).toSeq.sorted
  }

  test("every saved reproducer is a regression that now agrees or a known one that still differs") {
    files.foreach { file =>
      val r = parse(new String(Files.readAllBytes(file), java.nio.charset.StandardCharsets.UTF_8))
      val now = disagreement(r.fixture, r.select, r.where, r.ansi, partitions = true)
      withClue(s"$file: ") {
        if (r.status == "regression") {
          assert(now.isEmpty, s"a regression disagrees again: ${now.get}")
        } else {
          assert(r.status.startsWith("known "),
            s"status is 'regression' or 'known <row>': ${r.status}")
          assert(now.contains(r.kind),
            s"a known disagreement is now ${now.getOrElse("agreement")}, not ${r.kind}: " +
              "delete the file or mark it a regression")
        }
      }
    }
  }
}
