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
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite

/**
 * The rendered files under `sql/varka/proofs/` (VARKA-240) are what `VarkaProofFiles` renders
 * from the code: the multiply-high proof's pairs from `signedMagic`, and the prelude's checks
 * from the JVM's own results. A constant that moves in the code without its proof fails here,
 * rather than leaving a proof that goes on proving the old constant. The proofs themselves run
 * under `dev/varka_prove.sh`, in the linters' job, which needs no JVM.
 */
class VarkaProofFilesSuite extends SparkFunSuite {

  test("the committed proof files are the ones the code renders") {
    val dir = getWorkspaceFilePath("sql", "varka", "proofs")
    for ((name, text) <- VarkaProofFiles.render().asScala) {
      val path = dir.resolve(name)
      if (sys.env.get("VARKA_PROOFS_REGEN").contains("true")) {
        Files.write(path, text.getBytes(StandardCharsets.UTF_8))
        logInfo(s"regenerated $path")
      } else {
        val committed = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
        if (committed != text) {
          val was = committed.linesIterator.toSet
          val moved = text.linesIterator.filterNot(was.contains).take(20).toSeq
          fail(s"$name differs from what the code renders; regenerate it with " +
            "VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite', run " +
            "dev/varka_prove.sh, and say in the plan what moved. First lines that differ:\n  " +
            moved.mkString("\n  "))
        }
      }
    }
  }
}
