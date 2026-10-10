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

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.scalatest.Assertions

/**
 * The option configuration matrix (VARKA-248 step 2, `m7/VARKA-248.md` 3.2): the Varka suites
 * rerun once per configuration - the defaults with one option changed - and every kernel they
 * emit carries it, so a reference form or a machine's alternative meets every test rather than
 * only the few that set it.
 *
 * A JVM runs under one configuration, named by `-Dvarka.matrix.config=<name>=<value>` (several
 * pairs comma separated), and under the defaults without it. Tests start from [[base]] where they
 * would start from `VarkaEmitOptions.DEFAULTS`, and the SQL suites' emit hook starts there too, so
 * the configuration reaches every kernel while production code reads no property.
 * `dev/varka_matrix.sh` runs the configurations, several JVMs at a time.
 */
object VarkaMatrix {

  /** The system property naming this JVM's configuration. */
  val PROPERTY = "varka.matrix.config"

  /** The system property naming the file each test's fused-batch count is appended to. */
  val REPORT_PROPERTY = "varka.matrix.report"

  /** This JVM's configuration, `""` for the defaults. */
  val config: String = sys.props.getOrElse(PROPERTY, "").trim

  /**
   * The name of the batch-size axis (VARKA-301): `arrowBatchSize=<rows>` sets the rows per Arrow
   * cached batch (`spark.sql.execution.arrow.maxRecordsPerBatch`; the Arrow cache ignores
   * `spark.sql.inMemoryColumnarStorage.batchSize`) in the SQL suites' sessions. It is not an emit
   * option: [[parse]] passes over it, and the sessions read [[arrowBatchSize]].
   */
  final val ARROW_BATCH_SIZE = "arrowBatchSize"

  /**
   * The batch sizes the axis runs: one row; three, a batch that is all epilogue at any width;
   * and seventeen, just past two lane groups of eight.
   */
  val ARROW_BATCH_SIZES = Seq(1, 3, 17)

  /** This JVM's rows per Arrow cached batch, when its configuration names one. */
  val arrowBatchSize: Option[Int] = config.split(",").map(_.trim.split("=", 2)).collectFirst {
    case Array(ARROW_BATCH_SIZE, rows) => rows.trim.toInt
  }

  /**
   * The options every test starts from: the defaults with this JVM's configuration applied.
   * Declared after the axis's names, which `parse` reads while this object initialises.
   */
  val base: VarkaEmitOptions = parse(config)

  /**
   * `name=value[,name=value]` applied over the defaults, each name looked up in
   * `VarkaEmitOption.TABLE`. A flag takes only `true` or `false`, and an enum only one of its
   * constants, so a typo is refused rather than silently selecting the default.
   */
  def parse(spec: String): VarkaEmitOptions = {
    if (spec.isEmpty) return VarkaEmitOptions.DEFAULTS
    spec.split(",").foldLeft(VarkaEmitOptions.DEFAULTS) { (options, pair) =>
      pair.trim.split("=", 2) match {
        case Array(ARROW_BATCH_SIZE, rows) if rows.trim.toInt > 0 => options
        case Array(name, value) =>
          VarkaEmitOption.named(name.trim) match {
            case count: VarkaEmitOption.Count => count.`with`(options, value.trim.toInt)
            case flag: VarkaEmitOption.Flag =>
              value.trim.toLowerCase(Locale.ROOT) match {
                case "true" => flag.`with`(options, true)
                case "false" => flag.`with`(options, false)
                case other =>
                  throw new IllegalArgumentException(s"$name: expected true or false, got $other")
              }
            case choice: VarkaEmitOption.Choice[_] => choice.withNamed(options, value.trim)
          }
        case _ => throw new IllegalArgumentException(s"expected name=value, got '$pair'")
      }
    }
  }

  /**
   * The lane counts the matrix emits for besides the machine's own: the widths either side of
   * the laptop's eight. `lanesOverride` has no audit values - the bytes oracle emits every shape
   * at two widths already - so its matrix values are named here.
   */
  private val LANE_COUNTS = Seq(4, 16)

  /**
   * Every configuration, in table order: each non-default arm of each option but the fault
   * injectors - a flag's other value, an enum's other constants, a count's audit values - and
   * the lane counts above. Derived from the table, so a new option joins with its entry. Then
   * the batch-size axis, which changes the sessions' cached batches rather than an option.
   *
   * A flag whose `VarkaEmitOption.Subject` is not the defaults runs its other value over that
   * subject, `denseValidityOnce=false,validityByBitmap=false,validityOrFirst=false`: at the
   * defaults the value emits the defaults' bytes, and the configuration would test nothing
   * (VARKA-247).
   */
  def configurations: Seq[String] = {
    val defaults = VarkaEmitOptions.DEFAULTS
    VarkaEmitOption.TABLE.asScala.toSeq
      .filter(_.reason != VarkaEmitOption.Reason.FAULT_INJECTOR)
      .flatMap { option =>
        val arms = option.arms.asScala.toSeq
          .filter(arm => arm.apply.apply(defaults) != defaults)
          .map(_.name)
        option match {
          case _ if option.name == "lanesOverride" => LANE_COUNTS.map(n => s"lanesOverride=$n")
          case f: VarkaEmitOption.Flag if f.subject() != VarkaEmitOption.Subject.DEFAULTS =>
            arms.map(arm => s"${f.subject().label()},$arm")
          case _ => arms
        }
      } ++ ARROW_BATCH_SIZES.map(rows => s"$ARROW_BATCH_SIZE=$rows")
  }

  /**
   * The tag of a test whose subject is the defaults' emitted structure - op counts, a method
   * count, a `HugeMethodLimit` crossing, bytes compared byte for byte, the size a shape declines
   * at. Under a configuration it would measure nothing new, so the matrix cancels it there.
   */
  object PinsDefaults extends org.scalatest.Tag("org.apache.spark.sql.varka.PinsDefaults")

  /** What a configuration is expected to do to a test: fail it, or leave it fusing nothing. */
  object Kind extends Enumeration {
    val FAILS: Value = Value("fails")
    val DECLINES: Value = Value("declines")
  }

  /** One line of the skip list: a test a configuration is expected to break, and why. */
  case class Skip(config: String, suite: String, test: String, kind: Kind.Value, reason: String)

  /** The committed skip list, relative to `spark.test.home`. */
  val SKIPS_PATH = "sql/varka/matrix/skips.tsv"

  /**
   * `sql/varka/matrix/skips.tsv`: tab separated configuration, suite (simple name), test name,
   * kind and reason; `#` starts a comment line.
   */
  lazy val skips: Seq[Skip] = {
    val home = sys.props.getOrElse("spark.test.home", ".")
    val file = new File(home, SKIPS_PATH)
    if (!file.exists()) Seq.empty
    else parseSkips(new String(Files.readAllBytes(file.toPath), StandardCharsets.UTF_8))
  }

  private[varka] def parseSkips(text: String): Seq[Skip] =
    text.split("\n").toSeq.map(_.stripSuffix("\r"))
      .filter(line => line.trim.nonEmpty && !line.startsWith("#"))
      .map { line =>
        line.split("\t") match {
          case Array(config, suite, test, kind, reason) =>
            Skip(config, suite, test, Kind.withName(kind), reason)
          case _ => throw new IllegalArgumentException(s"$SKIPS_PATH: malformed line '$line'")
        }
      }

  /** The `fails` entry for this test under `config`, if the skip list has one. */
  private[varka] def expectedFailure(
      entries: Seq[Skip], config: String, suite: String, test: String): Option[Skip] =
    entries.find(s => s.kind == Kind.FAILS && s.config == config && s.suite == suite &&
      s.test == test)

  /**
   * Batches the Varka kernels have processed in this JVM so far, or -1 where nothing counts
   * them. The SQL suites' sessions set it; the emitter suites run their kernels directly and a
   * decline there fails the test, so they need no count.
   */
  @volatile var fusedBatches: () => Long = () => -1L

  /**
   * Runs one test under the matrix. A test the skip list expects to fail under this JVM's
   * configuration is cancelled with the entry's reason when it fails, and fails as a stale entry
   * when it passes. Every test's fused-batch count goes to the report file when one is named.
   */
  def around[T](suite: String, test: String)(body: => T): T =
    around(skips, config, sys.props.get(REPORT_PROPERTY), suite, test)(body)

  private[varka] def around[T](
      entries: Seq[Skip],
      config: String,
      report: Option[String],
      suite: String,
      test: String)(body: => T): T = {
    val before = fusedBatches()
    try {
      expectedFailure(entries, config, suite, test) match {
        case None => body
        case Some(skip) =>
          val failure =
            try {
              body
              None
            } catch {
              case e: org.scalatest.exceptions.TestCanceledException => throw e
              case e: Throwable => Some(e)
            }
          failure match {
            case Some(e) =>
              Assertions.cancel(s"expected to fail under ${skip.config}: ${skip.reason} " +
                s"(${e.getClass.getSimpleName})")
            case None =>
              Assertions.fail(s"stale entry in $SKIPS_PATH: '$test' of $suite passes under " +
                s"${skip.config}; remove the line")
          }
      }
    } finally {
      report.foreach { path =>
        val after = fusedBatches()
        val fused = if (before < 0 || after < 0) -1L else after - before
        VarkaMatrix.synchronized {
          val writer = new FileWriter(path, StandardCharsets.UTF_8, true)
          try writer.write(s"$suite\t$test\t$fused\n") finally writer.close()
        }
      }
    }
  }
}

/** Prints the matrix's configurations, one per line, for `dev/varka_matrix.sh`. */
object VarkaMatrixMain {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    VarkaMatrix.configurations.foreach(println)
    // scalastyle:on println
  }
}
