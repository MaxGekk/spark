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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaKernelCheck.Batch
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.{ColumnRef, LaneType}
import org.apache.spark.sql.types.{ByteType, ShortType}

/**
 * `TINYINT` and `SMALLINT` arithmetic in Spark's three evaluation modes, on both of 329's arms
 * (VARKA-330, `VARKA-329.md` 4): `LEGACY` wraps at the type's own width, `ANSI` declines the
 * batch Spark raises on, and `TRY` nulls the row it would raise on, for `+`, `-`, `*`, the
 * negation, `abs` and the `try_` forms. The expressions go through
 * [[VarkaExpressionCompiler]] and the emitted class, so each check holds the compiler's
 * admission, the emitter and the evaluator at once, and every row is held to
 * [[VarkaSparkOracle]], Spark's own `eval`.
 *
 * <p>The named cases - `-(-128Y)`, `abs(-128Y)`, `127Y + 1Y`, `-128Y * -1Y` and the short
 * analogues, with in-range neighbours - run at every length from 1 to 129 and on four null
 * patterns; every pair of a `TINYINT` square runs in one 2^16-row batch per operator, mode and
 * arm, and the `SMALLINT` sweeps run a 2^24-row slice on every build and all 2^32 pairs as
 * opt-in tests. A `LEGACY` chain `(b1 + b2) * b3 - b4` is checked as a root and under
 * consumers that are not ring operations, which is where the deferred wrap is emitted.
 *
 * <p>In its own JVM: the native arm needs a narrow species beside the int one, and the
 * `SMALLINT` sweeps keep one class loaded across thousands of batches.
 */
class VarkaEmitterNarrowArithSuite extends VarkaEmitterTestBase with VarkaOwnJvm {

  import VarkaEmitterNarrowArithSuite._

  // -------------------------------------------------------------------------------------------
  // The columns, the modes and the two arms
  // -------------------------------------------------------------------------------------------

  private val byteCols: Seq[Attribute] =
    Seq(AttributeReference("b1", ByteType)(), AttributeReference("b2", ByteType)())
  private val shortCols: Seq[Attribute] =
    Seq(AttributeReference("s1", ShortType)(), AttributeReference("s2", ShortType)())
  private val chainCols: Seq[Attribute] = byteCols ++ Seq(
    AttributeReference("b3", ByteType)(), AttributeReference("b4", ByteType)())

  private val legacy = Mode("LEGACY", EvalMode.LEGACY)
  private val ansi = Mode("ANSI", EvalMode.ANSI)
  private val tryMode = Mode("TRY", EvalMode.TRY)
  private val modes: Seq[Mode] = Seq(legacy, ansi, tryMode)

  /** The two arms: computed at the int lane and stored narrow, and computed narrow. */
  private val arms: Seq[(String, VarkaEmitOptions)] = Seq(
    "the widened arm" -> VarkaMatrix.base,
    "the native arm" -> VarkaMatrix.base.toBuilder.narrowLanes(true).build())

  /** The lengths the named cases run at: every length to 129, the 64-tail ladder's top included. */
  private val ladder: Seq[Int] = 1 to 129

  // -------------------------------------------------------------------------------------------
  // Compiling, emitting, running
  // -------------------------------------------------------------------------------------------

  /**
   * Whether the arm's lane count cannot widen `lane` (VARKA-329's `narrowRefusal`): the widened
   * arm reads a narrow column with the narrow species of the int species' width, which the
   * Vector API does not have for a four-lane int species, and a width with no baked lane count
   * names no species at all. The question is asked of a widened drive on the native arm too,
   * because a checked narrow multiply is driven at the int lane on either arm (VARKA-329.md 2.4)
   * and meets the same refusal there. The compiler demotes such an entry and the row engine
   * answers it, which is what the checks below assert instead of running.
   */
  private def cannotWiden(options: VarkaEmitOptions, lane: LaneType): Boolean =
    VarkaLoopEmitter.narrowRefusal(
      Seq[VarkaVectorIR](new ColumnRef(0, lane)).asJava,
      options.toBuilder.narrowLanes(false).build()).isPresent

  /**
   * Compiles `expr` over `attrs` at `options` and emits it, or returns None where the arm's lane
   * count cannot widen `lane`; the demotion is then asserted to be that refusal and not a
   * decline of another kind.
   */
  private def emitArm(expr: Expression, attrs: Seq[Attribute], lane: LaneType,
      options: VarkaEmitOptions, where: String): Option[Emitted] = {
    val plan = VarkaExpressionCompiler.compile(Seq(Alias(expr, "c")()), attrs, options)
    if (!plan.isPresent) {
      assert(cannotWiden(options, lane),
        s"$where: ${expr.sql} did not compile, and not because the width cannot widen the " +
          s"$lane lane")
      return None
    }
    val p = plan.get
    val className =
      s"org.apache.spark.sql.varka.execution.VarkaNarrowArith${classCounter.incrementAndGet()}"
    val bytes = VarkaLoopEmitter.emit(className, p.outputs, p.inputOrdinals.size,
      p.numLiterals, null, null, options)
    val oracle = new VarkaSparkOracle(Seq(expr),
      p.inputOrdinals.asScala.toSeq.map(i => attrs(i)))
    Some(Emitted(className, bytes, p.outputs.asScala.toSeq, p.inputOrdinals.size,
      p.literals.asScala.map(_.intValue).toArray, oracle))
  }

  /** Loads an emitted class, runs `f`, and releases the loader. */
  private def withKernel[T](emitted: Emitted)(f: VarkaFusedKernel => T): T = {
    val (kernel, loader) = load((emitted.name, emitted.bytes))
    try f(kernel) finally loader.release()
  }

  /**
   * One batch on a loaded kernel: whether the batch was answered, and so compared row by row
   * with the reference evaluator and with Spark. A declined batch is one the row engine answers.
   */
  private def run(emitted: Emitted, kernel: VarkaFusedKernel, batch: Batch, ctx: String,
      spark: Boolean): Boolean =
    VarkaKernelCheck.runLoadedNarrow(ctx, kernel, emitted.roots, emitted.numInputs, emitted.lits,
      batch, declineAllowed = true, spark = if (spark) Some(emitted.oracle) else None) == 0

  private def batch(length: Int, pattern: Int => Boolean, cols: Seq[Int]): Batch =
    Batch(length, cols.map(_ => pattern), cols.map(v => Array.fill(length)(v)).toArray,
      forceMasked = false)

  // -------------------------------------------------------------------------------------------
  // The named cases
  // -------------------------------------------------------------------------------------------

  private val namedByte: Seq[Named] = Seq(
    Named("127Y + 1Y", "+", 127, 1),
    Named("-128Y - 1Y", "-", -128, 1),
    Named("-128Y * -1Y", "*", -128, -1),
    Named("127Y * 127Y", "*", 127, 127),
    Named("-128Y + -128Y", "+", -128, -128),
    Named("126Y + 1Y", "+", 126, 1),
    Named("-127Y - 1Y", "-", -127, 1),
    Named("-128Y * 1Y", "*", -128, 1),
    Named("127Y - 127Y", "-", 127, 127))

  private val namedShort: Seq[Named] = Seq(
    Named("32767S + 1S", "+", 32767, 1),
    Named("-32768S - 1S", "-", -32768, 1),
    Named("-32768S * -1S", "*", -32768, -1),
    Named("32767S * 32767S", "*", 32767, 32767),
    Named("-32768S + -32768S", "+", -32768, -32768),
    Named("32766S + 1S", "+", 32766, 1),
    Named("-32767S - 1S", "-", -32767, 1),
    Named("-32768S * 1S", "*", -32768, 1),
    Named("32767S - 32767S", "-", 32767, 32767))

  private val namedUnaryByte: Seq[Unary] = Seq(
    Unary("-(-128Y)", "neg", -128), Unary("abs(-128Y)", "abs", -128),
    Unary("-127Y", "neg", -127), Unary("abs(-127Y)", "abs", -127),
    Unary("abs(0Y)", "abs", 0), Unary("-0Y", "neg", 0))

  private val namedUnaryShort: Seq[Unary] = Seq(
    Unary("-(-32768S)", "neg", -32768), Unary("abs(-32768S)", "abs", -32768),
    Unary("-32767S", "neg", -32767), Unary("abs(-32767S)", "abs", -32767))

  /**
   * Runs one named binary case at every length and null pattern on one arm. The valid-row count
   * lets a checked case tell "Spark raises" from "every row that would raise is null": a null
   * row does not raise, so an all-null batch is answered.
   */
  private def namedRuns(c: Named, lane: LaneType, attrs: Seq[Attribute], mode: Mode,
      options: VarkaEmitOptions, where: String): Seq[Run] = {
    val expr = c.expression(attrs, mode.evalMode)
    emitArm(expr, attrs, lane, options, where) match {
      case None => Seq.empty
      case Some(emitted) => withKernel(emitted) { kernel =>
        for (length <- ladder; (pname, pattern) <- nullPatterns) yield {
          val answered = run(emitted, kernel, batch(length, pattern, Seq(c.left, c.right)),
            s"$where ${c.label} len=$length $pname", spark = true)
          Run(length, pname, (0 until length).count(i => !pattern(i)), answered)
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // The width rule (VARKA-329's narrowRefusal), which the arms rest on
  // -------------------------------------------------------------------------------------------

  test("a widened narrow load is served exactly where the int species holds the narrow one") {
    // The claims are the emitter's own admission rule, so the test pins its options rather than
    // taking the configuration's: the subject is the width, not the configuration.
    val byteCol: VarkaVectorIR = new ColumnRef(0, LaneType.BYTE)
    val shortCol: VarkaVectorIR = new ColumnRef(0, LaneType.SHORT)
    def refused(options: VarkaEmitOptions): (Boolean, Boolean) =
      (VarkaLoopEmitter.narrowRefusal(Seq(byteCol).asJava, options).isPresent,
        VarkaLoopEmitter.narrowRefusal(Seq(shortCol).asJava, options).isPresent)
    def widened(lanes: Int): VarkaEmitOptions =
      VarkaEmitOptions.DEFAULTS.toBuilder.lanesOverride(lanes).build()
    // Two int lanes are 64 bits, so the byte species would be 16 bits and the short one 32: the
    // Vector API names neither. Four lanes hold a 64-bit short species and no byte one.
    assert(refused(widened(2)) === (true, true))
    assert(refused(widened(4)) === (true, false))
    assert(refused(widened(8)) === (false, false))
    assert(refused(widened(16)) === (false, false))
    // A width with no baked lane count names no species: validityByWidth off always refuses.
    assert(refused(VarkaEmitOptions.DEFAULTS.toBuilder.validityByWidth(false).build())
      === (true, true))
    // The native arm computes in the narrow lane itself, so nothing has to widen at any width.
    for (lanes <- Seq(0, 2, 4, 8, 16)) {
      val native = VarkaEmitOptions.DEFAULTS.toBuilder.narrowLanes(true).lanesOverride(lanes)
        .build()
      assert(refused(native) === (false, false), s"native at $lanes int lanes")
    }
  }

  // -------------------------------------------------------------------------------------------
  // The named cases, mode by mode
  // -------------------------------------------------------------------------------------------

  test("the named TINYINT and SMALLINT cases answer in LEGACY on both arms, every length") {
    for ((arm, options) <- arms; (lane, cases) <- Seq(
        LaneType.BYTE -> namedByte, LaneType.SHORT -> namedShort)) {
      val attrs = if (lane == LaneType.BYTE) byteCols else shortCols
      for (c <- cases) {
        val where = s"${c.label} LEGACY $arm"
        for (r <- namedRuns(c, lane, attrs, legacy, options, where)) {
          assert(r.answered,
            s"$where: the kernel declined the batch at length ${r.length} (${r.pattern}), and " +
              "LEGACY overflows at no value")
        }
      }
    }
  }

  test("the named TRY cases answer and null exactly the overflowing rows on both arms") {
    for ((arm, options) <- arms; (lane, cases) <- Seq(
        LaneType.BYTE -> namedByte, LaneType.SHORT -> namedShort)) {
      val attrs = if (lane == LaneType.BYTE) byteCols else shortCols
      for (c <- cases) {
        val where = s"${c.label} TRY $arm"
        // The harness holds every row to the reference's null and to Spark's null; what this
        // check adds is that the batch is answered at all, since a checked narrow lane under TRY
        // clears validity rather than condemning the batch.
        for (r <- namedRuns(c, lane, attrs, tryMode, options, where)) {
          assert(r.answered,
            s"$where: the kernel declined the batch at length ${r.length} (${r.pattern}), " +
              "where Spark answers a null")
        }
      }
    }
  }

  test("the named ANSI cases decline the batch where Spark raises, and answer the rest") {
    for ((arm, options) <- arms; (lane, cases) <- Seq(
        LaneType.BYTE -> namedByte, LaneType.SHORT -> namedShort)) {
      val attrs = if (lane == LaneType.BYTE) byteCols else shortCols
      for (c <- cases) {
        val where = s"${c.label} ANSI $arm"
        val raises = c.overflows(lane)
        for (r <- namedRuns(c, lane, attrs, ansi, options, where)) {
          // A pattern that nulls every row keeps Spark from raising, so the batch is answered;
          // this is also the check that a null row's value never reaches the range compare.
          assert(r.answered === !(raises && r.valid > 0),
            s"$where: at length ${r.length} (${r.pattern}) the kernel " +
              s"${if (r.answered) "answered" else "declined"} where Spark " +
              s"${if (raises && r.valid > 0) "raises" else "answers"}")
        }
      }
    }
  }

  test("the named negation and abs cases follow the mode on both arms, every length") {
    for ((arm, options) <- arms; (lane, cases) <- Seq(
        LaneType.BYTE -> namedUnaryByte, LaneType.SHORT -> namedUnaryShort);
        mode <- Seq(legacy, ansi)) {
      val attrs = if (lane == LaneType.BYTE) byteCols else shortCols
      for (u <- cases) {
        val where = s"${u.label} ${mode.name} $arm"
        emitArm(u.expression(attrs, mode.checked), attrs, lane, options, where) match {
          case None => ()
          case Some(emitted) => withKernel(emitted) { kernel =>
            for (length <- ladder; (pname, pattern) <- nullPatterns) {
              val answered = run(emitted, kernel, batch(length, pattern, Seq(u.value)),
                s"$where len=$length $pname", spark = true)
              // As above: a null row does not raise, so only a valid one can condemn the batch.
              val raises = mode.checked && u.overflows(lane) && (0 until length).exists(!pattern(_))
              assert(answered === !raises,
                s"$where: at length $length ($pname) the kernel " +
                  s"${if (answered) "answered" else "declined"} where Spark " +
                  s"${if (raises) "raises" else "answers"}")
            }
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // Every TINYINT pair, 2^16 rows in one batch
  // -------------------------------------------------------------------------------------------

  /** Row `i` holds the pair `((i >> 8) - 128, (i & 0xFF) - 128)`: every byte pair once each. */
  private val byteSquare: Batch = Batch(65536,
    Seq[Int => Boolean](_ => false, _ => false),
    Array(Array.tabulate(65536)(i => (i >> 8) - 128),
      Array.tabulate(65536)(i => (i & 0xFF) - 128)),
    forceMasked = false)

  /** The in-range square: every pair whose exact answer stays inside the byte. */
  private def inRangeSquare(op: String): Batch = op match {
    case "*" => Batch(529, Seq[Int => Boolean](_ => false, _ => false),
      Array(Array.tabulate(529)(i => i / 23 - 11), Array.tabulate(529)(i => i % 23 - 11)),
      forceMasked = false)
    case _ => Batch(16384, Seq[Int => Boolean](_ => false, _ => false),
      Array(Array.tabulate(16384)(i => (i >> 7) - 64),
        Array.tabulate(16384)(i => (i & 0x7F) - 64)),
      forceMasked = false)
  }

  private def byteRoot(op: String, mode: Mode, options: VarkaEmitOptions, where: String)
      : Option[Emitted] =
    emitArm(Named(op, op, 0, 0).expression(byteCols, mode.evalMode), byteCols, LaneType.BYTE,
      options, where)

  private def answered(emitted: Emitted, b: Batch, where: String): Boolean =
    withKernel(emitted)(kernel => run(emitted, kernel, b, where, spark = true))

  test("every TINYINT pair answers in LEGACY on both arms, 2^16 rows in one batch") {
    for ((arm, options) <- arms; op <- Seq("+", "-", "*")) {
      val where = s"TINYINT $op LEGACY $arm"
      byteRoot(op, legacy, options, where).foreach { emitted =>
        assert(answered(emitted, byteSquare, s"$where, the square"),
          s"$where: the kernel declined a LEGACY square, which overflows at no pair")
      }
    }
  }

  test("every TINYINT pair in TRY nulls exactly the overflowing rows, both arms") {
    for ((arm, options) <- arms; op <- Seq("+", "-", "*")) {
      val where = s"TINYINT $op TRY $arm"
      byteRoot(op, tryMode, options, where).foreach { emitted =>
        assert(answered(emitted, byteSquare, s"$where, the square"),
          s"$where: the kernel declined a TRY square, where Spark answers a null")
      }
    }
  }

  test("the overflowing TINYINT square declines under ANSI and the in-range one answers") {
    for ((arm, options) <- arms; op <- Seq("+", "-", "*")) {
      val where = s"TINYINT $op ANSI $arm"
      byteRoot(op, ansi, options, where).foreach { emitted =>
        // The square holds 127 + 1 and its kin, so Spark raises on a row of it under every
        // operator and the batch must decline rather than wrap. The in-range square must
        // answer, which is what shows the range compare does not condemn unconditionally.
        assert(!answered(emitted, byteSquare, s"$where, the square"),
          s"$where: the kernel answered a square with an overflowing row")
        assert(answered(emitted, inRangeSquare(op), s"$where, the in-range square"),
          s"$where: the kernel declined an in-range square")
      }
    }
  }

  test("the negation and abs over every TINYINT value answer in LEGACY and check in ANSI") {
    val all = Batch(256, Seq[Int => Boolean](_ => false),
      Array(Array.tabulate(256)(i => i - 128)), forceMasked = false)
    val aboveMin = Batch(255, Seq[Int => Boolean](_ => false),
      Array(Array.tabulate(255)(i => i - 127)), forceMasked = false)
    for ((arm, options) <- arms; op <- Seq("neg", "abs");
        (mode, checked) <- Seq((legacy, false), (ansi, true))) {
      val where = s"TINYINT $op ${mode.name} $arm"
      emitArm(Unary("v", op, -128).expression(byteCols, checked), byteCols, LaneType.BYTE,
        options, where) match {
        case None => ()
        case Some(emitted) =>
          assert(answered(emitted, aboveMin, s"$where, the values above the minimum"),
            s"$where: the kernel declined a batch with no overflowing value")
          val allAnswered = answered(emitted, all, s"$where, every value")
          assert(allAnswered === !checked,
            s"$where: the kernel ${if (allAnswered) "answered" else "declined"} the whole " +
              s"range, and Spark ${if (checked) "raises on its minimum" else "wraps it"}")
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // The deferred wrap: a LEGACY chain under its root and under non-ring consumers
  // -------------------------------------------------------------------------------------------

  /**
   * The three-node chain `(b1 + b2) * b3 - b4`, and four consumers of it: as a root (reduced at
   * its store), and under `greatest`, `abs` and a comparison used by a choice. Only the root may
   * leave the intermediate values unreduced, so a wrap emitted at the wrong node - or not at all
   * where a consumer reads the value - shows up as a wrong row.
   */
  private val chainShapes: Seq[(String, Expression => Expression)] = Seq(
    ("as a root", (c: Expression) => c),
    ("under greatest", (c: Expression) => Greatest(Seq(c, chainCols(3)))),
    ("under abs", (c: Expression) => Abs(c, false)),
    ("under a choice", (c: Expression) => If(GreaterThan(c, chainCols(3)),
      chainCols(0), chainCols(1))))

  private def chain(mode: EvalMode.Value): Expression =
    Subtract(Multiply(Add(chainCols(0), chainCols(1), mode), chainCols(2), mode),
      chainCols(3), mode)

  /** Data that stays in the byte range, and data whose intermediate sums leave it. */
  private val chainData: Seq[(String, (Int, Int) => Int)] = Seq(
    ("in range", (c, i) => ((i * (7 + 3 * c)) % 41) - 20),
    ("over the range", (c, i) => if (i % 2 == 0) ((i * (5 + c)) % 61) - 30 else 100 - 20 * c))

  test("a LEGACY chain answers at its root and under every non-ring consumer, both arms") {
    for ((arm, options) <- arms; (shape, consumer) <- chainShapes;
        (dataName, data) <- chainData) {
      val where = s"the chain $shape ($dataName) LEGACY $arm"
      emitArm(consumer(chain(EvalMode.LEGACY)), chainCols, LaneType.BYTE, options, where) match {
        case None => ()
        case Some(emitted) => withKernel(emitted) { kernel =>
          for (length <- ladder; (pname, pattern) <- nullPatterns) {
            val cols = (0 until 4).map(c => Array.tabulate(length)(i => data(c, i)))
            val b = Batch(length, Seq.fill(4)(pattern), cols.toArray, forceMasked = false)
            assert(run(emitted, kernel, b, s"$where len=$length $pname", spark = true),
              s"$where: the kernel declined at length $length ($pname)")
          }
        }
      }
    }
  }

  test("a checked chain raises on its first overflowing node, so the batch declines") {
    // Every node is checked, so `(b1 + b2) * b3 - b4` over data whose sum alone leaves the byte
    // raises even where a later wrap would have brought the value back into range.
    for ((arm, options) <- arms; (shape, consumer) <- chainShapes) {
      val where = s"the chain $shape ANSI $arm"
      emitArm(consumer(chain(EvalMode.ANSI)), chainCols, LaneType.BYTE, options, where) match {
        case None => ()
        case Some(emitted) => withKernel(emitted) { kernel =>
          val length = 64
          // 40 + 100 is 140 at the first node, out of range, though the later nodes and the
          // store would have brought some rows back into it.
          val cols = Array(
            Array.fill(length)(40), Array.fill(length)(100),
            Array.fill(length)(1), Array.fill(length)(0))
          val b = Batch(length, Seq.fill(4)(_ => false), cols, forceMasked = false)
          assert(!run(emitted, kernel, b, s"$where, overflowing", spark = true),
            s"$where: the kernel answered a chain whose first node overflows")
        }
      }
    }
  }

  test("a TRY chain nulls the overflowing row and keeps its neighbours") {
    // The per-node check clears the row's validity bit, so the chain is answered and the
    // overflowing rows come out null beside valid neighbours, which the harness compares row by
    // row with Spark.
    for ((arm, options) <- arms; (shape, consumer) <- chainShapes) {
      val where = s"the chain $shape TRY $arm"
      emitArm(consumer(chain(EvalMode.TRY)), chainCols, LaneType.BYTE, options, where) match {
        case None => ()
        case Some(emitted) => withKernel(emitted) { kernel =>
          val length = 64
          // Row `i` overflows at the first node exactly when `i` is odd, so every overflowing
          // row has a valid neighbour.
          val cols = Array(
            Array.tabulate(length)(i => if (i % 2 == 0) 1 else 100),
            Array.fill(length)(100), Array.fill(length)(1), Array.fill(length)(0))
          val b = Batch(length, Seq.fill(4)(_ => false), cols, forceMasked = false)
          assert(run(emitted, kernel, b, s"$where, alternating", spark = true),
            s"$where: the kernel declined a TRY chain, where Spark answers a null")
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // SMALLINT: the 2^24-row slice on every build, and all 2^32 pairs opt-in
  // -------------------------------------------------------------------------------------------

  /** Every short value, in order: the second column of a sweep batch, and the first's values. */
  private val allShorts: Array[Int] = Array.tabulate(65536)(i => i - 32768)

  /** Every short value, as the first column of a sweep batch. */
  private val shortOperands: Seq[Int] = allShorts.toSeq

  /**
   * The 256 constant first operands of the always-on slice: the two extremes, zero and one, and
   * a spread of every 256th value, so all three overflow shapes are met.
   */
  private val sliceOperands: Seq[Int] =
    Seq(Short.MinValue.toInt, Short.MaxValue.toInt, 0, 1) ++
      (4 until 256).map(k => (k * 256) - 32768)

  /**
   * Whether the rectangle of `op` over the constant operand `v` and every short holds a pair
   * whose exact result leaves the range - what Spark raises on under `ANSI`, and so what must
   * decline the batch. `+` and `-` are monotone in the second operand and `*` is linear in it,
   * so its two ends are its extremes.
   */
  private def shortSquareOverflows(op: String, v: Int): Boolean = {
    val (lo, hi) = bounds(LaneType.SHORT)
    op match {
      case "+" => v.toLong + hi > hi || v.toLong + lo < lo
      case "-" => v.toLong - hi < lo || v.toLong - lo > hi
      case "*" =>
        val atLo = v.toLong * lo
        val atHi = v.toLong * hi
        atLo < lo || atLo > hi || atHi < lo || atHi > hi
    }
  }

  /**
   * Whether the opt-in run asks for the sweep at `where`: all of them, or only those whose name
   * holds one of the comma-separated entries of `-Dvarka.sweep.filter`. The child JVM runs every
   * test whatever the parent's name filter, so this is how one 2^32 sweep is timed on its own -
   * the budget of 18 (`VARKA-329.md` 4.4) is extrapolated from that one.
   */
  private def sweeps(where: String): Boolean =
    Option(System.getProperty("varka.sweep.filter")) match {
      case None => true
      case Some(filters) => filters.split(",").exists(f => where.contains(f))
    }

  /**
   * `op` over SMALLINT pairs against Spark: each operand against every short value, 2^16 rows a
   * batch. The reference holds every row; Spark every `oracleEvery`-th batch, since Spark for
   * row through 2^24 rows costs more than the kernel and the reference together (VARKA-329.md
   * 4.4). Returns the number of batches run and the number answered, which the caller holds to
   * its expectation - a sweep that ran no batch at all must not pass.
   */
  private def sweepShort(emitted: Emitted, kernel: VarkaFusedKernel, operands: Seq[Int],
      oracleEvery: Int, answered: Int => Boolean, where: String): (Int, Int) = {
    var run = 0
    var took = 0
    for ((v, k) <- operands.zipWithIndex) {
      val b = Batch(65536, Seq[Int => Boolean](_ => false, _ => false),
        Array(Array.fill(65536)(v), allShorts), forceMasked = false)
      val got = this.run(emitted, kernel, b, s"$where v=$v", spark = k % oracleEvery == 0)
      assert(got === answered(v), s"$where: v=$v was ${if (got) "answered" else "declined"}")
      run += 1
      if (got) took += 1
    }
    (run, took)
  }

  private def shortSweep(mode: Mode, op: String, options: VarkaEmitOptions, operands: Seq[Int],
      oracleEvery: Int, arm: String): Unit = {
    val where = s"SMALLINT $op ${mode.name} $arm"
    emitArm(Named(op, op, 0, 0).expression(shortCols, mode.evalMode), shortCols,
      LaneType.SHORT, options, where) match {
      case None => ()
      case Some(emitted) => withKernel(emitted) { kernel =>
        val started = System.nanoTime()
        val (ran, took) = sweepShort(emitted, kernel, operands, oracleEvery,
          v => mode != ansi || !shortSquareOverflows(op, v), where)
        // Under a checked mode only the operands with no overflowing pair are answered, and the
        // prediction is exact: an unexecuted or unconditionally declining sweep fails here.
        val expected = operands.count(v => mode != ansi || !shortSquareOverflows(op, v))
        assert(ran === operands.size, s"$where: ran $ran of ${operands.size} batches")
        assert(took === expected, s"$where: answered $took batches, expected $expected")
        // One line per slice, so that the 2^32 sweeps' budget can be read off a build's own log.
        logInfo(s"$where: $ran batches in ${(System.nanoTime() - started) / 1000000L} ms")
      }
    }
  }

  for (mode <- modes; op <- Seq("+", "-", "*"); (arm, options) <- arms) {
    test(s"a 2^24-row SMALLINT $op slice in ${mode.name} agrees with Spark on $arm") {
      shortSweep(mode, op, options, sliceOperands, oracleEvery = 64, arm)
    }
  }

  for (mode <- modes; op <- Seq("+", "-", "*"); (arm, options) <- arms) {
    test(s"every SMALLINT pair for $op in ${mode.name} on $arm " +
        "(opt-in: -Dvarka.sweep=true)") {
      assume(System.getProperty("varka.sweep") == "true",
        "set -Dvarka.sweep=true to sweep all 2^32 SMALLINT pairs")
      val where = s"SMALLINT $op ${mode.name} $arm"
      assume(sweeps(where), s"-Dvarka.sweep.filter does not name '$where'")
      val expr = Named(op, op, 0, 0).expression(shortCols, mode.evalMode)
      emitArm(expr, shortCols, LaneType.SHORT, options, where) match {
        case None => cancel(s"$where: the emitter cannot widen the SHORT lane at this width")
        case Some(emitted) =>
          val started = System.nanoTime()
          val before = VarkaKernelCheck.sparkComparisons.get
          val (ran, took) = withKernel(emitted) { kernel =>
            sweepShort(emitted, kernel, shortOperands, oracleEvery = 256,
              v => mode != ansi || !shortSquareOverflows(op, v), where)
          }
          val seconds = (System.nanoTime() - started) / 1000000000L
          val rows = VarkaKernelCheck.sparkComparisons.get - before
          // Printed rather than logged: the child JVM's log does not reach the log of the run
          // that started it, and its output does, which is where a timed sweep is read.
          println(s"$where: $ran batches ($took answered) in $seconds s, " +
            s"$rows rows held to Spark")
      }
    }
  }

  test("the oracle's share of a sweep batch, timed (opt-in: -Dvarka.sweep=true)") {
    // The 2^32 budget is extrapolated from the sweeps' own timings; this separates the Spark
    // oracle from the kernel and the reference on one 2^16-row batch of the same shape.
    assume(System.getProperty("varka.sweep") == "true",
      "set -Dvarka.sweep=true to time the oracle's share")
    val b = Batch(65536, Seq[Int => Boolean](_ => false, _ => false),
      Array(Array.fill(65536)(1), allShorts), forceMasked = false)
    for ((arm, options) <- arms) {
      val where = s"the oracle's share $arm"
      assume(sweeps(where), s"-Dvarka.sweep.filter does not name '$where'")
      emitArm(Named("+", "+", 0, 0).expression(shortCols, EvalMode.LEGACY), shortCols,
        LaneType.SHORT, options, where) match {
        case None => ()
        case Some(emitted) => withKernel(emitted) { kernel =>
          var without = 0L
          var withOracle = 0L
          for (round <- 0 until 3) {
            val t0 = System.nanoTime()
            run(emitted, kernel, b, s"$where, no oracle, round $round", spark = false)
            val t1 = System.nanoTime()
            run(emitted, kernel, b, s"$where, with the oracle, round $round", spark = true)
            val t2 = System.nanoTime()
            without += t1 - t0
            withOracle += t2 - t1
          }
          println(s"$where: 2^16 rows in ${without / 3000} us without the oracle and " +
            s"${withOracle / 3000} us with it")
        }
      }
    }
  }
}

/**
 * The suite's small value types, kept here rather than in the suite because a case class nested
 * in a class carries an outer reference its `equals` cannot check.
 */
private object VarkaEmitterNarrowArithSuite {

  /** One evaluation mode, as a Spark arithmetic expression carries it. */
  private case class Mode(name: String, evalMode: EvalMode.Value) {
    /**
     * The flag `abs` and the negation take. Spark has no `try_negate` and no `try_abs`, so only
     * `ANSI` checks them; `LEGACY` and `TRY` wrap.
     */
    def checked: Boolean = evalMode == EvalMode.ANSI
  }

  /** One emitted class with what its rows are checked against. */
  private case class Emitted(name: String, bytes: Array[Byte], roots: Seq[VarkaVectorIR],
      numInputs: Int, lits: Array[Int], oracle: VarkaSparkOracle)

  /** The narrow type's range, as the reference and the checks compare against it. */
  private def bounds(lane: LaneType): (Long, Long) = lane match {
    case LaneType.BYTE => (Byte.MinValue.toLong, Byte.MaxValue.toLong)
    case LaneType.SHORT => (Short.MinValue.toLong, Short.MaxValue.toLong)
    case LaneType.INT => (Int.MinValue.toLong, Int.MaxValue.toLong)
    case LaneType.LONG => (Long.MinValue, Long.MaxValue)
  }

  /**
   * A named binary case: the operator over two narrow operands, and whether the exact result
   * leaves the type's range - what `ANSI` raises on and `TRY` nulls.
   */
  private case class Named(label: String, op: String, left: Int, right: Int) {
    def overflows(lane: LaneType): Boolean = {
      val exact = op match {
        case "+" => left.toLong + right
        case "-" => left.toLong - right
        case "*" => left.toLong * right
      }
      val (lo, hi) = bounds(lane)
      exact < lo || exact > hi
    }

    def expression(attrs: Seq[Attribute], mode: EvalMode.Value): Expression = op match {
      case "+" => Add(attrs(0), attrs(1), mode)
      case "-" => Subtract(attrs(0), attrs(1), mode)
      case "*" => Multiply(attrs(0), attrs(1), mode)
    }
  }

  /** A named unary case: the negation or `abs` of one narrow value. */
  private case class Unary(label: String, op: String, value: Int) {
    def overflows(lane: LaneType): Boolean = value == bounds(lane)._1

    def expression(attrs: Seq[Attribute], checked: Boolean): Expression = op match {
      case "neg" => UnaryMinus(attrs(0), checked)
      case "abs" => Abs(attrs(0), checked)
    }
  }

  /** One named batch's outcome: its length, pattern, number of valid rows and answer. */
  private case class Run(length: Int, pattern: String, valid: Int, answered: Boolean)
}
