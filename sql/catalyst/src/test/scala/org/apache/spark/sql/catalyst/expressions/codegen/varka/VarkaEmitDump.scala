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
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, SimpleAnalyzer, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{ByteType, DataType, DateType, IntegerType, ShortType, YearMonthIntervalType}

/**
 * The emitter's debugging view, one command: what a projection compiles to, what the emitter
 * emits for it, and what that costs per method. Driven by `dev/varka_emit.sh`, which also runs
 * it under `-XX:CompileCommand=print` for the assembly.
 *
 * {{{
 *   dev/varka_emit.sh "year(d)"
 *   dev/varka_emit.sh "year(d)" "month(d)" --options shareChronoPrefix=false
 *   dev/varka_emit.sh "date_add(d, 7)" --columns d:date --asm
 * }}}
 *
 * Expressions are SQL, parsed by Catalyst's parser and resolved against the columns given with
 * `--columns name:type,...` (default `d:date,d2:date,i:int,sh:short,by:byte`; `ymm`, `ymy` and
 * `ym` declare year-month interval columns by field) and the built-in
 * function registry, then handed to [[VarkaExpressionCompiler]] exactly as a projection would
 * be. The output is the IR each entry lowered to, the shape hash production would name the
 * class by, and for every emitted method its bytecode size, its `IntVector` and `VectorMask`
 * invocation counts (the metric `VarkaLoopEmitterSuite`'s op-count tests use, so a prediction
 * registered from here is on the suite's own scale) and its line-map entries. With
 * `--rounds N` the kernel is also loaded and run N times over synthetic data, which is what
 * lets the wrapper's `--asm` get C2's standard compilation of the loop method printed.
 *
 * Options take the record's own `with*` methods by name (`--options cse=false,groupBudget=24`),
 * found by reflection so a new option needs nothing here.
 *
 * `--table` prints instead the markdown a plan's registered-op-counts section wants: one row
 * per expression, one column per option variant given with `--variant k=v,...` (the defaults
 * first), each cell the `IntVector` invocation count of `loopDense0`, and a delta column per
 * variant against the defaults. That is the table tasks 53 and 54 built by hand.
 */
object VarkaEmitDump {

  private val defaultColumns = "d:date,d2:date,i:int,sh:short,by:byte"
  private val className = "org.apache.spark.sql.varka.execution.VarkaFusedDump"

  def main(args: Array[String]): Unit = {
    var exprs = Vector.empty[String]
    var columns = defaultColumns
    var optionSpec = ""
    var rounds = 0
    var nulls = 0
    var table = false
    var variants = Vector.empty[String]
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--columns" => columns = args(i + 1); i += 2
        case "--options" => optionSpec = args(i + 1); i += 2
        case "--rounds" => rounds = args(i + 1).toInt; i += 2
        case "--nulls" => nulls = args(i + 1).toInt; i += 2
        case "--table" => table = true; i += 1
        case "--variant" => variants :+= args(i + 1); i += 2
        case e => exprs :+= e; i += 1
      }
    }
    if (exprs.isEmpty) {
      // scalastyle:off println
      System.err.println("usage: VarkaEmitDump <sql expression>... " +
        "[--columns d:date,i:int] [--options cse=false,...] [--rounds N] [--nulls N]")
      // scalastyle:on println
      System.exit(2)
    }
    val childOutput = parseColumns(columns)
    val options = parseOptions(optionSpec)
    val resolved = exprs.map(e => resolve(CatalystSqlParser.parseExpression(e), childOutput))
    val named = resolved.map(e => Alias(e, "c")())
    if (table) {
      printTable(exprs, named, childOutput, options, variants)
      return
    }

    val partial = VarkaExpressionCompiler.compilePartial(named, childOutput).getOrElse {
      report("nothing fused: every entry declined or no column is referenced")
      System.exit(1)
      throw new IllegalStateException()
    }
    exprs.zip(partial.specs).zipWithIndex.foreach { case ((text, spec), position) =>
      val decline = partial.declines.get(position).map(d => s" - $d").getOrElse("")
      report(f"entry $position%2d  $text%-40s  $spec$decline")
    }
    val fused = partial.fused
    report("")
    report(s"inputs (child ordinals, in kernel order): ${fused.inputOrdinals.mkString(", ")}")
    report(s"literals (scalarArgs, in slot order):    ${fused.literals.mkString(", ")}")
    fused.outputs.zipWithIndex.foreach { case (o, k) =>
      report(s"output $k IR: ${VarkaVectorIR.canonical(o)}")
    }
    val key = new VarkaShapeKey(fused.outputs.asJava, fused.inputOrdinals.size,
      fused.literals.size, options)
    report(s"shape hash: ${VarkaShapeCacheImpl.shapeHash(key)}  options: " +
      (if (options.isDefault) "(defaults)" else options.canonical()))

    val bytes = VarkaLoopEmitter.emit(className, fused.outputs.asJava, fused.inputOrdinals.size,
      fused.literals.size, null, null, options)
    report("")
    report(f"${"method"}%-18s ${"bytes"}%6s ${"IntVector"}%9s ${"VectorMask"}%10s " +
      f"${"validity"}%8s ${"lines"}%5s")
    val methods = VarkaEmitterTestSupport.methodNames(bytes).asScala.filter(_ != "<init>").sorted
    methods.foreach { m =>
      val size = VarkaEmitterTestSupport.codeSize(bytes, m)
      val vectorOps =
        VarkaEmitterTestSupport.invocationCount(bytes, m, "jdk.incubator.vector.IntVector")
      val maskOps =
        VarkaEmitterTestSupport.invocationCount(bytes, m, "jdk.incubator.vector.VectorMask")
      // Validity work, which is the metric task 70 moves: everything the method invokes on
      // VarkaVectorSupport except the segment mapping, which every body mode emits per segment
      // and which would keep the count off zero however much validity work went away.
      val validityOps = VarkaEmitterTestSupport.invocationCount(
        bytes, m, "org.apache.spark.sql.varka.vector.VarkaVectorSupport", Seq("ofAddress").asJava)
      val lines = VarkaEmitterTestSupport.lineNumbers(bytes, m).size
      report(f"$m%-18s $size%6d $vectorOps%9d $maskOps%10d $validityOps%8d $lines%5d")
    }
    VarkaDebugInfo.read(bytes).ifPresent { info =>
      report("")
      report("line map (line=node):")
      info.lineMap().split("\n").foreach(l => report("  " + l))
    }

    if (rounds > 0) {
      runHot(bytes, fused.inputOrdinals.size, fused.outputs.size, fused.literals.toArray,
        fused.inputOrdinals.map(childOutput), rounds, nulls)
    }
  }

  /** `--table`: `loopDense0`'s `IntVector` count per expression, under the defaults and under
   *  each `--variant`, with the delta - each expression emitted alone, as a one-output kernel. */
  private def printTable(exprs: Seq[String], named: Seq[org.apache.spark.sql.catalyst.expressions
      .NamedExpression], childOutput: Seq[Attribute], base: VarkaEmitOptions,
      variants: Seq[String]): Unit = {
    val columns = ("defaults", base) +: variants.map(v => (v, applyOptions(base, v)))
    val header = "| expression | " + columns.map(_._1).mkString(" | ") +
      variants.map(v => s" | delta $v").mkString + " |"
    report(header)
    report("|---|" + columns.map(_ => "---|").mkString + variants.map(_ => "---|").mkString)
    exprs.zip(named).foreach { case (text, one) =>
      val fused = VarkaExpressionCompiler.compilePartial(Seq(one), childOutput).map(_.fused)
      fused match {
        case None => report(s"| `$text` | declined |")
        case Some(f) =>
          val counts = columns.map { case (_, opts) =>
            val bytes = VarkaLoopEmitter.emit(className, f.outputs.asJava,
              f.inputOrdinals.size, f.literals.size, null, null, opts)
            VarkaEmitterTestSupport.invocationCount(bytes, "loopDense0",
              "jdk.incubator.vector.IntVector")
          }
          val deltas = counts.tail.map(c => f"${c - counts.head}%+d")
          report(s"| `$text` | " + counts.mkString(" | ") +
            deltas.map(d => s" | $d").mkString + " |")
      }
    }
  }

  private def report(s: String): Unit = {
    // scalastyle:off println
    println(s)
    // scalastyle:on println
  }

  private def parseColumns(spec: String): Seq[Attribute] = spec.split(",").toSeq.map { c =>
    val Array(name, tpe) = c.trim.split(":")
    val dt: DataType = tpe.trim.toLowerCase(Locale.ROOT) match {
      case "date" => DateType
      case "int" | "integer" => IntegerType
      case "short" | "smallint" => ShortType
      case "byte" | "tinyint" => ByteType
      // Task 67's third type in the date lane. Spelled by field, because the field decides
      // what the emitter does with it: MONTH and YEAR are the ends a column can hold, and
      // "ym" is the YEAR TO MONTH pair make_ym_interval produces.
      case "ymm" | "interval month" => YearMonthIntervalType(YearMonthIntervalType.MONTH)
      case "ymy" | "interval year" => YearMonthIntervalType(YearMonthIntervalType.YEAR)
      case "ym" | "interval year to month" => YearMonthIntervalType()
      case other => throw new IllegalArgumentException(s"unsupported column type $other")
    }
    AttributeReference(name.trim, dt)()
  }

  /**
   * Bind attributes by name and functions through the built-in registry, then run the
   * analyzer over the result so that operators get the type coercion they need.
   *
   * The coercion pass is not optional and its absence was silent. `d + ym` parses to
   * `Add(date, yearmonthinterval)`, which only becomes `DateAddYMInterval` when
   * `AnsiTypeCoercion`/`TypeCoercion` rewrites it; without that the compiler sees an `Add`
   * over two types it has no arm for and declines. So every expression written with an
   * operator rather than a function - which is every date/interval arithmetic shape task 67
   * added - reported "declined" here while fusing perfectly well in a real session, and the
   * tool disagreed with `Surface`'s own `expectFused` on entries the surface has been timing
   * for weeks. Resolving through a `LocalRelation` and `SimpleAnalyzer` costs nothing and
   * makes the tool agree with the engine.
   */
  private def resolve(e: Expression, childOutput: Seq[Attribute]): Expression = {
    val byName = childOutput.map(a => a.name -> a).toMap
    val bound = e.transformUp {
      case UnresolvedAttribute(Seq(name)) =>
        byName.getOrElse(name, throw new IllegalArgumentException(
          s"unknown column $name; declare it with --columns"))
      case f: UnresolvedFunction =>
        FunctionRegistry.builtin.lookupFunction(FunctionIdentifier(f.nameParts.last), f.arguments)
    }
    val analyzed = SimpleAnalyzer.execute(
      Project(Seq(Alias(bound, "a")()), LocalRelation(childOutput)))
    analyzed.asInstanceOf[Project].projectList.head.asInstanceOf[Alias].child
  }

  /** `k=v,k=v` onto the record's `with<K>` methods, by reflection. */
  private def parseOptions(spec: String): VarkaEmitOptions =
    applyOptions(VarkaEmitOptions.DEFAULTS, spec)

  private def applyOptions(base: VarkaEmitOptions, spec: String): VarkaEmitOptions = {
    if (spec.trim.isEmpty) return base
    spec.split(",").foldLeft(base) { (opts, kv) =>
      val Array(k, v) = kv.trim.split("=", 2)
      val method = "with" + k.head.toUpper + k.tail
      val m = classOf[VarkaEmitOptions].getMethods.find(_.getName == method).getOrElse(
        throw new IllegalArgumentException(s"no option $k (no VarkaEmitOptions.$method)"))
      val param = m.getParameterTypes.head
      val value: AnyRef =
        if (param == classOf[Int] || param == classOf[java.lang.Integer]) Integer.valueOf(v.trim)
        else if (param == classOf[Boolean] || param == classOf[java.lang.Boolean]) {
          // `Boolean.valueOf` answers false for every string that is not "true", so a typo or
          // a plausible-looking `=on` silently selects the arm you did not ask for - and a
          // benchmark then measures it without saying so. The enum branch below already
          // refuses an unknown constant; this refuses an unknown boolean the same way.
          v.trim.toLowerCase(java.util.Locale.ROOT) match {
            case "true" => java.lang.Boolean.TRUE
            case "false" => java.lang.Boolean.FALSE
            case other =>
              throw new IllegalArgumentException(s"$k: expected true or false, got '$other'")
          }
        } else if (param.isEnum) {
          param.getEnumConstants.find(_.toString == v.trim).getOrElse(
            throw new IllegalArgumentException(s"$k: no constant $v")).asInstanceOf[AnyRef]
        } else throw new IllegalArgumentException(s"$k: unsupported option type $param")
      m.invoke(opts, value).asInstanceOf[VarkaEmitOptions]
    }
  }

  /** Load the class and run it `rounds` times over synthetic int32 columns, so a
   *  `-XX:CompileCommand=print` on the loop method has something to print. `outputs` is the
   *  kernel's output count, passed in rather than inferred from its loop methods: since task
   *  32 step B2 one loop method can hold several outputs, and a destination array sized by
   *  method count made the kernel index past it.
   *
   *  `nulls` is how many rows of each input are null. Zero - the default - reports a null-free
   *  batch, which the emitted `run` dispatches to the dense driver, so only the dense methods
   *  are ever compiled; any positive count takes the masked path instead. Without it a
   *  `--rounds` probe cannot see the masked body at all, which is what a
   *  `-XX:+PrintCompilation` run of the task 70 review needed. */
  private def runHot(bytes: Array[Byte], numInputs: Int, outputs: Int, literals: Array[Int],
      inputs: Seq[Attribute], rounds: Int, nulls: Int): Unit = {
    if (inputs.exists(a => a.dataType == ShortType || a.dataType == ByteType)) {
      report("(--rounds skipped: synthetic data is int32 only, and a short or byte column is read)")
      return
    }
    val rows = 1024
    val loader = new VarkaGeneratedClassLoader(getClass.getClassLoader)
    loader.defineGeneratedClass(className, bytes)
    val kernel = loader.loadClass(className).getConstructor().newInstance()
      .asInstanceOf[VarkaFusedKernel]
    val arena = Arena.ofConfined()
    try {
      def buffer(bytesLen: Long): MemorySegment = arena.allocate(bytesLen, 64)
      val src = Array.fill(numInputs)(buffer(rows * 4L))
      src.foreach(s => (0 until rows).foreach(r => s.set(ValueLayout.JAVA_INT, r * 4L, 18000 + r)))
      val validity = buffer((rows + 7) / 8L)
      validity.fill(0xFF.toByte)
      // Every `stride`-th row null, spread over the batch so both a full lane group and the
      // epilogue's partial one see a null; the kernel reads the count, so the two must agree.
      if (nulls > 0) {
        val stride = math.max(1, rows / nulls)
        var cleared = 0
        var r = 0
        while (r < rows && cleared < nulls) {
          val byte = validity.get(ValueLayout.JAVA_BYTE, (r / 8).toLong)
          validity.set(ValueLayout.JAVA_BYTE, (r / 8).toLong,
            (byte & ~(1 << (r % 8))).toByte)
          cleared += 1
          r += stride
        }
      }
      val dst = Array.fill(outputs)(buffer(rows * 4L).address())
      val dstValidity = Array.fill(outputs)(buffer((rows + 7) / 8L).address())
      var status = 0
      for (_ <- 0 until rounds) {
        status |= kernel.run(src.map(_.address()), Array.fill(numInputs)(validity.address()),
          Array.fill(numInputs)(nulls), dst, dstValidity, literals, rows)
      }
      report(s"ran $rounds rounds of $rows rows, $nulls null per input; status $status")
    } finally {
      arena.close()
    }
  }
}
