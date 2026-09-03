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
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaExpressionCompiler
import org.apache.spark.sql.catalyst.expressions.codegen.VarkaGeneratedClassLoader
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{ByteType, DataType, DateType, IntegerType, ShortType}

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
 * `--columns name:type,...` (default `d:date,d2:date,i:int,sh:short,by:byte`) and the built-in
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
 */
object VarkaEmitDump {

  private val defaultColumns = "d:date,d2:date,i:int,sh:short,by:byte"
  private val className = "org.apache.spark.sql.varka.execution.VarkaFusedDump"

  def main(args: Array[String]): Unit = {
    var exprs = Vector.empty[String]
    var columns = defaultColumns
    var optionSpec = ""
    var rounds = 0
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--columns" => columns = args(i + 1); i += 2
        case "--options" => optionSpec = args(i + 1); i += 2
        case "--rounds" => rounds = args(i + 1).toInt; i += 2
        case e => exprs :+= e; i += 1
      }
    }
    if (exprs.isEmpty) {
      // scalastyle:off println
      System.err.println("usage: VarkaEmitDump <sql expression>... " +
        "[--columns d:date,i:int] [--options cse=false,...] [--rounds N]")
      // scalastyle:on println
      System.exit(2)
    }
    val childOutput = parseColumns(columns)
    val options = parseOptions(optionSpec)
    val resolved = exprs.map(e => resolve(CatalystSqlParser.parseExpression(e), childOutput))
    val named = resolved.map(e => Alias(e, "c")())

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
    report(f"${"method"}%-18s ${"bytes"}%6s ${"IntVector"}%9s ${"VectorMask"}%10s ${"lines"}%5s")
    val methods = VarkaEmitterTestSupport.methodNames(bytes).asScala.filter(_ != "<init>").sorted
    methods.foreach { m =>
      val size = VarkaEmitterTestSupport.codeSize(bytes, m)
      val vectorOps =
        VarkaEmitterTestSupport.invocationCount(bytes, m, "jdk.incubator.vector.IntVector")
      val maskOps =
        VarkaEmitterTestSupport.invocationCount(bytes, m, "jdk.incubator.vector.VectorMask")
      val lines = VarkaEmitterTestSupport.lineNumbers(bytes, m).size
      report(f"$m%-18s $size%6d $vectorOps%9d $maskOps%10d $lines%5d")
    }
    VarkaDebugInfo.read(bytes).ifPresent { info =>
      report("")
      report("line map (line=node):")
      info.lineMap().split("\n").foreach(l => report("  " + l))
    }

    if (rounds > 0) {
      runHot(bytes, fused.inputOrdinals.size, fused.literals.toArray,
        fused.inputOrdinals.map(childOutput), rounds)
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
      case other => throw new IllegalArgumentException(s"unsupported column type $other")
    }
    AttributeReference(name.trim, dt)()
  }

  /** Bind attributes by name and functions through the built-in registry; nothing else. */
  private def resolve(e: Expression, childOutput: Seq[Attribute]): Expression = {
    val byName = childOutput.map(a => a.name -> a).toMap
    e.transformUp {
      case UnresolvedAttribute(Seq(name)) =>
        byName.getOrElse(name, throw new IllegalArgumentException(
          s"unknown column $name; declare it with --columns"))
      case f: UnresolvedFunction =>
        FunctionRegistry.builtin.lookupFunction(FunctionIdentifier(f.nameParts.last), f.arguments)
    }
  }

  /** `k=v,k=v` onto the record's `with<K>` methods, by reflection. */
  private def parseOptions(spec: String): VarkaEmitOptions = {
    if (spec.trim.isEmpty) return VarkaEmitOptions.DEFAULTS
    spec.split(",").foldLeft(VarkaEmitOptions.DEFAULTS) { (opts, kv) =>
      val Array(k, v) = kv.trim.split("=", 2)
      val method = "with" + k.head.toUpper + k.tail
      val m = classOf[VarkaEmitOptions].getMethods.find(_.getName == method).getOrElse(
        throw new IllegalArgumentException(s"no option $k (no VarkaEmitOptions.$method)"))
      val param = m.getParameterTypes.head
      val value: AnyRef =
        if (param == classOf[Int] || param == classOf[java.lang.Integer]) Integer.valueOf(v.trim)
        else if (param == classOf[Boolean] || param == classOf[java.lang.Boolean]) {
          java.lang.Boolean.valueOf(v.trim)
        } else if (param.isEnum) {
          param.getEnumConstants.find(_.toString == v.trim).getOrElse(
            throw new IllegalArgumentException(s"$k: no constant $v")).asInstanceOf[AnyRef]
        } else throw new IllegalArgumentException(s"$k: unsupported option type $param")
      m.invoke(opts, value).asInstanceOf[VarkaEmitOptions]
    }
  }

  /** Load the class and run it `rounds` times over synthetic int32 columns, so a
   *  `-XX:CompileCommand=print` on the loop method has something to print. */
  private def runHot(bytes: Array[Byte], numInputs: Int, literals: Array[Int],
      inputs: Seq[Attribute], rounds: Int): Unit = {
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
      val outputs = VarkaEmitterTestSupport.methodNames(bytes).asScala
        .count(_.startsWith("loopDense")) max 1
      val dst = Array.fill(outputs)(buffer(rows * 4L).address())
      val dstValidity = Array.fill(outputs)(buffer((rows + 7) / 8L).address())
      var status = 0
      for (_ <- 0 until rounds) {
        status |= kernel.run(src.map(_.address()), Array.fill(numInputs)(validity.address()),
          Array.fill(numInputs)(0), dst, dstValidity, literals, rows)
      }
      report(s"ran $rounds rounds of $rows rows; status $status")
    } finally {
      arena.close()
    }
  }
}
