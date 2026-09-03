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

import java.io.{BufferedReader, File, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite

/**
 * Task 31: assert the instructions, not the ratio.
 *
 * Every vectorization claim this project makes elsewhere is an inference from a throughput
 * ratio - the parity gate's "the emitted loop is within 0.9x of the hand-written kernel"
 * standing in for "C2 intrinsified the Vector API calls". Task 24 measured how weak that
 * inference is: the same kernels moved 50-190% under an inline {@code CompileCommand} aimed at
 * the incubating Vector API's package (the flag is quoted in full in {@code PLAN_TASK_31.md};
 * it cannot be written here, because Scala comments nest and the pattern contains a slash-star)
 * in the engine's JMH harness, and under 1% under that same flag in the catalyst harness. A
 * ratio moves for reasons that have nothing to do with the instructions emitted. This suite
 * reads the instructions instead.
 *
 * Each case forks a JVM running [[VarkaAssemblyProbe]] under
 * `-XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=print,<class>::<method>`, and asserts that
 * the *standard* (non-OSR) C2 nmethod for that method contains a member of the expected
 * instruction family on a vector register of the width the child reports. See
 * `PLAN_TASK_31.md`; the four ways this can go quietly wrong are in `SKILLS.md` beside the
 * hsdis notes.
 *
 * The suite skips, rather than fails, where no disassembler is available - that is the expected
 * state of a CI runner, and a gate that goes red for missing tooling is a gate people delete.
 * It says which of the two happened: no library found, or found and refused to load.
 */
class VarkaAssemblySuite extends SparkFunSuite {

  // --- The families -------------------------------------------------------------------------

  /**
   * A named set of mnemonics that mean the same thing. Never a single mnemonic and never a
   * count: the register class is a property of the host, and task 32's bimodality investigation
   * found identical vector-op counts with a 2x difference in total instructions, so a count
   * assertion would go red on a register-allocation roll with nothing actually wrong.
   */
  private case class Family(description: String, mnemonics: Set[String])

  private val packedIntAdd = Family("packed integer add", Set("vpaddd", "paddd"))
  private val packedLoadStore = Family("packed load/store",
    Set("vmovdqu32", "vmovdqu", "vmovdqa32", "vmovdqa", "movdqu", "movdqa"))

  /**
   * What a body looks like when the intrinsic did not fire. Reported alongside a miss, because
   * "0 packed adds, 47 scalar adds" is a diagnosis and "assertion failed" is not.
   */
  private val scalarIntAdd = Family("scalar integer add", Set("addl", "addq", "leal", "leaq"))

  /** x86-64 is the only table filled in; see `PLAN_TASK_31.md` 3.1 for why none is invented. */
  private val supportedArches = Set("amd64", "x86_64")

  // --- Locating a disassembler --------------------------------------------------------------

  private sealed trait Hsdis
  /** A directory holding `hsdis-<arch>.so`, to be passed to the child as `LD_LIBRARY_PATH`. */
  private case class OnLibraryPath(dir: File) extends Hsdis
  /** `libhsdis.so` already sits beside `libjvm.so`, where HotSpot finds it unaided. */
  private case object BesideLibjvm extends Hsdis
  private case object NoHsdis extends Hsdis

  private lazy val arch: String = System.getProperty("os.arch")

  private lazy val hsdis: Hsdis = {
    val libName = s"hsdis-$arch.so"
    val candidates: Seq[File] =
      Option(System.getProperty("varka.hsdis.dir")).map(new File(_)).toSeq ++
        Option(System.getenv("VARKA_HSDIS_DIR")).map(new File(_)).toSeq ++
        Option(System.getenv("LD_LIBRARY_PATH")).toSeq
          .flatMap(_.split(File.pathSeparatorChar).filter(_.nonEmpty).map(new File(_)))
    val besideLibjvm = new File(new File(System.getProperty("java.home"), "lib"), "server")
    candidates.find(d => new File(d, libName).isFile) match {
      case Some(dir) => OnLibraryPath(dir)
      case None if new File(besideLibjvm, "libhsdis.so").isFile => BesideLibjvm
      case None => NoHsdis
    }
  }

  // --- Running the child --------------------------------------------------------------------

  /** Everything a case needs from one child run. */
  private case class ProbeRun(output: String, exitCode: Int) {

    /** The child's own `IntVector.SPECIES_PREFERRED` width - the flags in force decide it. */
    def preferredBits: Int = {
      val line = output.linesIterator
        .find(_.startsWith(VarkaAssemblyProbe.PREFERRED_BITS_PREFIX))
        .getOrElse(fail(s"the probe printed no preferred width; output:\n${tail(output)}"))
      line.stripPrefix(VarkaAssemblyProbe.PREFERRED_BITS_PREFIX).trim.toInt
    }

    def ranToCompletion: Boolean =
      output.linesIterator.exists(_.startsWith(VarkaAssemblyProbe.DONE_PREFIX))
  }

  /** Cap on captured output. `print` over one method is a few thousand lines; a mistaken
   *  pattern that matches every method is not, and buffering that without bound would hang the
   *  suite rather than fail it. */
  private val maxOutputChars = 8 * 1024 * 1024

  private val childTimeoutSeconds = 300L

  private def testClasspath: String =
    Option(System.getenv("SPARK_DIST_CLASSPATH"))
      .filter(_.nonEmpty)
      .getOrElse(System.getProperty("java.class.path"))

  private def runProbe(
      caseName: String, printPattern: String, extraFlags: Seq[String] = Seq.empty): ProbeRun = {
    val javaBin = new File(new File(System.getProperty("java.home"), "bin"), "java")
    val command = new java.util.ArrayList[String]()
    command.add(javaBin.getAbsolutePath)
    command.add("--add-modules")
    command.add("jdk.incubator.vector")
    command.add("-XX:+UnlockDiagnosticVMOptions")
    command.add(s"-XX:CompileCommand=print,$printPattern")
    extraFlags.foreach(command.add)
    command.add("-cp")
    command.add(testClasspath)
    command.add(classOf[VarkaAssemblyProbe].getName)
    command.add(caseName)

    val builder = new ProcessBuilder(command)
    builder.redirectErrorStream(true)
    hsdis match {
      case OnLibraryPath(dir) =>
        val existing = Option(System.getenv("LD_LIBRARY_PATH")).getOrElse("")
        val value =
          if (existing.isEmpty) dir.getAbsolutePath
          else s"${dir.getAbsolutePath}${File.pathSeparator}$existing"
        builder.environment().put("LD_LIBRARY_PATH", value)
      case _ =>
    }

    val process = builder.start()
    val captured = new StringBuilder
    val reader = new BufferedReader(
      new InputStreamReader(process.getInputStream, StandardCharsets.UTF_8))
    try {
      var line = reader.readLine()
      var overflowed = false
      while (line != null && !overflowed) {
        captured.append(line).append('\n')
        if (captured.length > maxOutputChars) { overflowed = true }
        line = reader.readLine()
      }
      if (overflowed) {
        process.destroyForcibly()
        fail(s"the probe printed more than $maxOutputChars characters for pattern " +
          s"'$printPattern' - the CompileCommand pattern is probably matching every method")
      }
    } finally {
      reader.close()
    }
    if (!process.waitFor(childTimeoutSeconds, TimeUnit.SECONDS)) {
      process.destroyForcibly()
      fail(s"the probe did not finish within $childTimeoutSeconds s for case '$caseName'")
    }
    ProbeRun(captured.toString, process.exitValue())
  }

  // --- Reading the disassembly ----------------------------------------------------------------

  private case class Insn(mnemonic: String, operands: String)

  private case class Nmethod(tier: String, osr: Boolean, method: String, insns: Seq[Insn])

  private val nmethodHeader = """^=+\s*(C[12])-compiled nmethod\s*=+$""".r
  private val compiledMethod = """^Compiled method \((c[12])\)\s+(.*)$""".r
  private val methodName = """([\w$./]+::[\w$<>]+)""".r
  /**
   * An instruction line: an address, a colon, a mnemonic beginning with a letter, and operands.
   *
   * <p>This pattern alone is not enough to tell an instruction from a hex dump, which is the
   * point of {@link #disassemblyOpen} below. With no disassembler HotSpot does not stay silent:
   * it prints the nmethod under a `[MachCode]` header as raw hex words - `0x...: ff1f 0045 |
   * 85c9 0f84 | ...` - and those lines have the same address-colon shape. Requiring a letter
   * does not help either, since plenty of hex words begin with one (`ff1f`, `e929`, `c349`).
   * Measured rather than assumed: 68 such lines parsed as instructions before the section
   * marker was used, and the suite reported "the intrinsic did not fire" on a body it had never
   * actually read.
   */
  private val insnLine = """^\s*0x[0-9a-fA-F]+:\s+([a-z][a-zA-Z0-9._]*)\s*(.*)$""".r

  /** hsdis produced real instructions for this nmethod. */
  private val disassemblyMarker = "[Disassembly]"

  /** HotSpot fell back to a raw hex dump, because no disassembler was usable. */
  private val machCodeMarker = "[MachCode]"

  /**
   * Split the child's output into nmethods.
   *
   * HotSpot prints a C1 nmethod for the same method alongside the C2 one, and C1's body is
   * scalar by construction - reading the concatenated text would find scalar instructions in a
   * method that vectorized perfectly. It also prints OSR compilations, which are not what runs
   * in production. Both are separated here rather than filtered at the call site, so no case can
   * forget to.
   */
  private def parseNmethods(output: String): Seq[Nmethod] = {
    val result = ArrayBuffer.empty[Nmethod]
    var tier: Option[String] = None
    var method = ""
    var osr = false
    var disassembled = false
    var insns = ArrayBuffer.empty[Insn]

    def flush(): Unit = {
      tier.foreach(t => result += Nmethod(t, osr, method, insns.toSeq))
      tier = None
      method = ""
      osr = false
      disassembled = false
      insns = ArrayBuffer.empty[Insn]
    }

    output.linesIterator.foreach { line =>
      line match {
        case nmethodHeader(t) =>
          flush()
          tier = Some(t.toLowerCase(java.util.Locale.ROOT))
        case compiledMethod(_, rest) if tier.isDefined =>
          // HotSpot marks an on-stack-replacement compilation with '%' in this line.
          osr = rest.contains("%")
          method = methodName.findFirstIn(rest).getOrElse("")
        case insnLine(mnemonic, operands) if tier.isDefined && disassembled =>
          insns += Insn(mnemonic, operands)
        case _ =>
      }
      // Checked after the match so the marker's own line is never taken for an instruction.
      if (line.contains(disassemblyMarker)) { disassembled = true }
      else if (line.contains(machCodeMarker)) { disassembled = false }
    }
    flush()
    result.toSeq
  }

  /** The standard, non-OSR C2 compilation of `method`, or a failure that says which of the
   *  several distinguishable things went wrong instead. */
  private def standardC2(run: ProbeRun, method: String): Nmethod = {
    val all = parseNmethods(run.output)
    val forMethod = all.filter(_.method.endsWith(method))
    assert(forMethod.nonEmpty,
      s"HotSpot printed no nmethod for $method; it may never have been compiled. " +
        s"nmethods seen: ${all.map(n => s"${n.tier}${if (n.osr) "-osr" else ""} ${n.method}")
          .mkString(", ")}")
    forMethod.find(n => n.tier == "c2" && !n.osr).getOrElse {
      fail(s"no standard (non-OSR) C2 nmethod for $method; saw " +
        forMethod.map(n => s"${n.tier}${if (n.osr) "-osr" else ""}").mkString(", "))
    }
  }

  // --- The assertion -------------------------------------------------------------------------

  /** `zmm`, `ymm` or `xmm`, from the width the child actually ran at. */
  private def registerClass(bits: Int): String = bits match {
    case 512 => "zmm"
    case 256 => "ymm"
    case 128 => "xmm"
    case other => cancel(s"no register class known for a preferred vector width of $other bits")
  }

  private def countIn(nmethod: Nmethod, family: Family, regClass: Option[String]): Int =
    nmethod.insns.count { insn =>
      family.mnemonics.contains(insn.mnemonic) &&
        regClass.forall(cls => insn.operands.contains(s"%$cls"))
    }

  private def assertHasFamily(nmethod: Nmethod, family: Family, regClass: String): Unit = {
    val found = countIn(nmethod, family, Some(regClass))
    if (found == 0) {
      val scalar = countIn(nmethod, scalarIntAdd, None)
      fail(s"${nmethod.method}: expected at least one ${family.description} on a %$regClass " +
        s"register and found none, in ${nmethod.insns.size} instructions " +
        s"(${scalar} ${scalarIntAdd.description}). The intrinsic did not fire, or this body " +
        s"came out scalar.")
    }
  }

  private def assertLacksFamily(nmethod: Nmethod, family: Family): Unit = {
    val found = countIn(nmethod, family, None)
    assert(found == 0,
      s"${nmethod.method}: expected no ${family.description} anywhere in this body and found " +
        s"$found - the case is supposed to be unvectorizable, so either it is not or the " +
        s"detector is matching the wrong thing")
  }

  // --- Preconditions -------------------------------------------------------------------------

  /**
   * A child that failed to start or died is a bug in this suite, not missing tooling, so it
   * fails rather than cancels - and it is checked before the disassembler is, because a JVM that
   * refused its own flags produces no disassembly either and would otherwise be reported as a
   * missing hsdis. That is not hypothetical: it is how the `::` pattern bug below was first
   * mis-reported.
   */
  private def requireHealthyChild(run: ProbeRun): Unit = {
    assert(run.exitCode == 0,
      s"the probe exited with ${run.exitCode}; output:\n${tail(run.output)}")
    assert(run.ranToCompletion, s"the probe did not finish:\n${tail(run.output)}")
  }

  /**
   * Skip, with a message that distinguishes the two ways a disassembler can be unavailable.
   *
   * <p>The discrimination is made on this suite's own search result, not on HotSpot's
   * "Loading hsdis library failed" line. That line is printed in *both* cases - HotSpot says it
   * when it looked and found nothing just as much as when it found a library it could not load
   * - so keying off it reported "a disassembler was found but HotSpot refused to load it
   * (hsdis: NoHsdis)", which contradicts itself. A wrong skip message is the same class of
   * defect as a detector that matches nothing: the suite still goes green and still tells the
   * reader something false.
   */
  private def requireDisassembler(run: ProbeRun): Unit = {
    if (parseNmethods(run.output).exists(_.insns.nonEmpty)) { return }
    hsdis match {
      case NoHsdis =>
        cancel("no disassembler found: none of -Dvarka.hsdis.dir, VARKA_HSDIS_DIR, " +
          s"LD_LIBRARY_PATH holds hsdis-$arch.so, and <java.home>/lib/server holds no " +
          "libhsdis.so. This is the expected state of a CI runner; see SKILLS.md on " +
          "building hsdis.")
      case found =>
        val refused =
          if (run.output.contains("Loading hsdis library failed")) {
            " and HotSpot printed 'Loading hsdis library failed'"
          } else {
            ""
          }
        cancel(s"a disassembler was found ($found) but no disassembly came back$refused; " +
          "see SKILLS.md on building hsdis")
    }
  }

  private def requireSupportedArch(): Unit = {
    assume(supportedArches.contains(arch),
      s"no instruction-family table for $arch; see PLAN_TASK_31.md 3.1")
  }

  private def tail(s: String): String = s.linesIterator.toSeq.takeRight(40).mkString("\n")

  /**
   * HotSpot's method pattern accepts `package/class.method` or `package.class::method` and
   * rejects the two spellings mixed: a slashed package with `::` fails VM startup outright with
   * "Method pattern uses '/' together with '::'". `PLAN_TASK_31.md` 4.3 wrote the slashed form
   * with `::`, and the self-test is what caught it - the child never started, so there was no
   * disassembly to read. The dotted form is used throughout.
   */
  private val probeClass =
    "org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaAssemblyProbe"

  // --- The self-test -------------------------------------------------------------------------

  // This pair comes before any Varka shape on purpose. A mnemonic pattern that matches nothing
  // looks exactly like a body with no vector instructions, so without a case that must find
  // something and a case that must find nothing, every other assertion in this suite can pass
  // vacuously. SKILLS.md records the concrete way this nearly happened: this hsdis build
  // separates the mnemonic from its operands with tabs, and a regex written for spaces matches
  // no instruction at all.

  test("self-test: an explicit IntVector loop compiles to packed instructions") {
    requireSupportedArch()
    val run = runProbe("vectorAdd", s"$probeClass::vectorAdd")
    requireHealthyChild(run)
    requireDisassembler(run)
    val nmethod = standardC2(run, "VarkaAssemblyProbe::vectorAdd")
    val cls = registerClass(run.preferredBits)
    assertHasFamily(nmethod, packedIntAdd, cls)
    assertHasFamily(nmethod, packedLoadStore, cls)
  }

  test("self-test: a loop-carried recurrence compiles to no packed instructions") {
    requireSupportedArch()
    val run = runProbe("scalarChain", s"$probeClass::scalarChain")
    requireHealthyChild(run)
    requireDisassembler(run)
    val nmethod = standardC2(run, "VarkaAssemblyProbe::scalarChain")
    assertLacksFamily(nmethod, packedIntAdd)
    // ... and the body is not empty, so "found none" above is a fact about the body rather than
    // about the parser having produced nothing to look at.
    assert(nmethod.insns.size > 20,
      s"only ${nmethod.insns.size} instructions parsed - the parser, not the body, is the " +
        "thing under test here")
  }

  test("the narrow-width run reports a narrower register class") {
    requireSupportedArch()
    val run = runProbe("vectorAdd", s"$probeClass::vectorAdd", Seq("-XX:MaxVectorSize=16"))
    requireHealthyChild(run)
    requireDisassembler(run)
    val bits = run.preferredBits
    assert(bits == 128,
      s"-XX:MaxVectorSize=16 should give a 128-bit preferred species, got $bits")
    val nmethod = standardC2(run, "VarkaAssemblyProbe::vectorAdd")
    assertHasFamily(nmethod, packedIntAdd, registerClass(bits))
  }
}
