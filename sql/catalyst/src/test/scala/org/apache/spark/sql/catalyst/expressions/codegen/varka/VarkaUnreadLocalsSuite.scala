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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMatrix.PinsDefaults
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR._

/**
 * The cost corpus's emitted classes held to locals that are read (`VarkaUnreadLocals`). A shared
 * slot nothing reads means the body's use count (`Slots.bodyUses`) and the walk that emits the
 * body disagree about how often a node is visited: a count too high costs a `dup` and a store, as
 * it did for dates two calendar nodes reach through one shared prefix before the count learned
 * the walk's rule (`VARKA-223.md` 9.4). Under `elideUnreadLocals` no reference local of any
 * kind goes unread (VARKA-239).
 */
class VarkaUnreadLocalsSuite extends VarkaEmitterTestBase {

  // The corpus of VARKA-223.md 9.1: random shapes of both lanes, wide shapes whose groups
  // load materialized prefixes, and the ladders.
  private lazy val corpus = VarkaEmitCostCorpus.fuzz(1000).asScala.toSeq ++
    VarkaEmitCostCorpus.wide().asScala ++ VarkaEmitCostCorpus.ladders().asScala

  // The shipped options elide the unread locals; the reference form builds them all.
  private val elided = VarkaMatrix.base.withLanesOverride(VarkaEmitCostCorpus.LANES)
    .withElideUnreadLocals(true)
  private val built = elided.withElideUnreadLocals(false)

  /** A corpus shape the emitter declined, with the decline. */
  private case class Declined(family: String, label: String, decline: VarkaEmitDeclined)

  /** The corpus under one options value: the classes emitted, and the shapes declined. */
  private case class Emitted(classes: Seq[(String, Array[Byte])], declined: Seq[Declined])

  // Each options value's corpus, emitted once and shared by the tests that read it.
  private val emitted = mutable.Map.empty[VarkaEmitOptions, Emitted]

  /**
   * Whether `d` is a size decline of the kind the emitter documents: some outputs' own groups are
   * over the method byte budget and nothing else is over a limit (no class-file cap, parameter
   * slot or constant pool count). `VarkaEmitDeclined` carries no list of findings, only the
   * message that joins them with "; " and closes with a clause naming the outputs that cannot be
   * regrouped smaller, so this reads the message by those parts.
   */
  private def isByteBudgetDecline(d: VarkaEmitDeclined): Boolean = {
    val findings = d.getMessage.split("; ").filterNot(_.contains("cannot be regrouped smaller"))
    !d.outputs().isEmpty && findings.nonEmpty &&
      findings.forall(f => f.contains(" bytes, over the method budget of "))
  }

  /**
   * The declines a corpus family may have under `options`, by family: without prefix sharing each
   * node decomposes its own date, the widest wide shapes' methods outgrow the byte budget and the
   * emitter declines them, as documented (VARKA-411); under any other options the whole corpus is
   * emitted.
   */
  private def tolerated(options: VarkaEmitOptions): Map[String, Int] =
    if (options.shareChronoPrefix()) Map.empty else Map("wide int" -> 1, "wide long" -> 1)

  /** Every corpus shape's class under `options`, labelled with the shape's family and index. */
  private def classes(options: VarkaEmitOptions): Seq[(String, Array[Byte])] = {
    val e = emitted.getOrElseUpdate(options, {
      val declined = mutable.Buffer.empty[Declined]
      val out = corpus.flatMap { s =>
        val label = s"${s.family} ${s.index}"
        try {
          Some(label -> VarkaLoopEmitter.emit(
            "org.apache.spark.sql.varka.execution.VarkaUnreadLocals", s.roots, s.numInputs,
            s.numLiterals, null, null, options))
        } catch {
          case d: VarkaEmitDeclined if isByteBudgetDecline(d) =>
            declined += Declined(s.family, label, d)
            None
        }
      }
      Emitted(out, declined.toSeq)
    })
    // Checked outside the cache, so that a failure is reported on every call without emitting the
    // corpus again.
    val allowed = tolerated(options)
    for ((family, ds) <- e.declined.groupBy(_.family)) {
      assert(ds.size <= allowed.getOrElse(family, 0),
        s"${ds.size} shapes of $family declined for the method byte budget under " +
          s"${options.canonical()}, ${allowed.getOrElse(family, 0)} allowed: " +
          ds.map(d => s"${d.label}: ${d.decline.getMessage}"))
    }
    e.classes
  }

  /** The census of every unread reference local over the corpus under `options`, by kind. */
  private def census(options: VarkaEmitOptions): Map[String, Int] =
    classes(options).flatMap { case (_, bytes) =>
      VarkaUnreadLocalsTrim.census(bytes).asScala.map(_.kind)
    }.groupBy(identity).view.mapValues(_.size).toMap

  test("a date decomposed in two fragments of a group that stores its prefix computes the " +
      "month for the loading group once") {
    // The masked body decomposes col:1 twice, under the CASE arm and beside it, since the two
    // fragments' words differ; only the first stores the prefix, so only it computes the month a
    // later group loads. Shrunk from a wide corpus shape, and emitted under the weights'
    // grouping, so that no change of the prices can move it off the case.
    val c = (k: Int) => new ColumnRef(k)
    val roots = Seq[VarkaVectorIR](
      new IfElse(new Compare(CompareOp.LT, c(0), c(2)), new DayOfYear(c(1)),
        new AddMonths(c(1), c(2))),
      new ThursdayOf(c(1)),
      new Quarter(c(1)))
    val name = "org.apache.spark.sql.varka.execution.VarkaUnreadLocals"
    // The case is a fact of the emit defaults on four options, which a matrix configuration may
    // change out from under it: without sharing, or with a group budget past the shape's size or
    // no method byte budget, no group splits off to load a prefix; and with the month step kept
    // (elideChronoMonth off) every fragment computes a month by the option's design, which the
    // census counts as unread. A configuration that changes one is cancelled here: the case with
    // that option put back would be the defaults' run again. The premise option
    // materializeChronoPrefix is the reference form's (recomputePrefix). The division does not
    // matter: the double dividers' unread remainders are no longer emitted (VARKA-412).
    val defaults = VarkaEmitOptions.DEFAULTS
    val base = VarkaMatrix.base
    val changed = Seq(
      "shareChronoPrefix" -> (base.shareChronoPrefix() != defaults.shareChronoPrefix()),
      "groupBudget" -> (base.groupBudget() != defaults.groupBudget()),
      "methodByteBudget" -> (base.methodByteBudget() != defaults.methodByteBudget()),
      "materializeChronoPrefix" ->
        (base.materializeChronoPrefix() != defaults.materializeChronoPrefix()),
      "elideChronoMonth" -> (base.elideChronoMonth() != defaults.elideChronoMonth()))
      .filter(_._2).map(_._1)
    assume(changed.isEmpty, s"the configuration changes ${changed.mkString(", ")}, which this " +
      "case's premise depends on")
    // The census reads the class emitted at the corpus's width. The prefix check loads a class,
    // which the JVM's one species per lane type allows only at the width it is established at
    // (VARKA-246): emitted again at that width, the matrix's lanes override where one is set,
    // else the preferred width.
    val options = elided.withPredictGrouping(false)
    val bytes = VarkaLoopEmitter.emit(name, roots.asJava, 3, 0, null, null, options)
    val loaded = VarkaLoopEmitter.emit(name, roots.asJava, 3, 0, null, null,
      options.withLanesOverride(VarkaSpeciesGuard.establishedIntLanes()))
    // The case needs a prefix materialized across groups, which a grouping change could remove.
    val (kernel, loader) = load((name, loaded))
    try {
      assert(kernel.scratchBytesPerRow() > 0, "no prefix is materialized")
    } finally {
      loader.release()
    }
    val unread = VarkaUnreadLocalsTrim.census(bytes).asScala
    assert(unread.isEmpty, unread.mkString("; "))
  }

  test("no divider stores the century's remainder for a carry it does not emit") {
    // The double dividers' quotients are exact, so the round-down carry is not emitted and the
    // remainder only the carry reads must not be computed either (VARKA-412). Two sites compute
    // such a remainder, and each is reached by its own shape and option, so that a fix at one
    // cannot hide the other: the Julian map of the calendar prefix (a Quarter reads the prefix
    // and nothing recomposes) and the recomposition of a shifted date (an AddMonths under the
    // century-then-year prefix, whose own century remainder is read afterwards). An AddMonths
    // under the Julian map reaches both.
    val c = (k: Int) => new ColumnRef(k)
    val shapes = Seq[(String, Seq[VarkaVectorIR], Boolean)](
      ("Julian map only", Seq(new Quarter(c(0))), true),
      ("recomposition only", Seq(new AddMonths(c(0), c(1))), false),
      ("both", Seq(new AddMonths(c(0), c(1))), true))
    // The census counts what the options in play build, which a matrix configuration may change
    // out from under the case: the month step kept (elideChronoMonth off), no shared prefix, or
    // a group budget or method byte budget that splits the shape differently. The case pins them
    // to the emit defaults and sets the Julian map and the division itself.
    val defaults = VarkaEmitOptions.DEFAULTS
    val pinned = elided.withShareChronoPrefix(defaults.shareChronoPrefix())
      .withGroupBudget(defaults.groupBudget())
      .withMethodByteBudget(defaults.methodByteBudget())
      .withElideChronoMonth(defaults.elideChronoMonth())
    for ((site, roots, julianMap) <- shapes; division <- VarkaEmitOptions.Division.values()) {
      val options = pinned.withJulianMap(julianMap).withDivision(division)
      val bytes = VarkaLoopEmitter.emit(
        "org.apache.spark.sql.varka.execution.VarkaUnreadLocals", roots.asJava, 2, 0, null, null,
        options)
      val unread = VarkaUnreadLocalsTrim.census(bytes).asScala
      assert(unread.isEmpty, s"$site, $division: " + unread.mkString("; "))
    }
  }

  test("every shared slot of every loop and epilogue method in the cost corpus is read") {
    // Both bodies of each shape, with and without the switch: the switch moves the slots the
    // segments and the mask took, and must leave the shared ones alone.
    for (options <- Seq(built, elided)) {
      val unread = classes(options).flatMap { case (label, bytes) =>
        VarkaUnreadLocals.unreadSharedSlots(bytes).asScala.map(f => s"$label: $f")
      }
      assert(unread.isEmpty, s"${unread.size} shared slots nothing reads under " +
        s"${options.canonical()}, the first: " + unread.take(5).mkString("; "))
    }
  }

  test("under elideUnreadLocals no loop or epilogue method in the cost corpus stores a " +
      "reference local it never reads", PinsDefaults) {
    val on = census(elided)
    assert(on.isEmpty, s"unread reference locals under the switch: $on")
    // Without CSE the body counts its uses for the value columns alone (Slots.plan).
    val noCse = census(elided.withCse(false))
    assert(noCse.isEmpty, s"unread reference locals under the switch without CSE: $noCse")
    // The form that builds everything keeps the four kinds VARKA-239 found (VARKA-239.md 2.1)
    // and no other, so a leftover of a new kind added to the reference form is seen too.
    val off = census(built)
    assert(off.keySet.subsetOf(Set("segment: output validity", "vector: prefix reload",
      "segment: input data", "mask: indexInRange")) && off.nonEmpty,
      s"the reference form's unread reference locals: $off")
  }

  test("under elideUnreadLocals a group that loads a materialized prefix loads what each " +
      "calendar tail reads, under every option its reads depend on", PinsDefaults) {
    // The arms of VarkaChronoLowering.prefixReads that depend on the options - the century
    // without the Julian map, the day of year without the Neri-Schneider month, trunc's
    // recomposing form - matter only where a later group loads a prefix, which the corpus may
    // not reach under those options. Here a year alone in the first group stores the prefix and
    // each tail, alone in the second, loads it. A vector the tail reads and its arm does not
    // name fails verification or answers wrongly; one named and never read is a load the census
    // finds.
    val col = new ColumnRef(0)
    val tails: Seq[(VarkaVectorIR, Int, Array[Int])] = Seq(
      (new Year(col), 1, Array.empty[Int]),
      (new DayOfYear(col), 1, Array.empty[Int]),
      (new WeekOfYear(new ThursdayOf(col)), 1, Array.empty[Int]),
      (new AddMonths(col, new LiteralSlot(0)), 1, Array(7)),
      (new Month(col), 1, Array.empty[Int]),
      (new Quarter(col), 1, Array.empty[Int]),
      (new DayOfMonth(col), 1, Array.empty[Int]),
      (new LastDay(col), 1, Array.empty[Int]),
      (new TruncDate(col, TruncLevel.YEAR), 1, Array.empty[Int]),
      (new TruncDate(col, TruncLevel.QUARTER), 1, Array.empty[Int]),
      (new TruncDate(col, TruncLevel.MONTH), 1, Array.empty[Int]),
      (new TruncDateDynamic(col, new ColumnRef(1)), 2, Array.empty[Int]))
    // The reference form, which builds every prefix vector, so that the second group loads them.
    val split = VarkaMatrix.base.withElideUnreadLocals(false)
      .withMaterializeChronoPrefix(true).withGroupBudget(1).withFusedCeiling(1)
    val arms = Seq("the defaults" -> split, "no Julian map" -> split.withJulianMap(false),
      "no Neri-Schneider month" -> split.withNeriSchneiderMonth(false),
      "recomposed trunc" -> split.withTruncDate(VarkaEmitOptions.TruncDateForm.RECOMPOSE))
    // The calendar's boundaries, then days across four centuries; the level column of the
    // dynamic trunc walks the levels a date truncates to (DateTimeUtils' week, month, quarter
    // and year).
    val levels = Array(6, 7, 8, 9)
    def data(c: Int, i: Int): Int =
      if (c == 1) levels(i % levels.length)
      else if (i < calendarBoundaryDays.length) calendarBoundaryDays(i)
      else i * 293 - 150000
    def loads(bytes: Array[Byte]): Int = {
      val vector = "jdk.incubator.vector.IntVector"
      VarkaEmitterTestSupport.invocationCount(bytes, "loopDense1", vector) -
        VarkaEmitterTestSupport.invocationCount(bytes, "loopDense1", vector,
          java.util.List.of("fromMemorySegment"))
    }
    for ((arm, options) <- arms; (tail, numInputs, lits) <- tails) {
      // The producer decomposes the tail's own date - a week of year's is the Thursday of its
      // week - and, as equal roots are one output, the year's own tail takes a month.
      val date = tail match {
        case w: WeekOfYear => w.days()
        case _ => col
      }
      val producer = if (tail.isInstanceOf[Year]) new Month(date) else new Year(date)
      val roots = Seq[VarkaVectorIR](producer, tail)
      val ctx = s"$tail under $arm"
      // The second group loads the prefix: all five vectors when everything is built.
      val all = emitMulti(roots, numInputs, lits.length, options)._2
      assert(loads(all) >= 5, s"$ctx: the second group loads no prefix")
      val bytes = emitMulti(roots, numInputs, lits.length, options.withElideUnreadLocals(true))._2
      val unread = VarkaUnreadLocalsTrim.census(bytes).asScala.map(d => s"${d.method} ${d.kind}")
      assert(unread.isEmpty, s"$ctx: unread reference locals ${unread.mkString(", ")}")
      checkMatrix(roots, numInputs, lits, Seq(1, 1031), combos(numInputs), data = data,
        ctx = ctx, options = options.withElideUnreadLocals(true))
    }
  }
}
