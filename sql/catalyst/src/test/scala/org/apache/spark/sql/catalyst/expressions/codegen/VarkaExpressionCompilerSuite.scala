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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Add, AddMonths, Alias, Attribute, AttributeReference, CaseWhen, Cast, Coalesce, DateAdd, DateAddYMInterval, DateDiff, DateFromUnixDate, DateSub, DayOfMonth, DayOfWeek, Divide, EqualNullSafe, EqualTo, EvalMode, Expression, ExtractANSIIntervalDays, GreaterThan, Greatest, If, In, InSet, IsNotNull, IsNull, LessThan, Literal, Month, NamedExpression, NextDay, Not, NumericEvalContext, Nvl, Nvl2, Or, Quarter, UnixDate, WeekDay, Year}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaChrono, VarkaVectorIR}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.{AddDays, AddMonths => IRAddMonths, ColumnRef, Compare, CompareOp, DateDiff => IRDateDiff, DayOfMonth => IRDayOfMonth, DayOfWeek => IRDayOfWeek, Greatest => IRGreatest, IfElse, IsNotNull => IRIsNotNull, LiteralSlot, Month => IRMonth, NextDay => IRNextDay, Not => IRNot, Or => IROr, Quarter => IRQuarter, SubDays, WeekDay => IRWeekDay, Year => IRYear}
import org.apache.spark.sql.types.{ByteType, DateType, DayTimeIntervalType, IntegerType, ShortType, StringType, TimestampType, YearMonthIntervalType}

/**
 * Unit tests for [[VarkaExpressionCompiler]] (milestone 2, task 10): the recursive
 * Catalyst-to-IR compiler that both `VarkaColumnarRule` (eligibility) and
 * `VarkaKernelEvaluator` (execution) call. End-to-end coverage lives in
 * `VarkaDifferentialSuite`; here the compiled shape itself is pinned - dense input mapping,
 * literal slots deduplicated by value (what makes the emitter's CSE able to see two
 * `date_add(d, 1)` as one computation), output Spark types, the per-entry classification of
 * task 12's partial eligibility, and the shapes that decline.
 */
class VarkaExpressionCompilerSuite extends SparkFunSuite {

  private val d = AttributeReference("d", DateType)()
  private val d2 = AttributeReference("d2", DateType)()
  private val i = AttributeReference("i", IntegerType)()
  private val sh = AttributeReference("sh", ShortType)()
  private val by = AttributeReference("by", ByteType)()
  private val childOutput: Seq[Attribute] = Seq(d, d2, i, sh, by)

  private def out(e: org.apache.spark.sql.catalyst.expressions.Expression): NamedExpression =
    Alias(e, "c")()

  test("a nested chain compiles recursively with literal slots in first-occurrence order") {
    val expr = DateSub(DateAdd(d, Literal(1)), Literal(2))
    val compiled = VarkaExpressionCompiler.compile(Seq(out(expr)), childOutput).get
    assert(compiled.outputs === Seq(
      new SubDays(new AddDays(new ColumnRef(0), new LiteralSlot(0)), new LiteralSlot(1))))
    assert(compiled.outputTypes === Seq(DateType))
    assert(compiled.inputOrdinals === Seq(0))
    assert(compiled.literals === Seq(1, 2))
  }

  test("literal slots are assigned per distinct value, so equal subtrees compile equal") {
    val a = DateAdd(d, Literal(1))
    val b = DateDiff(DateAdd(d, Literal(1)), d2)
    val compiled = VarkaExpressionCompiler.compile(Seq(out(a), out(b)), childOutput).get
    val sharedNode = new AddDays(new ColumnRef(0), new LiteralSlot(0))
    assert(compiled.outputs === Seq(
      sharedNode, new IRDateDiff(sharedNode, new ColumnRef(1))))
    assert(compiled.outputs.head === compiled.outputs(1).asInstanceOf[IRDateDiff].end(),
      "the two occurrences of date_add(d, 1) must compile to equal records or CSE cannot fire")
    assert(compiled.outputTypes === Seq(DateType, IntegerType))
    assert(compiled.literals === Seq(1))
  }

  test("input ordinals map densely in first-occurrence order") {
    // d2 (child ordinal 1) is referenced first, so it becomes kernel input 0.
    val expr = DateDiff(d2, DateAdd(d, Literal(3)))
    val compiled = VarkaExpressionCompiler.compile(Seq(out(expr)), childOutput).get
    assert(compiled.inputOrdinals === Seq(1, 0))
    assert(compiled.outputs === Seq(new IRDateDiff(
      new ColumnRef(0), new AddDays(new ColumnRef(1), new LiteralSlot(0)))))
  }

  test("CASE WHEN right-folds into nested IfElse; no ELSE declines") {
    val expr = CaseWhen(
      Seq(
        LessThan(d, d2) -> DateAdd(d, Literal(1)),
        EqualTo(d, d2) -> DateAdd(d, Literal(2))),
      Some(d2))
    // Ineligible without task 11's recursion; now the first branch wins first, SQL's rule.
    val compiled = VarkaExpressionCompiler.compile(Seq(out(expr)), childOutput).get
    val c0 = new ColumnRef(0)
    val c1 = new ColumnRef(1)
    assert(compiled.outputs === Seq(new IfElse(
      new Compare(CompareOp.LT, c0, c1),
      new AddDays(c0, new LiteralSlot(0)),
      new IfElse(
        new Compare(CompareOp.EQ, c0, c1),
        new AddDays(c0, new LiteralSlot(1)),
        c1))))
    assert(compiled.outputTypes === Seq(DateType))
    // No ELSE means a null-literal branch, which breaks the dense body's all-valid invariant.
    assert(VarkaExpressionCompiler.compile(
      Seq(out(CaseWhen(Seq(LessThan(d, d2) -> DateAdd(d, Literal(1))), None))),
      childOutput).isEmpty)
  }

  test("n-ary greatest left-folds; connectives, NOT and date literals compile") {
    val expr = If(
      Or(Not(GreaterThan(d, d2)), EqualTo(d, Literal(19000, DateType))),
      Greatest(Seq(d, d2, DateAdd(d, Literal(19000)))),
      d2)
    val compiled = VarkaExpressionCompiler.compile(Seq(out(expr)), childOutput).get
    val c0 = new ColumnRef(0)
    val c1 = new ColumnRef(1)
    // The date literal and the equal-valued day offset share one slot, by value.
    assert(compiled.literals === Seq(19000))
    assert(compiled.outputs === Seq(new IfElse(
      new IROr(
        new IRNot(new Compare(CompareOp.GT, c0, c1)),
        new Compare(CompareOp.EQ, c0, new LiteralSlot(0))),
      new IRGreatest(new IRGreatest(c0, c1), new AddDays(c0, new LiteralSlot(0))),
      c1)))
  }

  test("dayofweek and weekday compile with IntegerType outputs") {
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(DayOfWeek(d)), out(WeekDay(DateAdd(d, Literal(3))))), childOutput).get
    assert(compiled.outputs === Seq(
      new IRDayOfWeek(new ColumnRef(0)),
      new IRWeekDay(new AddDays(new ColumnRef(0), new LiteralSlot(0)))))
    assert(compiled.outputTypes === Seq(IntegerType, IntegerType))
  }

  test("task 33: next_day with a literal weekday compiles; a column weekday declines") {
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(NextDay(d, Literal("MO"), false))), childOutput).get
    assert(compiled.outputs === Seq(new IRNextDay(new ColumnRef(0), new LiteralSlot(0))))
    assert(compiled.outputTypes === Seq(DateType))
    // MONDAY = 4 in DateTimeUtils's private weekday numbering, so k = dayOfWeek - 1 = 3.
    assert(compiled.literals === Seq(3))
    val dow = AttributeReference("dow", StringType)()
    assert(VarkaExpressionCompiler.compile(
      Seq(out(NextDay(d, dow, false))), childOutput :+ dow).isEmpty)
  }

  test("task 33: next_day's weekday range is [-1, 5], not [0, 6] - THURSDAY is the negative") {
    // DateTimeUtils.getDayOfWeekFromString returns [0, 6] with THURSDAY = 0, so
    // k = dayOfWeek - 1 = -1 for THURSDAY: the one weekday a naive [0, 6] assumption misses.
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(NextDay(d, Literal("THURSDAY"), false))), childOutput).get
    assert(compiled.outputs === Seq(new IRNextDay(new ColumnRef(0), new LiteralSlot(0))))
    assert(compiled.literals === Seq(-1))
  }

  test("task 33: next_day declines cleanly on a null weekday, without crashing planning") {
    assert(VarkaExpressionCompiler.compile(
      Seq(out(NextDay(d, Literal.create(null, StringType), false))), childOutput).isEmpty)
  }

  test("task 33: next_day declines cleanly on an unrecognized weekday name") {
    assert(VarkaExpressionCompiler.compile(
      Seq(out(NextDay(d, Literal("ZZ"), false))), childOutput).isEmpty)
  }

  test("task 33: next_day declines, rather than crashes planning, when the weekday " +
      "expression itself throws on eval") {
    // A computed (not bare-Literal) foldable expression whose eval() throws for a reason
    // that has nothing to do with the weekday name - forcing ANSI's divide-by-zero error
    // via an explicit NumericEvalContext so this does not depend on session configuration.
    val throwsOnEval = Divide(
      Literal(1.0), Literal(0.0), NumericEvalContext(EvalMode.ANSI))
    assert(throwsOnEval.foldable)
    assert(VarkaExpressionCompiler.compile(
      Seq(out(NextDay(d, throwsOnEval, false))), childOutput).isEmpty)
  }

  test("task 26: the four calendar extractions compile with IntegerType outputs") {
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(Year(d)), out(Month(d)), out(DayOfMonth(d)), out(Quarter(DateAdd(d, Literal(3))))),
      childOutput).get
    assert(compiled.outputs === Seq(
      new IRYear(new ColumnRef(0)),
      new IRMonth(new ColumnRef(0)),
      new IRDayOfMonth(new ColumnRef(0)),
      new IRQuarter(new AddDays(new ColumnRef(0), new LiteralSlot(0)))))
    assert(compiled.outputTypes === Seq(IntegerType, IntegerType, IntegerType, IntegerType))
  }

  test("task 40: add_months and date +- INTERVAL n MONTH/YEAR compile to the same node") {
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(AddMonths(d, Literal(3))),
        out(DateAddYMInterval(d, Literal.create(-5, YearMonthIntervalType())))),
      childOutput).get
    assert(compiled.outputs === Seq(
      new IRAddMonths(new ColumnRef(0), new LiteralSlot(0)),
      new IRAddMonths(new ColumnRef(0), new LiteralSlot(1))))
    assert(compiled.literals === Seq(3, -5))
    assert(compiled.outputTypes === Seq(DateType, DateType))
  }

  test("task 40 declines: a non-foldable month count, and a literal past the magic's range") {
    val n = AttributeReference("n", IntegerType)()
    assert(VarkaExpressionCompiler.compile(
      Seq(out(AddMonths(d, n))), childOutput :+ n).isEmpty)
    assert(VarkaExpressionCompiler.compile(
      Seq(out(AddMonths(d, Literal(VarkaChrono.MONTH_ARITH_MAX_MONTHS + 1)))),
      childOutput).isEmpty)
    assert(VarkaExpressionCompiler.compile(
      Seq(out(AddMonths(d, Literal(VarkaChrono.MONTH_ARITH_MIN_MONTHS - 1)))),
      childOutput).isEmpty)
    // The bound itself still compiles - it is the largest literal covered, not the smallest
    // one declined.
    assert(VarkaExpressionCompiler.compile(
      Seq(out(AddMonths(d, Literal(VarkaChrono.MONTH_ARITH_MAX_MONTHS)))), childOutput).isDefined)
  }

  test("task 26 declines: year over a timestamp, which the analyzer casts") {
    // GetDateField's input type is DateType, so year(timestamp) arrives as a Cast the compiler
    // does not unwrap - only the identity DateType-to-DateType cast is transparent. It declines
    // at the cast rather than at the extraction, exactly as dayofweek(timestamp) does today.
    val ts = AttributeReference("t", TimestampType)()
    val bound = Seq(out(Year(Cast(ts, DateType))))
    assert(VarkaExpressionCompiler.compile(bound, Seq(ts)).isEmpty)
  }

  test("task 41: unix_date/date_from_unix_date relabel rather than compiling to a node") {
    // unix_date's child is a date column, readable today: the relabel vanishes and the IR is
    // a bare ColumnRef, with the output type coming from the Catalyst expression (IntegerType)
    // rather than from anything the IR rendered.
    val unixDate = VarkaExpressionCompiler.compile(Seq(out(UnixDate(d))), childOutput).get
    assert(unixDate.outputs === Seq(new ColumnRef(0)))
    assert(unixDate.outputTypes === Seq(IntegerType))
    // date_from_unix_date's child is an integer column, which no general leaf can read, so
    // this declines through the ordinary non-date-column path exactly as any other read of
    // `i` would. Task 38 has since landed and does not change that: it opens IntegerType
    // columns through compileOffset only - deliberately not through compileNode, per that
    // method's own javadoc - so the offset of a date_add is readable and this is not.
    assert(VarkaExpressionCompiler.compile(
      Seq(out(DateFromUnixDate(i))), childOutput).isEmpty)
    // The actual argument for the task: a relabelled entry must not demote the rest of the
    // projection to the row path. Before this task UnixDate itself declined, taking `a` with it.
    val mixed = VarkaExpressionCompiler.compile(
      Seq(out(DateAdd(d, Literal(1))), out(UnixDate(d))), childOutput).get
    assert(mixed.outputs === Seq(new AddDays(new ColumnRef(0), new LiteralSlot(0)),
      new ColumnRef(0)))
    assert(mixed.outputTypes === Seq(DateType, IntegerType))
    // A relabel compiles to a bare ColumnRef, the same IR shape a bare column produces -
    // compileCoalesce and compileValidity both use that shape as their proxy for "this
    // operand is a bare column" (their own doc comments now say so), and a relabel is safe
    // to guard exactly because it is a null-intolerant identity like the column it wraps.
    val c0 = new ColumnRef(0)
    val c1 = new ColumnRef(1)
    val guarded = VarkaExpressionCompiler.compile(
      Seq(out(If(IsNotNull(UnixDate(d)), UnixDate(d), UnixDate(d2)))), childOutput).get
    assert(guarded.outputs === Seq(new IfElse(new IRIsNotNull(c0), c0, c1)))
    assert(guarded.outputTypes === Seq(IntegerType))
    val coalesced = VarkaExpressionCompiler.compile(
      Seq(out(Coalesce(Seq(UnixDate(d), UnixDate(d2))))), childOutput).get
    assert(coalesced.outputs === Seq(new IfElse(new IRIsNotNull(c0), c0, c1)))
    assert(coalesced.outputTypes === Seq(IntegerType))
  }

  test("task 38: date_add/date_sub with an IntegerType column offset compile to a two-column " +
      "AddDays/SubDays, and a foldable offset still compiles to a LiteralSlot") {
    val addCompiled = VarkaExpressionCompiler.compile(Seq(out(DateAdd(d, i))), childOutput).get
    assert(addCompiled.outputs === Seq(new AddDays(new ColumnRef(0), new ColumnRef(1))))
    assert(addCompiled.inputOrdinals === Seq(0, 2))
    assert(addCompiled.outputTypes === Seq(DateType))
    val subCompiled = VarkaExpressionCompiler.compile(Seq(out(DateSub(d, i))), childOutput).get
    assert(subCompiled.outputs === Seq(new SubDays(new ColumnRef(0), new ColumnRef(1))))
    // A foldable offset keeps today's LiteralSlot shape - existing plans and their cached
    // kernels are untouched by the fallback path this task adds.
    val literalCompiled =
      VarkaExpressionCompiler.compile(Seq(out(DateAdd(d, Literal(3)))), childOutput).get
    assert(literalCompiled.outputs === Seq(new AddDays(new ColumnRef(0), new LiteralSlot(0))))
  }

  test("task 38 declines: a ShortType or ByteType offset column, and an interval column") {
    // DateAdd.inputTypes accepts ShortType/ByteType with no cast, so a short or byte column
    // arrives as a bare BoundReference the leaf arm must not accept - its Arrow vector is 2 or
    // 1 bytes wide, which an int32 lane load would read as garbage rather than decline.
    assert(VarkaExpressionCompiler.compile(Seq(out(DateAdd(d, sh))), childOutput).isEmpty)
    assert(VarkaExpressionCompiler.compile(Seq(out(DateAdd(d, by))), childOutput).isEmpty)
    // `d + <non-foldable INTERVAL DAY column>` resolves to
    // DateAdd(d, ExtractANSIIntervalDays(intervalCol)) (BinaryArithmeticWithDatetimeResolver);
    // ExtractANSIIntervalDays has no compiler arm, so this declines through the ordinary
    // unsupported-expression path rather than needing its own guard.
    val iv = AttributeReference("iv", DayTimeIntervalType(DayTimeIntervalType.DAY))()
    val withInterval = Seq(out(DateAdd(d, ExtractANSIIntervalDays(iv))))
    assert(VarkaExpressionCompiler.compile(withInterval, childOutput :+ iv).isEmpty)
  }

  test("task 38: with two independently unfusable operands, the child's reason is reported") {
    // date_add compiles its date child before its offset (VarkaExpressionCompiler's own
    // reading-order rule, the same one CaseWhen documents), so when BOTH operands are
    // unfusable, DeclineSink's "first note wins" rule surfaces the child's reason here, not
    // the offset's - pinning that as intentional rather than an accident of evaluation order.
    val s = AttributeReference("s", StringType)()
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(out(DateAdd(Cast(s, DateType), sh)), out(DateAdd(d, Literal(1)))),
      s +: childOutput).get
    assert(partial.declines(0).reason === "unsupported expression")
  }

  test("task 11 declines: null-safe equality, bare boolean outputs") {
    // <=> on two nulls is true, which breaks the null-intolerant comparison rule.
    assert(VarkaExpressionCompiler.compile(
      Seq(out(If(EqualNullSafe(d, d2), d2, DateAdd(d, Literal(1))))), childOutput).isEmpty)
    // A comparison as a projection output is a boolean column - out of scope, interior only.
    assert(VarkaExpressionCompiler.compile(
      Seq(out(LessThan(d, d2))), childOutput).isEmpty)
  }

  test("task 20: IN dedups and sorts date literals into a balanced OR of EQ") {
    val expr = If(
      In(d, Seq(Literal(20, DateType), Literal(5, DateType), Literal(20, DateType),
        Literal(11, DateType))),
      d, d2)
    val compiled = VarkaExpressionCompiler.compile(Seq(out(expr)), childOutput).get
    val c0 = new ColumnRef(0)
    def eq(slot: Int): Compare = new Compare(CompareOp.EQ, c0, new LiteralSlot(slot))
    // Slots in sorted-day order (5, 11, 20), the duplicate collapsed; the fold is balanced
    // pairwise, so three leaves become Or(Or(e0, e1), e2) - the shape the cap arithmetic
    // and the shape hash both depend on.
    assert(compiled.literals === Seq(5, 11, 20))
    assert(compiled.outputs === Seq(new IfElse(
      new IROr(new IROr(eq(0), eq(1)), eq(2)), c0, new ColumnRef(1))))
    // InSet hands the same values over as an unordered set and must compile identically.
    val viaInSet = VarkaExpressionCompiler.compile(
      Seq(out(If(InSet(d, Set[Any](20, 5, 11)), d, d2))), childOutput).get
    assert(viaInSet.outputs === compiled.outputs)
    assert(viaInSet.literals === compiled.literals)
    // And at the cap size - the shape that actually arrives as InSet past the optimizer's
    // threshold of 10 - the full sorted slot sequence is pinned: sixteen elements handed
    // over in descending order must register ascending, or the shape hash drifts run to run.
    val days16 = (1 to 16).map(_ * 7)
    val atCap = VarkaExpressionCompiler.compile(
      Seq(out(If(InSet(d, Set[Any](days16.reverse: _*)), d, d2))), childOutput).get
    assert(atCap.literals === days16)
  }

  test("task 20: the IN cap - 16 literals fuse, 17 decline with the recorded reason") {
    def inIf(n: Int): NamedExpression =
      out(If(In(d, (1 to n).map(k => Literal(k * 3, DateType))), d, d2))
    assert(VarkaExpressionCompiler.compile(Seq(inIf(16)), childOutput).isDefined)
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(inIf(17), out(DateAdd(d, Literal(1)))), childOutput).get
    assert(partial.specs === Seq(ResidualOutput, FusedOutput(0)))
    assert(partial.declines(0).reason === "IN list longer than the fused cap of 16")
    // A null element can never match by SQL's IN semantics but makes the no-match result
    // unknown; it stays declined rather than modeled.
    val withNull = out(If(In(d, Seq(Literal(1, DateType), Literal(null, DateType))), d, d2))
    val p2 = VarkaExpressionCompiler.compilePartial(
      Seq(withNull, out(DateAdd(d, Literal(1)))), childOutput).get
    assert(p2.declines(0).reason === "IN list has a null or non-literal date element")
  }

  test("task 20: coalesce lowers onto the validity condition; guarded operands are columns") {
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(Coalesce(Seq(d, d2, Literal(7, DateType))))), childOutput).get
    val c0 = new ColumnRef(0)
    val c1 = new ColumnRef(1)
    assert(compiled.outputs === Seq(new IfElse(new IRIsNotNull(c0), c0,
      new IfElse(new IRIsNotNull(c1), c1, new LiteralSlot(0)))))
    // A computed operand before the last cannot be guarded - its validity word is not live
    // before value emission - and declines with its own reason.
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(out(Coalesce(Seq(DateAdd(d, Literal(1)), d2))), out(DateAdd(d, Literal(1)))),
      childOutput).get
    assert(partial.declines(0).reason ===
      "coalesce operand before the last is not a bare date column")
  }

  test("task 20: IS [NOT] NULL compile; nvl and nvl2 arrive through their replacements") {
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(If(IsNotNull(d), d, d2)), out(If(IsNull(d), d2, d))), childOutput).get
    val c0 = new ColumnRef(0)
    val c1 = new ColumnRef(1)
    assert(compiled.outputs === Seq(
      new IfElse(new IRIsNotNull(c0), c0, c1),
      new IfElse(new IRNot(new IRIsNotNull(c0)), c1, c0)))
    // Hand-built RuntimeReplaceables compile through their replacement - the same trees a
    // real query hands over after the optimizer's ReplaceExpressions.
    val viaNvl = VarkaExpressionCompiler.compile(Seq(out(new Nvl(d, d2))), childOutput).get
    assert(viaNvl.outputs === Seq(new IfElse(new IRIsNotNull(c0), c0, c1)))
    val viaNvl2 = VarkaExpressionCompiler.compile(
      Seq(out(new Nvl2(d, d2, DateAdd(d2, Literal(1))))), childOutput).get
    assert(viaNvl2.outputs === Seq(new IfElse(new IRIsNotNull(c0), c1,
      new AddDays(c1, new LiteralSlot(0)))))
    // A validity predicate over a computed operand declines: the emitter reads the child's
    // per-input validity word, which only a column has before value emission.
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(out(If(IsNotNull(DateAdd(d, Literal(1))), d, d2)), out(DateAdd(d, Literal(1)))),
      childOutput).get
    assert(partial.declines(0).reason === "validity predicate over a non-column operand")
  }

  test("task 20: the identity date cast unwraps; a string-column cast still declines") {
    val compiled = VarkaExpressionCompiler.compile(
      Seq(out(Cast(DateAdd(d, Literal(3)), DateType))), childOutput).get
    assert(compiled.outputs === Seq(new AddDays(new ColumnRef(0), new LiteralSlot(0))))
    // A string column cast is a per-row parse with no string lane.
    val s = AttributeReference("s", StringType)()
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(out(Cast(s, DateType)), out(DateAdd(d, Literal(1)))), s +: childOutput).get
    assert(partial.declines(0).reason === "unsupported expression")
  }

  test("task 20: the compiler mirrors the emitter budgets and demotes the overflow entry") {
    def inIf(base: Int): NamedExpression =
      out(If(In(d, (1 to 16).map(k => Literal(base + k, DateType))), d, d2))
    // Two 16-literal INs are exactly 64 distinct ops (2 x (16 EQ + 15 OR + 1 IfElse)); a
    // third entry's single op would be the 65th. Before task 20 this shape reached the
    // emitter and lost the whole kernel to a silent per-batch fallback; now the overflow
    // entry demotes to residual with a recorded reason.
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(inIf(0), inIf(1000), out(DateAdd(d, Literal(9999)))), childOutput).get
    assert(partial.specs === Seq(FusedOutput(0), FusedOutput(1), ResidualOutput))
    assert(partial.declines(2).reason === "exceeds the emitter's fused budget")
    // The depth budget is mirrored the same way: a 17-deep chain compiled fine before task
    // 20 and then failed at emission.
    val deep = out((0 until 17).foldLeft[Expression](d)((e, k) => DateAdd(e, Literal(k + 1))))
    val deepPartial = VarkaExpressionCompiler.compilePartial(
      Seq(deep, out(DateAdd(d, Literal(1)))), childOutput).get
    assert(deepPartial.specs === Seq(ResidualOutput, FusedOutput(0)))
    assert(deepPartial.declines(0).reason === "exceeds the emitter's fused budget")
    // The input-column budget is mirrored too: 33 shallow datediff entries over 66 distinct
    // columns are only 33 ops at height 1, but 66 kernel inputs - the 33rd entry (the one
    // that pushes past 64 columns) demotes instead of blowing up at emission.
    val wide = (0 until 66).map(k => AttributeReference(s"w$k", DateType)())
    val wideEntries = (0 until 33).map { k =>
      out(DateDiff(wide(2 * k), wide(2 * k + 1)))
    }
    val widePartial = VarkaExpressionCompiler.compilePartial(wideEntries, wide).get
    assert(widePartial.specs.count(_ == ResidualOutput) === 1)
    assert(widePartial.specs.last === ResidualOutput)
    assert(widePartial.declines(32).reason === "exceeds the emitter's fused budget")
  }

  test("compile is the all-entries-fused special case of compilePartial") {
    // A bare column output is never fused - it forwards - so `compile` declines the projection.
    assert(VarkaExpressionCompiler.compile(Seq(d.asInstanceOf[NamedExpression]),
      childOutput).isEmpty)
    // A forwarded entry beside a fused one: eligible partially, but not for `compile`.
    assert(VarkaExpressionCompiler.compile(
      Seq(out(DateAdd(d, Literal(1))), out(i)), childOutput).isEmpty)
    assert(VarkaExpressionCompiler.compilePartial(
      Seq(out(DateAdd(d, Literal(1))), out(i)), childOutput).isDefined)
    // An IntegerType column offset now compiles (task 38); a ShortType one still declines,
    // so `compile` still declines the whole projection over it.
    assert(VarkaExpressionCompiler.compile(
      Seq(out(DateAdd(d, i))), childOutput).isDefined)
    assert(VarkaExpressionCompiler.compile(
      Seq(out(DateAdd(d, sh))), childOutput).isEmpty)
    // A cast in the tree (how `date_add` over a `datediff` result reaches the planner).
    assert(VarkaExpressionCompiler.compile(
      Seq(out(DateAdd(Cast(DateDiff(d, d2), DateType), Literal(1)))), childOutput).isEmpty)
    // An empty projection.
    assert(VarkaExpressionCompiler.compile(Seq.empty, childOutput).isEmpty)
    assert(VarkaExpressionCompiler.compilePartial(Seq.empty, childOutput).isEmpty)
  }

  test("compilePartial classifies fused, forwarded and residual entries in projection order") {
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(
        out(DateAdd(d, Literal(3))),
        i.asInstanceOf[NamedExpression],
        out(Add(i, Literal(1))),
        out(DateSub(d2, Literal(2)))),
      childOutput).get
    // The int column forwards - forwarding does not care about lane types - and the fused
    // indices count fused entries only.
    assert(partial.specs ===
      Seq(FusedOutput(0), ForwardedOutput(2), ResidualOutput, FusedOutput(1)))
    // The fused sub-projection covers exactly the fused entries: their trees, types, columns
    // and literals - nothing of the residual entry leaks in.
    assert(partial.fused.outputs === Seq(
      new AddDays(new ColumnRef(0), new LiteralSlot(0)),
      new SubDays(new ColumnRef(1), new LiteralSlot(1))))
    assert(partial.fused.outputTypes === Seq(DateType, DateType))
    assert(partial.fused.inputOrdinals === Seq(0, 1))
    assert(partial.fused.literals === Seq(3, 2))
  }

  test("a bare date column forwards like any other bare column") {
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(out(DateAdd(d, Literal(1))), d.asInstanceOf[NamedExpression]), childOutput).get
    assert(partial.specs === Seq(FusedOutput(0), ForwardedOutput(0)))
  }

  test("forwards and residuals alone are not eligible: nothing to fuse gains nothing") {
    assert(VarkaExpressionCompiler.compilePartial(
      Seq(out(Add(i, Literal(1)))), childOutput).isEmpty)
    assert(VarkaExpressionCompiler.compilePartial(
      Seq(d.asInstanceOf[NamedExpression], i.asInstanceOf[NamedExpression]),
      childOutput).isEmpty)
    assert(VarkaExpressionCompiler.compilePartial(
      Seq(d.asInstanceOf[NamedExpression], out(Add(i, Literal(1)))), childOutput).isEmpty)
  }

  test("a declining entry rolls the shared tables back to their pre-entry state") {
    // The datediff entry compiles its end child - registering d2 and the literal 9 - before its
    // start child (an int column) declines the whole entry. Without the rollback, d2 and 9
    // would stay in the tables and widen the fused kernel's input set for no output.
    val partial = VarkaExpressionCompiler.compilePartial(
      Seq(
        out(DateDiff(DateAdd(d2, Literal(9)), i)),
        out(DateAdd(d, Literal(1)))),
      childOutput).get
    assert(partial.specs === Seq(ResidualOutput, FusedOutput(0)))
    assert(partial.fused.inputOrdinals === Seq(0),
      "the declined entry's column registration must be rolled back")
    assert(partial.fused.literals === Seq(1),
      "the declined entry's literal registration must be rolled back")
    assert(partial.fused.outputs === Seq(new AddDays(new ColumnRef(0), new LiteralSlot(0))))
  }

  test("task 21: a fully fusible predicate compiles to one condition root") {
    // The survey's BETWEEN shape, post-optimizer: paired comparisons on the AND spine.
    val condition = org.apache.spark.sql.catalyst.expressions.And(
      GreaterThan(d, Literal(10, DateType)), LessThan(d, Literal(20, DateType)))
    val predicate = VarkaExpressionCompiler.compilePredicate(condition, childOutput).get
    assert(predicate.specs.forall(_.fused))
    assert(predicate.residualConjuncts.isEmpty)
    assert(predicate.fused.outputs === Seq(new VarkaVectorIR.And(
      new Compare(CompareOp.GT, new ColumnRef(0), new LiteralSlot(0)),
      new Compare(CompareOp.LT, new ColumnRef(0), new LiteralSlot(1)))))
    assert(predicate.fused.outputTypes === Seq(org.apache.spark.sql.types.BooleanType))
    assert(predicate.fused.inputOrdinals === Seq(0))
    assert(predicate.fused.literals === Seq(10, 20))
  }

  test("task 21: a mixed predicate splits - fusible conjuncts in, the rest residual") {
    // The corpus norm: a date predicate AND a non-date one AND a validity guard. The int
    // comparison declines (no int lanes at a comparison), the date ones fuse, and the
    // residual keeps its reason for the report.
    val condition = org.apache.spark.sql.catalyst.expressions.And(
      org.apache.spark.sql.catalyst.expressions.And(
        LessThan(d, d2), GreaterThan(i, Literal(5))),
      IsNotNull(d))
    val predicate = VarkaExpressionCompiler.compilePredicate(condition, childOutput).get
    assert(predicate.specs.map(_.fused) === Seq(true, false, true))
    assert(predicate.fusedConjuncts === Seq(LessThan(d, d2), IsNotNull(d)))
    assert(predicate.residualConjuncts === Seq(GreaterThan(i, Literal(5))))
    val decline = predicate.specs(1).decline.get
    assert(decline.reason === "non-date column of type int")
    // The fused root is the balanced AND of the two fused conjuncts, in query order.
    assert(predicate.fused.outputs === Seq(new VarkaVectorIR.And(
      new Compare(CompareOp.LT, new ColumnRef(0), new ColumnRef(1)),
      new IRIsNotNull(new ColumnRef(0)))))
  }

  test("task 21: a declining conjunct rolls the shared tables back") {
    // The first conjunct registers d2 and the literal 9 before its int operand declines it;
    // the second fuses. The kernel must read only what the fused conjunct references.
    val condition = org.apache.spark.sql.catalyst.expressions.And(
      LessThan(DateDiff(DateAdd(d2, Literal(9)), i), Literal(3)),
      GreaterThan(d, Literal(11, DateType)))
    val predicate = VarkaExpressionCompiler.compilePredicate(condition, childOutput).get
    assert(predicate.specs.map(_.fused) === Seq(false, true))
    assert(predicate.fused.inputOrdinals === Seq(0),
      "the declined conjunct's column registration must be rolled back")
    assert(predicate.fused.literals === Seq(11),
      "the declined conjunct's literal registration must be rolled back")
  }

  test("task 21: predicates with nothing to fuse, or no columns, are not eligible") {
    // No conjunct compiles.
    assert(VarkaExpressionCompiler.compilePredicate(
      GreaterThan(i, Literal(5)), childOutput).isEmpty)
    // A conjunct compiles but references no column: nothing to vectorize over.
    assert(VarkaExpressionCompiler.compilePredicate(
      LessThan(Literal(1, DateType), Literal(2, DateType)), childOutput).isEmpty)
  }

  test("task 21: the balanced AND fold keeps many conjuncts inside the depth budget") {
    // 20 distinct comparisons: a left fold would be 21 deep and trip MAX_CHAIN_DEPTH = 16;
    // the balanced fold is ceil(log2 20) + 2 deep and every conjunct fuses.
    val condition = (1 to 20)
      .map(k => GreaterThan(d, Literal(k, DateType)): Expression)
      .reduceLeft(org.apache.spark.sql.catalyst.expressions.And(_, _))
    val predicate = VarkaExpressionCompiler.compilePredicate(condition, childOutput).get
    assert(predicate.specs.size === 20)
    assert(predicate.specs.forall(_.fused))
  }

  test("task 21 review: a nondeterministic conjunct declines the whole predicate") {
    // The split hoists fused conjuncts below residual ones, reordering evaluation; a seeded
    // rand must see every row (Spark's own pushdown stops at the first nondeterministic
    // conjunct), so one nondeterministic conjunct declines the whole predicate.
    val condition = org.apache.spark.sql.catalyst.expressions.And(
      LessThan(d, Literal(10, DateType)),
      LessThan(org.apache.spark.sql.catalyst.expressions.Rand(Literal(42L)), Literal(0.5)))
    assert(VarkaExpressionCompiler.compilePredicate(condition, childOutput).isEmpty)
  }

  test("task 21: the budget mirror demotes conjuncts past MAX_FUSED_NODES to residual") {
    // Each conjunct is one Compare op and the fold adds one And per accepted conjunct, so k
    // accepted conjuncts cost 2k - 1 distinct ops: 32 fit the 64-op budget, the 33rd would
    // make 65. The overflow conjuncts demote with the recorded budget reason.
    val condition = (1 to 40)
      .map(k => GreaterThan(d, Literal(k, DateType)): Expression)
      .reduceLeft(org.apache.spark.sql.catalyst.expressions.And(_, _))
    val predicate = VarkaExpressionCompiler.compilePredicate(condition, childOutput).get
    assert(predicate.fusedConjuncts.size === 32)
    assert(predicate.residualConjuncts.size === 8)
    val decline = predicate.specs.reverse.head.decline.get
    assert(decline.reason === "exceeds the emitter's fused budget")
  }
}
