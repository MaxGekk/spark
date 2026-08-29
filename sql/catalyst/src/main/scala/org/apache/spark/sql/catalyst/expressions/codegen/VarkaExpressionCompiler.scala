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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, BindReferences, BoundReference, CaseWhen, Cast, Coalesce, DateAdd, DateDiff, DateSub, DateVarkaSupport, DayOfWeek, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, Greatest, If, In, InSet, IsNotNull, IsNull, Least, LessThan, LessThanOrEqual, Literal, NamedExpression, Not, Or, RuntimeReplaceable, WeekDay}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.{VarkaLoopEmitter, VarkaVectorIR}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.{AddDays, ColumnRef, CompareOp, DateDiff => IRDateDiff, LiteralSlot, SubDays}
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.{And => IRAnd, Compare, Cond, DayOfWeek => IRDayOfWeek, Greatest => IRGreatest, IfElse, IsNotNull => IRIsNotNull, Least => IRLeast, Not => IRNot, Or => IROr, WeekDay => IRWeekDay}
import org.apache.spark.sql.types.{BooleanType, DataType, DateType}

/**
 * A whole projection compiled to the Varka vector IR (milestone 2, task 10): the trees
 * `VarkaLoopEmitter` turns into one fused loop, plus everything the evaluator needs to drive the
 * emitted class - which child columns it reads (dense kernel input index = position in
 * `inputOrdinals`), the runtime `scalarArgs` values (slot index = position in `literals`), and
 * each output's Spark type, which is what tells a `datediff` day-count column (`IntegerType`)
 * apart from a date column when the output vectors are allocated.
 */
private[sql] case class CompiledVarkaProjection(
    outputs: Seq[VarkaVectorIR],
    outputTypes: Seq[DataType],
    inputOrdinals: Seq[Int],
    literals: Seq[Int])

/**
 * How one projection entry is served under partial eligibility (task 12): computed by the fused
 * kernel, forwarded as the input's own vector, or evaluated per row by the residual projection.
 */
private[sql] sealed trait VarkaOutputSpec

/** A kernel column: output `fusedIndex` of the fused sub-projection. */
private[sql] case class FusedOutput(fusedIndex: Int) extends VarkaOutputSpec

/**
 * A bare column reference, forwarded zero-copy from child output ordinal `childOrdinal`. Any
 * type, not just dates: forwarding never reads the values, so it does not care about lanes.
 */
private[sql] case class ForwardedOutput(childOrdinal: Int) extends VarkaOutputSpec

/** Everything else: evaluated per row, one pass for all residual entries together. */
private[sql] case object ResidualOutput extends VarkaOutputSpec

/**
 * Why one entry could not be fused (task 16): the answer to "why didn't my projection fuse?",
 * which the compiler's per-entry `None` used to swallow. `reason` is the vocabulary term - the
 * same string the exec nodes' verbose `EXPLAIN` and debug logs print - and `expr` names the
 * offending expression, the innermost one that actually failed rather than the whole entry.
 */
private[sql] case class VarkaDecline(reason: String, expr: String) {
  override def toString: String = s"$reason: $expr"
}

/**
 * Collects the decline of one entry. The recursion reports at the point of failure and the
 * first note wins, so the recorded reason is the innermost cause rather than the outermost
 * expression that inherited it; [[take]] hands it over and resets for the next entry.
 *
 * The recursion works on bound expressions, whose `BoundReference`s render as
 * `input[1, int, true]`; the child's attributes go back in before the text is kept, so a
 * reason reads in the query's own column names.
 */
private final class DeclineSink(childOutput: Seq[Attribute]) {
  private var first: Option[VarkaDecline] = None

  def note(reason: String, expr: Expression): Unit = {
    if (first.isEmpty) {
      val named = expr.transformUp {
        case br: BoundReference if br.ordinal >= 0 && br.ordinal < childOutput.length =>
          childOutput(br.ordinal)
      }
      val text = named.sql
      val shown = if (text.length > 80) text.take(77) + "..." else text
      first = Some(VarkaDecline(reason, shown))
    }
  }

  def take(): Option[VarkaDecline] = {
    val taken = first
    first = None
    taken
  }
}

/**
 * A projection classified entry by entry (task 12): `specs` has one entry per projectList
 * position, in order, and `fused` is the sub-projection of just the [[FusedOutput]] entries -
 * their kernel-input and literal tables cover only what the fused trees reference, so a
 * residual entry constrains neither the emitted loop nor `canRun`'s Arrow check.
 *
 * `declines` (task 16) maps the position of each [[ResidualOutput]] entry to why it declined,
 * for the exec nodes' verbose `EXPLAIN`; it is diagnostics only and no execution path reads it.
 */
private[sql] case class PartialVarkaProjection(
    specs: Seq[VarkaOutputSpec],
    fused: CompiledVarkaProjection,
    declines: Map[Int, VarkaDecline] = Map.empty)

/**
 * One conjunct of a filter predicate under the task-21 split: the original (unbound)
 * expression, whether it joined the mask kernel, and - for a residual conjunct - why not.
 * The predicate counterpart of [[VarkaOutputSpec]] plus its decline entry.
 */
private[sql] case class VarkaConjunctSpec(
    conjunct: Expression,
    fused: Boolean,
    decline: Option[VarkaDecline])

/**
 * A filter predicate compiled conjunct by conjunct (task 21): `specs` classifies every
 * conjunct of the condition's `AND` spine in query order, and `fused` describes the mask
 * kernel - its single output is the fused conjuncts recombined into one condition root, and
 * its `outputTypes` entry is `BooleanType` as a description only, since a selection bitmap
 * never allocates an output vector. The split mirrors [[PartialVarkaProjection]]'s per-entry
 * eligibility: a mixed `WHERE` fuses what it can, and the rule keeps the residual conjuncts
 * in a row `FilterExec` above the Varka node.
 */
private[sql] case class CompiledVarkaPredicate(
    specs: Seq[VarkaConjunctSpec],
    fused: CompiledVarkaProjection) {

  /** The conjuncts the mask kernel serves, in query order, unbound. */
  def fusedConjuncts: Seq[Expression] = specs.filter(_.fused).map(_.conjunct)

  /** The conjuncts left to a row filter above, in query order, unbound. */
  def residualConjuncts: Seq[Expression] = specs.filterNot(_.fused).map(_.conjunct)
}

/**
 * Compiles a bound projection list to the Varka vector IR, recursing where the MVP's
 * flat matcher demanded bare attributes - `datediff(date_add(d, 7), d2)` compiles where
 * milestone 1 saw nothing, and since task 11 so do `CASE WHEN`/`IF` (via interior comparisons
 * and the three-valued connectives), `greatest`/`least`, `dayofweek`/`weekday` and date
 * literals. Task 20 widened the conditions with `IN` over date literals (capped, see
 * [[MaxInLiterals]]) and the validity predicates `IS [NOT] NULL` over bare columns, and the
 * values with `coalesce`/`nvl`/`nvl2` (lowered onto the validity condition) and the identity
 * date cast. Used by both `VarkaColumnarRule` (is the projection eligible?) and
 * `VarkaKernelEvaluator` (what does the emitted loop compute?), so eligibility cannot drift from
 * execution: there is one compiler and the rule's question is `compilePartial(...).isDefined`.
 *
 * Since task 12 eligibility is per entry, not all or nothing: [[compilePartial]] classifies
 * every entry as fused, forwarded (a bare column of any type, zero-copy) or residual (per-row),
 * and the projection is eligible when at least one entry fuses - a projection of forwards and
 * residuals alone gains nothing from Varka and stays on Janino untouched. [[compile]] remains
 * as the all-entries-fused special case for callers that need exactly that.
 *
 * Task 21 adds the third entry point, [[compilePredicate]]: a filter condition compiled to a
 * single condition root - the selection mask the emitter writes as a bitmap - with the same
 * per-part eligibility, split on the predicate's `AND` spine instead of projection entries.
 *
 * Literal day offsets fold through [[DateVarkaSupport.foldDaysOffset]] - the same rule the MVP
 * matched on - into slots of the runtime argument table, assigned per distinct '''value''': two
 * occurrences of `date_add(d, 1)` must compile to equal IR records, or the emitter's CSE could
 * not see they are one computation. Slots are numbered in first-occurrence order, so a chain's
 * shape does not depend on what its constants are - the identity milestone 3's cache will key
 * on.
 *
 * This is the only eligibility rule there is. Milestone 1's parallel one - the expressions'
 * `isClassFileGenEligible` and its genCode-time registration, deliberately left alone while two
 * generations of codegen coexisted - retired with the dispatcher layer in task 17.
 */
private[sql] object VarkaExpressionCompiler {

  /**
   * The most literals an `IN` list may hold and still fuse (task 20), counted after dedup.
   * The basis, recorded in `PLAN_TASK_20.md`: 16 is depth-safe under any fold shape
   * (`MAX_CHAIN_DEPTH` = 16 while the balanced chain here is `ceil(log2 16) + 1` = 5
   * levels), and its 31 op nodes leave half the emitter's `MAX_FUSED_NODES` = 64 budget to
   * the rest of the projection. (The emitter's broadcast hoist is NOT part of the basis:
   * its gate counts the kernel's total literal slots, so a capped IN plus any other
   * literal already re-broadcasts inline - the review pass corrected an earlier claim
   * here.) Above the cap the entry declines with a reason instead of silently losing the
   * whole kernel at emission.
   */
  private[codegen] val MaxInLiterals = 16

  /** The all-entries-fused special case of [[compilePartial]], kept for callers that need it. */
  def compile(
      projectList: Seq[NamedExpression],
      childOutput: Seq[Attribute]): Option[CompiledVarkaProjection] = {
    compilePartial(projectList, childOutput).collect {
      case partial if partial.specs.forall(_.isInstanceOf[FusedOutput]) => partial.fused
    }
  }

  /**
   * Classifies every projection entry (see [[VarkaOutputSpec]]) and compiles the fused entries
   * into one sub-projection. `Some` exactly when at least one entry fused and the fused trees
   * reference at least one column - the emitted loop reads columns or has nothing to
   * vectorize over.
   */
  def compilePartial(
      projectList: Seq[NamedExpression],
      childOutput: Seq[Attribute]): Option[PartialVarkaProjection] = {
    // Both tables assign dense indices in first-occurrence order, which makes the compiled
    // shape deterministic in the projection alone.
    val inputs = mutable.LinkedHashMap.empty[Int, Int]
    val literals = mutable.LinkedHashMap.empty[Int, Int]
    val outputs = mutable.ArrayBuffer.empty[VarkaVectorIR]
    val outputTypes = Seq.newBuilder[DataType]
    val sink = new DeclineSink(childOutput)
    val declines = Map.newBuilder[Int, VarkaDecline]
    var fusedCount = 0
    val specs = projectList.zipWithIndex.map { case (named, position) =>
      // Bound at Expression, not NamedExpression: a bare column entry binds to a
      // BoundReference, which is not a NamedExpression, and the cast inside bindReference
      // would throw instead of letting the match below classify it.
      val bound = BindReferences.bindReference[Expression](named, childOutput)
      val inner = bound match {
        case Alias(child, _) => child
        case e => e
      }
      inner match {
        // A bare column is compilable as a node but never emitted as an output: emitting it
        // would be a copy loop, while forwarding the input's vector is zero-copy.
        case br: BoundReference => ForwardedOutput(br.ordinal)
        case e =>
          // The tables are shared across entries (CSE across outputs depends on it), so a
          // declining entry must not leave the columns and literals its failing subtrees
          // registered: they would widen the kernel's input set - and `canRun`'s Arrow check -
          // for no output. Entries are appended in table order, so truncating to the
          // pre-entry size restores the exact prior state.
          val inputsMark = inputs.size
          val literalsMark = literals.size
          compileNode(e, inputs, literals, sink) match {
            // Task 20: an accepted entry must also fit the emitter's structural budgets
            // together with the entries accepted before it. The emitter enforces the same
            // limits, but at emission time, where a breach can only become a silent
            // per-batch fallback - no decline reason, and EXPLAIN still claims fusion. So
            // the compiler mirrors them and demotes the overflowing entry to residual.
            case Some(ir) if VarkaLoopEmitter.fitsBudgets((outputs :+ ir).asJava, inputs.size) =>
              sink.take()
              outputs += ir
              outputTypes += e.dataType
              fusedCount += 1
              FusedOutput(fusedCount - 1)
            case compiled =>
              truncate(inputs, inputsMark)
              truncate(literals, literalsMark)
              if (compiled.isDefined) {
                sink.take() // an over-budget entry compiled clean; its reason is the budget
                sink.note("exceeds the emitter's fused budget", e)
              }
              // A declining entry always leaves a reason: every `None` below notes one.
              sink.take().foreach(decline => declines += position -> decline)
              ResidualOutput
          }
      }
    }
    if (fusedCount > 0 && inputs.nonEmpty) {
      Some(PartialVarkaProjection(specs, CompiledVarkaProjection(
        outputs.toSeq, outputTypes.result(), inputs.keys.toSeq, literals.keys.toSeq),
        declines.result()))
    } else {
      None
    }
  }

  /** Drops the entries a failed compile appended after `mark` (insertion order). */
  private def truncate(table: mutable.LinkedHashMap[Int, Int], mark: Int): Unit = {
    if (table.size > mark) {
      table.keys.drop(mark).toSeq.foreach(table.remove)
    }
  }

  /**
   * Compiles a filter predicate conjunct by conjunct (task 21). The condition splits on its
   * `AND` spine - Kleene AND is associative, so the split changes nothing - and each conjunct
   * either joins the fused mask kernel or stays behind as a residual, mirroring
   * [[compilePartial]]'s per-entry eligibility including the table rollback: a declining
   * conjunct must not widen the kernel's input set or `canRun`'s Arrow check. An accepted
   * conjunct must also keep the '''recombined''' root within the emitter's budgets - the
   * AND fold adds a node per accepted conjunct, so the budgets are mirrored against the fold,
   * not the conjunct alone. `Some` exactly when at least one conjunct fused and the fused
   * tree reads at least one column; the caller keeps `residualConjuncts` in a row filter
   * above.
   *
   * The null rule needs no glue here: at the mask root unknown is false (see the IR's `Cond`
   * doc), and AND-splitting preserves it - a row where any conjunct is null or false has the
   * whole conjunction null or false, and both read as unselected.
   */
  def compilePredicate(
      condition: Expression,
      childOutput: Seq[Attribute]): Option[CompiledVarkaPredicate] = {
    val inputs = mutable.LinkedHashMap.empty[Int, Int]
    val literals = mutable.LinkedHashMap.empty[Int, Int]
    val sink = new DeclineSink(childOutput)
    val fusedConds = mutable.ArrayBuffer.empty[Cond]
    val specs = splitConjuncts(condition).map { conjunct =>
      val bound = BindReferences.bindReference[Expression](conjunct, childOutput)
      val inputsMark = inputs.size
      val literalsMark = literals.size
      compileCond(bound, inputs, literals, sink) match {
        case Some(cond) if VarkaLoopEmitter.fitsBudgets(
            java.util.List.of(andFold(fusedConds.toSeq :+ cond)), inputs.size) =>
          sink.take()
          fusedConds += cond
          VarkaConjunctSpec(conjunct, fused = true, decline = None)
        case compiled =>
          truncate(inputs, inputsMark)
          truncate(literals, literalsMark)
          if (compiled.isDefined) {
            sink.take()
            sink.note("exceeds the emitter's fused budget", bound)
          }
          // A declining conjunct always leaves a reason: every `None` in compileCond notes one.
          VarkaConjunctSpec(conjunct, fused = false, decline = sink.take())
      }
    }
    if (fusedConds.nonEmpty && inputs.nonEmpty) {
      Some(CompiledVarkaPredicate(specs,
        CompiledVarkaProjection(Seq(andFold(fusedConds.toSeq)), Seq(BooleanType),
          inputs.keys.toSeq, literals.keys.toSeq)))
    } else {
      None
    }
  }

  /** The `AND` spine of a condition, in query order - the split [[compilePredicate]] works. */
  private def splitConjuncts(condition: Expression): Seq[Expression] = condition match {
    case And(left, right) => splitConjuncts(left) ++ splitConjuncts(right)
    case other => Seq(other)
  }

  /**
   * Folds the fused conjuncts back into one root, '''balanced''' like [[balancedOr]] and for
   * the same reason: Kleene AND is associative, so the shape is a canonicalization, and a
   * left fold would grow the chain depth by one per conjunct - a WHERE of 16 fusible
   * conjuncts would trip `MAX_CHAIN_DEPTH` for no semantic reason, where the balanced fold
   * stays logarithmic.
   */
  private def andFold(conds: Seq[Cond]): Cond = balancedFold(conds, new IRAnd(_, _))

  /**
   * The recursive node compiler. `None` anywhere fails the enclosing entry, whose caller rolls
   * the tables back to their pre-entry state. Shapes that
   * cannot be served stay unmatched by construction: an integer `Add` over a `datediff` result
   * is not a date expression (and ANSI overflow cannot throw row-accurately from a lane), and a
   * `date_add` over a `datediff` result only type-checks through a `Cast`, which compiles to
   * nothing here.
   */
  private def compileNode(
      expr: Expression,
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[VarkaVectorIR] = expr match {
    case br: BoundReference if br.dataType == DateType =>
      Some(new ColumnRef(inputs.getOrElseUpdate(br.ordinal, inputs.size)))
    // A date literal's value is already an epoch-day int, so it takes a slot in the shared
    // per-distinct-value table like a folded day offset does (task 11) - what makes
    // `d < DATE'...'` and `greatest(d, DATE'...')` reachable at all.
    case Literal(days: Int, DateType) =>
      Some(new LiteralSlot(literals.getOrElseUpdate(days, literals.size)))
    // The identity cast (task 20): the corpus wraps date expressions in `CAST(... AS DATE)`
    // 85 times, and after optimization the wrapper is a no-op over an already-date child -
    // unwrap it. A `cast(<string literal> AS DATE)` never reaches here (constant-folded to a
    // date literal by the optimizer); a string *column* cast is a per-row parse with no
    // string lane and stays declined below.
    case c: Cast if c.dataType == DateType && c.child.dataType == DateType =>
      compileNode(c.child, inputs, literals, sink)
    case DateAdd(child, days) =>
      for {
        offset <- foldOffset(days, sink)
        node <- compileNode(child, inputs, literals, sink)
      } yield new AddDays(node, new LiteralSlot(literals.getOrElseUpdate(offset, literals.size)))
    case DateSub(child, days) =>
      for {
        offset <- foldOffset(days, sink)
        node <- compileNode(child, inputs, literals, sink)
      } yield new SubDays(node, new LiteralSlot(literals.getOrElseUpdate(offset, literals.size)))
    case DateDiff(end, start) =>
      for {
        endNode <- compileNode(end, inputs, literals, sink)
        startNode <- compileNode(start, inputs, literals, sink)
      } yield new IRDateDiff(endNode, startNode)
    case If(pred, thenValue, elseValue) =>
      for {
        cond <- compileCond(pred, inputs, literals, sink)
        thenNode <- compileNode(thenValue, inputs, literals, sink)
        elseNode <- compileNode(elseValue, inputs, literals, sink)
      } yield new IfElse(cond, thenNode, elseNode)
    // With no ELSE the missing branch is a null literal, which would break the dense body's
    // all-valid invariant (task 11 plan, 2.1): decline.
    case c @ CaseWhen(_, None) =>
      sink.note("CASE WHEN without an ELSE branch", c)
      None
    // CASE WHEN with an ELSE right-folds into nested IfElse - SQL's first-match semantics is
    // exactly nested if-else. Compilation runs in query order (branches left to right, then
    // the ELSE) so input ordinals and literal slots register deterministically in reading
    // order; only the fold is right-associative.
    case CaseWhen(branches, elseValue) =>
      elseValue.flatMap { elseExpr =>
        val compiledBranches = branches.map { case (pred, value) =>
          (compileCond(pred, inputs, literals, sink),
            compileNode(value, inputs, literals, sink))
        }
        val compiledElse = compileNode(elseExpr, inputs, literals, sink)
        if (compiledBranches.forall(b => b._1.isDefined && b._2.isDefined)
            && compiledElse.isDefined) {
          Some(compiledBranches.foldRight(compiledElse.get) { case ((cond, value), rest) =>
            new IfElse(cond.get, value.get, rest)
          })
        } else {
          None
        }
      }
    // Coalesce (task 20) right-folds onto the validity condition: `coalesce(a, b)` is
    // `IfElse(IsNotNull(a), a, b)`, whose masked validity - (kT & valid(a)) | (~kT & valid(b))
    // with kT = valid(a) - reduces to valid(a) | valid(b), exactly SQL's coalesce. Every
    // operand before the last must be a bare date column (the IsNotNull child restriction);
    // `nvl`/`ifnull` arrive here already rewritten to Coalesce by the optimizer, and `nvl2`
    // arrives as `If(IsNotNull(...), ...)` and rides the same condition node.
    case Coalesce(children) if children.nonEmpty =>
      compileCoalesce(children, inputs, literals, sink)
    // Spark's greatest/least are n-ary; the null-skipping algebra is associative, so a left
    // fold into the binary IR nodes is exact.
    case Greatest(children) =>
      foldPick(children, inputs, literals, sink, new IRGreatest(_, _))
    case Least(children) =>
      foldPick(children, inputs, literals, sink, new IRLeast(_, _))
    case DayOfWeek(child) =>
      compileNode(child, inputs, literals, sink).map(new IRDayOfWeek(_))
    case WeekDay(child) =>
      compileNode(child, inputs, literals, sink).map(new IRWeekDay(_))
    // A column of any other type: eligible to be forwarded as a whole entry, never to be read
    // by the int32 lanes of a kernel.
    case br: BoundReference =>
      sink.note(s"non-date column of type ${br.dataType.simpleString}", br)
      None
    // Defensive: a real query never carries an unreplaced RuntimeReplaceable this far (the
    // optimizer's ReplaceExpressions runs long before physical planning), but hand-built
    // expressions in tests and the plan-side fusion report can - compile what would run.
    case r: RuntimeReplaceable =>
      compileNode(r.replacement, inputs, literals, sink)
    case other =>
      sink.note("unsupported expression", other)
      None
  }

  /**
   * The Coalesce right-fold (task 20). Every operand except the last compiles and must be a
   * bare date column: `IsNotNull` reads the per-input validity word, which only a column has
   * before value emission (the recorded milestone-3 restriction) - a computed operand
   * declines with its own reason.
   */
  private def compileCoalesce(
      children: Seq[Expression],
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[VarkaVectorIR] = children match {
    case Seq(last) => compileNode(last, inputs, literals, sink)
    case head +: rest =>
      compileNode(head, inputs, literals, sink) match {
        case Some(ref: ColumnRef) =>
          compileCoalesce(rest, inputs, literals, sink)
            .map(restNode => new IfElse(new IRIsNotNull(ref), ref, restNode))
        case Some(_) =>
          sink.note("coalesce operand before the last is not a bare date column", head)
          None
        case None => None
      }
  }

  /**
   * The literal day offset of a `date_add`/`date_sub`, or `None` with the reason noted: a
   * non-foldable offset is a per-row value, and the kernel's offsets are runtime arguments
   * fixed for the whole batch.
   */
  private def foldOffset(days: Expression, sink: DeclineSink): Option[Int] = {
    val folded = DateVarkaSupport.foldDaysOffset(days)
    if (folded.isEmpty) {
      sink.note("day offset is not a foldable literal", days)
    }
    folded
  }

  private def foldPick(
      children: Seq[Expression],
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink,
      combine: (VarkaVectorIR, VarkaVectorIR) => VarkaVectorIR): Option[VarkaVectorIR] = {
    val compiled = children.map(compileNode(_, inputs, literals, sink))
    if (compiled.nonEmpty && compiled.forall(_.isDefined)) {
      Some(compiled.flatten.reduceLeft(combine))
    } else {
      None
    }
  }

  /**
   * The condition compiler (task 11): interior comparisons and the connectives, three-valued
   * at run time via the emitter's known-true/known-false pairs. `EqualNullSafe` deliberately
   * declines - its both-null-is-true case breaks the null-intolerant comparison rule and earns
   * its own algebra entry or nothing (plan section 4).
   */
  private def compileCond(
      expr: Expression,
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[Cond] = expr match {
    case LessThan(l, r) => compare(CompareOp.LT, l, r, inputs, literals, sink)
    case LessThanOrEqual(l, r) => compare(CompareOp.LE, l, r, inputs, literals, sink)
    case GreaterThan(l, r) => compare(CompareOp.GT, l, r, inputs, literals, sink)
    case GreaterThanOrEqual(l, r) => compare(CompareOp.GE, l, r, inputs, literals, sink)
    case EqualTo(l, r) => compare(CompareOp.EQ, l, r, inputs, literals, sink)
    // IN over date literals (task 20): an EQ chain joined by OR, which the mask algebra
    // makes exactly SQL's IN inside a condition - a null value leaves every comparison
    // unknown, the OR of unknowns is unknown, and an unknown condition falls to ELSE.
    case in @ In(value, list) if value.dataType == DateType =>
      compileInList(value, list.map(literalDays), in, inputs, literals, sink)
    case inSet: InSet if inSet.child.dataType == DateType =>
      // InSet's set is unordered; compileInList sorts, which is what keeps the literal
      // slots and the shape hash deterministic across runs.
      compileInList(inSet.child,
        inSet.hset.toSeq.map { case days: Int => Some(days); case _ => None },
        inSet, inputs, literals, sink)
    case And(l, r) =>
      for {
        left <- compileCond(l, inputs, literals, sink)
        right <- compileCond(r, inputs, literals, sink)
      } yield new IRAnd(left, right)
    case Or(l, r) =>
      for {
        left <- compileCond(l, inputs, literals, sink)
        right <- compileCond(r, inputs, literals, sink)
      } yield new IROr(left, right)
    case Not(child) => compileCond(child, inputs, literals, sink).map(new IRNot(_))
    // The validity predicates (task 20): IS NOT NULL is the IR's first total condition
    // (never unknown), and IS NULL is its NOT - a slot swap in the emitter, no code.
    case IsNotNull(child) =>
      compileValidity(child, expr, inputs, literals, sink)
    case IsNull(child) =>
      compileValidity(child, expr, inputs, literals, sink).map(new IRNot(_))
    // Defensive, mirroring compileNode: hand-built Nvl/Nvl2 in tests and the fusion report
    // arrive unreplaced; real queries never do.
    case r: RuntimeReplaceable =>
      compileCond(r.replacement, inputs, literals, sink)
    case other =>
      sink.note("unsupported predicate", other)
      None
  }

  /** The epoch-day value of a date literal, or `None` for anything else (null included). */
  private def literalDays(e: Expression): Option[Int] = e match {
    case Literal(days: Int, DateType) => Some(days)
    case _ => None
  }

  /**
   * Compiles an IN list (task 20): dedup and sort the literal days - Kleene OR is commutative
   * and EQ is pure, so the order is free, and a canonical order keeps the literal slots and
   * the shape hash deterministic (`InSet` hands the values over as an unordered set) - then a
   * '''balanced''' pairwise fold of OR over the EQ leaves. The fold shape is part of the cap
   * arithmetic: balanced, [[MaxInLiterals]] literals are `ceil(log2 n) + 1` levels and
   * `2n - 1` op nodes; a right-nested fold would hit the emitter's depth cap at 15. Above the
   * cap, or with any non-literal or null element, the entry declines with its reason.
   */
  private def compileInList(
      value: Expression,
      elements: Seq[Option[Int]],
      whole: Expression,
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[Cond] = {
    if (elements.isEmpty || elements.exists(_.isEmpty)) {
      sink.note("IN list has a null or non-literal date element", whole)
      None
    } else {
      val days = elements.flatten.distinct.sorted
      if (days.size > MaxInLiterals) {
        sink.note(s"IN list longer than the fused cap of $MaxInLiterals", whole)
        None
      } else {
        compileNode(value, inputs, literals, sink).map { compiledValue =>
          val leaves: Seq[Cond] = days.map { d =>
            new Compare(CompareOp.EQ, compiledValue,
              new LiteralSlot(literals.getOrElseUpdate(d, literals.size)))
          }
          balancedOr(leaves)
        }
      }
    }
  }

  /** Pairwise-reduces conditions into a balanced OR tree; the base of the cap arithmetic. */
  private def balancedOr(level: Seq[Cond]): Cond = balancedFold(level, new IROr(_, _))

  /** Pairwise-reduces conditions into a balanced tree of `combine` - the shared shape behind
   * [[balancedOr]] and the predicate's [[andFold]]. */
  @scala.annotation.tailrec
  private def balancedFold(level: Seq[Cond], combine: (Cond, Cond) => Cond): Cond = {
    require(level.nonEmpty, "balancedFold needs at least one condition")
    if (level.size == 1) {
      level.head
    } else {
      balancedFold(level.grouped(2).map {
        case Seq(a, b) => combine(a, b)
        case Seq(a) => a
      }.toSeq, combine)
    }
  }

  /**
   * Compiles the operand of a validity predicate, which must land on a bare date column: the
   * emitter reads the column's per-lane-group validity word, and only a column's word is live
   * before value emission (the recorded milestone-3 restriction).
   */
  private def compileValidity(
      child: Expression,
      whole: Expression,
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[Cond] = {
    compileNode(child, inputs, literals, sink) match {
      case Some(ref: ColumnRef) => Some(new IRIsNotNull(ref))
      case Some(_) =>
        sink.note("validity predicate over a non-column operand", whole)
        None
      case None => None
    }
  }

  private def compare(
      op: CompareOp,
      l: Expression,
      r: Expression,
      inputs: mutable.LinkedHashMap[Int, Int],
      literals: mutable.LinkedHashMap[Int, Int],
      sink: DeclineSink): Option[Cond] = {
    for {
      left <- compileNode(l, inputs, literals, sink)
      right <- compileNode(r, inputs, literals, sink)
    } yield new Compare(op, left, right)
  }
}
