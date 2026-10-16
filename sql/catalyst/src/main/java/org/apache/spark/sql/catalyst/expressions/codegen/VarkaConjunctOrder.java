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
package org.apache.spark.sql.catalyst.expressions.codegen;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import scala.jdk.javaapi.CollectionConverters;

import org.apache.spark.sql.catalyst.expressions.Add;
import org.apache.spark.sql.catalyst.expressions.And;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.BinaryComparison;
import org.apache.spark.sql.catalyst.expressions.BoundReference;
import org.apache.spark.sql.catalyst.expressions.CaseWhen;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Contains;
import org.apache.spark.sql.catalyst.expressions.DateAdd;
import org.apache.spark.sql.catalyst.expressions.DateDiff;
import org.apache.spark.sql.catalyst.expressions.DateSub;
import org.apache.spark.sql.catalyst.expressions.EndsWith;
import org.apache.spark.sql.catalyst.expressions.EvalMode$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.expressions.InSet;
import org.apache.spark.sql.catalyst.expressions.IsNaN;
import org.apache.spark.sql.catalyst.expressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.KnownNotNull;
import org.apache.spark.sql.catalyst.expressions.Length;
import org.apache.spark.sql.catalyst.expressions.Like;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Lower;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.expressions.Or;
import org.apache.spark.sql.catalyst.expressions.StartsWith;
import org.apache.spark.sql.catalyst.expressions.Substring;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus;
import org.apache.spark.sql.catalyst.expressions.Upper;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegralType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;

/**
 * Which fused conjuncts of a split filter would run ahead of an error Spark raises (VARKA-380).
 *
 * <p>A split filter runs its fused conjuncts in the Varka node below and its residual conjuncts in
 * a row {@code FilterExec} above, so the residual ones see only the rows every fused conjunct
 * kept. Spark evaluates the conjuncts in its own order and stops at the first false one. Where a
 * residual conjunct that can raise comes before a fused one in Spark's order, a row the fused
 * conjunct rejects never reaches it: Spark raises and Varka returns rows. Such a fused conjunct is
 * demoted to the residual filter, which then evaluates it in Spark's order with the rest.
 *
 * <p>Spark has two orders, and the check holds a fused conjunct to both. The interpreted
 * predicate evaluates the {@code AND} tree as written. Whole-stage codegen
 * ({@code FilterExec.generatePredicateCode}) evaluates the other conjuncts as written but moves
 * each {@code IsNotNull} to just before the first of them that references its column, and the
 * {@code IsNotNull} conjuncts no other conjunct references to the end. So an {@code isnotnull(l)}
 * written first still runs after a raising conjunct that does not read {@code l}. That also means
 * a fused conjunct can decide where a residual {@code IsNotNull} runs: taken out of the residual
 * filter, it no longer pulls the test ahead of a raising conjunct there, so such a conjunct is
 * demoted too. {@link #codegenOrder} is a port of that algorithm and has to follow it.
 *
 * <p>Whether a conjunct can raise is answered conservatively by {@link #canRaise}: a conjunct is
 * error-free only when every node in it is one of a short list of expressions that never throw in
 * the mode they carry. Anything else counts as raising, which costs fusion and never an answer.
 *
 * <p>Internal to Spark SQL; public for {@code sql/core}, whose filter fallback evaluates the
 * condition in codegen order ({@link #inCodegenOrder}).
 */
public final class VarkaConjunctOrder {

  private VarkaConjunctOrder() {
  }

  /**
   * The fused conjuncts to demote so that no fused conjunct changes which errors Spark's orders
   * raise, by index, each with the reason its spec reports. Empty when the split is safe. The
   * demotions are iterated here to their fixed point - a demoted conjunct that can raise moves
   * the line again - so the compiler classifies once more, not once per demotion.
   *
   * @param conjuncts the condition's {@code AND} spine in written order
   * @param fused whether each conjunct fused, by the same index
   */
  static Map<Integer, String> behindRaising(List<Expression> conjuncts, List<Boolean> fused) {
    int size = conjuncts.size();
    var raises = new boolean[size];
    var isFused = new boolean[size];
    boolean anyResidualRaises = false;
    for (int index = 0; index < size; index++) {
      isFused[index] = fused.get(index);
      raises[index] = canRaise(conjuncts.get(index));
      anyResidualRaises |= raises[index] && !isFused[index];
    }
    Map<Integer, String> late = new LinkedHashMap<>();
    if (!anyResidualRaises) {
      return late;
    }
    List<Integer> codegen = codegenOrder(conjuncts);
    boolean changed = true;
    while (changed) {
      changed = demoteBehind(writtenOrder(size), conjuncts, raises, isFused, late)
          | demoteBehind(codegen, conjuncts, raises, isFused, late);
      if (!changed) {
        changed = demotePullers(conjuncts, codegen, raises, isFused, late);
      }
    }
    return late;
  }

  /** Demotes the fused conjuncts {@code order} places after its first raising residual one. */
  private static boolean demoteBehind(List<Integer> order, List<Expression> conjuncts,
      boolean[] raises, boolean[] isFused, Map<Integer, String> late) {
    Expression raising = null;
    boolean changed = false;
    for (int index : order) {
      if (raising != null && isFused[index]) {
        isFused[index] = false;
        late.putIfAbsent(index, "Spark evaluates it after " + raising.sql()
            + ", which can raise an error; fused, it would drop the rows that raise it");
        changed = true;
      } else if (raising == null && !isFused[index] && raises[index]) {
        raising = conjuncts.get(index);
      }
    }
    return changed;
  }

  /**
   * Demotes the fused conjuncts that place a residual null test on the other side of a raising
   * residual conjunct in codegen order than the residual filter would place it without them. The
   * residual filter orders its own conjuncts by the same algorithm, and a null test runs before
   * the first conjunct that reads its column: when that conjunct is fused, the residual filter
   * moves the test later. Demoting every fused conjunct that reads the column restores the place.
   */
  private static boolean demotePullers(List<Expression> conjuncts, List<Integer> codegen,
      boolean[] raises, boolean[] isFused, Map<Integer, String> late) {
    var residual = new ArrayList<Integer>();
    var residualConjuncts = new ArrayList<Expression>();
    for (int index = 0; index < conjuncts.size(); index++) {
      if (!isFused[index]) {
        residual.add(index);
        residualConjuncts.add(conjuncts.get(index));
      }
    }
    var fullPosition = new int[conjuncts.size()];
    for (int position = 0; position < codegen.size(); position++) {
      fullPosition[codegen.get(position)] = position;
    }
    var residualPosition = new int[conjuncts.size()];
    List<Integer> residualOrder = codegenOrder(residualConjuncts);
    for (int position = 0; position < residualOrder.size(); position++) {
      residualPosition[residual.get(residualOrder.get(position))] = position;
    }
    boolean changed = false;
    boolean mismatched = false;
    for (int r : residual) {
      if (!raises[r]) {
        continue;
      }
      for (int c : residual) {
        boolean beforeInFull = fullPosition[c] < fullPosition[r];
        boolean beforeInResidual = residualPosition[c] < residualPosition[r];
        if (c == r || beforeInFull == beforeInResidual) {
          continue;
        }
        mismatched = true;
        for (int test : List.of(c, r)) {
          Expression tested = nullTested(conjuncts.get(test));
          if (tested == null) {
            continue;
          }
          for (int f = 0; f < conjuncts.size(); f++) {
            if (isFused[f] && nullTested(conjuncts.get(f)) == null
                && pulls(conjuncts.get(f), tested)) {
              isFused[f] = false;
              late.putIfAbsent(f, "Spark evaluates " + conjuncts.get(c).sql() + " before "
                  + conjuncts.get(r).sql() + ", which can raise an error, or after it, by where "
                  + "this conjunct is; fused, it would leave the residual filter the other order");
              changed = true;
            }
          }
        }
      }
    }
    if (mismatched && !changed) {
      // Unreachable while codegenOrder ports Spark's algorithm: only a null test moves between the
      // two orders, and its pullers are what moves it. Should the port drift, nothing stays fused.
      for (int f = 0; f < conjuncts.size(); f++) {
        if (isFused[f]) {
          isFused[f] = false;
          late.putIfAbsent(f, "the residual filter would order its conjuncts unlike Spark");
          changed = true;
        }
      }
    }
    return changed;
  }

  /** The tested expression of a null test codegen moves, or null for any other conjunct. */
  private static Expression nullTested(Expression conjunct) {
    return conjunct instanceof IsNotNull n && isNullIntolerant(n.child()) ? n.child() : null;
  }

  /** Whether codegen places the null test of {@code tested} just before {@code conjunct}. */
  private static boolean pulls(Expression conjunct, Expression tested) {
    if (tested instanceof Attribute) {
      for (Attribute reference : CollectionConverters.asJava(conjunct.references().toSeq())) {
        if (tested.semanticEquals(reference)) {
          return true;
        }
      }
      return false;
    }
    return conjunct.exists(e -> e.semanticEquals(tested));
  }

  /**
   * {@code condition} with its conjuncts in the order whole-stage codegen evaluates them, for the
   * per-row fallback of a Varka filter, whose generated predicate evaluates an {@code AND} tree as
   * written: folded in this order, it stops where the codegen filter stops, and raises what it
   * raises.
   */
  public static Expression inCodegenOrder(Expression condition) {
    List<Expression> conjuncts = new ArrayList<>();
    collectConjuncts(condition, conjuncts);
    Expression folded = null;
    for (int index : codegenOrder(conjuncts)) {
      folded = folded == null ? conjuncts.get(index) : new And(folded, conjuncts.get(index));
    }
    return folded;
  }

  private static void collectConjuncts(Expression condition, List<Expression> conjuncts) {
    if (condition instanceof And and) {
      collectConjuncts(and.left(), conjuncts);
      collectConjuncts(and.right(), conjuncts);
    } else {
      conjuncts.add(condition);
    }
  }

  /** The conjuncts' indices in the order the {@code AND} tree evaluates them. */
  private static List<Integer> writtenOrder(int size) {
    var order = new ArrayList<Integer>(size);
    for (int index = 0; index < size; index++) {
      order.add(index);
    }
    return order;
  }

  /**
   * The conjuncts' indices in the order {@code FilterExec.generatePredicateCode} evaluates them:
   * each conjunct that is not a null test as written, preceded by the null tests of the columns it
   * references and of the null-intolerant expressions it contains, then the null tests left over.
   */
  static List<Integer> codegenOrder(List<Expression> conjuncts) {
    var notNull = new ArrayList<Integer>();
    var other = new ArrayList<Integer>();
    for (int index = 0; index < conjuncts.size(); index++) {
      if (nullTested(conjuncts.get(index)) != null) {
        notNull.add(index);
      } else {
        other.add(index);
      }
    }
    var placed = new boolean[conjuncts.size()];
    var order = new ArrayList<Integer>(conjuncts.size());
    for (int c : other) {
      Expression conjunct = conjuncts.get(c);
      // Spark looks up the first null test of each referenced column, and places it unless an
      // earlier conjunct already did.
      for (Attribute reference : CollectionConverters.asJava(conjunct.references().toSeq())) {
        for (int n : notNull) {
          if (nullTested(conjuncts.get(n)).semanticEquals(reference)) {
            if (!placed[n]) {
              placed[n] = true;
              order.add(n);
            }
            break;
          }
        }
      }
      for (int n : notNull) {
        Expression tested = nullTested(conjuncts.get(n));
        if (!placed[n] && !(tested instanceof Attribute) && pulls(conjunct, tested)) {
          placed[n] = true;
          order.add(n);
        }
      }
      order.add(c);
    }
    for (int n : notNull) {
      if (!placed[n]) {
        order.add(n);
      }
    }
    return order;
  }

  /** {@code PredicateHelper.isNullIntolerant}: null in, null out, all the way down. */
  private static boolean isNullIntolerant(Expression expr) {
    if (!expr.nullIntolerant()) {
      return false;
    }
    for (Expression child : CollectionConverters.asJava(expr.children())) {
      if (!isNullIntolerant(child)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether evaluating {@code expr} on some row can throw. False only when every node is on the
   * list below; the list is what residual conjuncts beside fused ones usually are - comparisons,
   * null tests, {@code IN}, string matches, and arithmetic that cannot overflow in its mode - and
   * anything off it is assumed to raise.
   */
  static boolean canRaise(Expression expr) {
    if (!errorFreeNode(expr)) {
      return true;
    }
    for (Expression child : CollectionConverters.asJava(expr.children())) {
      if (canRaise(child)) {
        return true;
      }
    }
    return false;
  }

  /** Whether {@code expr} itself, given children that do not throw, cannot throw. */
  private static boolean errorFreeNode(Expression expr) {
    return switch (expr) {
      case Attribute _, BoundReference _, Literal _ -> true;
      case IsNull _, IsNotNull _, KnownNotNull _, Not _, And _, Or _ -> true;
      case BinaryComparison _, In _, InSet _ -> true;
      case If _, CaseWhen _, Coalesce _ -> true;
      case StartsWith _, EndsWith _, Contains _, Upper _, Lower _, Length _, Substring _ -> true;
      case IsNaN _, DateAdd _, DateSub _, DateDiff _ -> true;
      // A pattern that does not parse raises, so only a constant one, which Spark checks when it
      // generates the code - before any row, on Varka's plan as on its own.
      case Like l -> l.right().foldable();
      // Year-month and day-time interval arithmetic raises on overflow in every mode, and decimal
      // arithmetic is left off rather than reasoned about, so only the plain numeric types pass.
      case Add a -> arithmeticCannotOverflow(a.evalMode(), a.dataType());
      case Subtract a -> arithmeticCannotOverflow(a.evalMode(), a.dataType());
      case Multiply a -> arithmeticCannotOverflow(a.evalMode(), a.dataType());
      case UnaryMinus n -> arithmeticCannotOverflow(
          n.failOnError() ? EvalMode$.MODULE$.ANSI() : EvalMode$.MODULE$.LEGACY(), n.dataType());
      case Cast c -> wideningCast(c.child().dataType(), c.dataType());
      default -> false;
    };
  }

  /**
   * Whether {@code +}, {@code -}, {@code *} or a negation in {@code mode} over {@code type} cannot
   * throw: floating point never does, and integers do only under ANSI. Division and remainder
   * stay off the list, since a zero divisor raises under ANSI whatever the type.
   */
  private static boolean arithmeticCannotOverflow(Object mode, DataType type) {
    if (type == DoubleType$.MODULE$ || type == FloatType$.MODULE$) {
      return true;
    }
    return type instanceof IntegralType && !EvalMode$.MODULE$.ANSI().equals(mode);
  }

  /**
   * A cast that only widens: a number to a wider number, or anything to a string. A date to a
   * timestamp is a widening Spark allows but not one that cannot raise: the microseconds of a
   * date past the year 294247 overflow a long.
   */
  private static boolean wideningCast(DataType from, DataType to) {
    if (!Cast.canUpCast(from, to)) {
      return false;
    }
    return (from instanceof NumericType && to instanceof NumericType)
        || (to instanceof StringType && !(from instanceof StringType));
  }
}
