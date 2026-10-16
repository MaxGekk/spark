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
import org.apache.spark.sql.catalyst.expressions.EndsWith;
import org.apache.spark.sql.catalyst.expressions.EvalMode$;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.If;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.catalyst.expressions.InSet;
import org.apache.spark.sql.catalyst.expressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.KnownNotNull;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.expressions.Multiply;
import org.apache.spark.sql.catalyst.expressions.Not;
import org.apache.spark.sql.catalyst.expressions.Or;
import org.apache.spark.sql.catalyst.expressions.StartsWith;
import org.apache.spark.sql.catalyst.expressions.Subtract;
import org.apache.spark.sql.catalyst.expressions.UnaryMinus;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegralType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

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
 * predicate, and Varka's own fallback, evaluate the {@code AND} tree as written. Whole-stage
 * codegen ({@code FilterExec.generatePredicateCode}) evaluates the other conjuncts as written but
 * moves each {@code IsNotNull} to just before the first of them that references its column, and
 * the {@code IsNotNull} conjuncts no other conjunct references to the end. So an
 * {@code isnotnull(l)} written first still runs after a raising conjunct that does not read
 * {@code l}.
 *
 * <p>Whether a conjunct can raise is answered conservatively by {@link #canRaise}: a conjunct is
 * error-free only when every node in it is one of a short list of expressions that never throw in
 * the mode they carry. Anything else counts as raising, which costs fusion and never an answer.
 */
final class VarkaConjunctOrder {

  private VarkaConjunctOrder() {
  }

  /**
   * The fused conjuncts that Spark, in either of its orders, evaluates after a residual conjunct
   * that can raise, by index, each with the reason its spec reports. Empty when the split is safe.
   *
   * @param conjuncts the condition's {@code AND} spine in written order
   * @param fused whether each conjunct fused, by the same index
   */
  static Map<Integer, String> behindRaising(List<Expression> conjuncts, List<Boolean> fused) {
    Map<Integer, String> late = new LinkedHashMap<>();
    for (List<Integer> order : List.of(writtenOrder(conjuncts.size()), codegenOrder(conjuncts))) {
      Expression raising = null;
      for (int index : order) {
        if (raising != null && fused.get(index)) {
          late.putIfAbsent(index, "Spark evaluates it after " + raising.sql()
              + ", which can raise an error; fused, it would drop the rows that raise it");
        } else if (raising == null && !fused.get(index) && canRaise(conjuncts.get(index))) {
          raising = conjuncts.get(index);
        }
      }
    }
    return late;
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
      if (conjuncts.get(index) instanceof IsNotNull n && isNullIntolerant(n.child())) {
        notNull.add(index);
      } else {
        other.add(index);
      }
    }
    var placed = new boolean[conjuncts.size()];
    var order = new ArrayList<Integer>(conjuncts.size());
    for (int c : other) {
      Expression conjunct = conjuncts.get(c);
      for (Attribute reference : CollectionConverters.asJava(conjunct.references().toSeq())) {
        for (int n : notNull) {
          if (((IsNotNull) conjuncts.get(n)).child().semanticEquals(reference)) {
            if (!placed[n]) {
              placed[n] = true;
              order.add(n);
            }
            break;
          }
        }
      }
      for (int n : notNull) {
        Expression tested = ((IsNotNull) conjuncts.get(n)).child();
        if (!placed[n] && !(tested instanceof Attribute) && contains(conjunct, tested)) {
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

  /** Whether {@code tree} has a node semantically equal to {@code target}. */
  private static boolean contains(Expression tree, Expression target) {
    if (tree.semanticEquals(target)) {
      return true;
    }
    for (Expression child : CollectionConverters.asJava(tree.children())) {
      if (contains(child, target)) {
        return true;
      }
    }
    return false;
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
      case StartsWith _, EndsWith _, Contains _ -> true;
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

  /** A cast that only widens: a number to a wider number, a date to a timestamp, or to a string. */
  private static boolean wideningCast(DataType from, DataType to) {
    if (!Cast.canUpCast(from, to)) {
      return false;
    }
    return (from instanceof NumericType && to instanceof NumericType)
        || (from == DateType$.MODULE$ && (to instanceof TimestampType
            || to instanceof TimestampNTZType))
        || (to instanceof StringType && !(from instanceof StringType));
  }
}
