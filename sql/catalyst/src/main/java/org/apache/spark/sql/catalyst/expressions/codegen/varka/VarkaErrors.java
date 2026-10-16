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
package org.apache.spark.sql.catalyst.expressions.codegen.varka;

import java.util.Arrays;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import scala.Option;

import org.apache.spark.QueryContext;
import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.SparkIllegalStateException;
import org.apache.spark.SparkThrowable;
import org.apache.spark.SparkUnsupportedOperationException;

/**
 * Varka's broken invariants as Spark's {@code INTERNAL_ERROR} (VARKA-314): a bug in Varka that a
 * user meets is a bug report, and Spark's answer to one is that condition with a request to
 * report it. The two carriers keep the Java type a site threw before - a
 * {@link SparkIllegalArgumentException} is an {@link IllegalArgumentException}, a
 * {@link SparkIllegalStateException} an {@link IllegalStateException} - so every caller and test
 * that catches by type is unchanged. Both are unchecked, which {@code SparkException} is not; a
 * checked carrier would need a {@code throws} clause through records, emitters and lambdas.
 *
 * <p>The condition's one message parameter is the site's own text, then the context record's
 * block when the site has one ({@link VarkaErrorContext}), then the request to report it:
 *
 * <pre>
 * [INTERNAL_ERROR] Varka's compiler failed while planning.
 * Varka context:
 *   kind: planning
 *   identity: projection [date_add(d#1, 3) AS a#4] over [d#1, i#2]
 *   shape hash: none
 *   options: the defaults
 *   cause: java.lang.IllegalArgumentException: injected Varka compiler failure
 * This is a bug in Varka, Spark's vectorized code generator. Please report it with this message
 * and the full stack trace. SQLSTATE: XX000
 * </pre>
 *
 * <p>The record's {@code cause} becomes the exception's cause, so the original stack trace is
 * kept. A site deep in the compiler, the IR or the emitter knows only its own invariant, so it
 * raises the error without a record ({@link #illegalArgument(String)} and its siblings), and the
 * catch that knows more - the compiler's planning catch, an evaluator's fallback catch - adds the
 * record around it ({@link #planningFailure}, {@link #siteText}).
 */
public final class VarkaErrors {

  private VarkaErrors() {
  }

  /** Spark's condition for a bug, as {@code SparkException.internalError} raises it. */
  public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

  /** What every internal error from Varka asks of the user who meets it. */
  public static final String REPORT = "This is a bug in Varka, Spark's vectorized code generator."
      + " Please report it with this message and the full stack trace.";

  /**
   * A broken argument invariant as {@code INTERNAL_ERROR}, an {@link IllegalArgumentException},
   * from a site that has no record to give: a planning-time or fallback catch adds one around it.
   */
  public static SparkIllegalArgumentException illegalArgument(String what) {
    return illegalArgument(what, null);
  }

  /** A broken argument invariant as {@code INTERNAL_ERROR}, an {@link IllegalArgumentException}. */
  public static SparkIllegalArgumentException illegalArgument(
      String what, @Nullable VarkaErrorContext ctx) {
    return atSite(
        new SparkIllegalArgumentException(INTERNAL_ERROR, parameters(what, ctx), cause(ctx)));
  }

  /**
   * A broken state invariant as {@code INTERNAL_ERROR}, an {@link IllegalStateException}, from a
   * site that has no record to give: a planning-time or fallback catch adds one around it.
   */
  public static SparkIllegalStateException illegalState(String what) {
    return illegalState(what, (VarkaErrorContext) null);
  }

  /**
   * A broken state invariant found by catching {@code cause}, as {@code INTERNAL_ERROR}, an
   * {@link IllegalStateException} whose cause it is, from a site that has no record to give.
   */
  public static SparkIllegalStateException illegalState(String what, Throwable cause) {
    return state(what, null, cause);
  }

  /** A broken state invariant as {@code INTERNAL_ERROR}, an {@link IllegalStateException}. */
  public static SparkIllegalStateException illegalState(
      String what, @Nullable VarkaErrorContext ctx) {
    return state(what, ctx, cause(ctx));
  }

  private static SparkIllegalStateException state(
      String what, @Nullable VarkaErrorContext ctx, @Nullable Throwable cause) {
    return atSite(new SparkIllegalStateException(INTERNAL_ERROR, parameters(what, ctx),
        new QueryContext[0], cause, Option.empty()));
  }

  /**
   * An operation a Varka type does not support, reached anyway, as {@code INTERNAL_ERROR}, an
   * {@link UnsupportedOperationException}: a kernel called through the entry point of the other
   * lane width, or a class-file attribute mapper used in the direction it does not serve. Spark's
   * carrier of that type takes no cause, and no such site has a record to give.
   */
  public static SparkUnsupportedOperationException unsupportedOperation(String what) {
    return atSite(
        new SparkUnsupportedOperationException(INTERNAL_ERROR, parameters(what, null)));
  }

  /**
   * {@code e} with this class's own frames taken off the top of its stack trace, so the trace
   * starts at the site that raised it, as a {@code throw new} there would have: a reader and a
   * once-per-site log key ({@code VarkaSizeAdmission}'s) both read the top frame as the site.
   */
  private static <T extends Throwable> T atSite(T e) {
    StackTraceElement[] frames = e.getStackTrace();
    int top = 0;
    String self = VarkaErrors.class.getName();
    while (top < frames.length && frames[top].getClassName().equals(self)) {
      top++;
    }
    if (top > 0) {
      e.setStackTrace(Arrays.copyOfRange(frames, top, frames.length));
    }
    return e;
  }

  /**
   * The site's own text of {@code e} when {@code e} is an internal error a Varka site raised
   * without a record, or null for anything else - an exception of another origin, or one that
   * already carries a record. The planning catch wraps such an error with the record its site
   * could not build, and a record renders it as its cause by this text, so the condition's
   * framing and the request to report it are not repeated inside the message that has them.
   */
  public static @Nullable String siteText(@Nullable Throwable e) {
    if (!(e instanceof SparkThrowable st) || !INTERNAL_ERROR.equals(st.getCondition())) {
      return null;
    }
    String message = st.getMessageParameters().get("message");
    String tail = "\n" + REPORT;
    if (message == null || !message.endsWith(tail)
        || message.contains("\n" + VarkaErrorContext.HEAD)) {
      return null;
    }
    return message.substring(0, message.length() - tail.length());
  }

  /**
   * The planning-time catch's answer to {@code e}, a throw out of the compiler while it compiled
   * the entries {@code identity} renders under {@code options}: the internal error with the
   * record, of {@code e}'s type where a carrier exists - an {@link IllegalArgumentException} stays
   * one, and any other runtime exception (an {@link IllegalStateException}, and the bugs no
   * carrier matches, such as a {@link NullPointerException}) becomes an
   * {@link IllegalStateException}. An internal error a Varka site raised without a record
   * ({@link #siteText}) is wrapped the same way, its site's error the cause. Returned unchanged:
   * any other exception that carries a condition (a user's error Catalyst raised while the
   * compiler folded a constant, or a Varka error that already has its record), and the emitter's
   * designed decline, which is control flow.
   */
  public static RuntimeException planningFailure(
      RuntimeException e, Supplier<String> identity, VarkaEmitOptions options) {
    if (e instanceof VarkaEmitDeclined || (e instanceof SparkThrowable && siteText(e) == null)) {
      return e;
    }
    var ctx = new VarkaErrorContext(VarkaErrorContext.PLANNING,
        VarkaErrorContext.safely(identity), null,
        VarkaErrorContext.safely(() -> VarkaErrorContext.renderOptions(options)), null, e);
    // The original's message is the record's cause line; the sentence does not repeat it.
    String what = "Varka's compiler failed while planning.";
    return e instanceof IllegalArgumentException
        ? illegalArgument(what, ctx)
        : illegalState(what, ctx);
  }

  private static scala.collection.immutable.Map<String, String> parameters(
      String what, @Nullable VarkaErrorContext ctx) {
    String message = (what == null ? VarkaErrorContext.UNAVAILABLE : what)
        + (ctx == null ? "" : "\n" + ctx.render())
        + "\n" + REPORT;
    // A ${...} in the text - a projection's string literal, rendered into the record - needs no
    // escape: Spark's own StringSubstitutor does not substitute inside a parameter's value, and
    // it preserves escapes, so an escape would show.
    return new scala.collection.immutable.Map.Map1<>("message", message);
  }

  private static @Nullable Throwable cause(@Nullable VarkaErrorContext ctx) {
    return ctx == null ? null : ctx.cause();
  }
}
