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

import java.util.function.Supplier;

import javax.annotation.Nullable;

import scala.Option;

import org.apache.spark.QueryContext;
import org.apache.spark.SparkIllegalArgumentException;
import org.apache.spark.SparkIllegalStateException;
import org.apache.spark.SparkThrowable;

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
 * [INTERNAL_ERROR] injected Varka compiler failure
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
 * kept.
 */
public final class VarkaErrors {

  private VarkaErrors() {
  }

  /** Spark's condition for a bug, as {@code SparkException.internalError} raises it. */
  public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

  /** What every internal error from Varka asks of the user who meets it. */
  public static final String REPORT = "This is a bug in Varka, Spark's vectorized code generator."
      + " Please report it with this message and the full stack trace.";

  /** A broken argument invariant as {@code INTERNAL_ERROR}, an {@link IllegalArgumentException}. */
  public static SparkIllegalArgumentException illegalArgument(
      String what, @Nullable VarkaErrorContext ctx) {
    return new SparkIllegalArgumentException(INTERNAL_ERROR, parameters(what, ctx), cause(ctx));
  }

  /** A broken state invariant as {@code INTERNAL_ERROR}, an {@link IllegalStateException}. */
  public static SparkIllegalStateException illegalState(
      String what, @Nullable VarkaErrorContext ctx) {
    return new SparkIllegalStateException(INTERNAL_ERROR, parameters(what, ctx),
        new QueryContext[0], cause(ctx), Option.empty());
  }

  /**
   * The planning-time catch's answer to {@code e}, a throw out of the compiler while it compiled
   * the entries {@code identity} renders under {@code options}: the internal error with the
   * record, of {@code e}'s type where a carrier exists - an {@link IllegalArgumentException} stays
   * one, and any other runtime exception (an {@link IllegalStateException}, and the bugs no
   * carrier matches, such as a {@link NullPointerException}) becomes an
   * {@link IllegalStateException}. Returned unchanged: an exception that already carries a
   * condition, and the emitter's designed decline, which is control flow.
   */
  public static RuntimeException planningFailure(
      RuntimeException e, Supplier<String> identity, VarkaEmitOptions options) {
    if (e instanceof SparkThrowable || e instanceof VarkaEmitDeclined) {
      return e;
    }
    var ctx = new VarkaErrorContext(VarkaErrorContext.PLANNING,
        VarkaErrorContext.safely(identity), null,
        VarkaErrorContext.safely(() -> VarkaErrorContext.renderOptions(options)), null, e);
    // A bug such as a NullPointerException may carry no message; its class then says what it is.
    String what = VarkaErrorContext.safely(() -> {
      String message = e.getMessage();
      return message != null ? message : e.getClass().getName();
    });
    return e instanceof IllegalArgumentException
        ? illegalArgument(what, ctx)
        : illegalState(what, ctx);
  }

  private static scala.collection.immutable.Map<String, String> parameters(
      String what, @Nullable VarkaErrorContext ctx) {
    String message = (what == null ? VarkaErrorContext.UNAVAILABLE : what)
        + (ctx == null ? "" : "\n" + ctx.render())
        + "\n" + REPORT;
    return new scala.collection.immutable.Map.Map1<>("message", message);
  }

  private static @Nullable Throwable cause(@Nullable VarkaErrorContext ctx) {
    return ctx == null ? null : ctx.cause();
  }
}
