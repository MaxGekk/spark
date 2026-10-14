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

import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nullable;

/**
 * What Varka knew when something failed: one record, rendered as one block, that a fallback's
 * warning logs after its sentence and that an internal error ({@link VarkaErrors}) carries in its
 * message (VARKA-314). It answers the questions a bug report needs answered first - which kernel,
 * compiled under which options, over what batch - without a second run.
 *
 * <p>A record is built only inside a catch block, from what the evaluator, the runner or the
 * compiler already hold when the failure reaches them, so the path that runs per batch allocates
 * nothing for it. Building and rendering never throw: they run while a failure is being handled,
 * and an exception here would replace the one being reported. A field the catch could not obtain
 * is built through {@link #safely}, and reads as unavailable instead.
 *
 * @param kind the failure's kind: a {@link VarkaFallbackEvent} cause tag, {@link #PLANNING}
 *        for a throw out of the compiler, or {@link #MEMORY_VIOLATION}
 * @param identity what failed: the kernel's source file (named by its shape hash), the canonical
 *        IR of each output and the operator and stage, as the fallback warnings name a kernel;
 *        at planning, where no IR exists yet, the Catalyst entries being compiled
 * @param shapeHash the shape cache's hash of the kernel; null at planning, where there is none
 * @param options the emit options that differ from {@link VarkaEmitOptions#DEFAULTS}, as
 *        {@link #renderOptions} renders them; empty at the defaults
 * @param batch the failing batch's row count and null counts; null when no batch was involved
 * @param cause the original exception, kept whole
 */
public record VarkaErrorContext(
    String kind,
    String identity,
    @Nullable String shapeHash,
    String options,
    @Nullable Batch batch,
    @Nullable Throwable cause) {

  /** The kind of a throw out of the compiler, at planning time. */
  public static final String PLANNING = "planning";

  /**
   * The kind of a memory-sanitizer violation in a kernel, which is not a fallback cause: the
   * violation fails the task (VARKA-263). Its record is built only for a reproduction file
   * (VARKA-315).
   */
  public static final String MEMORY_VIOLATION = "memory-violation";

  /** What a field reads as when the catch could not obtain it. */
  public static final String UNAVAILABLE = "unavailable";

  /**
   * How long a rendered identity may grow: a projection of thousands of entries would otherwise
   * put all of them in one log line.
   */
  public static final int MAX_IDENTITY_LENGTH = 4000;

  /**
   * The failing batch: its rows, and the null count of the column each kernel input reads, in
   * kernel-input order.
   *
   * @param rows the batch's row count
   * @param nullCounts each kernel input's column's null count over the batch; empty where the
   *        catch could not tell which columns the failing kernel reads
   */
  public record Batch(int rows, List<Integer> nullCounts) {
    public Batch {
      nullCounts = List.copyOf(nullCounts);
    }
  }

  // Null fields are replaced rather than rejected: a constructor that threw while a failure is
  // handled would replace the failure.
  public VarkaErrorContext {
    kind = kind == null ? UNAVAILABLE : kind;
    identity = identity == null ? UNAVAILABLE : bounded(identity);
    options = options == null ? UNAVAILABLE : options;
  }

  /**
   * {@code field}'s value, or {@link #UNAVAILABLE} with the exception's class when computing it
   * throws or returns null. Only a {@link VirtualMachineError} passes: the JVM is failing, and no
   * report is worth hiding that.
   */
  public static String safely(Supplier<String> field) {
    try {
      String value = field.get();
      return value == null ? UNAVAILABLE : value;
    } catch (VirtualMachineError e) {
      throw e;
    } catch (Throwable t) {
      return UNAVAILABLE + " (" + t.getClass().getName() + ")";
    }
  }

  /**
   * The emit options that differ from {@link VarkaEmitOptions#DEFAULTS}, each as
   * {@code name=value} in {@link VarkaEmitOption#TABLE}'s order and comma-separated, as the option
   * matrix labels a configuration ({@link VarkaEmitOption.Subject#label}), so a reader can rerun
   * a suite under exactly these options; empty at the defaults. The shape hash folds the same
   * options into its key ({@link VarkaEmitOptions#canonical()}), but by position, which a reader
   * cannot decode without the table; this names them.
   */
  public static String renderOptions(VarkaEmitOptions options) {
    return new VarkaEmitOption.Subject(ignored -> options).label();
  }

  /** {@code text} cut to {@link #MAX_IDENTITY_LENGTH}, saying how much was cut. */
  static String bounded(String text) {
    if (text.length() <= MAX_IDENTITY_LENGTH) {
      return text;
    }
    return text.substring(0, MAX_IDENTITY_LENGTH) + "... (" + (text.length() - MAX_IDENTITY_LENGTH)
        + " more characters)";
  }

  /**
   * The record as the block a warning and an internal error carry, one field to a line:
   *
   * <pre>
   * Varka context:
   *   kind: kernel-failure
   *   identity: VarkaFusedProjection_1a2b3c4d.java [date_add(c0, s0)] (Varka_Project_Stage3)
   *   shape hash: 1a2b3c4d
   *   options: misdescribeAdd=true
   *   batch: 3 rows, nulls per input [1]
   *   cause: java.lang.NoSuchMethodError: ...
   * </pre>
   */
  public String render() {
    var sb = new StringBuilder("Varka context:");
    sb.append("\n  kind: ").append(kind);
    sb.append("\n  identity: ").append(identity);
    sb.append("\n  shape hash: ").append(shapeHash == null ? "none" : shapeHash);
    sb.append("\n  options: ").append(options.isEmpty() ? "the defaults" : options);
    if (batch != null) {
      sb.append("\n  batch: ").append(batch.rows()).append(" rows");
      if (!batch.nullCounts().isEmpty()) {
        sb.append(", nulls per input ").append(batch.nullCounts());
      }
    }
    if (cause != null) {
      // A cause's toString runs its getMessage, which a Throwable may override, and may be as
      // long as an IR dump.
      sb.append("\n  cause: ").append(bounded(safely(cause::toString)));
    }
    return sb.toString();
  }
}
