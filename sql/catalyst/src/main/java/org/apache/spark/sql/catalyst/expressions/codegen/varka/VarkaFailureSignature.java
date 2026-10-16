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

import java.util.regex.Pattern;

/**
 * What groups fuzz failures and what a shrinker must preserve (VARKA-277): the failure's kind and
 * its message with the numbers taken out, so that "output 0 row 7 differs (want 12)" and "output 1
 * row 3 differs (want -4)" are one signature, and a reduction cannot slide from one bug to another.
 */
public record VarkaFailureSignature(String kind, String text) {

  /** The numbers of a message, decimal and hexadecimal, and a generated class's counter. */
  private static final Pattern NUMBERS = Pattern.compile("0x[0-9a-fA-F]+|-?\\d+");

  @Override
  public String toString() {
    return kind + ": " + text;
  }

  public static String normalize(String message) {
    return NUMBERS.matcher(message).replaceAll("#").trim();
  }

  /**
   * The signature of what a run threw. {@code label} is the context a failure's message starts
   * with ({@code VarkaFuzzCase.label}), which carries the seed and the whole drawn case and so is
   * not part of what the failure is. Whatever a check appends to the label before its
   * {@code ": "} - the other body's " (the other body)", a coverage row's expression - is part of
   * the context too and is taken off with it, so a decorated context still classifies
   * (VARKA-311).
   */
  public static VarkaFailureSignature of(Throwable t, String label) {
    // A Varka site's internal error is read by its site's text, without Spark's condition
    // framing around it, and named by the JDK type its Spark carrier extends (VARKA-314), so it
    // classifies, and a reproduction file written before the carriers replays, as it did.
    String site = VarkaErrors.siteText(t);
    String message = site != null ? site : t.getMessage() == null ? "" : t.getMessage();
    String bare = message;
    if (!label.isEmpty() && message.startsWith(label)) {
      String rest = message.substring(label.length());
      int colon = rest.indexOf(": ");
      bare = colon >= 0 ? rest.substring(colon + 2) : rest;
    }
    String text = normalize(bare.lines().findFirst().orElse(""));
    if (t instanceof VarkaMemoryViolation) {
      return new VarkaFailureSignature("memory violation", text);
    } else if (t instanceof LinkageError) {
      return new VarkaFailureSignature("generated class: " + t.getClass().getSimpleName(), text);
    } else if (text.startsWith("validity of output")) {
      return new VarkaFailureSignature("validity mismatch", text);
    } else if (text.startsWith("selection row")) {
      return new VarkaFailureSignature("selection mismatch", text);
    } else if (text.startsWith("output") && text.contains("differs")) {
      return new VarkaFailureSignature("output mismatch", text);
    } else if (text.startsWith("the kernel declined the batch")) {
      return new VarkaFailureSignature("declined batch", text);
    } else if (text.startsWith("the emitter")) {
      return new VarkaFailureSignature("emitter rejection", text);
    }
    Class<?> type = site != null ? t.getClass().getSuperclass() : t.getClass();
    return new VarkaFailureSignature(type.getSimpleName(), text);
  }
}
