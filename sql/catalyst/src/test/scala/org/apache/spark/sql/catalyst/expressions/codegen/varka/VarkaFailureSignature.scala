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

/**
 * What groups fuzz failures and what a shrinker must preserve (VARKA-277): the failure's kind and
 * its message with the numbers taken out, so that "output 0 row 7 differs (want 12)" and "output 1
 * row 3 differs (want -4)" are one signature, and a reduction cannot slide from one bug to another.
 */
final case class VarkaFailureSignature(kind: String, text: String) {
  override def toString: String = s"$kind: $text"
}

object VarkaFailureSignature {

  /** The numbers of a message, decimal and hexadecimal, and a generated class's counter. */
  private val numbers = """0x[0-9a-fA-F]+|-?\d+""".r

  def normalize(message: String): String = numbers.replaceAllIn(message, "#").trim

  /**
   * The signature of what a run threw. `label` is the context a failure's message starts with
   * (`VarkaFuzzCase.label`), which carries the seed and the whole drawn case and so is not part of
   * what the failure is.
   */
  def of(t: Throwable, label: String): VarkaFailureSignature = {
    val message = Option(t.getMessage).getOrElse("")
    val bare = if (label.nonEmpty && message.startsWith(label + ": ")) {
      message.substring(label.length + 2)
    } else {
      message
    }
    val text = normalize(bare.linesIterator.nextOption().getOrElse(""))
    t match {
      case _: VarkaMemoryViolation => VarkaFailureSignature("memory violation", text)
      case e: LinkageError =>
        VarkaFailureSignature(s"generated class: ${e.getClass.getSimpleName}", text)
      case _ if text.startsWith("validity of output") =>
        VarkaFailureSignature("validity mismatch", text)
      case _ if text.startsWith("selection row") =>
        VarkaFailureSignature("selection mismatch", text)
      case _ if text.startsWith("output") && text.contains("differs") =>
        VarkaFailureSignature("output mismatch", text)
      case _ if text.startsWith("the kernel declined the batch") =>
        VarkaFailureSignature("declined batch", text)
      case _ if text.startsWith("the emitter") =>
        VarkaFailureSignature("emitter rejection", text)
      case _ => VarkaFailureSignature(t.getClass.getSimpleName, text)
    }
  }
}
