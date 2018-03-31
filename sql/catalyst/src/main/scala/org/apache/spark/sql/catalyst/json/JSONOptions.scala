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

package org.apache.spark.sql.catalyst.json

import java.util.{Locale, TimeZone}

import com.fasterxml.jackson.core.{JsonFactory, JsonParser}
import org.apache.commons.lang3.time.FastDateFormat

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util._

/**
 * Options for parsing JSON data into Spark SQL rows.
 *
 * Most of these map directly to Jackson's internal options, specified in [[JsonParser.Feature]].
 */
private[sql] class JSONOptions(
    @transient private val parameters: CaseInsensitiveMap[String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String)
  extends Logging with Serializable  {

  def this(
    parameters: Map[String, String],
    defaultTimeZoneId: String,
    defaultColumnNameOfCorruptRecord: String = "") = {
      this(
        CaseInsensitiveMap(parameters),
        defaultTimeZoneId,
        defaultColumnNameOfCorruptRecord)
  }

  val samplingRatio =
    parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
  val primitivesAsString =
    parameters.get("primitivesAsString").map(_.toBoolean).getOrElse(false)
  val prefersDecimal =
    parameters.get("prefersDecimal").map(_.toBoolean).getOrElse(false)
  val allowComments =
    parameters.get("allowComments").map(_.toBoolean).getOrElse(false)
  val allowUnquotedFieldNames =
    parameters.get("allowUnquotedFieldNames").map(_.toBoolean).getOrElse(false)
  val allowSingleQuotes =
    parameters.get("allowSingleQuotes").map(_.toBoolean).getOrElse(true)
  val allowNumericLeadingZeros =
    parameters.get("allowNumericLeadingZeros").map(_.toBoolean).getOrElse(false)
  val allowNonNumericNumbers =
    parameters.get("allowNonNumericNumbers").map(_.toBoolean).getOrElse(true)
  val allowBackslashEscapingAnyCharacter =
    parameters.get("allowBackslashEscapingAnyCharacter").map(_.toBoolean).getOrElse(false)
  private val allowUnquotedControlChars =
    parameters.get("allowUnquotedControlChars").map(_.toBoolean).getOrElse(false)
  val compressionCodec = parameters.get("compression").map(CompressionCodecs.getCodecClassName)
  val parseMode: ParseMode =
    parameters.get("mode").map(ParseMode.fromString).getOrElse(PermissiveMode)
  val columnNameOfCorruptRecord =
    parameters.getOrElse("columnNameOfCorruptRecord", defaultColumnNameOfCorruptRecord)

  val timeZone: TimeZone = DateTimeUtils.getTimeZone(
    parameters.getOrElse(DateTimeUtils.TIMEZONE_OPTION, defaultTimeZoneId))

  // Uses `FastDateFormat` which can be direct replacement for `SimpleDateFormat` and thread-safe.
  val dateFormat: FastDateFormat =
    FastDateFormat.getInstance(parameters.getOrElse("dateFormat", "yyyy-MM-dd"), Locale.US)

  val timestampFormat: FastDateFormat =
    FastDateFormat.getInstance(
      parameters.getOrElse("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"), timeZone, Locale.US)

  val multiLine = parameters.get("multiLine").map(_.toBoolean).getOrElse(false)

  /**
   * Standard charset name. For example UTF-8, UTF-16LE and UTF-32BE.
   * If the encoding is not specified (None), it will be detected automatically.
   */
  val encoding: Option[String] = parameters.get("encoding")
    .orElse(parameters.get("charset")).map { enc =>
    val blacklist = List("UTF16", "UTF32")
    val isBlacklisted = blacklist.contains(enc.toUpperCase.replaceAll("-|_", ""))
    require(multiLine || !isBlacklisted,
      s"""The ${enc} encoding must not be included in the blacklist:
         | ${blacklist.mkString(", ")}""".stripMargin)

    enc
  }

  /**
   * A sequence of bytes between two consecutive json objects.
   * Format of the option is:
   *   selector (1 char) + separator spec (any length) | sequence of chars
   *
   * Currently the following selectors are supported:
   * - 'x' + sequence of bytes in hexadecimal format. For example: "x0a 0d".
   *   Hex pairs can be separated by any chars different from 0-9,A-F,a-f
   * - '\' - reserved for a sequence of control chars like "\r\n"
   *         and unicode escape like "\u000D\u000A"
   * - 'r' and '/' - reserved for future use
   */
  val lineSeparator: Option[Array[Byte]] = parameters.get("lineSep").collect {
    case hexs if hexs.startsWith("x") =>
      hexs.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray
        .map(Integer.parseInt(_, 16).toByte)
    case reserved if reserved.startsWith("r") || reserved.startsWith("/") =>
      throw new NotImplementedError(s"The $reserved selector has not supported yet")
    case "" => throw new IllegalArgumentException("lineSep cannot be empty string")
    case lineSep => lineSep.getBytes(encoding.getOrElse("UTF-8"))
  }.orElse {
    val forcingLineSep = multiLine || encoding.isEmpty || encoding == Some("UTF-8")
    require(forcingLineSep,
      s"""The lineSep option must be specified for the ${encoding.get} encoding.
         |Example: .option("lineSep", "|^|")
         |Note: lineSep can be detected automatically for UTF-8 only.""".stripMargin)
    None
  }

  /**
   * A sequence of bytes between two consecutive json objects used by JSON Reader to
   * split input stream/text.
   */
  val lineSeparatorInRead: Option[Array[Byte]] = lineSeparator
  /**
   * JSON Writer puts the string between json objects in output stream/text.
   */
  val lineSeparatorInWrite: Option[Array[Byte]] = lineSeparator

  /** Sets config options on a Jackson [[JsonFactory]]. */
  def setJacksonOptions(factory: JsonFactory): Unit = {
    factory.configure(JsonParser.Feature.ALLOW_COMMENTS, allowComments)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, allowUnquotedFieldNames)
    factory.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, allowSingleQuotes)
    factory.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, allowNumericLeadingZeros)
    factory.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, allowNonNumericNumbers)
    factory.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER,
      allowBackslashEscapingAnyCharacter)
    factory.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, allowUnquotedControlChars)
  }

  def getTextOptions: Map[String, String] = {
    Map[String, String]() ++ encoding.map("encoding" -> _) ++
      lineSeparator.map("lineSep" -> _.map("x%02x".format(_)).mkString)
  }
}
