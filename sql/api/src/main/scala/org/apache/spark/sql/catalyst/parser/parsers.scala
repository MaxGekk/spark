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
package org.apache.spark.sql.catalyst.parser

import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.{ATN, ParserATNSimulator, PredictionContextCache, PredictionMode}
import org.antlr.v4.runtime.dfa.DFA
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.TerminalNodeImpl

import org.apache.spark.{QueryContext, SparkException, SparkThrowable, SparkThrowableHelper}
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin, SQLQueryContext, WithOrigin}
import org.apache.spark.sql.catalyst.util.SparkParserUtils
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Base SQL parsing infrastructure.
 */
abstract class AbstractParser extends DataTypeParserInterface with Logging {

  /** Creates/Resolves DataType for a given SQL string. */
  override def parseDataType(sqlText: String): DataType = parse(sqlText) { parser =>
    astBuilder.visitSingleDataType(parser.singleDataType())
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = parse(sqlText) { parser =>
    astBuilder.visitSingleTableSchema(parser.singleTableSchema())
  }

  /** Get the builder (visitor) which converts a ParseTree into an AST. */
  protected def astBuilder: DataTypeAstBuilder

  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logDebug(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    if (conf.manageParserCaches) AbstractParser.installCaches(parser)
    parser.addParseListener(PostProcessor)
    parser.addParseListener(UnclosedCommentProcessor(command, tokenStream))
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.legacy_setops_precedence_enabled = conf.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = conf.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = conf.enforceReservedKeywords
    parser.double_quoted_identifiers = conf.doubleQuotedIdentifiers

    // https://github.com/antlr/antlr4/issues/192#issuecomment-15238595
    // Save a great deal of time on correct inputs by using a two-stage parsing strategy.
    try {
      try {
        // first, try parsing with potentially faster SLL mode w/ SparkParserBailErrorStrategy
        parser.setErrorHandler(new SparkParserBailErrorStrategy())
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      } catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode w/ SparkParserErrorStrategy
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.setErrorHandler(new SparkParserErrorStrategy())
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    } catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: SparkThrowable with WithOrigin =>
        throw new ParseException(
          command = Option(command),
          start = e.origin,
          errorClass = e.getCondition,
          messageParameters = e.getMessageParameters.asScala.toMap,
          queryContext = e.getQueryContext)
    } finally {
      // Antlr4 uses caches to make parsing faster but its caches are unbounded and never purged,
      // which can cause OOMs when parsing a huge number of SQL queries. Clearing these caches too
      // often will slow down parsing and cause performance regressions, but will prevent OOMs
      // caused by the parser cache. We use a heuristic and clear the cache if the number of states
      // in the DFA cache has exceeded the threshold
      // configured by `spark.sql.parser.parserDfaCacheFlushThreshold`. These states generally
      // represent the bulk of the memory consumed by the parser, and the size of a single state
      // is approximately `BYTES_PER_DFA_STATE` bytes.
      //
      // Negative values mean we should never clear the cache
      AbstractParser.maybeClearParserCaches(parser, conf)
    }
  }

  private def conf: SqlApiConf = SqlApiConf.get
}

/**
 * This string stream provides the lexer with upper case characters only. This greatly simplifies
 * lexing the stream, while we can maintain the original command.
 *
 * This is based on Hive's org.apache.hadoop.hive.ql.parse.ParseDriver.ANTLRNoCaseStringStream
 *
 * The comment below (taken from the original class) describes the rationale for doing this:
 *
 * This class provides and implementation for a case insensitive token checker for the lexical
 * analysis part of antlr. By converting the token stream into upper case at the time when lexical
 * rules are checked, this class ensures that the lexical rules need to just match the token with
 * upper case letters as opposed to combination of upper case and lower case characters. This is
 * purely used for matching lexical rules. The actual token text is stored in the same way as the
 * user input without actually converting it into an upper case. The token values are generated by
 * the consume() function of the super class ANTLRStringStream. The LA() function is the lookahead
 * function and is purely used for matching lexical rules. This also means that the grammar will
 * only accept capitalized tokens in case it is run from other tools like antlrworks which do not
 * have the UpperCaseCharStream implementation.
 */

private[parser] class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

/**
 * The ParseErrorListener converts parse errors into ParseExceptions.
 */
case object ParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val start = offendingSymbol match {
      case token: CommonToken =>
        Origin(Some(line), Some(token.getCharPositionInLine))
      case _ =>
        Origin(Some(line), Some(charPositionInLine))
    }
    e match {
      case sre: SparkRecognitionException if sre.errorClass.isDefined =>
        throw new ParseException(None, start, sre.errorClass.get, sre.messageParameters)
      case _ =>
        throw new ParseException(
          command = None,
          start = start,
          errorClass = "PARSE_SYNTAX_ERROR",
          messageParameters = Map("error" -> msg, "hint" -> ""))
    }
  }
}

/**
 * A [[ParseException]] is an [[SparkException]] that is thrown during the parse process. It
 * contains fields and an extended error message that make reporting and diagnosing errors easier.
 */
class ParseException private (
    val command: Option[String],
    message: String,
    val start: Origin,
    errorClass: Option[String] = None,
    messageParameters: Map[String, String] = Map.empty,
    queryContext: Array[QueryContext] = ParseException.getQueryContext())
    extends AnalysisException(
      message,
      start.line,
      start.startPosition,
      None,
      errorClass,
      messageParameters,
      queryContext) {

  def this(errorClass: String, messageParameters: Map[String, String], ctx: ParserRuleContext) =
    this(
      Option(SparkParserUtils.command(ctx)),
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      SparkParserUtils.position(ctx.getStart),
      Some(errorClass),
      messageParameters)

  def this(errorClass: String, ctx: ParserRuleContext) =
    this(errorClass = errorClass, messageParameters = Map.empty, ctx = ctx)

  /** Compose the message through SparkThrowableHelper given errorClass and messageParameters. */
  def this(
      command: Option[String],
      start: Origin,
      errorClass: String,
      messageParameters: Map[String, String]) =
    this(
      command,
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      start,
      Some(errorClass),
      messageParameters,
      queryContext = ParseException.getQueryContext())

  def this(
      command: Option[String],
      start: Origin,
      errorClass: String,
      messageParameters: Map[String, String],
      queryContext: Array[QueryContext]) =
    this(
      command,
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      start,
      Some(errorClass),
      messageParameters,
      queryContext)

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    if (queryContext.nonEmpty) {
      builder ++= "\n"
      queryContext.foreach { ctx =>
        builder ++= ctx.summary()
      }
    } else {
      start match {
        case Origin(Some(l), Some(p), _, _, _, _, _, _, _) =>
          builder ++= s" (line $l, pos $p)\n"
          command.foreach { cmd =>
            val (above, below) = cmd.split("\n").splitAt(l)
            builder ++= "\n== SQL ==\n"
            above.foreach(builder ++= _ += '\n')
            builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
            below.foreach(builder ++= _ += '\n')
          }
        case _ =>
          command.foreach { cmd =>
            builder ++= "\n== SQL ==\n" ++= cmd
          }
      }
    }
    builder.toString
  }

  def withCommand(cmd: String): ParseException = {
    val cl = getCondition
    val (newCl, params) = if (cl == "PARSE_SYNTAX_ERROR" && cmd.trim().isEmpty) {
      // PARSE_EMPTY_STATEMENT error class overrides the PARSE_SYNTAX_ERROR when cmd is empty
      ("PARSE_EMPTY_STATEMENT", Map.empty[String, String])
    } else {
      (cl, messageParameters)
    }
    new ParseException(Option(cmd), start, newCl, params, queryContext)
  }

  override def getQueryContext: Array[QueryContext] = queryContext

  override def getCondition: String = errorClass.getOrElse {
    throw SparkException.internalError("ParseException shall have an error class.")
  }
}

object ParseException {
  def getQueryContext(): Array[QueryContext] = {
    Some(CurrentOrigin.get.context).collect { case b: SQLQueryContext if b.isValid => b }.toArray
  }
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
case object PostProcessor extends SqlBaseParserBaseListener {

  /** Throws error message when exiting a explicitly captured wrong identifier rule */
  override def exitErrorIdent(ctx: SqlBaseParser.ErrorIdentContext): Unit = {
    val ident = ctx.getParent.getText

    throw QueryParsingErrors.invalidIdentifierError(ident, ctx)
  }

  /**
   * Throws error message when unquoted identifier contains characters outside a-z, A-Z, 0-9, _
   */
  override def exitUnquotedIdentifier(ctx: SqlBaseParser.UnquotedIdentifierContext): Unit = {
    val ident = ctx.getText
    if (ident.exists(c =>
        !(c >= 'a' && c <= 'z') &&
          !(c >= 'A' && c <= 'Z') &&
          !(c >= '0' && c <= '9') &&
          c != '_')) {
      throw QueryParsingErrors.invalidIdentifierError(ident, ctx)
    }
  }

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Unit = {
    if (ctx.BACKQUOTED_IDENTIFIER() != null) {
      replaceTokenByIdentifier(ctx, 1) { token =>
        // Remove the double back ticks in the string.
        token.setText(token.getText.replace("``", "`"))
        token
      }
    } else if (ctx.DOUBLEQUOTED_STRING() != null) {
      replaceTokenByIdentifier(ctx, 1) { token =>
        // Remove the double quotes in the string.
        token.setText(token.getText.replace("\"\"", "\""))
        token
      }
    }
  }

  /** Remove the back ticks from an Identifier. */
  override def exitBackQuotedIdentifier(ctx: SqlBaseParser.BackQuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: SqlBaseParser.NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(ctx: ParserRuleContext, stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      SqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}

/**
 * The post-processor checks the unclosed bracketed comment.
 */
case class UnclosedCommentProcessor(command: String, tokenStream: CommonTokenStream)
    extends SqlBaseParserBaseListener {

  override def exitSingleDataType(ctx: SqlBaseParser.SingleDataTypeContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleExpression(ctx: SqlBaseParser.SingleExpressionContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleTableIdentifier(
      ctx: SqlBaseParser.SingleTableIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleFunctionIdentifier(
      ctx: SqlBaseParser.SingleFunctionIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleMultipartIdentifier(
      ctx: SqlBaseParser.SingleMultipartIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleTableSchema(ctx: SqlBaseParser.SingleTableSchemaContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitQuery(ctx: SqlBaseParser.QueryContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): Unit = {
    // SET command uses a wildcard to match anything, and we shouldn't parse the comments, e.g.
    // `SET myPath =/a/*`.
    if (!ctx.setResetStatement().isInstanceOf[SqlBaseParser.SetConfigurationContext]) {
      checkUnclosedComment(tokenStream, command)
    }
  }

  override def exitCompoundOrSingleStatement(
      ctx: SqlBaseParser.CompoundOrSingleStatementContext): Unit = {
    // Same as in exitSingleStatement, we shouldn't parse the comments in SET command.
    if (Option(ctx.singleStatement()).forall(
        !_.setResetStatement().isInstanceOf[SqlBaseParser.SetConfigurationContext])) {
      checkUnclosedComment(tokenStream, command)
    }
  }

  override def exitSingleCompoundStatement(
      ctx: SqlBaseParser.SingleCompoundStatementContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  /** check `has_unclosed_bracketed_comment` to find out the unclosed bracketed comment. */
  private def checkUnclosedComment(tokenStream: CommonTokenStream, command: String) = {
    assert(tokenStream.getTokenSource.isInstanceOf[SqlBaseLexer])
    val lexer = tokenStream.getTokenSource.asInstanceOf[SqlBaseLexer]
    if (lexer.has_unclosed_bracketed_comment) {
      // The last token is 'EOF' and the penultimate is unclosed bracketed comment
      val failedToken = tokenStream.get(tokenStream.size() - 2)
      assert(failedToken.getType() == SqlBaseParser.BRACKETED_COMMENT)
      val position =
        Origin(Option(failedToken.getLine), Option(failedToken.getCharPositionInLine))
      throw QueryParsingErrors.unclosedBracketedCommentError(
        command = command,
        start = Origin(Option(failedToken.getStartIndex)),
        stop = Origin(Option(failedToken.getStopIndex)))
    }
  }
}

object DataTypeParser extends AbstractParser {
  override protected def astBuilder: DataTypeAstBuilder = new DataTypeAstBuilder
}

object AbstractParser extends Logging {
  // Approximation based on experiments. Used to estimate the size of the DFA cache for the
  // `parserDfaCacheFlushRatio` threshold.
  final val BYTES_PER_DFA_STATE = 9700

  private val DRIVER_MEMORY = Runtime.getRuntime.maxMemory()

  private case class AntlrCaches(atn: ATN) {
    private[parser] val predictionContextCache: PredictionContextCache =
      new PredictionContextCache
    private[parser] val decisionToDFACache: Array[DFA] = AntlrCaches.makeDecisionToDFACache(atn)

    def installManagedParserCaches(parser: SqlBaseParser): Unit = {
      parser.setInterpreter(
        new ParserATNSimulator(parser, atn, decisionToDFACache, predictionContextCache))
    }
  }

  private object AntlrCaches {
    private def makeDecisionToDFACache(atn: ATN): Array[DFA] = {
      val decisionToDFA = new Array[DFA](atn.getNumberOfDecisions)
      for (i <- 0 until atn.getNumberOfDecisions) {
        decisionToDFA(i) = new DFA(atn.getDecisionState(i), i)
      }
      decisionToDFA
    }
  }

  private val parserCaches = new AtomicReference[AntlrCaches](AntlrCaches(SqlBaseParser._ATN))

  private var numDFACacheStates: Long = 0
  def getDFACacheNumStates: Long = numDFACacheStates

  /**
   * Returns the number of DFA states in the DFA cache.
   *
   * DFA states empirically consume about `BYTES_PER_DFA_STATE` bytes of memory each.
   */
  private def computeDFACacheNumStates: Long = {
    parserCaches.get().decisionToDFACache.map(_.states.size).sum
  }

  /**
   * Install the managed parser caches into the given parser. Configuring the parser to use the
   * managed `AntlrCaches` enables us to manage the size of the cache and clear it when required
   * as the parser caches are unbounded by default.
   *
   * This method should be called before parsing any input.
   */
  private[parser] def installCaches(parser: SqlBaseParser): Unit = {
    parserCaches.get().installManagedParserCaches(parser)
  }

  /**
   * Drop the existing parser caches and create a new one.
   *
   * ANTLR retains caches in its parser that are never released. This speeds up parsing of future
   * input, but it can consume a lot of memory depending on the input seen so far.
   *
   * This method provides a mechanism to free the retained caches, which can be useful after
   * parsing very large SQL inputs, especially if those large inputs are unlikely to be similar to
   * future inputs seen by the driver.
   */
  private[parser] def clearParserCaches(parser: SqlBaseParser): Unit = {
    parserCaches.set(AntlrCaches(SqlBaseParser._ATN))
    logInfo(log"ANTLR parser caches cleared")
    numDFACacheStates = 0
    installCaches(parser)
  }

  /**
   * Check cache size and config values to determine if we should clear the parser caches. Also
   * logs the current cache size and the delta since the last check. This method should be called
   * after parsing each input.
   */
  private[parser] def maybeClearParserCaches(parser: SqlBaseParser, conf: SqlApiConf): Unit = {
    if (!conf.manageParserCaches) {
      return
    }

    val numDFACacheStatesCurrent: Long = computeDFACacheNumStates
    val numDFACacheStatesDelta = numDFACacheStatesCurrent - numDFACacheStates
    numDFACacheStates = numDFACacheStatesCurrent
    logInfo(
      log"EXPERIMENTAL: Query cached " +
        log"${MDC(LogKeys.ANTLR_DFA_CACHE_DELTA, numDFACacheStatesDelta)} " +
        log"DFA states in the parser. Total cached DFA states: " +
        log"${MDC(LogKeys.ANTLR_DFA_CACHE_SIZE, numDFACacheStatesCurrent)}." +
        log"Driver memory: ${MDC(LogKeys.DRIVER_JVM_MEMORY, DRIVER_MEMORY)}.")

    val staticThresholdExceeded = 0 <= conf.parserDfaCacheFlushThreshold &&
      conf.parserDfaCacheFlushThreshold <= numDFACacheStatesCurrent

    val estCacheBytes: Long = numDFACacheStatesCurrent * BYTES_PER_DFA_STATE
    if (estCacheBytes < 0) {
      logWarning(log"Estimated cache size is negative, likely due to an integer overflow.")
    }
    val dynamicThresholdExceeded = 0 <= conf.parserDfaCacheFlushRatio &&
      conf.parserDfaCacheFlushRatio * DRIVER_MEMORY / 100 <= estCacheBytes

    if (staticThresholdExceeded || dynamicThresholdExceeded) {
      AbstractParser.clearParserCaches(parser)
    }
  }
}
