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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{UTF8String}

/**
 * A typed column reader that reads from an Arrow FieldVector and writes directly
 * to an UnsafeRowWriter, avoiding per-row pattern matching overhead.
 */
private abstract class ArrowColumnReader {
  def vector: org.apache.arrow.vector.FieldVector
  def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit
  def setVector(v: org.apache.arrow.vector.FieldVector): Unit
}

private object ArrowColumnReader {
  import org.apache.arrow.vector._

  def create(dataType: DataType): ArrowColumnReader = dataType match {
    case BooleanType => new ArrowColumnReader {
      private var _vector: BitVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[BitVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex) != 0)
    }
    case ByteType => new ArrowColumnReader {
      private var _vector: TinyIntVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[TinyIntVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case ShortType => new ArrowColumnReader {
      private var _vector: SmallIntVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[SmallIntVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case IntegerType | DateType | _: YearMonthIntervalType => new ArrowColumnReader {
      private var _vector: FieldVector = _
      // Pre-bind accessor at setVector time to avoid per-row pattern match
      private var _accessor: Int => Int = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = {
        _vector = v
        _accessor = v match {
          case iv: IntVector => iv.get
          case dv: DateDayVector => dv.get
          case iv: org.apache.arrow.vector.IntervalYearVector => iv.get
        }
      }
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _accessor(rowIndex))
    }
    case LongType | TimestampType | TimestampNTZType | _: DayTimeIntervalType | _: TimeType =>
      new ArrowColumnReader {
      private var _vector: FieldVector = _
      private var _accessor: Int => Long = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = {
        _vector = v
        _accessor = v match {
          case bv: BigIntVector => bv.get(_)
          case tv: TimeStampMicroTZVector => tv.get(_)
          case tv: TimeStampMicroVector => tv.get(_)
          case dv: org.apache.arrow.vector.DurationVector =>
            i => org.apache.arrow.vector.DurationVector.get(dv.getDataBuffer, i)
          case tv: org.apache.arrow.vector.TimeNanoVector => tv.get(_)
        }
      }
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _accessor(rowIndex))
    }
    case FloatType => new ArrowColumnReader {
      private var _vector: Float4Vector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[Float4Vector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case DoubleType => new ArrowColumnReader {
      private var _vector: Float8Vector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[Float8Vector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case _: StringType => new ArrowColumnReader {
      private var _vector: VarCharVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[VarCharVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
        val bytes = _vector.get(rowIndex)
        writer.write(ordinal, UTF8String.fromBytes(bytes))
      }
    }
    case BinaryType => new ArrowColumnReader {
      private var _vector: VarBinaryVector = _
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[VarBinaryVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit =
        writer.write(ordinal, _vector.get(rowIndex))
    }
    case dt: DecimalType if dt.precision <= Decimal.MAX_LONG_DIGITS =>
      // Fast path for compact decimals (precision <= 18):
      // Read the unscaled long directly from the Arrow buffer, zero allocation.
      // Arrow Java stores Decimal128 in the platform's native byte order, so the position of
      // the low-order word inside the 16-byte slot depends on endianness: first 8 bytes on
      // little-endian, last 8 bytes on big-endian (the other word is sign extension). ArrowBuf
      // getLong also reads in native order, so selecting the right word is all that is needed.
      // See ArrowCachedBatchSerializer.compactDecimalUnscaledOffset.
      new ArrowColumnReader {
        private var _vector: DecimalVector = _
        private var _dataBuffer: org.apache.arrow.memory.ArrowBuf = _
        private val typeWidth = DecimalVector.TYPE_WIDTH // 16 bytes
        private val unscaledOffset = ArrowCachedBatchSerializer
          .compactDecimalUnscaledOffset(java.nio.ByteOrder.nativeOrder())
        def vector: FieldVector = _vector
        def setVector(v: FieldVector): Unit = {
          _vector = v.asInstanceOf[DecimalVector]
          _dataBuffer = _vector.getDataBuffer
        }
        def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
          val startIndex = rowIndex.toLong * typeWidth
          val unscaledLong = _dataBuffer.getLong(startIndex + unscaledOffset)
          writer.write(ordinal, unscaledLong)
        }
      }
    case dt: DecimalType => new ArrowColumnReader {
      // Slow path for wide decimals (precision > 18): must go through BigDecimal
      private var _vector: DecimalVector = _
      private val precision = dt.precision
      private val scale = dt.scale
      def vector: FieldVector = _vector
      def setVector(v: FieldVector): Unit = _vector = v.asInstanceOf[DecimalVector]
      def read(rowIndex: Int, ordinal: Int, writer: UnsafeRowWriter): Unit = {
        val decimal = Decimal(_vector.getObject(rowIndex), precision, scale)
        writer.write(ordinal, decimal, precision, scale)
      }
    }
    case _ =>
      throw new UnsupportedOperationException(
        s"Complex type $dataType is handled by the fallback path")
  }
}
