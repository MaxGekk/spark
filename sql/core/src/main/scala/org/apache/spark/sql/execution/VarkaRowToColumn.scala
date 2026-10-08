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

package org.apache.spark.sql.execution

import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, MapType, NullType, StructField,
  StructType}

/**
 * Spark's [[RowToColumnConverter]], for the schemas Varka's row fallbacks meet (VARKA-299).
 *
 * The evaluators answer a declined batch a row at a time and write the survivors back into
 * column vectors with the converter. The converter has no case for `NullType`, which Spark's own
 * columnar paths never carry; but a relation can (`SELECT NULL AS n`), the Arrow cache holds it,
 * and the kernels pass it through, so the fallback met it and raised `UNSUPPORTED_DATATYPE` where
 * Spark answers. The vectors support `NullType` (allocation, `appendNull`, reading a null back);
 * only the converter does not.
 *
 * Every cell of a `NullType` column is null, so the converter is built over the schema with each
 * `NullType` - at any depth - replaced by a nullable `BooleanType`: its null check runs first and
 * appends a null, which is exactly what a `NullType` cell is. The vectors keep the real schema.
 */
private[execution] object VarkaRowToColumn {

  def apply(schema: StructType): RowToColumnConverter =
    new RowToColumnConverter(withoutNullType(schema))

  /** `schema` with every `NullType` made a nullable `BooleanType`. */
  def withoutNullType(schema: StructType): StructType =
    StructType(schema.fields.map(f => StructField(f.name, replace(f.dataType),
      f.nullable || f.dataType == NullType, f.metadata)))

  private def replace(dataType: DataType): DataType = dataType match {
    case NullType => BooleanType
    case ArrayType(element, containsNull) =>
      ArrayType(replace(element), containsNull || element == NullType)
    case MapType(key, value, valueContainsNull) =>
      MapType(replace(key), replace(value), valueContainsNull || value == NullType)
    case struct: StructType => withoutNullType(struct)
    case other => other
  }
}
