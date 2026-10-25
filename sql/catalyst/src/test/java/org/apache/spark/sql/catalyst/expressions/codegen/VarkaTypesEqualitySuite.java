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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaSuite;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaTypes;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * While the type registry and the compiler's {@code laneOf} coexist (VARKA-494 step 1), the two
 * give the same lane for every form the registry holds and for the types it does not. Deleted
 * with {@code laneOf}.
 */
@VarkaSuite
public final class VarkaTypesEqualitySuite {

  @Test
  @DisplayName("the registry's lane is laneOf's for every form and for types outside the table")
  void registryAgreesWithLaneOf() {
    var types = new ArrayList<DataType>();
    VarkaTypes.all().forEach(i -> types.addAll(i.forms()));
    types.addAll(List.of(DataTypes.StringType, DataTypes.BinaryType, DataTypes.NullType,
        DataTypes.CalendarIntervalType, DataTypes.createArrayType(DataTypes.IntegerType)));
    for (DataType t : types) {
      assertEquals(VarkaNodeCompiler.laneOf(t), VarkaTypes.lane(t), t.sql());
      assertEquals(VarkaExpressionCompiler.readsColumnOf(t), VarkaTypes.lane(t).isPresent(),
          t.sql());
    }
  }
}
