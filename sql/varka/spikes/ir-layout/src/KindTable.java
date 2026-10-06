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

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The IR's kinds as a fixed table, read off {@link VarkaVectorIR}'s records by reflection (the same
 * way {@link VarkaIrDescription} reads them), so that a layout can be built for any kind without
 * a line of code for it. A kind's index is its position among the kinds' simple names in order,
 * which is stable until a kind is added; a flat row stores the index and nothing else of the type.
 *
 * <p>A record's components are its children, which are nodes, and its scalars, which are an
 * {@code int}, a {@code long}, a {@code boolean}, an enum constant, or the one list of ints.
 */
final class KindTable {

  /** What a scalar component is. */
  enum Scalar { INT, LONG, BOOL, ENUM, LIST }

  /** One kind: its index and name, how many children it has, and its scalars in component order. */
  record Kind(int index, String name, int children, List<Scalar> scalars, List<Class<?>> enums) {
    Kind {
      scalars = List.copyOf(scalars);
      enums = new ArrayList<>(enums);
    }

    /** The scalars that are not the list, which is stored apart. */
    int plainScalars() {
      return (int) scalars.stream().filter(s -> s != Scalar.LIST).count();
    }

    boolean hasList() {
      return scalars.contains(Scalar.LIST);
    }

    /** Whether a scalar is wider than a flat row's small field, so that it does not fit a word. */
    boolean wide() {
      long ints = scalars.stream().filter(s -> s == Scalar.INT).count();
      return hasList() || scalars.contains(Scalar.LONG) || ints > 1;
    }
  }

  private final List<Kind> kinds = new ArrayList<>();
  private final Map<String, Kind> byName = new TreeMap<>();

  KindTable() {
    int index = 0;
    for (String name : VarkaIrDescription.allKinds()) {
      Class<?> type = VarkaIrDescription.kindOf(name);
      int children = 0;
      var scalars = new ArrayList<Scalar>();
      var enums = new ArrayList<Class<?>>();
      for (RecordComponent component : type.getRecordComponents()) {
        Class<?> componentType = component.getType();
        if (VarkaVectorIR.class.isAssignableFrom(componentType)) {
          children++;
        } else if (componentType == int.class) {
          scalars.add(Scalar.INT);
          enums.add(null);
        } else if (componentType == long.class) {
          scalars.add(Scalar.LONG);
          enums.add(null);
        } else if (componentType == boolean.class) {
          scalars.add(Scalar.BOOL);
          enums.add(null);
        } else if (componentType.isEnum()) {
          scalars.add(Scalar.ENUM);
          enums.add(componentType);
        } else if (componentType == List.class) {
          scalars.add(Scalar.LIST);
          enums.add(null);
        } else {
          throw new IllegalStateException(name + " has a component of type " + componentType);
        }
      }
      if (children > 3) {
        throw new IllegalStateException(name + " has " + children + " children; a row holds 3");
      }
      Kind kind = new Kind(index++, name, children, scalars, enums);
      kinds.add(kind);
      byName.put(name, kind);
    }
  }

  int size() {
    return kinds.size();
  }

  Kind kind(int index) {
    return kinds.get(index);
  }

  Kind kind(String name) {
    Kind kind = byName.get(name);
    if (kind == null) {
      throw new IllegalArgumentException("no IR kind named " + name);
    }
    return kind;
  }
}
