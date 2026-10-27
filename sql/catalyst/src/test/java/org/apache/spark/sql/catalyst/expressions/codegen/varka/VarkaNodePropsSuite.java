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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaNodeProps.Validity;
import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.Cond;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Every IR node answers its own {@link VarkaNodeProps}: the interface method has no default, so a
 * new record that forgets it does not compile, and these tests keep that guarantee from being
 * softened by a default added later, on the interface or on one of its sealed families. The
 * second test holds the validity rules to the operand shapes the passes that fold them assume.
 */
@VarkaSuite
public final class VarkaNodePropsSuite {

  /** The records under {@code type}, walking its sealed families. */
  private static void leaves(Class<?> type, List<Class<?>> into) {
    if (type.isRecord()) {
      into.add(type);
      return;
    }
    for (Class<?> sub : type.getPermittedSubclasses()) {
      leaves(sub, into);
    }
  }

  @Test
  @DisplayName("every IR record declares its own props and no family supplies a default")
  void everyNodeDecides() throws ReflectiveOperationException {
    Method props = VarkaVectorIR.class.getMethod("props");
    assertTrue(Modifier.isAbstract(props.getModifiers()), "props() must stay abstract");
    assertFalse(props.isDefault(), "props() must have no default");
    for (Class<?> family : List.of(Cond.class, VarkaVectorIR.Chrono.class)) {
      for (Method m : family.getDeclaredMethods()) {
        assertFalse(m.getName().equals("props"), family.getSimpleName() + " defines props()");
      }
    }
    var records = new ArrayList<Class<?>>();
    leaves(VarkaVectorIR.class, records);
    assertTrue(records.size() > 40, "the walk found only " + records.size() + " records");
    for (Class<?> r : records) {
      Method own = r.getDeclaredMethod("props");
      assertEquals(VarkaNodeProps.class, own.getReturnType(), r.getSimpleName());
    }
  }

  @Test
  @DisplayName("a strict or null-skipping node's operands are values, and a condition has no word")
  void validityShapes() {
    int seen = 0;
    for (int seed = 0; seed < 500; seed++) {
      var roots = new ArrayList<VarkaVectorIR>();
      Random rnd = new Random(seed);
      roots.addAll(seed % 2 == 0
          ? VarkaIrGrammar.drawShape(rnd).roots()
          : VarkaIrGrammar.drawMixedShape(rnd).roots());
      for (VarkaVectorIR root : roots) {
        var nodes = new ArrayList<VarkaVectorIR>();
        VarkaVariants.walk(root, nodes::add);
        for (VarkaVectorIR n : nodes) {
          seen++;
          String at = VarkaVectorIR.canonical(n);
          VarkaVectorIR[] children = VarkaVectorIR.childrenOf(n);
          switch (n.props().validity()) {
            case Validity.Strict _, Validity.SkipsNull _ -> {
              assertTrue(children.length > 0, at);
              for (VarkaVectorIR c : children) {
                assertFalse(c instanceof Cond, at);
              }
            }
            case Validity.Input in -> {
              assertEquals(0, children.length, at);
              assertEquals(in.ordinal(), VarkaVectorIR.inputOrdinal(n), at);
            }
            case Validity.AllTrue _ -> assertEquals(0, children.length, at);
            case Validity.NoWord _ -> assertTrue(n instanceof Cond, at);
            case Validity.Own _ -> assertFalse(n instanceof Cond, at);
          }
        }
      }
    }
    assertTrue(seen > 0);
  }
}
