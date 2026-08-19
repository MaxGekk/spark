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

import java.lang.classfile.ClassFile;
import java.lang.classfile.constantpool.ClassEntry;
import java.lang.constant.ClassDesc;
import java.lang.reflect.AccessFlag;

/**
 * Task 5 test helper: asserts that the bytes assembled by
 * {@code JavaClassFileEngine.assembleGeneratedClass} expose the full {@code GeneratedClass}
 * shape -- a public wrapper extending {@code GeneratedClass} with a default constructor and
 * {@code generate(Object[])}, plus a public {@code SpecificVarkaProjection} extending
 * {@code UnsafeProjection} with a {@code references} field and the {@code apply}/{@code
 * initialize} methods. A Java helper is used because Scala 2.13 hits a cyclic-reference bug
 * when it reads the sealed Class-File hierarchy; for the same reason no Class-File type
 * appears in any method signature here (each helper parses its own {@code byte[]}).
 */
public final class ClassFileShapeVerifier {

  private static final ClassDesc GENERATED_CLASS =
      ClassDesc.of("org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass");
  private static final ClassDesc UNSAFE_PROJECTION =
      ClassDesc.of("org.apache.spark.sql.catalyst.expressions.UnsafeProjection");
  private static final ClassDesc OBJECT_ARRAY =
      ClassDesc.ofDescriptor("[Ljava/lang/Object;");

  private static final String WRAPPER_GENERATE =
      "([Ljava/lang/Object;)Ljava/lang/Object;";
  private static final String SPEC_INIT = "([Ljava/lang/Object;)V";
  private static final String SPEC_APPLY = "(Lorg/apache/spark/sql/catalyst/InternalRow;)"
      + "Lorg/apache/spark/sql/catalyst/expressions/UnsafeRow;";
  private static final String SPEC_INITIALIZE = "(I)V";

  private ClassFileShapeVerifier() {
  }

  public static void assertGeneratedClassShape(byte[] wrapperBytes, byte[] specBytes) {
    assertWrapperShape(wrapperBytes);
    assertProjectionShape(specBytes);
  }

  private static void assertWrapperShape(byte[] bytes) {
    var model = ClassFile.of().parse(bytes);
    if ((model.flags().flagsMask() & AccessFlag.PUBLIC.mask()) == 0) {
      throw new AssertionError("wrapper class is not public");
    }
    ClassEntry superclass = model.superclass().orElse(null);
    if (superclass == null || !superclass.asSymbol().equals(GENERATED_CLASS)) {
      throw new AssertionError(
          "wrapper superclass must be " + GENERATED_CLASS.displayName() + ", got " + superclass);
    }
    assertHasMethod(bytes, "<init>", "()V", true, "wrapper default constructor");
    assertHasMethod(bytes, "generate", WRAPPER_GENERATE, true, "wrapper generate");
  }

  private static void assertProjectionShape(byte[] bytes) {
    var model = ClassFile.of().parse(bytes);
    if ((model.flags().flagsMask() & AccessFlag.PUBLIC.mask()) == 0) {
      throw new AssertionError("projection class is not public");
    }
    ClassEntry superclass = model.superclass().orElse(null);
    if (superclass == null || !superclass.asSymbol().equals(UNSAFE_PROJECTION)) {
      throw new AssertionError(
          "projection superclass must be " + UNSAFE_PROJECTION.displayName()
              + ", got " + superclass);
    }
    boolean hasField = model.fields().stream()
        .anyMatch(f -> f.fieldName().stringValue().equals("references")
            && f.fieldTypeSymbol().equals(OBJECT_ARRAY));
    if (!hasField) {
      throw new AssertionError("missing field references:" + OBJECT_ARRAY.displayName());
    }
    assertHasMethod(bytes, "<init>", SPEC_INIT, true, "projection constructor");
    assertHasMethod(bytes, "apply", SPEC_APPLY, true, "projection apply");
    assertHasMethod(bytes, "initialize", SPEC_INITIALIZE, true, "projection initialize");
  }

  private static void assertHasMethod(
      byte[] bytes, String name, String descriptor, boolean isPublic, String what) {
    var model = ClassFile.of().parse(bytes);
    var found = model.methods().stream()
        .filter(m -> m.methodName().stringValue().equals(name)
            && m.methodType().stringValue().equals(descriptor))
        .toList();
    if (found.isEmpty()) {
      throw new AssertionError("missing method " + name + descriptor + " (" + what + ")");
    }
    if (isPublic
        && (found.get(0).flags().flagsMask() & AccessFlag.PUBLIC.mask()) == 0) {
      throw new AssertionError("method " + name + descriptor + " (" + what + ") is not public");
    }
  }
}