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

import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;

/**
 * Assembles the Varka {@code GeneratedClass} shell with the Class-File API (Task 5): a public
 * wrapper extending {@code GeneratedClass} with {@code generate(Object[])}, plus a public
 * {@code VarkaProjection} extending {@code UnsafeProjection} with a {@code references}
 * field and the {@code apply}/{@code initialize} methods. A Java helper is used because Scala
 * 2.13 hits a cyclic-reference bug when it typechecks the Class-File API's sealed instruction
 * hierarchy.
 */
public final class ClassFileAssembler {

  private ClassFileAssembler() {
  }

  private static final ClassDesc GENERATED_CLASS =
      ClassDesc.of("org.apache.spark.sql.catalyst.expressions.codegen.GeneratedClass");
  private static final ClassDesc UNSAFE_PROJECTION =
      ClassDesc.of("org.apache.spark.sql.catalyst.expressions.UnsafeProjection");
  private static final ClassDesc INTERNAL_ROW =
      ClassDesc.of("org.apache.spark.sql.catalyst.InternalRow");
  private static final ClassDesc UNSAFE_ROW =
      ClassDesc.of("org.apache.spark.sql.catalyst.expressions.UnsafeRow");
  private static final ClassDesc UNSUPPORTED_OP =
      ClassDesc.of("java.lang.UnsupportedOperationException");
  private static final ClassDesc OBJECT_ARRAY =
      ClassDesc.ofDescriptor("[Ljava/lang/Object;");

  private static final MethodTypeDesc INIT = MethodTypeDesc.of(ConstantDescs.CD_void);
  private static final MethodTypeDesc SPEC_INIT =
      MethodTypeDesc.of(ConstantDescs.CD_void, OBJECT_ARRAY);
  private static final MethodTypeDesc GENERATE =
      MethodTypeDesc.of(ConstantDescs.CD_Object, OBJECT_ARRAY);
  private static final MethodTypeDesc APPLY = MethodTypeDesc.of(UNSAFE_ROW, INTERNAL_ROW);
  private static final MethodTypeDesc INITIALIZE =
      MethodTypeDesc.of(ConstantDescs.CD_void, ConstantDescs.CD_int);
  private static final MethodTypeDesc UNSUPPORTED_OP_INIT =
      MethodTypeDesc.of(ConstantDescs.CD_void, ConstantDescs.CD_String);

  /**
   * Assembles the wrapper and projection classes.
   *
   * @param wrapperClassName the binary name of the wrapper class.
   * @param specClassName the binary name of the projection class.
   * @return {@code {wrapperBytes, specBytes}}.
   */
  public static byte[][] assembleGeneratedClass(String wrapperClassName, String specClassName) {
    ClassDesc wrapperDesc = ClassDesc.of(wrapperClassName);
    ClassDesc specDesc = ClassDesc.of(specClassName);
    byte[] wrapper = ClassFile.of().build(wrapperDesc, (ClassBuilder b) -> b
        .withFlags(AccessFlag.PUBLIC)
        .withSuperclass(GENERATED_CLASS)
        .withMethodBody("<init>", INIT, AccessFlag.PUBLIC.mask(), (CodeBuilder cb) -> {
          cb.aload(0);
          cb.invokespecial(GENERATED_CLASS, "<init>", INIT);
          cb.return_();
        })
        .withMethodBody("generate", GENERATE, AccessFlag.PUBLIC.mask(), (CodeBuilder cb) -> {
          cb.new_(specDesc);
          cb.dup();
          cb.aload(1);
          cb.invokespecial(specDesc, "<init>", SPEC_INIT);
          cb.areturn();
        }));
    byte[] spec = ClassFile.of().build(specDesc, (ClassBuilder b) -> b
        .withFlags(AccessFlag.PUBLIC)
        .withSuperclass(UNSAFE_PROJECTION)
        .withField("references", OBJECT_ARRAY, AccessFlag.PRIVATE.mask())
        .withMethodBody("<init>", SPEC_INIT, AccessFlag.PUBLIC.mask(), (CodeBuilder cb) -> {
          cb.aload(0);
          cb.invokespecial(UNSAFE_PROJECTION, "<init>", INIT);
          cb.aload(0);
          cb.aload(1);
          cb.putfield(specDesc, "references", OBJECT_ARRAY);
          cb.return_();
        })
        .withMethodBody("apply", APPLY, AccessFlag.PUBLIC.mask(), (CodeBuilder cb) -> {
          cb.new_(UNSUPPORTED_OP);
          cb.dup();
          cb.ldc("Varka batch execution wired in Task 6");
          cb.invokespecial(UNSUPPORTED_OP, "<init>", UNSUPPORTED_OP_INIT);
          cb.athrow();
        })
        .withMethodBody("initialize", INITIALIZE, AccessFlag.PUBLIC.mask(), (CodeBuilder cb) -> {
          cb.return_();
        }));
    return new byte[][]{wrapper, spec};
  }
}
