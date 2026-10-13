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

import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.lang.classfile.MethodModel;
import java.lang.classfile.MethodTransform;
import java.lang.classfile.Opcode;
import java.lang.classfile.TypeKind;
import java.lang.classfile.instruction.InvokeInstruction;
import java.lang.constant.ClassDesc;
import java.lang.constant.ConstantDescs;
import java.lang.constant.MethodTypeDesc;

/**
 * A planted fault for the kernel checks' validity fill (VARKA-138 test 1): an emitted kernel with
 * every write to its outputs' validity bitmaps taken out of its bytecode, so that it leaves each
 * bitmap holding whatever the caller put there.
 *
 * <p>An emitted kernel writes validity only through static helpers of {@code VarkaVectorSupport}:
 * the driver zeroes, fills or copies each output's bitmap before anything can return
 * ({@code zero}, {@code setValid}, {@code copyColumnValidity}, or {@code prepareOutputValidity}
 * from a plan), and the loop and epilogue OR each lane group's bits in ({@code orValidityBitsAt*},
 * {@code orPartialValidityBitsAt}, {@code putValidityWord}). Every one of them returns nothing, so
 * a call is removed by popping its arguments in its place, which leaves the stack as the call
 * would have; the Class-File API recomputes the stack maps. The values a kernel stores go through
 * the Vector API, not this class, and are untouched.
 *
 * <p>A rewrite of the bytes in the test, rather than an emit option: the fault needs no seam in
 * the emitter and so cannot be reached from production, and it is the same rewrite whatever the
 * options chose to emit. No Class-File API type appears in a signature here, because the Scala
 * suites call this class and scalac cannot load that API's sealed builders.
 */
final class VarkaValidityFault {

  private VarkaValidityFault() {
  }

  private static final ClassDesc SUPPORT =
      ClassDesc.of("org.apache.spark.sql.varka.vector.VarkaVectorSupport");

  /** Whether {@code call} writes an output's validity (or a filter's selection) bitmap. */
  private static boolean writesValidity(InvokeInstruction call) {
    if (call.opcode() != Opcode.INVOKESTATIC || !call.owner().asSymbol().equals(SUPPORT)) {
      return false;
    }
    if (!call.typeSymbol().returnType().equals(ConstantDescs.CD_void)) {
      return false;
    }
    String name = call.name().stringValue();
    return name.contains("Validity") || name.equals("setValid") || name.equals("setBit")
        || name.equals("zero");
  }

  /** How many validity writes {@code bytes} calls, over all of its methods. */
  static int validityWrites(byte[] bytes) {
    int count = 0;
    for (MethodModel mm : ClassFile.of().parse(bytes).methods()) {
      if (mm.code().isPresent()) {
        for (var element : mm.code().get().elementList()) {
          if (element instanceof InvokeInstruction call && writesValidity(call)) {
            count++;
          }
        }
      }
    }
    return count;
  }

  /** {@code bytes} with every validity write replaced by pops of the call's arguments. */
  static byte[] withoutValidityWrites(byte[] bytes) {
    ClassModel cm = ClassFile.of().parse(bytes);
    return ClassFile.of().transformClass(cm, (clb, ce) -> {
      if (ce instanceof MethodModel mm) {
        clb.transformMethod(mm, MethodTransform.transformingCode((cob, coe) -> {
          if (coe instanceof InvokeInstruction call && writesValidity(call)) {
            // The last argument is on top of the stack, so pop from the last parameter back; a
            // long or a double is one two-slot value, which pop2 takes whole.
            MethodTypeDesc type = call.typeSymbol();
            for (int p = type.parameterCount() - 1; p >= 0; p--) {
              if (TypeKind.from(type.parameterType(p)).slotSize() == 2) {
                cob.pop2();
              } else {
                cob.pop();
              }
            }
          } else {
            cob.with(coe);
          }
        }));
      } else {
        clb.with(ce);
      }
    });
  }
}
