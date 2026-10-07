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

package org.apache.spark.sql.varka.vector;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

/**
 * The engine's way to the memory sanitizer (VARKA-263), which lives in catalyst because the
 * runner that registers the buffers is in {@code sql/core}, and neither it nor the engine can see
 * the other at compile time. Only loaded when {@code -Dvarka.sanitizeMemory=true}: {@link
 * VarkaVectorSupport#ofAddress} reads that flag into a {@code static final}, so with it off this
 * class is never initialised and the call is removed. The handle is resolved once, by name, from
 * the engine's own class loader, where catalyst's classes are in a running Spark and in the
 * suites; an engine-only run with the flag on fails here, naming what is missing.
 */
final class MemorySanitizerBridge {

  private static final String SANITIZER =
      "org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaMemorySanitizer";

  private static final MethodHandle CHECK = resolve();

  private MemorySanitizerBridge() {}

  /** Fails when {@code bytes} at {@code addr} leave every buffer the evaluator registered. */
  static void check(long addr, long bytes) {
    try {
      CHECK.invokeExact(addr, bytes);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new IllegalStateException(e);
    }
  }

  private static MethodHandle resolve() {
    try {
      Class<?> sanitizer = Class.forName(SANITIZER, true,
          MemorySanitizerBridge.class.getClassLoader());
      return MethodHandles.publicLookup().findStatic(sanitizer, "check",
          MethodType.methodType(void.class, long.class, long.class));
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "-Dvarka.sanitizeMemory is set, but " + SANITIZER + " cannot be loaded", e);
    }
  }
}
