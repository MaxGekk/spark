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

import java.lang.foreign.MemorySegment;

/**
 * The one place catalyst's Varka code turns a raw address into a segment. A kernel and the helpers
 * beside it receive buffers as bare {@code long} addresses, so this is where each says how far it
 * is entitled to read or write, and the {@link MemorySegment} bounds check enforces it from there
 * on. It is a function of its own, and the code that maps an address does not do it inline, so
 * that a check of the size against the buffer's real capacity (VARKA-263) has one place to stand;
 * {@code dev/varka_precommit.sh} rejects a raw {@code .reinterpret(} in main code elsewhere.
 *
 * <p>The engine has the same function, {@code VarkaVectorSupport.ofAddress}, which the emitted
 * kernels call. The two are separate because catalyst and the engine cannot see each other at
 * compile time.
 */
public final class VarkaSegments {

  private VarkaSegments() {}

  /** {@code addr} as a segment of exactly {@code bytes} bytes. */
  public static MemorySegment map(long addr, long bytes) {
    return MemorySegment.ofAddress(addr).reinterpret(bytes);
  }
}
