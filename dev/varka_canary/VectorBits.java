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

/**
 * Prints the bits of the JVM's preferred int vector species, the width the Varka kernels emit for
 * when no lane count is forced. {@code dev/varka_gate.sh} runs it with the narrow step's flags,
 * so the gate's log shows the narrow step really runs at 128 bits (VARKA-286.md 2):
 *
 * <pre>java --add-modules jdk.incubator.vector -XX:MaxVectorSize=16 \
 *     dev/varka_canary/VectorBits.java</pre>
 *
 * <p>It is not directly in {@code dev/}: that is the base directory of Spark's sbt project
 * {@code oldDeps}, which MiMa builds, and sbt compiles the Java files there without the
 * incubator vector module.
 */
public class VectorBits {
  public static void main(String[] args) {
    System.out.println(jdk.incubator.vector.IntVector.SPECIES_PREFERRED.vectorBitSize());
  }
}
