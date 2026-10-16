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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.pitest.bytecode.analysis.ClassTree;
import org.pitest.mutationtest.build.InterceptorParameters;
import org.pitest.mutationtest.build.InterceptorType;
import org.pitest.mutationtest.build.MutationInterceptor;
import org.pitest.mutationtest.build.MutationInterceptorFactory;
import org.pitest.mutationtest.engine.Mutater;
import org.pitest.mutationtest.engine.MutationDetails;
import org.pitest.mutationtest.engine.MutationIdentifier;
import org.pitest.plugin.Feature;

/**
 * Keeps only the mutants a file names (VARKA-265): {@code dev/varka_pit.sh}'s second run, the
 * bytes oracle's, needs only the mutants no behavioural test killed in the first, and its one
 * pinned-shapes test is the slowest in the suites. Written against PIT's own interceptor SPI; on
 * by default, and a no-op unless the launching JVM sets {@code -Dvarka.pit.only=FILE}.
 *
 * <p>FILE has one mutant a line, as {@code dev/varka_pit_verdicts.py --keep} writes it: the
 * class, the method, its descriptor, the mutator and the instruction indexes, tab-separated -
 * the identity PIT derives from the bytecode, which two runs over the same classes share.
 */
public final class VarkaPitOnly implements MutationInterceptorFactory {

  static final String PROPERTY = "varka.pit.only";

  @Override
  public String description() {
    return "only the mutants named in -Dvarka.pit.only (VARKA-265)";
  }

  @Override
  public Feature provides() {
    return Feature.named("varkaonly").withOnByDefault(true).withDescription(description());
  }

  @Override
  public MutationInterceptor createInterceptor(InterceptorParameters params) {
    String file = System.getProperty(PROPERTY);
    Set<String> keep = file == null ? null : read(Path.of(file));
    return new MutationInterceptor() {
      @Override
      public InterceptorType type() {
        return InterceptorType.FILTER;
      }

      @Override
      public void begin(ClassTree clazz) {}

      @Override
      public Collection<MutationDetails> intercept(Collection<MutationDetails> mutations,
          Mutater m) {
        if (keep == null) {
          return mutations;
        }
        return mutations.stream().filter(d -> keep.contains(key(d.getId())))
            .collect(Collectors.toList());
      }

      @Override
      public void end() {}
    };
  }

  static String key(MutationIdentifier id) {
    return String.join("\t", id.getClassName().asJavaName(), id.getLocation().getMethodName(),
        id.getLocation().getMethodDesc(), id.getMutator(),
        id.getIndexes().stream().map(String::valueOf).collect(Collectors.joining(",")));
  }

  static Set<String> read(Path file) {
    try {
      List<String> lines = Files.readAllLines(file);
      return new HashSet<>(lines);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
