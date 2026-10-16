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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
 * Keeps only the mutants a file names, or one shard's share of them (VARKA-265). Written against
 * PIT's own interceptor SPI; on by default, and a no-op unless the launching JVM sets one of the
 * two properties.
 *
 * <p>{@code -Dvarka.pit.only=FILE}: {@code dev/varka_pit.sh}'s second run, the bytes oracle's,
 * needs only the mutants no behavioural test killed in the first, and its one pinned-shapes test
 * is the slowest in the suites. FILE has one mutant a line, as {@code dev/varka_pit_verdicts.py
 * --keep} writes it: the class, the method, its descriptor, the mutator and the instruction
 * indexes, tab-separated - the identity PIT derives from the bytecode, which two runs over the
 * same classes share.
 *
 * <p>{@code -Dvarka.pit.shard=K/N} (K from zero): the mutants at positions K, K + N, K + 2N, ...
 * of each class's mutants sorted by that identity, the class's offset into the cycle taken from
 * its name's hash so that the classes' remainders do not all land on shard 0. A run's cost is
 * not its mutant count: a killed mutant runs one test at the median and a survivor every test
 * covering it, about a hundred times longer, and survivors cluster in a method. Sorted by
 * identity a method's mutants are adjacent, so dealing them round-robin spreads each cluster
 * over the shards, where a hash of each mutant would balance only on average. The N shards
 * together hold every mutant exactly once, and two runs of a shard hold the same ones.
 */
public final class VarkaPitOnly implements MutationInterceptorFactory {

  static final String PROPERTY = "varka.pit.only";
  static final String SHARD = "varka.pit.shard";

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
    String shard = System.getProperty(SHARD);
    int index = 0;
    int count = 1;
    if (shard != null) {
      String[] parts = shard.split("/", -1);
      if (parts.length != 2) {
        throw new IllegalArgumentException(SHARD + " is K/N, not " + shard);
      }
      index = Integer.parseInt(parts[0]);
      count = Integer.parseInt(parts[1]);
      if (count < 1 || index < 0 || index >= count) {
        throw new IllegalArgumentException(SHARD + " needs 0 <= K < N, not " + shard);
      }
    }
    int shardIndex = index;
    int shardCount = count;
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
        Collection<MutationDetails> slice = shardCount == 1 ? mutations
            : shard(mutations, shardIndex, shardCount);
        if (keep == null) {
          return slice;
        }
        return slice.stream().filter(d -> keep.contains(key(d.getId())))
            .collect(Collectors.toList());
      }

      @Override
      public void end() {}
    };
  }

  /** Shard {@code index} of {@code count}, dealt round-robin per class (see the class comment). */
  static List<MutationDetails> shard(Collection<MutationDetails> mutations, int index,
      int count) {
    var byClass = new LinkedHashMap<String, List<MutationDetails>>();
    for (MutationDetails d : mutations) {
      byClass.computeIfAbsent(d.getId().getClassName().asJavaName(), c -> new ArrayList<>())
          .add(d);
    }
    var out = new ArrayList<MutationDetails>();
    for (Map.Entry<String, List<MutationDetails>> e : byClass.entrySet()) {
      List<MutationDetails> sorted = new ArrayList<>(e.getValue());
      sorted.sort(Comparator.comparing(d -> key(d.getId())));
      // String.hashCode is specified by the language, so the offset is the same in every JVM.
      int offset = Math.floorMod(e.getKey().hashCode(), count);
      for (int i = 0; i < sorted.size(); i++) {
        if ((i + offset) % count == index) {
          out.add(sorted.get(i));
        }
      }
    }
    return out;
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
