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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.catalyst.expressions.codegen.varka.VarkaVectorIR.LaneType;

/**
 * Everything one fuzz run reads, as a value: the trees, the options and the batch. A fuzzer draws
 * one and runs it; keeping the two apart is what lets a shrinker run the same case again with a
 * part of it changed (VARKA-277).
 *
 * <p>Both lanes share the shape: {@code lits} and {@code data} are longs, which an int-lane case
 * holds as the ints it drew and the run narrows back. A null bitmap is materialised, one array per
 * input, where the draw used a function of the row, so that a case can be cut by length or have a
 * null cleared. {@code smallOrdinal} and {@code levelOrdinal} name the two inputs whose data the
 * grammar constrains (a month-arithmetic bound and the codes of a truncation level), which a
 * change to the data must leave alone; -1 where the shape has none. {@code label} is the context
 * a failure message starts with: the seed, the iteration and the parts of the case, written once
 * at the draw.
 *
 * <p>The arrays are shared, not copied: a case is never changed in place, and a {@code with}
 * method returns a new case holding the array it is given, so its caller passes an array it will
 * not change afterwards - as the shrinker does, building each candidate's arrays new.
 */
public record VarkaFuzzCase(
    LaneType lane,
    List<VarkaVectorIR> roots,
    int numInputs,
    long[] lits,
    int length,
    boolean[][] nulls,
    long[][] data,
    boolean forceMasked,
    VarkaEmitOptions options,
    int smallOrdinal,
    int levelOrdinal,
    String label) {

  public VarkaFuzzCase {
    roots = List.copyOf(roots);
  }

  public VarkaFuzzCase withRoots(List<VarkaVectorIR> newRoots) {
    return new VarkaFuzzCase(lane, newRoots, numInputs, lits, length, nulls, data, forceMasked,
        options, smallOrdinal, levelOrdinal, label);
  }

  public VarkaFuzzCase withOptions(VarkaEmitOptions newOptions) {
    return new VarkaFuzzCase(lane, roots, numInputs, lits, length, nulls, data, forceMasked,
        newOptions, smallOrdinal, levelOrdinal, label);
  }

  public VarkaFuzzCase withLabel(String newLabel) {
    return new VarkaFuzzCase(lane, roots, numInputs, lits, length, nulls, data, forceMasked,
        options, smallOrdinal, levelOrdinal, newLabel);
  }

  public VarkaFuzzCase withLits(long[] newLits) {
    return new VarkaFuzzCase(lane, roots, numInputs, newLits, length, nulls, data, forceMasked,
        options, smallOrdinal, levelOrdinal, label);
  }

  public VarkaFuzzCase withForceMasked(boolean newForceMasked) {
    return new VarkaFuzzCase(lane, roots, numInputs, lits, length, nulls, data, newForceMasked,
        options, smallOrdinal, levelOrdinal, label);
  }

  public VarkaFuzzCase withNulls(boolean[][] newNulls) {
    return new VarkaFuzzCase(lane, roots, numInputs, lits, length, newNulls, data, forceMasked,
        options, smallOrdinal, levelOrdinal, label);
  }

  public VarkaFuzzCase withData(long[][] newData) {
    return new VarkaFuzzCase(lane, roots, numInputs, lits, length, nulls, newData, forceMasked,
        options, smallOrdinal, levelOrdinal, label);
  }

  /** The options that differ from the defaults, as {@code name=value}; empty for the defaults. */
  public static List<String> optionDelta(VarkaEmitOptions options) {
    var delta = new ArrayList<String>();
    for (VarkaEmitOption o : VarkaEmitOption.TABLE) {
      if (!o.text(options).equals(o.text(VarkaEmitOptions.DEFAULTS))) {
        delta.add(o.name() + "=" + o.text(options));
      }
    }
    return delta;
  }

  /** The case on one line for a person: the trees, the delta from the defaults and the batch. */
  public static String describe(VarkaFuzzCase c) {
    return "lane=" + c.lane().toString().toLowerCase(Locale.ROOT) + " "
        + "roots=" + c.roots().stream().map(VarkaVectorIR::canonical)
            .collect(Collectors.joining(", ", "[", "]")) + " "
        + "options=" + String.join(", ", optionDelta(c.options())).transform(s -> "{" + s + "}")
        + " length=" + c.length() + " forceMasked=" + c.forceMasked() + " "
        + "literals=" + Arrays.stream(c.lits()).mapToObj(Long::toString)
            .collect(Collectors.joining(",", "[", "]")) + " "
        + "columns=" + IntStream.range(0, c.numInputs()).mapToObj(c::rows)
            .collect(Collectors.joining(" "));
  }

  /** The first eight rows of column {@code col}, a null as {@code null}. */
  private String rows(int col) {
    String shown = IntStream.range(0, Math.min(length, 8))
        .mapToObj(i -> nulls[col][i] ? "null" : Long.toString(data[col][i]))
        .collect(Collectors.joining(","));
    return "[" + shown + (length > 8 ? ",...]" : "]");
  }
}
