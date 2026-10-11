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

import java.util.Map;

/**
 * The prices {@link VarkaEmitCost} predicts with: for each feature, what it adds to each
 * of a group's four methods in bytes and then in Vector API call sites, in
 * {@link VarkaMethodNames#GROUP_METHOD_KINDS} order, fitted by least squares over emitted
 * groups; and the margins the planned grouping keeps under the budgets, the largest
 * under-prediction the fit makes on its own groups in the band where each budget binds
 * ({@code VARKA-236.md} 3.3).
 *
 * <p>Generated, not written: {@code VarkaEmitCostSuite} derives it from emitted classes at
 * the default options and sixteen int lanes, fails while this file differs from what it
 * derives, and rewrites it under {@code VARKA_COST_REGEN=true build/sbt
 * 'catalyst/testOnly *VarkaEmitCostSuite'}. How it is derived is
 * {@code VarkaEmitCostFit}'s doc.
 */
final class VarkaEmitCostTable {

  private VarkaEmitCostTable() {}

  /**
   * The share of the byte budget a planned group is closed under, so that a method the
   * prices under-predict still measures within it.
   */
  static final double BYTES_MARGIN = 0.211;

  /** The same for the call-site budget. */
  static final double SITES_MARGIN = 0.177;

  /** Each feature's price, by feature. */
  static final Map<String, double[]> PRICES = Map.ofEntries(
      Map.entry("AddDays",
          new double[] {11.686, 15.079, 12.408, 15.88, 1.533, 1.632, 1.576, 1.647}),
      Map.entry("AddMonths",
          new double[] {611.64, 676.533, 614.915, 682.047, 79.835, 82.034, 80.193, 82.097}),
      Map.entry("And/INT",
          new double[] {5.083, -0.102, 4.133, -0.938, -0.467, -0.997, -0.662, -1.044}),
      Map.entry("And/LONG",
          new double[] {9.019, 16.657, 10.225, 16.845, -0.244, -0.867, -0.103, -0.838}),
      Map.entry("BoundedDivide",
          new double[] {13.16, 15.635, 13.23, 15.656, 1.97, 2.28, 1.993, 2.278}),
      Map.entry("ColumnRef/INT",
          new double[] {19.097, 91.513, 21.149, 94.932, 0.792, 1.022, 0.743, 1.013}),
      Map.entry("ColumnRef/LONG",
          new double[] {21.131, 105.605, 23.345, 107.809, 0.984, 0.63, 0.919, 0.626}),
      Map.entry("Compare/EQ/INT",
          new double[] {21.118, 57.203, 24.303, 58.969, 1.735, 2.325, 2.233, 2.44}),
      Map.entry("Compare/EQ/LONG",
          new double[] {16.286, 48.3, 16.837, 48.229, 1.525, 2.797, 1.435, 2.753}),
      Map.entry("Compare/GE/INT",
          new double[] {18.772, 52.673, 21.495, 54.229, 1.626, 1.666, 2.113, 1.818}),
      Map.entry("Compare/GE/LONG",
          new double[] {16.656, 46.127, 17.018, 46.288, 1.361, 2.564, 1.393, 2.586}),
      Map.entry("Compare/GT/INT",
          new double[] {21.134, 46.107, 23.481, 46.855, 1.789, 1.552, 2.12, 1.644}),
      Map.entry("Compare/GT/LONG",
          new double[] {16.595, 50.247, 16.845, 50.261, 1.525, 2.744, 1.428, 2.733}),
      Map.entry("Compare/LE/INT",
          new double[] {24.775, 43.47, 27.577, 44.266, 1.796, 1.65, 2.129, 1.742}),
      Map.entry("Compare/LE/LONG",
          new double[] {17.834, 48.242, 18.743, 48.441, 1.531, 2.26, 1.537, 2.28}),
      Map.entry("Compare/LT/INT",
          new double[] {23.927, 44.792, 27.437, 45.619, 1.633, 1.78, 2.087, 1.932}),
      Map.entry("Compare/LT/LONG",
          new double[] {18.046, 46.331, 18.785, 46.494, 1.468, 2.59, 1.476, 2.612}),
      Map.entry("ConstDivide/INT/neg",
          new double[] {105.827, 108.483, 106.246, 108.494, 11.687, 12.139, 11.792, 12.154}),
      Map.entry("ConstDivide/INT/pos",
          new double[] {104.929, 102.098, 105.119, 102.037, 11.011, 10.818, 11.058, 10.824}),
      Map.entry("ConstDivide/LONG/neg",
          new double[] {32.267, 27.82, 32.158, 27.82, 2.891, 2.991, 2.835, 2.975}),
      Map.entry("ConstDivide/LONG/pos",
          new double[] {31.994, 19.885, 32.367, 20.037, 2.952, 2.815, 2.993, 2.82}),
      Map.entry("DateDiff",
          new double[] {4.97, 4.842, 5.003, 5.132, 1.179, 1.239, 1.198, 1.242}),
      Map.entry("DayOfMonth",
          new double[] {20.125, 37.609, 20.23, 37.889, 3.672, 5.132, 3.793, 5.128}),
      Map.entry("DayOfWeek",
          new double[] {114.605, 116.425, 114.556, 116.459, 15.96, 15.911, 15.985, 15.912}),
      Map.entry("DayOfWeekIso",
          new double[] {116.484, 113.002, 116.838, 112.924, 15.986, 15.594, 15.963, 15.592}),
      Map.entry("DayOfYear",
          new double[] {92.08, 114.237, 91.431, 116.794, 12.652, 12.751, 12.619, 12.759}),
      Map.entry("Greatest/INT",
          new double[] {6.626, 71.753, 6.968, 72.338, 1.11, 3.522, 1.152, 3.522}),
      Map.entry("Greatest/LONG",
          new double[] {6.247, 60.537, 6.708, 60.58, 1.119, 3.255, 1.184, 3.261}),
      Map.entry("GuardedDay",
          new double[] {43.274, 66.031, 48.487, 71.39, 2.057, 2.13, 2.09, 2.133}),
      Map.entry("GuardedRange/INT",
          new double[] {45.323, 69.079, 50.675, 74.539, 2.018, 2.426, 2.023, 2.424}),
      Map.entry("GuardedRange/LONG",
          new double[] {41.718, 51.361, 47.124, 56.431, 2.033, 2.185, 2.055, 2.187}),
      Map.entry("IfElse/INT",
          new double[] {3.764, 63.331, 2.141, 64.577, 0.749, 0.519, 0.47, 0.419}),
      Map.entry("IfElse/LONG",
          new double[] {5.066, 43.844, 5.015, 43.889, 0.779, 0.046, 0.887, 0.062}),
      Map.entry("InRanges",
          new double[] {92.097, 105.375, 96.448, 106.7, 3.816, 4.903, 4.325, 5.045}),
      Map.entry("IntArith/ADD/FAIL/INT",
          new double[] {69.409, 82.533, 75.221, 87.676, 5.163, 4.746, 5.385, 4.805}),
      Map.entry("IntArith/ADD/FAIL/LONG",
          new double[] {62.137, 82.079, 67.577, 87.123, 5.051, 5.322, 5.069, 5.321}),
      Map.entry("IntArith/ADD/NULL/INT",
          new double[] {0, 107.316, 0, 109.484, 0, 5.259, 0, 5.292}),
      Map.entry("IntArith/ADD/NULL/LONG",
          new double[] {0, 90.423, 0, 90.494, 0, 5.19, 0, 5.202}),
      Map.entry("IntArith/ADD/WRAP/INT",
          new double[] {8.439, -11.667, 8.786, -12.603, 1.426, 0.604, 1.447, 0.591}),
      Map.entry("IntArith/ADD/WRAP/LONG",
          new double[] {6.164, 10.402, 6.17, 10.399, 1.069, 0.961, 1.074, 0.959}),
      Map.entry("IntArith/MUL/WRAP/INT",
          new double[] {7.753, 10.968, 8.014, 11.404, 1.496, 1.701, 1.537, 1.712}),
      Map.entry("IntArith/MUL/WRAP/LONG",
          new double[] {5.125, 9.235, 5.496, 9.243, 1.021, 1.177, 1.099, 1.18}),
      Map.entry("IntArith/SUB/FAIL/INT",
          new double[] {69.961, 78.463, 76.204, 83.189, 5.386, 4.526, 5.605, 4.549}),
      Map.entry("IntArith/SUB/FAIL/LONG",
          new double[] {60.65, 81.136, 66.684, 86.263, 5.119, 5.292, 5.295, 5.309}),
      Map.entry("IntArith/SUB/NULL/INT",
          new double[] {0, 104.529, 0, 106.391, 0, 5.572, 0, 5.57}),
      Map.entry("IntArith/SUB/NULL/LONG",
          new double[] {0, 89.616, 0, 89.799, 0, 5.41, 0, 5.429}),
      Map.entry("IntArith/SUB/WRAP/INT",
          new double[] {3.842, -2.052, 2.483, -2.127, 0.943, 0.773, 0.944, 0.792}),
      Map.entry("IntArith/SUB/WRAP/LONG",
          new double[] {5.889, 12.507, 5.851, 12.509, 1.034, 0.858, 1.11, 0.859}),
      Map.entry("IntNeg/FAIL/INT",
          new double[] {31.736, 52.272, 36.647, 57.492, 1.811, 1.912, 1.803, 1.912}),
      Map.entry("IntNeg/FAIL/LONG",
          new double[] {29.585, 37.989, 34.007, 43.043, 1.989, 2.072, 1.866, 2.065}),
      Map.entry("IntNeg/WRAP/INT",
          new double[] {1.865, -0.269, 1.801, -0.537, 0.849, 1.024, 0.867, 1.03}),
      Map.entry("IntNeg/WRAP/LONG",
          new double[] {7.093, 2.773, 7.655, 2.855, 1.093, 0.986, 1.067, 0.994}),
      Map.entry("IsNotNull/INT",
          new double[] {5.714, 23.944, 1.2, 22.534, 0.156, 0.873, 0.352, 0.871}),
      Map.entry("IsNotNull/LONG",
          new double[] {7.222, 13.504, 2.863, 12.445, 0.135, 1.056, -0.041, 1.017}),
      Map.entry("LastDay",
          new double[] {218.172, 263.643, 219.367, 267.015, 30.97, 33.453, 31.096, 33.452}),
      Map.entry("Least/INT",
          new double[] {6.35, 73.867, 6.484, 74.586, 1.141, 3.593, 1.124, 3.587}),
      Map.entry("Least/LONG",
          new double[] {5.306, 62.047, 5.101, 62.024, 1.05, 3.186, 1.044, 3.185}),
      Map.entry("LiteralSlot/INT",
          new double[] {7.522, 9.116, 6.346, 8.21, 0.976, 1.391, 1.048, 1.413}),
      Map.entry("LiteralSlot/LONG",
          new double[] {14.114, 6.84, 13.507, 6.849, 1.319, 2.015, 1.467, 2.071}),
      Map.entry("MakeDate/ansi",
          new double[] {475.265, 530.851, 481.01, 537.528, 53.632, 53.757, 53.519, 53.751}),
      Map.entry("MakeDate/null",
          new double[] {0, 533.96, 0, 540.401, 0, 54.556, 0, 54.568}),
      Map.entry("Month",
          new double[] {19.576, 36.945, 19.695, 37.05, 2.731, 4.35, 2.874, 4.349}),
      Map.entry("NarrowLane",
          new double[] {36.5, 33.605, 30.631, 30.921, 1.454, 2.815, 1.462, 2.807}),
      Map.entry("NextDay",
          new double[] {106.249, 103.517, 106.468, 103.613, 15.186, 15.27, 15.193, 15.269}),
      Map.entry("Not/INT",
          new double[] {8.103, 4.972, 8.9, 5.902, -0.009, 0.553, 0.028, 0.605}),
      Map.entry("Not/LONG",
          new double[] {8.115, -3.597, 8.286, -3.595, 0.122, -0.172, -0.039, -0.207}),
      Map.entry("Or/INT",
          new double[] {-3.018, 1.871, -4.353, 0.674, -0.766, -0.648, -0.787, -0.673}),
      Map.entry("Or/LONG",
          new double[] {5.962, 17.085, 6.891, 17.159, -0.383, -1.258, -0.13, -1.229}),
      Map.entry("Quarter",
          new double[] {35.158, 58.036, 34.99, 58.51, 5.654, 7.203, 5.743, 7.194}),
      Map.entry("SubDays",
          new double[] {10.435, 15.82, 11.092, 16.648, 1.464, 1.71, 1.47, 1.715}),
      Map.entry("ThursdayOf",
          new double[] {121.173, 119.201, 121.655, 119.461, 16.967, 16.689, 17.045, 16.703}),
      Map.entry("TruncDate/MONTH",
          new double[] {16.395, 46.704, 15.701, 47.503, 3.497, 5.607, 3.502, 5.615}),
      Map.entry("TruncDate/QUARTER",
          new double[] {211.84, 267.197, 211.418, 270.288, 29.781, 32.379, 29.882, 32.41}),
      Map.entry("TruncDate/YEAR",
          new double[] {112.131, 127.05, 111.939, 129.773, 14.913, 14.588, 14.89, 14.588}),
      Map.entry("TruncDateDynamic",
          new double[] {414.126, 468.224, 413.683, 469.986, 57.952, 62.511, 58.066, 62.508}),
      Map.entry("WeekDay",
          new double[] {110.513, 111.709, 110.19, 111.792, 15.029, 15.046, 14.97, 15.037}),
      Map.entry("WeekOfYear",
          new double[] {112.366, 111.559, 111.116, 114.306, 16.513, 12.709, 16.41, 12.715}),
      Map.entry("Year",
          new double[] {22.738, 42.753, 22.043, 45.572, 3.669, 3.333, 3.638, 3.34}),
      Map.entry("fixed/INT",
          new double[] {81.15, 52.51, 93.539, 64.697, -0.127, -0.879, -0.037, -0.872}),
      Map.entry("fixed/LONG",
          new double[] {87.476, 51.404, 98.837, 63.769, -0.466, -0.804, -0.329, -0.807}),
      Map.entry("out/INT",
          new double[] {24.755, 21.447, 26.838, 23.799, 1.214, 1.537, 1.145, 1.525}),
      Map.entry("out/LONG",
          new double[] {22.867, 33.253, 25.042, 35.19, 1.199, 1.105, 1.086, 1.083}),
      Map.entry("out/cond/INT",
          new double[] {13.072, 11.488, 9.693, 9.067, -0.701, -1.971, -0.949, -1.713}),
      Map.entry("out/cond/LONG",
          new double[] {14.263, 25.761, 12.125, 24.125, -0.634, -1.811, -0.332, -1.226}),
      Map.entry("prefix",
          new double[] {265.758, 275.686, 266.62, 274.278, 28.448, 31.117, 28.481, 31.117}),
      Map.entry("prefix/load",
          new double[] {15.593, -6.809, 18.533, -5.519, -1.259, -3.083, -1.334, -3.099}),
      Map.entry("prefix/month",
          new double[] {16.216, -2.645, 15.664, -0.717, 2.006, -1.881, 1.842, -1.873}));
}
