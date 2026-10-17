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
  static final double BYTES_MARGIN = 0.21;

  /** The same for the call-site budget. */
  static final double SITES_MARGIN = 0.182;

  /** Each feature's price, by feature. */
  static final Map<String, double[]> PRICES = Map.ofEntries(
      Map.entry("AddDays",
          new double[] {11.442, 13.868, 12.156, 14.664, 1.488, 1.565, 1.524, 1.582}),
      Map.entry("AddMonths",
          new double[] {611.043, 677.48, 614.896, 682.983, 79.79, 82.188, 80.214, 82.225}),
      Map.entry("And/INT",
          new double[] {6.298, 8.496, 4.099, 8.215, -0.313, -0.469, -0.659, -0.53}),
      Map.entry("And/LONG",
          new double[] {9.033, 16.911, 10.24, 17.08, -0.245, -0.921, -0.104, -0.891}),
      Map.entry("BoundedDivide",
          new double[] {13.187, 15.959, 13.195, 15.922, 1.983, 2.243, 1.991, 2.229}),
      Map.entry("ColumnRef/INT",
          new double[] {19.505, 91.68, 21.148, 94.961, 0.852, 1.016, 0.754, 0.982}),
      Map.entry("ColumnRef/LONG",
          new double[] {21.169, 105.769, 23.378, 107.957, 0.985, 0.637, 0.919, 0.633}),
      Map.entry("Compare/EQ/INT",
          new double[] {20.643, 52.325, 24.309, 52.936, 1.663, 2.037, 2.224, 2.078}),
      Map.entry("Compare/EQ/LONG",
          new double[] {16.258, 47.914, 16.805, 47.856, 1.525, 2.794, 1.436, 2.75}),
      Map.entry("Compare/GE/INT",
          new double[] {19.184, 48.862, 21.437, 50.512, 1.666, 1.554, 2.104, 1.754}),
      Map.entry("Compare/GE/LONG",
          new double[] {16.623, 46.207, 16.982, 46.361, 1.361, 2.585, 1.394, 2.606}),
      Map.entry("Compare/GT/INT",
          new double[] {21.127, 38.959, 23.61, 38.469, 1.751, 1.067, 2.106, 1.078}),
      Map.entry("Compare/GT/LONG",
          new double[] {16.582, 50.701, 16.832, 50.782, 1.526, 2.799, 1.429, 2.785}),
      Map.entry("Compare/LE/INT",
          new double[] {24.501, 40.376, 27.586, 40.301, 1.736, 1.581, 2.111, 1.612}),
      Map.entry("Compare/LE/LONG",
          new double[] {17.8, 47.654, 18.705, 47.84, 1.531, 2.295, 1.537, 2.315}),
      Map.entry("Compare/LT/INT",
          new double[] {24.016, 39.161, 27.225, 38.714, 1.672, 1.533, 2.074, 1.587}),
      Map.entry("Compare/LT/LONG",
          new double[] {18.023, 44.324, 18.759, 44.504, 1.469, 2.661, 1.476, 2.684}),
      Map.entry("ConstDivide/INT/neg",
          new double[] {106.026, 109.33, 106.559, 109, 11.717, 12.175, 11.838, 12.15}),
      Map.entry("ConstDivide/INT/pos",
          new double[] {105.319, 103.266, 104.98, 103.127, 11.076, 10.912, 11.06, 10.913}),
      Map.entry("ConstDivide/LONG/neg",
          new double[] {32.276, 26.61, 32.167, 26.605, 2.891, 2.92, 2.835, 2.904}),
      Map.entry("ConstDivide/LONG/pos",
          new double[] {31.937, 19.882, 32.305, 20.001, 2.952, 2.827, 2.993, 2.833}),
      Map.entry("DateDiff",
          new double[] {5.605, 4.709, 5.029, 4.853, 1.253, 1.208, 1.193, 1.199}),
      Map.entry("DayOfMonth",
          new double[] {20.363, 38.723, 20.397, 39.468, 3.716, 5.089, 3.822, 5.12}),
      Map.entry("DayOfWeek",
          new double[] {114.21, 117.624, 114.368, 117.748, 15.949, 16.018, 15.992, 16.024}),
      Map.entry("DayOfWeekIso",
          new double[] {116.488, 113.225, 117.12, 113.298, 15.974, 15.573, 15.988, 15.586}),
      Map.entry("DayOfYear",
          new double[] {90.273, 114.122, 91.357, 117.857, 12.449, 12.586, 12.633, 12.722}),
      Map.entry("Greatest/INT",
          new double[] {6.977, 70.644, 7.01, 71.105, 1.145, 3.413, 1.153, 3.412}),
      Map.entry("Greatest/LONG",
          new double[] {6.265, 61.504, 6.73, 61.53, 1.12, 3.234, 1.185, 3.24}),
      Map.entry("GuardedDay",
          new double[] {43.124, 67.884, 48.515, 73.289, 2.043, 2.192, 2.093, 2.194}),
      Map.entry("GuardedRange/INT",
          new double[] {56.425, 69.187, 50.685, 64.649, 3.37, 3.595, 2.017, 2.42}),
      Map.entry("GuardedRange/LONG",
          new double[] {41.727, 51.623, 47.129, 56.676, 2.033, 2.189, 2.055, 2.192}),
      Map.entry("IfElse/INT",
          new double[] {3.266, 67.653, 2.181, 69.196, 0.705, 0.765, 0.487, 0.67}),
      Map.entry("IfElse/LONG",
          new double[] {5.102, 43.912, 5.055, 43.902, 0.779, -0.014, 0.887, 0.002}),
      Map.entry("InRanges",
          new double[] {92.708, 99.892, 96.377, 100.244, 3.874, 4.756, 4.317, 4.811}),
      Map.entry("IntArith/ADD/FAIL/INT",
          new double[] {68.234, 83.981, 75.255, 88.667, 5.022, 4.838, 5.395, 4.831}),
      Map.entry("IntArith/ADD/FAIL/LONG",
          new double[] {62.175, 80.947, 67.62, 86.031, 5.052, 5.25, 5.07, 5.251}),
      Map.entry("IntArith/ADD/NULL/INT",
          new double[] {0, 107.965, 0, 109.784, 0, 5.27, 0, 5.267}),
      Map.entry("IntArith/ADD/NULL/LONG",
          new double[] {0, 90.12, 0, 90.23, 0, 5.249, 0, 5.261}),
      Map.entry("IntArith/ADD/WRAP/INT",
          new double[] {6.967, -13.347, 8.954, -14.311, 1.221, 0.519, 1.441, 0.547}),
      Map.entry("IntArith/ADD/WRAP/LONG",
          new double[] {6.17, 10.573, 6.176, 10.554, 1.069, 0.983, 1.074, 0.982}),
      Map.entry("IntArith/MUL/WRAP/INT",
          new double[] {8.061, 10.541, 7.976, 10.857, 1.534, 1.789, 1.536, 1.788}),
      Map.entry("IntArith/MUL/WRAP/LONG",
          new double[] {5.111, 8.3, 5.479, 8.302, 1.021, 1.174, 1.099, 1.177}),
      Map.entry("IntArith/SUB/FAIL/INT",
          new double[] {69.443, 82.21, 76.465, 87.089, 5.256, 4.646, 5.565, 4.652}),
      Map.entry("IntArith/SUB/FAIL/LONG",
          new double[] {60.65, 81.142, 66.683, 86.273, 5.119, 5.24, 5.295, 5.259}),
      Map.entry("IntArith/SUB/NULL/INT",
          new double[] {0, 104.202, 0, 106.545, 0, 5.539, 0, 5.588}),
      Map.entry("IntArith/SUB/NULL/LONG",
          new double[] {0, 88.259, 0, 88.44, 0, 5.405, 0, 5.424}),
      Map.entry("IntArith/SUB/WRAP/INT",
          new double[] {3.954, -6.181, 3.229, -5.956, 0.905, 0.558, 0.967, 0.636}),
      Map.entry("IntArith/SUB/WRAP/LONG",
          new double[] {5.885, 12.161, 5.85, 12.207, 1.034, 0.872, 1.11, 0.873}),
      Map.entry("IntNeg/FAIL/INT",
          new double[] {31.233, 53.386, 36.73, 58.804, 1.744, 1.894, 1.812, 1.901}),
      Map.entry("IntNeg/FAIL/LONG",
          new double[] {29.639, 37.949, 34.071, 42.942, 1.99, 2.102, 1.868, 2.095}),
      Map.entry("IntNeg/WRAP/INT",
          new double[] {1.563, -1.036, 2.086, -0.991, 0.799, 0.988, 0.888, 1.038}),
      Map.entry("IntNeg/WRAP/LONG",
          new double[] {7.121, 2.988, 7.673, 3.086, 1.092, 0.975, 1.066, 0.982}),
      Map.entry("IsNotNull/INT",
          new double[] {5.627, 16.243, 1.283, 14.56, 0.121, 0.439, 0.353, 0.45}),
      Map.entry("IsNotNull/LONG",
          new double[] {7.233, 14.007, 2.881, 12.976, 0.137, 1.219, -0.04, 1.179}),
      Map.entry("LastDay",
          new double[] {216.853, 264.627, 219.774, 268.64, 30.801, 33.366, 31.126, 33.41}),
      Map.entry("Least/INT",
          new double[] {6.605, 74.192, 6.489, 74.384, 1.176, 3.629, 1.124, 3.567}),
      Map.entry("Least/LONG",
          new double[] {5.287, 61.608, 5.083, 61.642, 1.05, 3.173, 1.043, 3.172}),
      Map.entry("LiteralSlot/INT",
          new double[] {7.082, 9.902, 6.447, 9.913, 0.925, 1.251, 1.062, 1.377}),
      Map.entry("LiteralSlot/LONG",
          new double[] {14.1, 7.15, 13.495, 7.155, 1.319, 2.014, 1.466, 2.07}),
      Map.entry("MakeDate/ansi",
          new double[] {476.002, 526.088, 481.29, 531.89, 53.668, 53.598, 53.498, 53.545}),
      Map.entry("MakeDate/null",
          new double[] {0, 528.932, 0, 533.89, 0, 54.448, 0, 54.327}),
      Map.entry("Month",
          new double[] {18.3, 37.117, 19.767, 38.096, 2.593, 4.232, 2.9, 4.322}),
      Map.entry("NarrowLane",
          new double[] {36.439, 34.147, 30.579, 31.401, 1.436, 2.657, 1.443, 2.644}),
      Map.entry("NextDay",
          new double[] {104.823, 104.809, 106.361, 105.188, 15.008, 15.309, 15.178, 15.338}),
      Map.entry("Not/INT",
          new double[] {7.773, 3.946, 8.837, 4.792, -0.057, 0.472, 0.016, 0.538}),
      Map.entry("Not/LONG",
          new double[] {8.117, -5.048, 8.287, -5.032, 0.122, -0.154, -0.039, -0.19}),
      Map.entry("Or/INT",
          new double[] {-3.336, 8.615, -4.155, 8.372, -0.824, -0.192, -0.782, -0.173}),
      Map.entry("Or/LONG",
          new double[] {5.981, 19.166, 6.912, 19.311, -0.383, -1.242, -0.131, -1.213}),
      Map.entry("Quarter",
          new double[] {34.777, 58.693, 34.896, 59.694, 5.616, 7.097, 5.738, 7.136}),
      Map.entry("SubDays",
          new double[] {10.493, 17.431, 11.125, 18.193, 1.471, 1.921, 1.464, 1.909}),
      Map.entry("ThursdayOf",
          new double[] {120.807, 119.137, 121.638, 119.819, 16.924, 16.587, 17.048, 16.655}),
      Map.entry("TruncDate/MONTH",
          new double[] {16.155, 49.921, 15.787, 50.522, 3.481, 5.713, 3.522, 5.661}),
      Map.entry("TruncDate/QUARTER",
          new double[] {213.012, 259.059, 211.576, 263.441, 29.931, 31.428, 29.906, 31.647}),
      Map.entry("TruncDate/YEAR",
          new double[] {109.794, 126.593, 112.043, 131.237, 14.648, 14.392, 14.918, 14.641}),
      Map.entry("TruncDateDynamic",
          new double[] {414.472, 460.341, 413.749, 462.311, 57.966, 61.735, 58.027, 61.744}),
      Map.entry("WeekDay",
          new double[] {109.679, 111.476, 110.001, 111.795, 14.944, 14.863, 14.965, 14.878}),
      Map.entry("WeekOfYear",
          new double[] {111.703, 111.573, 111, 114.802, 16.467, 12.621, 16.431, 12.665}),
      Map.entry("Year",
          new double[] {21.12, 45.402, 22.146, 49.298, 3.48, 3.386, 3.659, 3.493}),
      Map.entry("fixed/INT",
          new double[] {80.939, 49.966, 93.583, 61.621, -0.148, -0.805, -0.028, -0.835}),
      Map.entry("fixed/LONG",
          new double[] {87.545, 52.974, 98.935, 65.37, -0.466, -0.805, -0.328, -0.808}),
      Map.entry("out/INT",
          new double[] {25.101, 21.228, 26.741, 23.318, 1.248, 1.576, 1.127, 1.532}),
      Map.entry("out/LONG",
          new double[] {22.793, 33.114, 24.956, 35.065, 1.198, 1.128, 1.086, 1.106}),
      Map.entry("out/cond/INT",
          new double[] {13.3, 17.97, 9.595, 16.234, -0.657, -1.743, -0.964, -1.465}),
      Map.entry("out/cond/LONG",
          new double[] {14.159, 24.611, 12, 22.939, -0.636, -1.89, -0.334, -1.305}),
      Map.entry("prefix",
          new double[] {267.077, 274.904, 266.664, 272.719, 28.585, 31.22, 28.463, 31.142}),
      Map.entry("prefix/load",
          new double[] {16.373, -6.017, 19.463, -4.752, -1.076, -3.04, -1.116, -3.049}),
      Map.entry("prefix/month",
          new double[] {15.34, -1.494, 15.549, 0.433, 1.903, -1.845, 1.837, -1.843}));
}
