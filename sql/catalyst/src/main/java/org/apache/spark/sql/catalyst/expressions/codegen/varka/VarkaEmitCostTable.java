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
  static final double BYTES_MARGIN = 0.163;

  /** The same for the call-site budget. */
  static final double SITES_MARGIN = 0.286;

  /** Each feature's price, by feature. */
  static final Map<String, double[]> PRICES = Map.ofEntries(
      Map.entry("AddDays",
          new double[] {10.559, 15.959, 11.177, 16.35, 1.407, 2.313, 1.426, 2.314}),
      Map.entry("AddMonths",
          new double[] {626.339, 723.831, 629.894, 729.9, 80.691, 88.347, 80.765, 88.339}),
      Map.entry("And/INT",
          new double[] {-6.912, 12.057, -6.03, 12.285, -1.895, -1.043, -1.831, -1.061}),
      Map.entry("And/LONG",
          new double[] {9.446, 17.932, 9.814, 18.159, -0.103, -0.374, -0.04, -0.352}),
      Map.entry("BitCount/INT",
          new double[] {23.57, 22.53, 23.308, 22.587, 3.983, 3.846, 3.991, 3.851}),
      Map.entry("BitCount/LONG",
          new double[] {7.124, 4.446, 7.183, 4.488, 1.033, 1.028, 1.012, 1.027}),
      Map.entry("BitOp/AND/INT",
          new double[] {14.537, 2.048, 15.308, 1.938, 1.443, 1.204, 1.535, 1.22}),
      Map.entry("BitOp/AND/LONG",
          new double[] {13.246, 18.765, 13.51, 18.823, 1.301, 1.2, 1.351, 1.194}),
      Map.entry("BitOp/OR/INT",
          new double[] {9.855, 13.447, 9.817, 14.182, 1.215, 1.806, 1.268, 1.812}),
      Map.entry("BitOp/OR/LONG",
          new double[] {10.118, 12.549, 10.024, 12.647, 1.057, 1.014, 1.052, 1.015}),
      Map.entry("BitOp/SHL/INT",
          new double[] {7.133, 5.525, 7.191, 5.633, 0.698, 0.667, 0.653, 0.663}),
      Map.entry("BitOp/SHL/LONG",
          new double[] {11.259, 8.597, 11.473, 8.549, 1.088, 1.09, 1.075, 1.088}),
      Map.entry("BitOp/SHR/INT",
          new double[] {9.443, 15.642, 9.372, 16.236, 1.098, 1.305, 1.055, 1.299}),
      Map.entry("BitOp/SHR/LONG",
          new double[] {11.065, 12.752, 11.054, 12.746, 1.188, 1.096, 1.189, 1.101}),
      Map.entry("BitOp/USHR/INT",
          new double[] {8.208, -0.474, 8.356, -0.534, 0.925, 1.057, 0.947, 1.056}),
      Map.entry("BitOp/USHR/LONG",
          new double[] {11.002, 7.063, 11.186, 7.061, 1.135, 1.097, 1.108, 1.094}),
      Map.entry("BitOp/XOR/INT",
          new double[] {11.246, 3.921, 11.241, 3.663, 1.246, 1.494, 1.239, 1.489}),
      Map.entry("BitOp/XOR/LONG",
          new double[] {10.79, 12.974, 10.914, 12.911, 1.261, 1.126, 1.232, 1.126}),
      Map.entry("BoundedDivide",
          new double[] {14.934, 16.776, 15.093, 17.022, 2.043, 2.22, 2.064, 2.229}),
      Map.entry("ColumnRef/INT",
          new double[] {18.424, 91.788, 20.37, 95.406, 0.778, 1.364, 0.735, 1.364}),
      Map.entry("ColumnRef/LONG",
          new double[] {20.976, 105.296, 23.17, 107.48, 0.836, 0.953, 0.811, 0.951}),
      Map.entry("Compare/EQ/INT",
          new double[] {25.835, 41.392, 26.308, 41.009, 2.221, 2.467, 2.241, 2.511}),
      Map.entry("Compare/EQ/LONG",
          new double[] {15.086, 47.8, 15.806, 47.86, 1.263, 1.699, 1.278, 1.703}),
      Map.entry("Compare/GE/INT",
          new double[] {23.911, 47.104, 24.556, 47.033, 1.978, 2.74, 2.176, 2.825}),
      Map.entry("Compare/GE/LONG",
          new double[] {15.074, 49.704, 15.254, 49.709, 1.262, 2.229, 1.26, 2.244}),
      Map.entry("Compare/GT/INT",
          new double[] {20.164, 58.072, 20.911, 58.825, 1.41, 3.764, 1.537, 3.824}),
      Map.entry("Compare/GT/LONG",
          new double[] {14.735, 46.145, 15.648, 46.113, 1.206, 2.408, 1.23, 2.399}),
      Map.entry("Compare/LE/INT",
          new double[] {17.958, 54.919, 18.108, 55.201, 1.229, 3.581, 1.265, 3.624}),
      Map.entry("Compare/LE/LONG",
          new double[] {12.572, 50.743, 13.116, 50.8, 1.173, 1.753, 1.232, 1.756}),
      Map.entry("Compare/LT/INT",
          new double[] {24.174, 47.574, 25.072, 48.022, 1.995, 2.641, 2.162, 2.72}),
      Map.entry("Compare/LT/LONG",
          new double[] {15.096, 47.406, 15.88, 47.456, 1.217, 1.918, 1.26, 1.92}),
      Map.entry("ConstDivide/INT/neg",
          new double[] {108.131, 106.364, 108.36, 106.338, 11.989, 11.774, 11.998, 11.774}),
      Map.entry("ConstDivide/INT/pos",
          new double[] {106.396, 99.375, 106.685, 99.208, 11.168, 10.715, 11.188, 10.715}),
      Map.entry("ConstDivide/LONG/neg",
          new double[] {31.611, 22.16, 31.337, 22.052, 2.907, 3.102, 2.877, 3.095}),
      Map.entry("ConstDivide/LONG/pos",
          new double[] {32.192, 22.336, 32.597, 22.437, 2.989, 3.136, 3.053, 3.143}),
      Map.entry("DateDiff",
          new double[] {7.01, 6.43, 7.026, 6.515, 1.151, 1.047, 1.125, 1.038}),
      Map.entry("DayOfMonth",
          new double[] {22.92, 40.495, 22.605, 40.87, 3.916, 5.547, 3.96, 5.533}),
      Map.entry("DayOfWeek",
          new double[] {116.857, 116.151, 116.947, 116.371, 16.203, 15.804, 16.209, 15.81}),
      Map.entry("DayOfWeekIso",
          new double[] {114.729, 116.694, 114.582, 116.76, 15.936, 15.868, 15.897, 15.864}),
      Map.entry("DayOfYear",
          new double[] {99.319, 120.576, 99.457, 123.318, 13.307, 13.293, 13.318, 13.291}),
      Map.entry("Greatest/INT",
          new double[] {6.288, 138.887, 6.391, 139.789, 0.907, 17.601, 0.927, 17.604}),
      Map.entry("Greatest/LONG",
          new double[] {4.978, 125.002, 5.24, 125.039, 0.95, 17.274, 1.022, 17.284}),
      Map.entry("GuardedDay",
          new double[] {44.224, 94.612, 49.173, 99.943, 2.01, 8.127, 2.006, 8.129}),
      Map.entry("GuardedRange/INT",
          new double[] {45.466, 97.753, 50.626, 103.215, 2.145, 8.622, 2.135, 8.619}),
      Map.entry("GuardedRange/LONG",
          new double[] {42.169, 82.6, 47.344, 87.695, 2.021, 8.835, 2.016, 8.84}),
      Map.entry("IfElse/INT",
          new double[] {1.217, 95.12, 0.956, 96.713, 0.489, 7.042, 0.429, 6.98}),
      Map.entry("IfElse/LONG",
          new double[] {7.931, 77.864, 7.97, 77.877, 1.05, 7.876, 1.123, 7.873}),
      Map.entry("InRanges",
          new double[] {78.217, 102.757, 79.073, 102.674, 2.571, 5.284, 2.639, 5.329}),
      Map.entry("IntArith/ADD/FAIL/INT",
          new double[] {68.785, 122.871, 75.115, 128.641, 5.296, 12.018, 5.539, 12.034}),
      Map.entry("IntArith/ADD/FAIL/LONG",
          new double[] {61.17, 117.956, 66.43, 123.055, 4.984, 12.604, 5.059, 12.614}),
      Map.entry("IntArith/ADD/NULL/INT",
          new double[] {0, 112.285, 0, 114.423, 0, 6.021, 0, 6.021}),
      Map.entry("IntArith/ADD/NULL/LONG",
          new double[] {0, 92.529, 0, 92.625, 0, 4.911, 0, 4.918}),
      Map.entry("IntArith/ADD/WRAP/INT",
          new double[] {5.45, -3.725, 5.356, -3.851, 1.006, 1.087, 1.091, 1.094}),
      Map.entry("IntArith/ADD/WRAP/LONG",
          new double[] {4.975, 10.224, 4.927, 10.147, 1.072, 1.056, 1.086, 1.056}),
      Map.entry("IntArith/MUL/WRAP/INT",
          new double[] {6.127, 13.325, 6.653, 13.816, 1.368, 2.284, 1.445, 2.29}),
      Map.entry("IntArith/MUL/WRAP/LONG",
          new double[] {6.277, 7.288, 6.396, 7.278, 1.085, 1.113, 1.107, 1.114}),
      Map.entry("IntArith/SUB/FAIL/INT",
          new double[] {62.528, 122.624, 67.224, 127.858, 5.296, 12.26, 5.255, 12.243}),
      Map.entry("IntArith/SUB/FAIL/LONG",
          new double[] {62.325, 112.593, 67.352, 117.607, 4.993, 12.939, 5.019, 12.938}),
      Map.entry("IntArith/SUB/NULL/INT",
          new double[] {0, 106.208, 0, 107.803, 0, 6.204, 0, 6.217}),
      Map.entry("IntArith/SUB/NULL/LONG",
          new double[] {0, 91.347, 0, 91.349, 0, 5.169, 0, 5.171}),
      Map.entry("IntArith/SUB/WRAP/INT",
          new double[] {5.224, 10.188, 5.303, 10.271, 1.24, 1.898, 1.322, 1.916}),
      Map.entry("IntArith/SUB/WRAP/LONG",
          new double[] {5.375, 7.559, 5.675, 7.565, 1.133, 1.014, 1.18, 1.023}),
      Map.entry("IntNeg/FAIL/INT",
          new double[] {31.435, 77.744, 36.426, 82.838, 2.09, 8.105, 2.093, 8.103}),
      Map.entry("IntNeg/FAIL/LONG",
          new double[] {30.262, 70.652, 35.638, 75.711, 2.023, 8.373, 2.057, 8.378}),
      Map.entry("IntNeg/WRAP/INT",
          new double[] {2.678, 3.403, 2.445, 3.324, 0.929, 0.951, 0.897, 0.945}),
      Map.entry("IntNeg/WRAP/LONG",
          new double[] {4.337, 3.508, 4.468, 3.54, 0.866, 1.192, 0.843, 1.194}),
      Map.entry("IsNotNull/INT",
          new double[] {-21.785, 31.998, -31.747, 29.301, -2.335, 1.916, -2.401, 1.869}),
      Map.entry("IsNotNull/LONG",
          new double[] {4.963, 33.618, 0.893, 32.475, -0.054, 2.828, -0.206, 2.779}),
      Map.entry("IsNotNull/cond/INT",
          new double[] {-54.342, 13.928, -56.852, 13.883, -6.004, 1.1, -6.16, 1.148}),
      Map.entry("IsNotNull/cond/LONG",
          new double[] {-17.911, 14.802, -20.816, 14.644, -2.178, 0.142, -2.355, 0.123}),
      Map.entry("LastDay",
          new double[] {220.573, 261.329, 220.663, 264.253, 31.084, 33.857, 31.081, 33.845}),
      Map.entry("Least/INT",
          new double[] {7.224, 136.873, 7.55, 137.49, 1.187, 17.238, 1.275, 17.249}),
      Map.entry("Least/LONG",
          new double[] {5.765, 126.106, 6.109, 126.179, 1.058, 17.247, 1.134, 17.254}),
      Map.entry("LiteralSlot/INT",
          new double[] {8.554, 3.115, 7.27, 2.451, 0.998, -0.247, 1.049, -0.228}),
      Map.entry("LiteralSlot/LONG",
          new double[] {13.715, 1.489, 13.198, 1.425, 1.317, 0.889, 1.46, 0.935}),
      Map.entry("MakeDate/ansi",
          new double[] {465.807, 555.365, 471.061, 561.768, 52.97, 59.949, 52.964, 59.961}),
      Map.entry("MakeDate/null",
          new double[] {0, 562.701, 0, 569.063, 0, 60.894, 0, 60.911}),
      Map.entry("Month",
          new double[] {19.065, 42.472, 18.72, 42.901, 2.524, 4.963, 2.597, 4.965}),
      Map.entry("NarrowLane",
          new double[] {32.16, 27.682, 29.409, 25.377, 1.101, 0.962, 1.087, 0.957}),
      Map.entry("NextDay",
          new double[] {103.507, 103.517, 103.775, 103.52, 14.957, 15.421, 15.001, 15.429}),
      Map.entry("Not/INT",
          new double[] {14.166, 1.23, 14.272, 1.587, 0.972, 0.132, 1.005, 0.116}),
      Map.entry("Not/LONG",
          new double[] {8.386, 13.16, 8.503, 13.117, 0.117, 1.225, 0.02, 1.201}),
      Map.entry("Or/INT",
          new double[] {11.868, 11.857, 11.674, 12.553, 0.902, -0.969, 0.84, -0.949}),
      Map.entry("Or/LONG",
          new double[] {9.932, 12.824, 10.384, 13.147, -0.015, -0.018, 0.07, 0.04}),
      Map.entry("Quarter",
          new double[] {39.448, 56.938, 38.811, 57.311, 5.872, 7.724, 5.913, 7.715}),
      Map.entry("SubDays",
          new double[] {11.695, 17.99, 12.408, 18.49, 1.552, 2.693, 1.586, 2.7}),
      Map.entry("ThursdayOf",
          new double[] {117.659, 124.678, 117.412, 125.086, 16.536, 17.045, 16.501, 17.042}),
      Map.entry("TruncDate/MONTH",
          new double[] {24.072, 29.398, 23.664, 30.132, 3.951, 5.243, 4.001, 5.232}),
      Map.entry("TruncDate/QUARTER",
          new double[] {214.984, 246.907, 214.009, 249.125, 29.798, 31.98, 29.824, 31.989}),
      Map.entry("TruncDate/YEAR",
          new double[] {107.088, 124.571, 107.418, 127.425, 14.695, 15.177, 14.795, 15.19}),
      Map.entry("TruncDateDynamic",
          new double[] {417.162, 465.001, 416.394, 467.097, 57.896, 61.76, 57.985, 61.744}),
      Map.entry("WeekDay",
          new double[] {110.085, 110.409, 110.212, 110.494, 14.815, 14.942, 14.832, 14.95}),
      Map.entry("WeekOfYear",
          new double[] {126.621, 116.28, 127.039, 119.027, 17.713, 13.574, 17.711, 13.576}),
      Map.entry("WidenLane",
          new double[] {33.814, -11.556, 75.03, 27.36, 1.199, 1.342, 1.268, 1.368}),
      Map.entry("Year",
          new double[] {29.421, 51.031, 29.447, 54.094, 4.14, 4.383, 4.107, 4.38}),
      Map.entry("fixed/INT",
          new double[] {83.264, 49.319, 95.899, 61.156, 0.064, -0.581, 0.135, -0.585}),
      Map.entry("fixed/LONG",
          new double[] {86.482, 52.128, 98.25, 64.555, -0.152, -0.497, -0.059, -0.509}),
      Map.entry("out/INT",
          new double[] {24.288, 20.746, 26.524, 23.054, 1.173, 1.279, 1.131, 1.27}),
      Map.entry("out/LONG",
          new double[] {23.246, 29.652, 25.275, 31.652, 1.174, 0.789, 1.055, 0.776}),
      Map.entry("out/cond/INT",
          new double[] {18.718, 23.722, 18.73, 22.796, -0.369, -3.374, -0.294, -3.143}),
      Map.entry("out/cond/LONG",
          new double[] {18.281, 24.226, 16.298, 23.76, -0.455, -2.442, -0.217, -1.84}),
      Map.entry("prefix",
          new double[] {259.26, 272.555, 259.506, 271.158, 27.963, 30.551, 28.005, 30.56}),
      Map.entry("prefix/load",
          new double[] {40.111, -7.538, 44.145, -6.238, 0.021, -3.536, 0.027, -3.538}),
      Map.entry("prefix/month",
          new double[] {17.19, -6.609, 17.49, -5.099, 2.216, -1.969, 2.127, -1.962}));
}
