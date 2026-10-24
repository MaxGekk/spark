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
  static final double BYTES_MARGIN = 0.156;

  /** The same for the call-site budget. */
  static final double SITES_MARGIN = 0.206;

  /** Each feature's price, by feature. */
  static final Map<String, double[]> PRICES = Map.ofEntries(
      Map.entry("AddDays",
          new double[] {10.559, 12.056, 11.177, 12.447, 1.407, 1.323, 1.426, 1.324}),
      Map.entry("AddMonths",
          new double[] {626.339, 704.204, 629.894, 710.273, 80.691, 83.426, 80.765, 83.418}),
      Map.entry("And/INT",
          new double[] {-6.912, 13.21, -6.03, 13.438, -1.895, -0.754, -1.831, -0.771}),
      Map.entry("And/LONG",
          new double[] {9.446, 14.607, 9.814, 14.834, -0.103, -1.236, -0.04, -1.214}),
      Map.entry("BitCount/INT",
          new double[] {23.57, 22.364, 23.308, 22.421, 3.983, 3.837, 3.991, 3.842}),
      Map.entry("BitCount/LONG",
          new double[] {7.124, 4.358, 7.183, 4.401, 1.033, 1.005, 1.012, 1.004}),
      Map.entry("BitOp/AND/INT",
          new double[] {14.537, 2.378, 15.308, 2.268, 1.443, 1.253, 1.535, 1.269}),
      Map.entry("BitOp/AND/LONG",
          new double[] {13.246, 18.093, 13.51, 18.151, 1.301, 1.026, 1.351, 1.02}),
      Map.entry("BitOp/OR/INT",
          new double[] {9.855, 13.933, 9.817, 14.668, 1.215, 1.95, 1.268, 1.956}),
      Map.entry("BitOp/OR/LONG",
          new double[] {10.118, 12.211, 10.024, 12.309, 1.057, 0.926, 1.052, 0.927}),
      Map.entry("BitOp/SHL/INT",
          new double[] {7.133, 6.943, 7.191, 7.05, 0.698, 1.007, 0.653, 1.003}),
      Map.entry("BitOp/SHL/LONG",
          new double[] {11.259, 8.778, 11.473, 8.73, 1.088, 1.137, 1.075, 1.135}),
      Map.entry("BitOp/SHR/INT",
          new double[] {9.443, 16.875, 9.372, 17.469, 1.098, 1.599, 1.055, 1.593}),
      Map.entry("BitOp/SHR/LONG",
          new double[] {11.065, 11.43, 11.054, 11.424, 1.188, 0.753, 1.189, 0.758}),
      Map.entry("BitOp/USHR/INT",
          new double[] {8.208, 0.408, 8.356, 0.347, 0.925, 1.253, 0.947, 1.253}),
      Map.entry("BitOp/USHR/LONG",
          new double[] {11.002, 6.778, 11.186, 6.775, 1.135, 1.023, 1.108, 1.02}),
      Map.entry("BitOp/XOR/INT",
          new double[] {11.246, 3.935, 11.241, 3.677, 1.246, 1.481, 1.239, 1.475}),
      Map.entry("BitOp/XOR/LONG",
          new double[] {10.79, 12.728, 10.914, 12.666, 1.261, 1.063, 1.232, 1.062}),
      Map.entry("BoundedDivide",
          new double[] {14.934, 17.109, 15.093, 17.355, 2.043, 2.304, 2.064, 2.314}),
      Map.entry("ColumnRef/INT",
          new double[] {18.424, 90.163, 20.37, 93.782, 0.778, 0.943, 0.735, 0.943}),
      Map.entry("ColumnRef/LONG",
          new double[] {20.976, 103.888, 23.17, 106.073, 0.836, 0.588, 0.811, 0.586}),
      Map.entry("Compare/EQ/INT",
          new double[] {25.835, 38.516, 26.308, 38.133, 2.221, 1.858, 2.241, 1.902}),
      Map.entry("Compare/EQ/LONG",
          new double[] {15.086, 49.842, 15.806, 49.903, 1.263, 2.229, 1.278, 2.232}),
      Map.entry("Compare/GE/INT",
          new double[] {23.911, 45.963, 24.556, 45.892, 1.978, 2.511, 2.176, 2.596}),
      Map.entry("Compare/GE/LONG",
          new double[] {15.074, 51.522, 15.254, 51.527, 1.262, 2.701, 1.26, 2.716}),
      Map.entry("Compare/GT/INT",
          new double[] {20.164, 52.417, 20.911, 53.17, 1.41, 2.423, 1.537, 2.483}),
      Map.entry("Compare/GT/LONG",
          new double[] {14.735, 47.376, 15.648, 47.344, 1.206, 2.728, 1.23, 2.718}),
      Map.entry("Compare/LE/INT",
          new double[] {17.958, 50.483, 18.108, 50.765, 1.229, 2.628, 1.265, 2.672}),
      Map.entry("Compare/LE/LONG",
          new double[] {12.572, 53.06, 13.116, 53.117, 1.173, 2.354, 1.232, 2.357}),
      Map.entry("Compare/LT/INT",
          new double[] {24.174, 45.668, 25.072, 46.116, 1.995, 2.22, 2.162, 2.298}),
      Map.entry("Compare/LT/LONG",
          new double[] {15.096, 49.622, 15.88, 49.673, 1.217, 2.493, 1.26, 2.495}),
      Map.entry("ConstDivide/INT/neg",
          new double[] {108.131, 107.062, 108.36, 107.035, 11.989, 11.914, 11.998, 11.914}),
      Map.entry("ConstDivide/INT/pos",
          new double[] {106.396, 98.637, 106.685, 98.47, 11.168, 10.553, 11.188, 10.553}),
      Map.entry("ConstDivide/LONG/neg",
          new double[] {31.611, 22.447, 31.337, 22.339, 2.907, 3.176, 2.877, 3.169}),
      Map.entry("ConstDivide/LONG/pos",
          new double[] {32.192, 21.863, 32.597, 21.964, 2.989, 3.014, 3.053, 3.02}),
      Map.entry("DateDiff",
          new double[] {7.01, 7.415, 7.026, 7.501, 1.151, 1.307, 1.125, 1.297}),
      Map.entry("DayOfMonth",
          new double[] {22.92, 38.718, 22.605, 39.093, 3.916, 5.079, 3.96, 5.065}),
      Map.entry("DayOfWeek",
          new double[] {116.857, 115.944, 116.947, 116.164, 16.203, 15.735, 16.209, 15.741}),
      Map.entry("DayOfWeekIso",
          new double[] {114.729, 116.52, 114.582, 116.586, 15.936, 15.826, 15.897, 15.822}),
      Map.entry("DayOfYear",
          new double[] {99.319, 117.449, 99.457, 120.19, 13.307, 12.493, 13.318, 12.492}),
      Map.entry("Greatest/INT",
          new double[] {6.288, 82.478, 6.391, 83.38, 0.907, 3.492, 0.927, 3.495}),
      Map.entry("Greatest/LONG",
          new double[] {4.978, 71.198, 5.24, 71.235, 0.95, 3.325, 1.022, 3.335}),
      Map.entry("GuardedDay",
          new double[] {44.224, 69.666, 49.173, 74.998, 2.01, 1.948, 2.006, 1.95}),
      Map.entry("GuardedRange/INT",
          new double[] {45.466, 72.477, 50.626, 77.939, 2.145, 2.307, 2.135, 2.304}),
      Map.entry("GuardedRange/LONG",
          new double[] {42.169, 56.818, 47.344, 61.913, 2.021, 2.151, 2.016, 2.156}),
      Map.entry("IfElse/INT",
          new double[] {1.217, 67.457, 0.956, 69.05, 0.489, 0.077, 0.429, 0.015}),
      Map.entry("IfElse/LONG",
          new double[] {7.931, 48.487, 7.97, 48.5, 1.05, 0.259, 1.123, 0.257}),
      Map.entry("InRanges",
          new double[] {78.217, 98.149, 79.073, 98.066, 2.571, 4.17, 2.639, 4.215}),
      Map.entry("IntArith/ADD/FAIL/INT",
          new double[] {68.785, 96.726, 75.115, 102.495, 5.296, 5.602, 5.539, 5.618}),
      Map.entry("IntArith/ADD/FAIL/LONG",
          new double[] {61.17, 88.78, 66.43, 93.88, 4.984, 5.04, 5.059, 5.05}),
      Map.entry("IntArith/ADD/NULL/INT",
          new double[] {0, 110.074, 0, 112.213, 0, 5.553, 0, 5.553}),
      Map.entry("IntArith/ADD/NULL/LONG",
          new double[] {0, 93.108, 0, 93.205, 0, 5.061, 0, 5.068}),
      Map.entry("IntArith/ADD/WRAP/INT",
          new double[] {5.45, -3.823, 5.356, -3.949, 1.006, 1.129, 1.091, 1.136}),
      Map.entry("IntArith/ADD/WRAP/LONG",
          new double[] {4.975, 10.405, 4.927, 10.328, 1.072, 1.103, 1.086, 1.103}),
      Map.entry("IntArith/MUL/WRAP/INT",
          new double[] {6.127, 11.348, 6.653, 11.839, 1.368, 1.757, 1.445, 1.763}),
      Map.entry("IntArith/MUL/WRAP/LONG",
          new double[] {6.277, 7.865, 6.396, 7.855, 1.085, 1.262, 1.107, 1.264}),
      Map.entry("IntArith/SUB/FAIL/INT",
          new double[] {62.528, 94.045, 67.224, 99.279, 5.296, 5.199, 5.255, 5.182}),
      Map.entry("IntArith/SUB/FAIL/LONG",
          new double[] {62.325, 83.668, 67.352, 88.683, 4.993, 5.44, 5.019, 5.439}),
      Map.entry("IntArith/SUB/NULL/INT",
          new double[] {0, 101.75, 0, 103.344, 0, 5.203, 0, 5.217}),
      Map.entry("IntArith/SUB/NULL/LONG",
          new double[] {0, 92.571, 0, 92.574, 0, 5.487, 0, 5.488}),
      Map.entry("IntArith/SUB/WRAP/INT",
          new double[] {5.224, 7.395, 5.303, 7.477, 1.24, 1.207, 1.322, 1.224}),
      Map.entry("IntArith/SUB/WRAP/LONG",
          new double[] {5.375, 7.559, 5.675, 7.566, 1.133, 1.014, 1.18, 1.023}),
      Map.entry("IntNeg/FAIL/INT",
          new double[] {31.435, 51.963, 36.426, 57.057, 2.09, 1.65, 2.093, 1.647}),
      Map.entry("IntNeg/FAIL/LONG",
          new double[] {30.262, 45.438, 35.638, 50.496, 2.023, 1.836, 2.057, 1.841}),
      Map.entry("IntNeg/WRAP/INT",
          new double[] {2.678, 3.027, 2.445, 2.948, 0.929, 0.911, 0.897, 0.906}),
      Map.entry("IntNeg/WRAP/LONG",
          new double[] {4.337, 3.332, 4.468, 3.364, 0.866, 1.147, 0.843, 1.148}),
      Map.entry("IsNotNull/INT",
          new double[] {-21.785, 25.874, -31.747, 23.177, -2.335, 0.6, -2.401, 0.553}),
      Map.entry("IsNotNull/LONG",
          new double[] {4.963, 27.059, 0.893, 25.916, -0.054, 1.128, -0.206, 1.079}),
      Map.entry("IsNotNull/cond/INT",
          new double[] {-54.342, 8.991, -56.852, 8.947, -6.004, -0.144, -6.16, -0.096}),
      Map.entry("IsNotNull/cond/LONG",
          new double[] {-17.911, 13.269, -20.816, 13.111, -2.178, -0.255, -2.355, -0.274}),
      Map.entry("LastDay",
          new double[] {220.573, 258.925, 220.663, 261.849, 31.084, 33.232, 31.081, 33.221}),
      Map.entry("Least/INT",
          new double[] {7.224, 80.461, 7.55, 81.077, 1.187, 3.123, 1.275, 3.134}),
      Map.entry("Least/LONG",
          new double[] {5.765, 72.174, 6.109, 72.246, 1.058, 3.265, 1.134, 3.271}),
      Map.entry("LiteralSlot/INT",
          new double[] {8.554, 9.66, 7.27, 8.996, 0.998, 1.383, 1.049, 1.402}),
      Map.entry("LiteralSlot/LONG",
          new double[] {13.715, 5.878, 13.198, 5.814, 1.317, 2.027, 1.46, 2.073}),
      Map.entry("MakeDate/ansi",
          new double[] {465.807, 528.363, 471.061, 534.766, 52.97, 53.252, 52.964, 53.264}),
      Map.entry("MakeDate/null",
          new double[] {0, 535.926, 0, 542.289, 0, 54.224, 0, 54.241}),
      Map.entry("Month",
          new double[] {19.065, 40.603, 18.72, 41.031, 2.524, 4.469, 2.597, 4.471}),
      Map.entry("NarrowLane",
          new double[] {32.16, 31.793, 29.409, 29.488, 1.101, 2.028, 1.087, 2.023}),
      Map.entry("NextDay",
          new double[] {103.507, 103.318, 103.775, 103.322, 14.957, 15.365, 15.001, 15.373}),
      Map.entry("Not/INT",
          new double[] {14.166, 1.119, 14.272, 1.475, 0.972, 0.097, 1.005, 0.081}),
      Map.entry("Not/LONG",
          new double[] {8.386, 8.948, 8.503, 8.905, 0.117, 0.133, 0.02, 0.109}),
      Map.entry("Or/INT",
          new double[] {11.868, 13.985, 11.674, 14.682, 0.902, -0.629, 0.84, -0.61}),
      Map.entry("Or/LONG",
          new double[] {9.932, 7.866, 10.384, 8.189, -0.015, -1.303, 0.07, -1.245}),
      Map.entry("Quarter",
          new double[] {39.448, 55.778, 38.811, 56.15, 5.872, 7.408, 5.913, 7.399}),
      Map.entry("SubDays",
          new double[] {11.695, 14.009, 12.408, 14.509, 1.552, 1.736, 1.586, 1.743}),
      Map.entry("ThursdayOf",
          new double[] {117.659, 123.78, 117.412, 124.189, 16.536, 16.838, 16.501, 16.835}),
      Map.entry("TruncDate/MONTH",
          new double[] {24.072, 28.238, 23.664, 28.973, 3.951, 5.038, 4.001, 5.027}),
      Map.entry("TruncDate/QUARTER",
          new double[] {214.984, 244.067, 214.009, 246.285, 29.798, 31.39, 29.824, 31.399}),
      Map.entry("TruncDate/YEAR",
          new double[] {107.088, 125.939, 107.418, 128.794, 14.695, 15.485, 14.795, 15.498}),
      Map.entry("TruncDateDynamic",
          new double[] {417.162, 463.024, 416.394, 465.121, 57.896, 61.302, 57.985, 61.286}),
      Map.entry("WeekDay",
          new double[] {110.085, 110.217, 110.212, 110.302, 14.815, 14.885, 14.832, 14.893}),
      Map.entry("WeekOfYear",
          new double[] {126.621, 113.87, 127.039, 116.617, 17.713, 12.926, 17.711, 12.928}),
      Map.entry("WidenLane",
          new double[] {33.814, -9.924, 75.03, 28.992, 1.199, 1.765, 1.268, 1.791}),
      Map.entry("Year",
          new double[] {29.421, 48.386, 29.447, 51.449, 4.14, 3.705, 4.107, 3.701}),
      Map.entry("fixed/INT",
          new double[] {83.264, 48.784, 95.899, 60.621, 0.064, -0.762, 0.135, -0.766}),
      Map.entry("fixed/LONG",
          new double[] {86.482, 51.465, 98.25, 63.892, -0.152, -0.669, -0.059, -0.681}),
      Map.entry("out/INT",
          new double[] {24.288, 21.785, 26.524, 24.093, 1.173, 1.568, 1.131, 1.559}),
      Map.entry("out/LONG",
          new double[] {23.246, 30.305, 25.275, 32.305, 1.174, 0.958, 1.055, 0.945}),
      Map.entry("out/cond/INT",
          new double[] {18.718, 29.782, 18.73, 28.855, -0.369, -1.931, -0.294, -1.7}),
      Map.entry("out/cond/LONG",
          new double[] {18.281, 26.933, 16.298, 26.468, -0.455, -1.741, -0.217, -1.138}),
      Map.entry("prefix",
          new double[] {259.26, 274.437, 259.506, 273.04, 27.963, 31.065, 28.005, 31.073}),
      Map.entry("prefix/load",
          new double[] {40.111, -4.047, 44.145, -2.747, 0.021, -2.657, 0.027, -2.66}),
      Map.entry("prefix/month",
          new double[] {17.19, -7.239, 17.49, -5.728, 2.216, -2.116, 2.127, -2.109}));
}
