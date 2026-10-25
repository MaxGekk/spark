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
  static final double BYTES_MARGIN = 0.157;

  /** The same for the call-site budget. */
  static final double SITES_MARGIN = 0.206;

  /** Each feature's price, by feature. */
  static final Map<String, double[]> PRICES = Map.ofEntries(
      Map.entry("AddDays",
          new double[] {10.559, 12.572, 11.177, 12.963, 1.407, 1.323, 1.426, 1.324}),
      Map.entry("AddMonths",
          new double[] {626.339, 706.828, 629.894, 712.896, 80.691, 83.426, 80.765, 83.418}),
      Map.entry("And/INT",
          new double[] {-6.912, 13.148, -6.03, 13.375, -1.895, -0.754, -1.831, -0.771}),
      Map.entry("And/LONG",
          new double[] {9.446, 15.1, 9.814, 15.327, -0.103, -1.236, -0.04, -1.214}),
      Map.entry("BitCount/INT",
          new double[] {23.57, 22.402, 23.308, 22.459, 3.983, 3.837, 3.991, 3.842}),
      Map.entry("BitCount/LONG",
          new double[] {7.124, 4.371, 7.183, 4.414, 1.033, 1.005, 1.012, 1.004}),
      Map.entry("BitOp/AND/INT",
          new double[] {14.537, 2.331, 15.308, 2.221, 1.443, 1.253, 1.535, 1.269}),
      Map.entry("BitOp/AND/LONG",
          new double[] {13.246, 18.193, 13.51, 18.25, 1.301, 1.026, 1.351, 1.02}),
      Map.entry("BitOp/OR/INT",
          new double[] {9.855, 13.882, 9.817, 14.617, 1.215, 1.95, 1.268, 1.956}),
      Map.entry("BitOp/OR/LONG",
          new double[] {10.118, 12.261, 10.024, 12.359, 1.057, 0.926, 1.052, 0.927}),
      Map.entry("BitOp/SHL/INT",
          new double[] {7.133, 6.744, 7.191, 6.852, 0.698, 1.007, 0.653, 1.003}),
      Map.entry("BitOp/SHL/LONG",
          new double[] {11.259, 8.752, 11.473, 8.703, 1.088, 1.137, 1.075, 1.135}),
      Map.entry("BitOp/SHR/INT",
          new double[] {9.443, 16.642, 9.372, 17.236, 1.098, 1.599, 1.055, 1.593}),
      Map.entry("BitOp/SHR/LONG",
          new double[] {11.065, 11.626, 11.054, 11.62, 1.188, 0.753, 1.189, 0.758}),
      Map.entry("BitOp/USHR/INT",
          new double[] {8.208, 0.261, 8.356, 0.2, 0.925, 1.253, 0.947, 1.253}),
      Map.entry("BitOp/USHR/LONG",
          new double[] {11.002, 6.82, 11.186, 6.818, 1.135, 1.023, 1.108, 1.02}),
      Map.entry("BitOp/XOR/INT",
          new double[] {11.246, 3.988, 11.241, 3.729, 1.246, 1.481, 1.239, 1.475}),
      Map.entry("BitOp/XOR/LONG",
          new double[] {10.79, 12.765, 10.914, 12.702, 1.261, 1.063, 1.232, 1.062}),
      Map.entry("BoundedDivide",
          new double[] {14.934, 17.067, 15.093, 17.314, 2.043, 2.304, 2.064, 2.314}),
      Map.entry("ColumnRef/INT",
          new double[] {18.424, 90.41, 20.37, 94.029, 0.778, 0.943, 0.735, 0.943}),
      Map.entry("ColumnRef/LONG",
          new double[] {20.976, 104.097, 23.17, 106.281, 0.836, 0.588, 0.811, 0.586}),
      Map.entry("Compare/EQ/INT",
          new double[] {25.835, 39.053, 26.308, 38.67, 2.221, 1.858, 2.241, 1.902}),
      Map.entry("Compare/EQ/LONG",
          new double[] {15.086, 49.539, 15.806, 49.6, 1.263, 2.229, 1.278, 2.232}),
      Map.entry("Compare/GE/INT",
          new double[] {23.911, 46.222, 24.556, 46.151, 1.978, 2.511, 2.176, 2.596}),
      Map.entry("Compare/GE/LONG",
          new double[] {15.074, 51.253, 15.254, 51.257, 1.262, 2.701, 1.26, 2.716}),
      Map.entry("Compare/GT/INT",
          new double[] {20.164, 53.24, 20.911, 53.993, 1.41, 2.423, 1.537, 2.483}),
      Map.entry("Compare/GT/LONG",
          new double[] {14.735, 47.194, 15.648, 47.162, 1.206, 2.728, 1.23, 2.718}),
      Map.entry("Compare/LE/INT",
          new double[] {17.958, 51.287, 18.108, 51.569, 1.229, 2.628, 1.265, 2.672}),
      Map.entry("Compare/LE/LONG",
          new double[] {12.572, 52.717, 13.116, 52.774, 1.173, 2.354, 1.232, 2.357}),
      Map.entry("Compare/LT/INT",
          new double[] {24.174, 46.008, 25.072, 46.456, 1.995, 2.22, 2.162, 2.298}),
      Map.entry("Compare/LT/LONG",
          new double[] {15.096, 49.294, 15.88, 49.344, 1.217, 2.493, 1.26, 2.495}),
      Map.entry("ConstDivide/INT/neg",
          new double[] {108.131, 106.939, 108.36, 106.912, 11.989, 11.914, 11.998, 11.914}),
      Map.entry("ConstDivide/INT/pos",
          new double[] {106.396, 98.757, 106.685, 98.59, 11.168, 10.553, 11.188, 10.553}),
      Map.entry("ConstDivide/LONG/neg",
          new double[] {31.611, 22.404, 31.337, 22.297, 2.907, 3.176, 2.877, 3.169}),
      Map.entry("ConstDivide/LONG/pos",
          new double[] {32.192, 21.933, 32.597, 22.034, 2.989, 3.014, 3.053, 3.02}),
      Map.entry("DateDiff",
          new double[] {7.01, 7.248, 7.026, 7.334, 1.151, 1.307, 1.125, 1.297}),
      Map.entry("DayOfMonth",
          new double[] {22.92, 38.837, 22.605, 39.212, 3.916, 5.079, 3.96, 5.065}),
      Map.entry("DayOfWeek",
          new double[] {116.857, 115.968, 116.947, 116.188, 16.203, 15.735, 16.209, 15.741}),
      Map.entry("DayOfWeekIso",
          new double[] {114.729, 116.539, 114.582, 116.605, 15.936, 15.826, 15.897, 15.822}),
      Map.entry("DayOfYear",
          new double[] {99.319, 117.796, 99.457, 120.537, 13.307, 12.493, 13.318, 12.492}),
      Map.entry("Greatest/INT",
          new double[] {6.288, 90.529, 6.391, 91.431, 0.907, 3.492, 0.927, 3.495}),
      Map.entry("Greatest/LONG",
          new double[] {4.978, 79.169, 5.24, 79.206, 0.95, 3.325, 1.022, 3.335}),
      Map.entry("GuardedDay",
          new double[] {44.224, 73.231, 49.173, 78.563, 2.01, 1.948, 2.006, 1.95}),
      Map.entry("GuardedRange/INT",
          new double[] {45.466, 76.092, 50.626, 81.554, 2.145, 2.307, 2.135, 2.304}),
      Map.entry("GuardedRange/LONG",
          new double[] {42.169, 60.637, 47.344, 65.733, 2.021, 2.151, 2.016, 2.156}),
      Map.entry("IfElse/INT",
          new double[] {1.217, 71.34, 0.956, 72.933, 0.489, 0.077, 0.429, 0.015}),
      Map.entry("IfElse/LONG",
          new double[] {7.931, 52.839, 7.97, 52.852, 1.05, 0.259, 1.123, 0.257}),
      Map.entry("InRanges",
          new double[] {78.217, 98.837, 79.073, 98.754, 2.571, 4.17, 2.639, 4.215}),
      Map.entry("IntArith/ADD/FAIL/INT",
          new double[] {68.785, 100.435, 75.115, 106.205, 5.296, 5.602, 5.539, 5.618}),
      Map.entry("IntArith/ADD/FAIL/LONG",
          new double[] {61.17, 93.103, 66.43, 98.202, 4.984, 5.04, 5.059, 5.05}),
      Map.entry("IntArith/ADD/NULL/INT",
          new double[] {0, 110.461, 0, 112.6, 0, 5.553, 0, 5.553}),
      Map.entry("IntArith/ADD/NULL/LONG",
          new double[] {0, 93.022, 0, 93.119, 0, 5.061, 0, 5.068}),
      Map.entry("IntArith/ADD/WRAP/INT",
          new double[] {5.45, -3.846, 5.356, -3.972, 1.006, 1.129, 1.091, 1.136}),
      Map.entry("IntArith/ADD/WRAP/LONG",
          new double[] {4.975, 10.379, 4.927, 10.301, 1.072, 1.103, 1.086, 1.103}),
      Map.entry("IntArith/MUL/WRAP/INT",
          new double[] {6.127, 11.584, 6.653, 12.075, 1.368, 1.757, 1.445, 1.763}),
      Map.entry("IntArith/MUL/WRAP/LONG",
          new double[] {6.277, 7.78, 6.396, 7.769, 1.085, 1.262, 1.107, 1.264}),
      Map.entry("IntArith/SUB/FAIL/INT",
          new double[] {62.528, 98.106, 67.224, 103.34, 5.296, 5.199, 5.255, 5.182}),
      Map.entry("IntArith/SUB/FAIL/LONG",
          new double[] {62.325, 87.953, 67.352, 92.968, 4.993, 5.44, 5.019, 5.439}),
      Map.entry("IntArith/SUB/NULL/INT",
          new double[] {0, 102.728, 0, 104.323, 0, 5.203, 0, 5.217}),
      Map.entry("IntArith/SUB/NULL/LONG",
          new double[] {0, 92.39, 0, 92.393, 0, 5.487, 0, 5.488}),
      Map.entry("IntArith/SUB/WRAP/INT",
          new double[] {5.224, 7.997, 5.303, 8.079, 1.24, 1.207, 1.322, 1.224}),
      Map.entry("IntArith/SUB/WRAP/LONG",
          new double[] {5.375, 7.559, 5.675, 7.566, 1.133, 1.014, 1.18, 1.023}),
      Map.entry("IntNeg/FAIL/INT",
          new double[] {31.435, 55.607, 36.426, 60.701, 2.09, 1.65, 2.093, 1.647}),
      Map.entry("IntNeg/FAIL/LONG",
          new double[] {30.262, 49.173, 35.638, 54.232, 2.023, 1.836, 2.057, 1.841}),
      Map.entry("IntNeg/WRAP/INT",
          new double[] {2.678, 3.088, 2.445, 3.01, 0.929, 0.911, 0.897, 0.906}),
      Map.entry("IntNeg/WRAP/LONG",
          new double[] {4.337, 3.358, 4.468, 3.39, 0.866, 1.147, 0.843, 1.148}),
      Map.entry("IsNotNull/INT",
          new double[] {-21.785, 27.29, -31.747, 24.592, -2.335, 0.6, -2.401, 0.553}),
      Map.entry("IsNotNull/LONG",
          new double[] {4.963, 28.031, 0.893, 26.887, -0.054, 1.128, -0.206, 1.079}),
      Map.entry("IsNotNull/cond/INT",
          new double[] {-54.342, 9.612, -56.852, 9.567, -6.004, -0.144, -6.16, -0.096}),
      Map.entry("IsNotNull/cond/LONG",
          new double[] {-17.911, 13.496, -20.816, 13.338, -2.178, -0.255, -2.355, -0.274}),
      Map.entry("LastDay",
          new double[] {220.573, 259.07, 220.663, 261.993, 31.084, 33.232, 31.081, 33.221}),
      Map.entry("Least/INT",
          new double[] {7.224, 88.531, 7.55, 89.147, 1.187, 3.123, 1.275, 3.134}),
      Map.entry("Least/LONG",
          new double[] {5.765, 80.164, 6.109, 80.236, 1.058, 3.265, 1.134, 3.271}),
      Map.entry("LiteralSlot/INT",
          new double[] {8.554, 8.71, 7.27, 8.046, 0.998, 1.383, 1.049, 1.402}),
      Map.entry("LiteralSlot/LONG",
          new double[] {13.715, 5.228, 13.198, 5.164, 1.317, 2.027, 1.46, 2.073}),
      Map.entry("MakeDate/ansi",
          new double[] {465.807, 532.426, 471.061, 538.829, 52.97, 53.252, 52.964, 53.264}),
      Map.entry("MakeDate/null",
          new double[] {0, 539.942, 0, 546.304, 0, 54.224, 0, 54.241}),
      Map.entry("Month",
          new double[] {19.065, 40.71, 18.72, 41.138, 2.524, 4.469, 2.597, 4.471}),
      Map.entry("NarrowLane",
          new double[] {32.16, 31.184, 29.409, 28.879, 1.101, 2.028, 1.087, 2.023}),
      Map.entry("NextDay",
          new double[] {103.507, 103.279, 103.775, 103.282, 14.957, 15.365, 15.001, 15.373}),
      Map.entry("Not/INT",
          new double[] {14.166, 1.163, 14.272, 1.519, 0.972, 0.097, 1.005, 0.081}),
      Map.entry("Not/LONG",
          new double[] {8.386, 9.572, 8.503, 9.529, 0.117, 0.133, 0.02, 0.109}),
      Map.entry("Or/INT",
          new double[] {11.868, 13.46, 11.674, 14.157, 0.902, -0.629, 0.84, -0.61}),
      Map.entry("Or/LONG",
          new double[] {9.932, 8.6, 10.384, 8.923, -0.015, -1.303, 0.07, -1.245}),
      Map.entry("Quarter",
          new double[] {39.448, 55.88, 38.811, 56.253, 5.872, 7.408, 5.913, 7.399}),
      Map.entry("SubDays",
          new double[] {11.695, 14.644, 12.408, 15.144, 1.552, 1.736, 1.586, 1.743}),
      Map.entry("ThursdayOf",
          new double[] {117.659, 123.878, 117.412, 124.286, 16.536, 16.838, 16.501, 16.835}),
      Map.entry("TruncDate/MONTH",
          new double[] {24.072, 28.289, 23.664, 29.024, 3.951, 5.038, 4.001, 5.027}),
      Map.entry("TruncDate/QUARTER",
          new double[] {214.984, 244.315, 214.009, 246.532, 29.798, 31.39, 29.824, 31.399}),
      Map.entry("TruncDate/YEAR",
          new double[] {107.088, 125.609, 107.418, 128.463, 14.695, 15.485, 14.795, 15.498}),
      Map.entry("TruncDateDynamic",
          new double[] {417.162, 463.179, 416.394, 465.275, 57.896, 61.302, 57.985, 61.286}),
      Map.entry("WeekDay",
          new double[] {110.085, 110.213, 110.212, 110.298, 14.815, 14.885, 14.832, 14.893}),
      Map.entry("WeekOfYear",
          new double[] {126.621, 114.083, 127.039, 116.83, 17.713, 12.926, 17.711, 12.928}),
      Map.entry("WidenLane",
          new double[] {33.814, -10.172, 75.03, 28.744, 1.199, 1.765, 1.268, 1.791}),
      Map.entry("Year",
          new double[] {29.421, 48.694, 29.447, 51.757, 4.14, 3.705, 4.107, 3.701}),
      Map.entry("fixed/INT",
          new double[] {83.264, 48.818, 95.899, 60.655, 0.064, -0.762, 0.135, -0.766}),
      Map.entry("fixed/LONG",
          new double[] {86.482, 51.564, 98.25, 63.99, -0.152, -0.669, -0.059, -0.681}),
      Map.entry("out/INT",
          new double[] {24.288, 21.654, 26.524, 23.962, 1.173, 1.568, 1.131, 1.559}),
      Map.entry("out/LONG",
          new double[] {23.246, 30.208, 25.275, 32.208, 1.174, 0.958, 1.055, 0.945}),
      Map.entry("out/cond/INT",
          new double[] {18.718, 28.742, 18.73, 27.816, -0.369, -1.931, -0.294, -1.7}),
      Map.entry("out/cond/LONG",
          new double[] {18.281, 26.532, 16.298, 26.067, -0.455, -1.741, -0.217, -1.138}),
      Map.entry("prefix",
          new double[] {259.26, 274.309, 259.506, 272.912, 27.963, 31.065, 28.005, 31.073}),
      Map.entry("prefix/load",
          new double[] {40.111, -4.399, 44.145, -3.098, 0.021, -2.657, 0.027, -2.66}),
      Map.entry("prefix/month",
          new double[] {17.19, -7.117, 17.49, -5.607, 2.216, -2.116, 2.127, -2.109}));
}
