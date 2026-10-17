* task: VARKA-379 (6.1 #2)
* added: 2026-10-17
* predicted: every bounded row of VarkaThroughputBenchmark within 3% on the runners
* measured: 14 to 17% slower by median and 9 to 11% by best on four bounded rows, ten rounds on
  GitHub runners
* ratio: 3 to 6
* note: a pass of its own re-reads the column from memory before the kernel reads it again, which is
  as much work as a cheap kernel's; VARKA-379.md 10 folds the check into the kernel's loop
