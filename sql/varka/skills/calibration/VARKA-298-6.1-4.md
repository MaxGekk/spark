* task: VARKA-298 (6.1 #4)
* added: 2026-10-16
* predicted: nested trees on half the Spark-level compositions add 10 to 45% to a run
* measured: none measurable above the noise: a 300-composition run at the default seed, five times
  with and five without, alternating, 70 to 306 s with and 85 to 432 s without, 70 s against 85 s by
  minimums, at a load of 20 to 58
* ratio: none
* note: the run's time is the machine's load, not the extra outputs; an earlier single pair under
  load 48 read 148 s against 85 s
