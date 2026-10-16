* task: VARKA-281 (6.1 #3)
* added: 2026-10-15
* order: 9
* predicted: the calendar sweep suite under 20 s
* measured: 1.5 s for its eight tests over 20 million days each
* ratio: 0.08
* note: the estimate priced single-threaded `LocalDate` decodes and then doubled; a parallel stream
  over the test JVM's cores and the JIT's inlining of the decode left little of either
