* task: VARKA-317 (6.1 #1)
* added: 2026-10-16
* order: 11
* predicted: the coverage differential's added test time at most 10 s
* measured: 0.3 s: the planted test 126 ms, the extended one 182 ms
* ratio: 0.03
* note: the estimate priced a kernel compile per query and doubled it by VARKA-243's ratio; where
  the 0.3 s went was not split
