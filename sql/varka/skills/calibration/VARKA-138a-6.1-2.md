* task: VARKA-138a (6.1 #2)
* added: 2026-10-13
* order: 1
* predicted: per-row coverage check at most 25% slower
* measured: +38% by minimums, +27% after the review dropped a redundant dense run
* ratio: 1.5, then 1.1
* note: the fixed poisoned batch alone added about 27% of the work
