* task: VARKA-318 (6.1 #2)
* added: 2026-10-17
* predicted: the sql/core switch suite's tests under 10 s
* measured: 3.7 s alone, about 5 s in the gate
* ratio: 0.37
* note: short queries over one cached view, as VARKA-315's eight took 4.5 s
