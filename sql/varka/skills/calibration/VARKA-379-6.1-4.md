* task: VARKA-379 (6.1 #4)
* added: 2026-10-16
* predicted: a new sql/core suite under 60 s, its tests under 30 s, at VARKA-243's ratio of 2
* measured: 25 s, its tests 16 s
* ratio: 0.5
* note: about 200 queries over one-batch cached views, each view cached once per fixture rather than
  per query
