* task: VARKA-381 (6.1 #3)
* added: 2026-10-17
* predicted: the one-partition rerun adds under 3% to a run, one more check per raising composition
* measured: 47.9 against 48.5 s by minimums on the 200-composition run; 1.1 to 1.3% of the nightly
  step's compositions checked again
* ratio: under 1
* note: a check is rerun only when its first run raised, and few do
