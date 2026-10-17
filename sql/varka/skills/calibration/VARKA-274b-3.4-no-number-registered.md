* task: VARKA-274b (3.4, no number registered)
* added: 2026-10-16
* predicted: every coverage row's kernel over three histories in the suite's test: 92 kernels at
  9.6's medians (two warm-ups of about 1 s, `released` 1.3 s), about 5 minutes
* measured: over 9 minutes at load 30 with the sanitizer, unfinished at the suite's watchdog; 321 s
  at load 9 without it, on six cores
* ratio: 1.1 to over 1.8
* note: load and the sanitizer, not the kernels; the suite now samples 4 rows a lane and the nightly
  runs all
