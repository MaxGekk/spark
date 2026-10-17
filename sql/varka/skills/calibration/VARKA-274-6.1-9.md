* task: VARKA-274 (6.1 #9)
* added: 2026-10-16
* predicted: the nightly's 4,000 kernels over eight JVMs under 30 minutes
* measured: about 95 minutes, extrapolated from two JVMs of three cores (20 draws a lane each in 7.2
  minutes) plus the coverage arm; not yet a night's measurement
* ratio: about 3.2
* note: the 30 assumed whole-machine case times; a JVM held to three cores ran a case about 2.6
  times slower
