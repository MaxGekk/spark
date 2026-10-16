* task: VARKA-257 (6.1 #5)
* added: 2026-10-16
* order: 15
* predicted: the nightly's kernel-after-kernels arm about 3 minutes, six forks of 16 kernels at the
  compiled test's 1 to 2 s each, at VARKA-378's ratio
* measured: 3 min 52 s on three pinned cores at a load of 14
* ratio: 1.3
* note: a history kernel the warm-up releases costs the full 30 s deadline, and 7 of 72 long kernels
  past position 3 were released
