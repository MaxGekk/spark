* task: VARKA-378 (6.1 #2)
* added: 2026-10-16
* order: 12
* predicted: the NULL-mode guard step about 12 minutes, two widths of 200 kernels at a trial's 1.4 s
  each, at a ratio of 1.2
* measured: 10.7 to 15.4 minutes over three runs on three pinned cores at a load of 21 to 46
* ratio: 0.9 to 1.3
* note: the time moves with the load: a kernel still in the cycle costs the full 10 s deadline, and
  a busy machine leaves more of them in it
