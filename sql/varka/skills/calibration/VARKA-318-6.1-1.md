* task: VARKA-318 (6.1 #1)
* added: 2026-10-17
* predicted: the compile-only family switch suite under 5 s
* measured: 1.9 to 2.3 s, most of it resolving the 92 coverage rows
* ratio: 0.46
* note: a first draft that resolved the rows once per test instance took 13.8 s; reading the table
  in `@BeforeAll` removed it
