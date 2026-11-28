# Master stays green, ahead of everything else

* **A red master comes first.** Master is green only when the latest run of `varka-master.yml`
  on its current head that was not cancelled succeeded; when that run was cancelled, rerun it.
  When master is red, or a merge breaks a suite every pull request runs, other work waits: no
  new pull requests, no feature merges, and no refactor starts until master is green again. The
  exceptions are the pull request that makes master green and the plan or results pull requests
  the owner allows. Read the failure before calling it anything: a test near its time budget
  fails on a slower runner class and looks like a slow runner, but it is a loss of headroom.
* **Fix forward when the fix is small and sure; otherwise revert.** When the cause needs design
  work, the default is a revert of the pull request that turned master red, with a dated note on
  why it must be redesigned: in its row's plan, or, for a pull request with no row, in the
  revert's description and a new Filed row. On 23 November 2026 #1626 (row 611) pushed the
  emitted-bytes pin test past its 600 s limit on AVX2 runners (511 to 551 s before it). The run on
  #1626's own merge passed on an AVX-512 runner, and the next master run, 45256702183, failed 3 of
  3 attempts, all on EPYC 7763s. The owner decided to revert #1626 before row 489's refactor.
* **Check before merging that a pull request cannot turn master red.** If master moved since its
  last green CI and the pull request commits generated records or test-time-sensitive changes,
  merge master, regenerate and rerun before merging. #1646 merged a generated record built before
  #1628 and turned master red on 23 November 2026. A test past half its time limit on the slowest
  runner class is a finding, not a pass.
