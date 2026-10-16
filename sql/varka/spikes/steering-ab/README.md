# Steering A/B (VARKA-280): results only

The fixed-seed A/B of steering the IR fuzzer toward emitter mechanisms it has not reached, which
`sql/varka/plans/m7/VARKA-280.md` 9.1 quotes, in `results/ab.txt`. The test that produced it is
`VarkaIrFuzzSuite`'s opt-in "steering toward unseen mechanisms" test (`VarkaSteering`), still in
the suite; rerun one seed with

    build/sbt 'set Test/javaOptions ++= Seq("-Dvarka.fuzz.steerSeeds=2026101601",
      "-Dvarka.fuzz.steer=300")' 'catalyst/testOnly *VarkaIrFuzzSuite'
