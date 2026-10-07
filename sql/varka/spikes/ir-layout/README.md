# IR layout spike (VARKA-291): results only

The harness, its graphs and the exporter were removed once the spike was decided; what is left is
the data that `sql/varka/plans/m7/VARKA-291.md` quotes, in `results/` (`step5-summary.txt` is the
summarized reading of the `step5-*.txt` files). To rerun or extend it, restore the code from the
last commit that has it:

    t=sql/catalyst/src/test
    v=org/apache/spark/sql/catalyst/expressions/codegen/varka
    git checkout f77e5fb9ea6 -- sql/varka/spikes/ir-layout \
      $t/java/$v/VarkaIrDescription.java $t/java/$v/VarkaIrLayoutExport.java \
      $t/scala/$v/VarkaIrDescriptionSuite.scala

The old README and the run scripts come back with it.
