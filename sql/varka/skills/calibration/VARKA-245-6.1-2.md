* task: VARKA-245 (6.1 #2)
* added: 2026-10-16
* order: 14
* predicted: a new proof file of under 250 checks under 8 s under Z3 and under 8 s under cvc5
* measured: 209 checks (216 after the review), 3.7 to 6.8 s under Z3 and 5.8 to 6.7 s under cvc5 at
  load 25 to 35
* ratio: about 0.8
* note: held only after the forms were reworked: the first rendering of the same claims took Z3 98 s
  and left cvc5 unknown on checks it decides alone, so price a solver from the spiked form, not the
  claim
