; Licensed to the Apache Software Foundation (ASF) under one or more
; contributor license agreements.  See the NOTICE file distributed with
; this work for additional information regarding copyright ownership.
; The ASF licenses this file to You under the Apache License, Version 2.0
; (the "License"); you may not use this file except in compliance with
; the License.  You may obtain a copy of the License at
;
;    http://www.apache.org/licenses/LICENSE-2.0
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.

; The bounds VarkaTimeCompiler states on the long lane hold (VARKA-281), under the premise
; that a TIME is a count of nanoseconds of day in [0, NANOS_PER_DAY), which is the type's
; range (LocalTime.toNanoOfDay). The compiler's sites, restated with DateTimeConstants:
;
;   subtractTimes, timeDiff:  (end - start) / unit, bound DAY_OF_NANOS on the difference
;   timeTrunc:                (t / unit) * unit, both in wrapping long arithmetic
;   the field extracts:       t / HOUR, t / MINUTE, t / SECOND, each under its field's
;                             limit, the latter two then x - (x / 60) * 60 under
;                             quotientBound(x) = max(1, ceil(bound / divisor))
;   timeToUnit:               t / unit, bound DAY_OF_NANOS on t
;   timeFromUnits:            a count guarded to [0, (NANOS_PER_DAY - 1) / unit], times
;                             the unit
;
; Each claim is linear: every divisor and unit is a constant, and quotientBound's bound is
; symbolic. Beside each, its neighbour: the floor in place of the ceiling, a count one
; past the guard, a bound one too narrow.
;
; Rendered by VarkaProofFiles. VarkaProofFilesSuite fails when this file differs from its
; rendering, and VARKA_PROOFS_REGEN=true build/sbt 'catalyst/testOnly *VarkaProofFilesSuite'
; rewrites it. Run by dev/varka_prove.sh, which inserts java.smt2 after the set-logic line.

(set-logic QF_NIA)

(echo "the dividend bound is one the long lane's forms honour: DAY_OF_NANOS is at most ConstDivide.EXACT_DIVIDEND_BOUND: expect unsat")
(push 1)
(assert (not (<= 86400000000000 4503599627370496)))
(check-sat)
(pop 1)

(echo "subtractTimes, timeDiff: a difference of two times does not wrap and is under DAY_OF_NANOS in magnitude: expect unsat")
(push 1)
(declare-const e Int) (declare-const s Int)
(assert (and (<= 0 e) (< e 86400000000000) (<= 0 s) (< s 86400000000000)))
(assert (not (and (= (jlong.sub e s) (- e s)) (< (abs (- e s)) 86400000000000))))
(check-sat)
(pop 1)

(echo "subtractTimes, timeDiff: and the difference reaches DAY_OF_NANOS - 1: expect sat")
(push 1)
(declare-const e Int) (declare-const s Int)
(assert (and (<= 0 e) (< e 86400000000000) (<= 0 s) (< s 86400000000000)))
(assert (= (- e s) 86399999999999))
(check-sat)
(pop 1)

(echo "timeTrunc to a microsecond: (t / unit) * unit does not wrap and is at most t: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(define-fun q () Int (jlong.div t 1000))
(assert (not (and (= (jlong.mul q 1000) (* q 1000)) (<= (* q 1000) t) (<= 0 q))))
(check-sat)
(pop 1)

(echo "timeTrunc to a millisecond: (t / unit) * unit does not wrap and is at most t: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(define-fun q () Int (jlong.div t 1000000))
(assert (not (and (= (jlong.mul q 1000000) (* q 1000000)) (<= (* q 1000000) t) (<= 0 q))))
(check-sat)
(pop 1)

(echo "timeTrunc to a second: (t / unit) * unit does not wrap and is at most t: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(define-fun q () Int (jlong.div t 1000000000))
(assert (not (and (= (jlong.mul q 1000000000) (* q 1000000000)) (<= (* q 1000000000) t) (<= 0 q))))
(check-sat)
(pop 1)

(echo "timeTrunc to a minute: (t / unit) * unit does not wrap and is at most t: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(define-fun q () Int (jlong.div t 60000000000))
(assert (not (and (= (jlong.mul q 60000000000) (* q 60000000000)) (<= (* q 60000000000) t) (<= 0 q))))
(check-sat)
(pop 1)

(echo "timeTrunc to an hour: (t / unit) * unit does not wrap and is at most t: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(define-fun q () Int (jlong.div t 3600000000000))
(assert (not (and (= (jlong.mul q 3600000000000) (* q 3600000000000)) (<= (* q 3600000000000) t) (<= 0 q))))
(check-sat)
(pop 1)

(echo "hour: t / HOUR lies in [0, 23]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 3600000000000)) (<= (jlong.div t 3600000000000) 23))))
(check-sat)
(pop 1)

(echo "hour: t / HOUR lies in [0, 23]: with the low end raised by one it is not: expect sat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= (+ 0 1) (jlong.div t 3600000000000)) (<= (jlong.div t 3600000000000) 23))))
(check-sat)
(pop 1)

(echo "hour: t / HOUR lies in [0, 23]: with the high end lowered by one it is not: expect sat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 3600000000000)) (<= (jlong.div t 3600000000000) (- 23 1)))))
(check-sat)
(pop 1)

(echo "minute: t / MINUTE lies in [0, 1439]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 60000000000)) (<= (jlong.div t 60000000000) 1439))))
(check-sat)
(pop 1)

(echo "minute: t / MINUTE lies in [0, 1439]: with the low end raised by one it is not: expect sat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= (+ 0 1) (jlong.div t 60000000000)) (<= (jlong.div t 60000000000) 1439))))
(check-sat)
(pop 1)

(echo "minute: t / MINUTE lies in [0, 1439]: with the high end lowered by one it is not: expect sat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 60000000000)) (<= (jlong.div t 60000000000) (- 1439 1)))))
(check-sat)
(pop 1)

(echo "second: t / SECOND lies in [0, 86399]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 1000000000)) (<= (jlong.div t 1000000000) 86399))))
(check-sat)
(pop 1)

(echo "second: t / SECOND lies in [0, 86399]: with the low end raised by one it is not: expect sat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= (+ 0 1) (jlong.div t 1000000000)) (<= (jlong.div t 1000000000) 86399))))
(check-sat)
(pop 1)

(echo "second: t / SECOND lies in [0, 86399]: with the high end lowered by one it is not: expect sat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 1000000000)) (<= (jlong.div t 1000000000) (- 86399 1)))))
(check-sat)
(pop 1)

(echo "quotientBound: |x| < b gives |x / 60000000000| < max(1, ceil(b / 60000000000)), with b symbolic: expect unsat")
(push 1)
(declare-const x Int) (declare-const b Int)
(assert (and (>= b 1) (< (abs x) b) (jlong.in x)))
(define-fun c () Int (div (+ b 59999999999) 60000000000))
(assert (not (< (abs (jlong.div x 60000000000)) (ite (>= c 1) c 1))))
(check-sat)
(pop 1)

(echo "quotientBound: with the floor in place of the ceiling it does not: expect sat")
(push 1)
(declare-const x Int) (declare-const b Int)
(assert (and (>= b 1) (< (abs x) b) (jlong.in x)))
(define-fun c () Int (div b 60000000000))
(assert (not (< (abs (jlong.div x 60000000000)) (ite (>= c 1) c 1))))
(check-sat)
(pop 1)

(echo "quotientBound(DAY_OF_NANOS, 60000000000) is 1440, as the renderer computes it: expect unsat")
(push 1)
(assert (not (= (div (+ 86400000000000 59999999999) 60000000000) 1440)))
(check-sat)
(pop 1)

(echo "remainderOfSixty: for x in [0, 1440), x - (x / 60) * 60 in wrapping arithmetic is x mod 60, in [0, 59]: expect unsat")
(push 1)
(declare-const x Int)
(assert (and (<= 0 x) (< x 1440)))
(define-fun r () Int (jlong.sub x (jlong.mul (jlong.div x 60) 60)))
(assert (= r (mod x 60)))
(assert (not (and (<= 0 r) (<= r 59))))
(check-sat)
(pop 1)

(echo "remainderOfSixty: for x in [0, 1440), x - (x / 60) * 60 in wrapping arithmetic is x mod 60, in [0, 59]: with the low end raised by one it is not: expect sat")
(push 1)
(declare-const x Int)
(assert (and (<= 0 x) (< x 1440)))
(define-fun r () Int (jlong.sub x (jlong.mul (jlong.div x 60) 60)))
(assert (= r (mod x 60)))
(assert (not (and (<= (+ 0 1) r) (<= r 59))))
(check-sat)
(pop 1)

(echo "remainderOfSixty: for x in [0, 1440), x - (x / 60) * 60 in wrapping arithmetic is x mod 60, in [0, 59]: with the high end lowered by one it is not: expect sat")
(push 1)
(declare-const x Int)
(assert (and (<= 0 x) (< x 1440)))
(define-fun r () Int (jlong.sub x (jlong.mul (jlong.div x 60) 60)))
(assert (= r (mod x 60)))
(assert (not (and (<= 0 r) (<= r (- 59 1)))))
(check-sat)
(pop 1)

(echo "quotientBound: |x| < b gives |x / 1000000000| < max(1, ceil(b / 1000000000)), with b symbolic: expect unsat")
(push 1)
(declare-const x Int) (declare-const b Int)
(assert (and (>= b 1) (< (abs x) b) (jlong.in x)))
(define-fun c () Int (div (+ b 999999999) 1000000000))
(assert (not (< (abs (jlong.div x 1000000000)) (ite (>= c 1) c 1))))
(check-sat)
(pop 1)

(echo "quotientBound: with the floor in place of the ceiling it does not: expect sat")
(push 1)
(declare-const x Int) (declare-const b Int)
(assert (and (>= b 1) (< (abs x) b) (jlong.in x)))
(define-fun c () Int (div b 1000000000))
(assert (not (< (abs (jlong.div x 1000000000)) (ite (>= c 1) c 1))))
(check-sat)
(pop 1)

(echo "quotientBound(DAY_OF_NANOS, 1000000000) is 86400, as the renderer computes it: expect unsat")
(push 1)
(assert (not (= (div (+ 86400000000000 999999999) 1000000000) 86400)))
(check-sat)
(pop 1)

(echo "remainderOfSixty: for x in [0, 86400), x - (x / 60) * 60 in wrapping arithmetic is x mod 60, in [0, 59]: expect unsat")
(push 1)
(declare-const x Int)
(assert (and (<= 0 x) (< x 86400)))
(define-fun r () Int (jlong.sub x (jlong.mul (jlong.div x 60) 60)))
(assert (= r (mod x 60)))
(assert (not (and (<= 0 r) (<= r 59))))
(check-sat)
(pop 1)

(echo "remainderOfSixty: for x in [0, 86400), x - (x / 60) * 60 in wrapping arithmetic is x mod 60, in [0, 59]: with the low end raised by one it is not: expect sat")
(push 1)
(declare-const x Int)
(assert (and (<= 0 x) (< x 86400)))
(define-fun r () Int (jlong.sub x (jlong.mul (jlong.div x 60) 60)))
(assert (= r (mod x 60)))
(assert (not (and (<= (+ 0 1) r) (<= r 59))))
(check-sat)
(pop 1)

(echo "remainderOfSixty: for x in [0, 86400), x - (x / 60) * 60 in wrapping arithmetic is x mod 60, in [0, 59]: with the high end lowered by one it is not: expect sat")
(push 1)
(declare-const x Int)
(assert (and (<= 0 x) (< x 86400)))
(define-fun r () Int (jlong.sub x (jlong.mul (jlong.div x 60) 60)))
(assert (= r (mod x 60)))
(assert (not (and (<= 0 r) (<= r (- 59 1)))))
(check-sat)
(pop 1)

(echo "timeToUnit by a microsecond: a time's quotient lies in [0, 86399999999]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 1000)) (<= (jlong.div t 1000) 86399999999))))
(check-sat)
(pop 1)

(echo "timeToUnit by a millisecond: a time's quotient lies in [0, 86399999]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 1000000)) (<= (jlong.div t 1000000) 86399999))))
(check-sat)
(pop 1)

(echo "timeToUnit by a second: a time's quotient lies in [0, 86399]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 1000000000)) (<= (jlong.div t 1000000000) 86399))))
(check-sat)
(pop 1)

(echo "timeToUnit by a minute: a time's quotient lies in [0, 1439]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 60000000000)) (<= (jlong.div t 60000000000) 1439))))
(check-sat)
(pop 1)

(echo "timeToUnit by an hour: a time's quotient lies in [0, 23]: expect unsat")
(push 1)
(declare-const t Int)
(assert (and (<= 0 t) (< t 86400000000000)))
(assert (not (and (<= 0 (jlong.div t 3600000000000)) (<= (jlong.div t 3600000000000) 23))))
(check-sat)
(pop 1)

(echo "timeFromUnits by a microsecond: a count in [0, 86399999999] times the unit does not wrap and is a time of day: expect unsat")
(push 1)
(declare-const n Int)
(assert (and (<= 0 n) (<= n 86399999999)))
(define-fun p () Int (* n 1000))
(assert (not (and (= (jlong.mul n 1000) p) (<= 0 p) (< p 86400000000000))))
(check-sat)
(pop 1)

(echo "timeFromUnits by a microsecond: one count past the guard is not a time of day: expect sat")
(push 1)
(declare-const n Int)
(assert (= n 86400000000))
(assert (not (< (* n 1000) 86400000000000)))
(check-sat)
(pop 1)

(echo "timeFromUnits by a millisecond: a count in [0, 86399999] times the unit does not wrap and is a time of day: expect unsat")
(push 1)
(declare-const n Int)
(assert (and (<= 0 n) (<= n 86399999)))
(define-fun p () Int (* n 1000000))
(assert (not (and (= (jlong.mul n 1000000) p) (<= 0 p) (< p 86400000000000))))
(check-sat)
(pop 1)

(echo "timeFromUnits by a millisecond: one count past the guard is not a time of day: expect sat")
(push 1)
(declare-const n Int)
(assert (= n 86400000))
(assert (not (< (* n 1000000) 86400000000000)))
(check-sat)
(pop 1)

(echo "timeFromUnits by a second: a count in [0, 86399] times the unit does not wrap and is a time of day: expect unsat")
(push 1)
(declare-const n Int)
(assert (and (<= 0 n) (<= n 86399)))
(define-fun p () Int (* n 1000000000))
(assert (not (and (= (jlong.mul n 1000000000) p) (<= 0 p) (< p 86400000000000))))
(check-sat)
(pop 1)

(echo "timeFromUnits by a second: one count past the guard is not a time of day: expect sat")
(push 1)
(declare-const n Int)
(assert (= n 86400))
(assert (not (< (* n 1000000000) 86400000000000)))
(check-sat)
(pop 1)
